"""RFFabric: multi-endpoint RF container.

Registers N radios, stamps each RX with ``radio_id``, and selects a default or
explicit radio for TX. Each physical callback yields one ``RFIngress`` with one
``RadioReception``. Dispatcher-level dedup collapses duplicate mesh handling.
"""

from __future__ import annotations

import inspect
import hashlib
import logging
import time
from contextlib import contextmanager
from contextvars import ContextVar
from .ordered_tx import OrderedTxContext
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Optional, Union

from .models import RadioReception, RFIngress

logger = logging.getLogger("RFFabric")

LegacyRxCallback = Callable[..., Any]

# A TX selector names the radio that should carry a frame. Two shapes are
# accepted, and which one a selector is gets decided once, when it is
# registered:
#
#     selector(data)                  the original shape, unchanged
#     selector(data, rx_radio_id=...) also told the radio the frame arrived on,
#                                     or None when this node originated it
#
# The second shape exists so a selector can be a pure function of the packet in
# front of it. One that reaches for ``last_rx_radio_id`` instead is answering
# about whatever arrived most recently, which after a retransmit delay need not
# be this packet -- on a busy dual-band node that puts traffic on the wrong
# band. Being a pure function also means a caller can ask
# ``resolve_tx_radio_id`` before the send and rely on the answer.
#
# A selector opts in by **naming** a parameter ``rx_radio_id``, and is called
# with it by keyword. Counting positional parameters instead would misread
# ``def choose(data, options=DEFAULTS)`` -- a perfectly ordinary one-argument
# selector -- as context-aware, hand it a radio id where it expected its own
# default, and reject whatever came back.
LegacyTxSelector = Callable[[bytes], Optional[str]]
ContextTxSelector = Callable[..., Optional[str]]
TxSelector = Union[LegacyTxSelector, ContextTxSelector]


def _selector_takes_rx(selector: Optional[TxSelector]) -> bool:
    """Whether ``selector`` names ``rx_radio_id`` and so wants to be told it.

    Deliberately name-based, matching how the Dispatcher decides whether to
    offer the same keyword to a radio. Arity is not evidence of intent: a
    second parameter with a default belongs to the selector, not to us, and
    ``*args``/``**kwargs`` would swallow anything offered without meaning it.

    Decided from the signature once, rather than by calling with two arguments
    and retrying on TypeError: a TypeError raised *inside* a context-aware
    selector would be indistinguishable from one raised by calling it wrongly,
    and the retry would then call it wrongly for real.
    """
    if selector is None:
        return False
    try:
        params = inspect.signature(selector).parameters
    except (TypeError, ValueError):
        # Builtins and other C callables have no introspectable signature.
        # One argument is what every selector written before this existed
        # expects, so that is the safe reading.
        logger.debug(
            "TX selector %r has no readable signature; passing data only",
            getattr(selector, "__name__", selector),
        )
        return False

    param = params.get("rx_radio_id")
    return param is not None and param.kind is not inspect.Parameter.POSITIONAL_ONLY


class RFFabric:
    """Multi-radio fabric with per-edge ingress delivery and selectable TX."""

    def __init__(self) -> None:
        # Stable insertion order for default TX / attribute pass-through.
        self._radios: "OrderedDict[str, Any]" = OrderedDict()
        self._default_radio_id: Optional[str] = None
        self._ingress_callback: Optional[Callable[[RFIngress], Any]] = None
        self._legacy_rx_callback: Optional[LegacyRxCallback] = None
        self._observation_callbacks = []
        self._rx_bindings = {}
        self._tx_selector: Optional[TxSelector] = None
        self._tx_selector_takes_rx = False
        self.topology = None
        self.comparison = None
        self.logical_receiver = None
        self._network_tx_seen = OrderedDict()
        self._ordered_tx = ContextVar("ordered_tx_group", default=None)
        self.network_tx_duplicates = 0
        self.network_tx_capacity_drops = 0
        self._armed = False
        # Latest delivered reception metrics (for FabricRadio get_last_*).
        self._last_rssi: int = 0
        self._last_snr: float = 0.0
        self._last_rx_radio_id: Optional[str] = None

    # ------------------------------------------------------------------
    # Registry
    # ------------------------------------------------------------------

    @property
    def radio(self) -> Any:
        """Default / first registered radio."""
        rid = self.default_radio_id
        if rid is None:
            return None
        return self._radios.get(rid)

    @property
    def radio_id(self) -> Optional[str]:
        """Default radio id."""
        return self.default_radio_id

    @property
    def last_rx_radio_id(self) -> Optional[str]:
        """Radio id that delivered the most recent RX, if any."""
        return self._last_rx_radio_id

    @property
    def default_radio_id(self) -> Optional[str]:
        if self._default_radio_id and self._default_radio_id in self._radios:
            return self._default_radio_id
        if self._radios:
            return next(iter(self._radios.keys()))
        return None

    @property
    def radios(self) -> "OrderedDict[str, Any]":
        """Copy-like view of registered radios (insertion order)."""
        return OrderedDict(self._radios)

    def get_radio(self, radio_id: str) -> Any:
        try:
            return self._radios[radio_id]
        except KeyError as exc:
            raise KeyError(f"Unknown radio_id={radio_id!r}") from exc

    def register_radio(self, radio: Any, *, radio_id: str = "radio0") -> None:
        """Register a radio endpoint under a unique ``radio_id``."""
        if not radio_id:
            raise ValueError("radio_id must be a non-empty string")
        if radio_id in self._radios:
            raise RuntimeError(f"radio_id={radio_id!r} is already registered")
        # Prevent the same object under two ids (ambiguous TX/RX ownership).
        for existing_id, existing in self._radios.items():
            if existing is radio:
                raise RuntimeError(f"Radio object already registered as radio_id={existing_id!r}")
        self._radios[radio_id] = radio
        if self._default_radio_id is None:
            self._default_radio_id = radio_id
        if self._armed:
            self._bind_one(radio_id, radio)
        logger.debug(
            "RFFabric registered radio_id=%s (%s); count=%d",
            radio_id,
            type(radio).__name__,
            len(self._radios),
        )

    def unregister_radio(self, radio_id: Optional[str] = None) -> None:
        """Detach one radio (by id) or all radios when ``radio_id`` is None."""
        if radio_id is None:
            for rid in list(self._radios.keys()):
                self._unbind_one(rid)
            self._radios.clear()
            self._default_radio_id = None
            return

        radio = self._radios.pop(radio_id, None)
        if radio is None:
            return
        self._unbind_one(radio_id, radio)
        if self._default_radio_id == radio_id:
            self._default_radio_id = next(iter(self._radios.keys()), None)

    def configure_topology(self, topology):
        from .topology import ReceptionComparison
        missing = set(topology.members) - set(self._radios)
        if set(self._radios) - set(topology.members) or any(not topology.is_shadow(rid) for rid in missing):
            raise ValueError("Topology must describe all radios and have both transmitters")
        from .logical_receiver import LogicalReceiver
        logical_receiver = LogicalReceiver(topology)
        if self.logical_receiver is not None:
            self.logical_receiver.close(require_unsubscribed=True)
        self.topology = topology
        self.comparison = ReceptionComparison(topology)
        self.logical_receiver = logical_receiver

    def set_default_radio(self, radio_id: str) -> None:
        if radio_id not in self._radios:
            raise KeyError(f"Unknown radio_id={radio_id!r}")
        if self.topology:
            self.topology.check_tx(radio_id)
        self._default_radio_id = radio_id

    def set_tx_selector(self, selector: Optional[TxSelector]) -> None:
        """Optional policy choosing the TX radio when none is named explicitly.

        Accepts either ``selector(data) -> radio_id | None`` or a selector that
        names an ``rx_radio_id`` parameter, which is passed by keyword and
        carries the radio the frame arrived on. Which one this is is settled
        here, not at send time.
        """
        self._tx_selector = selector
        self._tx_selector_takes_rx = _selector_takes_rx(selector)

    # ------------------------------------------------------------------
    # Callbacks / arming
    # ------------------------------------------------------------------

    def add_observation_callback(self, callback):
        """Synchronous nonblocking physical RX observation, including shadows."""
        if callback not in self._observation_callbacks:
            self._observation_callbacks.append(callback)

    def remove_observation_callback(self, callback):
        if callback in self._observation_callbacks:
            self._observation_callbacks.remove(callback)

    def set_ingress_callback(self, callback: Optional[Callable[[RFIngress], Any]]) -> None:
        """Receive each fabric ingress (typically one RadioReception)."""
        self._ingress_callback = callback

    def set_legacy_rx_callback(self, callback: Optional[LegacyRxCallback]) -> None:
        """Receive each reception with legacy ``(data, rssi, snr)`` signature."""
        self._legacy_rx_callback = callback

    def arm(self) -> None:
        """Bind fabric RX callbacks on every registered radio."""
        self._armed = True
        for radio_id, radio in self._radios.items():
            self._bind_one(radio_id, radio)

    def disarm(self) -> None:
        """Unbind fabric RX callbacks from every registered radio."""
        self._armed = False
        for radio_id in list(self._radios.keys()):
            self._unbind_one(radio_id)

    def _bind_one(self, radio_id: str, radio: Any) -> None:
        if not hasattr(radio, "set_rx_callback"):
            logger.warning(
                "Radio %s (%s) has no set_rx_callback; fabric will not receive",
                radio_id,
                type(radio).__name__,
            )
            return
        callback = self._make_rx_handler(radio_id)
        radio.set_rx_callback(callback)
        self._rx_bindings[radio_id] = callback
        logger.debug(
            "RFFabric armed on radio_id=%s (%s)",
            radio_id,
            type(radio).__name__,
        )

    def has_rx_binding(self, radio_id: str) -> bool:
        """Whether the armed physical callback still belongs to this fabric."""
        callback = self._rx_bindings.get(radio_id)
        return (self._armed and callback is not None
                and getattr(self._radios.get(radio_id), "rx_callback", None) is callback)

    def _unbind_one(self, radio_id: str, radio: Any = None) -> None:
        self._rx_bindings.pop(radio_id, None)
        target = radio if radio is not None else self._radios.get(radio_id)
        if target is None or not hasattr(target, "set_rx_callback"):
            return
        try:
            target.set_rx_callback(None)
        except Exception:
            logger.debug(
                "Clearing fabric radio RX callback failed for %s",
                radio_id,
                exc_info=True,
            )

    def _make_rx_handler(self, radio_id: str):
        def _handler(
            data: bytes,
            rssi: Optional[int] = None,
            snr: Optional[float] = None,
        ) -> None:
            self._on_radio_rx(radio_id, data, rssi, snr)

        return _handler

    def _notify_observation(self, data, rssi, snr, radio_id, own_tx_match):
        # Synchronous observation edge never admits packets to routing.
        for callback in tuple(self._observation_callbacks):
            try:
                callback(bytes(data), rssi, snr, rx_radio_id=radio_id,
                         own_tx_match=bool(own_tx_match))
            except Exception:
                logger.exception("Physical RX observer failed")

    def _on_radio_rx(
        self,
        radio_id: str,
        data: bytes,
        rssi: Optional[int] = None,
        snr: Optional[float] = None,
    ) -> None:
        """Convert one radio callback into one RFIngress + one legacy fire."""
        own_tx_match = False
        if self.topology:
            from ..protocol.packet import Packet
            try:
                packet = Packet()
                packet.read_from(data)
                own_tx_match = self.comparison.receive(radio_id, packet.calculate_packet_hash().hex(), hashlib.sha256(data).digest())
            except Exception:
                # Observers count malformed drops, without admitting routing.
                self._notify_observation(data, rssi, snr, radio_id, False)
                return
        self._notify_observation(data, rssi, snr, radio_id, own_tx_match)
        if self.logical_receiver is not None:
            self.logical_receiver.observe(data, rssi, snr, rx_radio_id=radio_id,
                                          own_tx_match=own_tx_match)
        if self.topology and self.topology.is_shadow(radio_id):
            return
        reception = RadioReception(
            data=data,
            rssi=rssi,
            snr=snr,
            radio_id=radio_id,
        )
        ingress = RFIngress.from_reception(reception)

        if rssi is not None:
            self._last_rssi = int(rssi)
        elif radio_id in self._radios and hasattr(self._radios[radio_id], "get_last_rssi"):
            try:
                self._last_rssi = int(self._radios[radio_id].get_last_rssi())
            except Exception:
                pass
        if snr is not None:
            self._last_snr = float(snr)
        elif radio_id in self._radios and hasattr(self._radios[radio_id], "get_last_snr"):
            try:
                self._last_snr = float(self._radios[radio_id].get_last_snr())
            except Exception:
                pass
        self._last_rx_radio_id = radio_id

        if self._ingress_callback is not None:
            try:
                result = self._ingress_callback(ingress)
                if isinstance(result, Awaitable):
                    # Sync radio edge; async consumers schedule themselves.
                    pass
            except Exception:
                logger.exception("RFFabric ingress callback failed")

        if self._legacy_rx_callback is not None:
            try:
                if _selector_takes_rx(self._legacy_rx_callback):
                    self._legacy_rx_callback(data, rssi, snr, rx_radio_id=radio_id)
                else:
                    try:
                        inspect.signature(self._legacy_rx_callback).bind(data, rssi, snr)
                    except (TypeError, ValueError):
                        self._legacy_rx_callback(data)
                    else:
                        self._legacy_rx_callback(data, rssi, snr)
            except Exception:
                logger.exception("RFFabric legacy RX callback failed")

    # ------------------------------------------------------------------
    # TX
    # ------------------------------------------------------------------

    @contextmanager
    def ordered_tx_group(self, logical_primary):
        """Authorize one bounded ordered group, with all CAD modes confirmed OFF.

        The caller awaits each scheduled leg inside this scope. No hardware
        settings are changed here. A missing/unknown CAD state rejects the group.
        """
        if self._ordered_tx.get() is not None:
            raise ValueError("Nested ordered TX groups are forbidden")
        if self.topology is None:
            raise ValueError("Ordered TX requires explicit topology")
        targets = self.topology.tx_targets(logical_primary)
        if len(targets) < 2:
            raise ValueError("No ordered TX group configured")
        self._check_group_cad(targets)
        group = OrderedTxContext(logical_primary, targets, time.monotonic() + 300)
        token = self._ordered_tx.set(group)
        try:
            yield group
        finally:
            group.active = False
            if group.claimed and not group.preserve_claim and group.claim is not None:
                self._network_tx_seen.pop(group.claim, None)
            self._ordered_tx.reset(token)

    def _check_group_cad(self, targets):
        for rid in targets:
            radio = self._radios.get(rid)
            if (radio is None or getattr(radio, "_cad_mode_explicit", None) is not False
                    or getattr(radio, "lbt_enabled", None) is not False):
                raise ValueError(f"Ordered TX requires confirmed CAD OFF on {rid}")

    def resolve_tx_radio_id(
        self,
        data: bytes,
        radio_id: Optional[str] = None,
        *,
        rx_radio_id: Optional[str] = None,
        reply_to_radio_id: Optional[str] = None,
    ) -> str:
        """Choose which registered radio should transmit ``data``.

        ``rx_radio_id`` is the radio the frame arrived on, or None when this
        node originated it. It is passed to a context-aware selector, which
        makes the answer a function of the arguments alone: ask before the send
        and the send will agree. Selectors of the original one-argument shape
        never see it and behave exactly as they did.
        """
        if self.topology:
            if radio_id is None:
                if reply_to_radio_id is not None:
                    radio_id = self.topology.reply_destination(reply_to_radio_id)
                elif rx_radio_id is None:
                    radio_id = self.topology.default_radio
                else:
                    destinations = self.topology.relay_destinations(rx_radio_id)
                    if not destinations:
                        raise ValueError("Shadow receivers cannot cause TX")
                    radio_id = destinations[0]
            group = self._ordered_tx.get()
            if group is not None:
                group.validate(radio_id)
            self.topology.check_tx(radio_id, rx_radio_id, reply_to_radio_id=reply_to_radio_id,
                                   group_primary=group.primary if group else None)
        if radio_id is not None:
            if radio_id not in self._radios:
                raise KeyError(f"Unknown radio_id={radio_id!r}")
            return radio_id
        if self._tx_selector is not None:
            if self._tx_selector_takes_rx:
                selected = self._tx_selector(data, rx_radio_id=rx_radio_id)
            else:
                selected = self._tx_selector(data)
            if selected is not None:
                if selected not in self._radios:
                    raise KeyError(f"TX selector returned unknown radio_id={selected!r}")
                return selected
        default = self.default_radio_id
        if default is None:
            raise RuntimeError("RFFabric has no registered radio")
        return default

    async def send(
        self,
        data: bytes,
        *,
        radio_id: Optional[str] = None,
        rx_radio_id: Optional[str] = None,
        reply_to_radio_id: Optional[str] = None,
    ) -> Any:
        """Transmit via default, selector, or explicit ``radio_id``.

        ``rx_radio_id`` names the radio the frame arrived on and is only read by
        a context-aware selector; an explicit ``radio_id`` still wins outright.

        When the physical radio returns a dict metadata blob, attach
        ``radio_id`` so callers (Dispatcher TX logs) know which endpoint TX'd.
        Non-dict results are returned unchanged for legacy radios.
        """
        rid = self.resolve_tx_radio_id(data, radio_id, rx_radio_id=rx_radio_id, reply_to_radio_id=reply_to_radio_id)
        radio = self._radios[rid]
        if self.topology and getattr(radio, 'is_connected', True) is False:
            return {'ok': False, 'radio_id': rid, 'reason': 'designated transmitter disconnected'}
        if not hasattr(radio, "send"):
            raise RuntimeError(f"Radio {rid!r} ({type(radio).__name__}) does not support send()")
        network = None
        claim = None
        group = self._ordered_tx.get()
        if group is not None:
            self._check_group_cad(group.targets)
            group.begin(rid, data)
        if self.topology:
            from ..protocol.packet import Packet
            from ..protocol.constants import PAYLOAD_TYPE_ACK, PAYLOAD_TYPE_MULTIPART
            packet = Packet()
            packet.read_from(data)
            # ACK redundancy is protocol-defined and must retain its extra copies.
            is_ack = packet.get_payload_type() == PAYLOAD_TYPE_ACK or (
                packet.get_payload_type() == PAYLOAD_TYPE_MULTIPART and packet.payload
                and packet.payload[0] & 0x0f == PAYLOAD_TYPE_ACK)
            if not is_ack:
                now = time.monotonic()
                while self._network_tx_seen and now - next(iter(self._network_tx_seen.values())) > 300:
                    self._network_tx_seen.popitem(last=False)
                claim = (self.topology.network(rid), packet.calculate_packet_hash().hex())
                if claim in self._network_tx_seen and not (group and group.claimed and group.claim == claim):
                    self.network_tx_duplicates += 1
                    if group:
                        group.inflight = False
                        group.aborted = True
                    return {'ok': False, 'radio_id': rid, 'reason': 'destination duplicate',
                            'suppressed': True, 'retryable': False, 'preserve_tx_claim': True,
                            'tx_outcome_uncertain': False, 'airtime_ms': 0.0}
                if claim not in self._network_tx_seen and len(self._network_tx_seen) >= 10000:
                    self.network_tx_capacity_drops += 1
                    if group:
                        group.inflight = False
                        group.aborted = True
                    return {'ok': False, 'radio_id': rid, 'reason': 'destination cache full',
                            'retryable': False, 'tx_outcome_uncertain': False, 'airtime_ms': 0.0}
                # Claim synchronously before any await, including concurrent sends.
                self._network_tx_seen[claim] = now
                if group:
                    group.claim = claim
                    group.claimed = True
        if self.comparison:
            from ..protocol.packet import Packet
            packet = Packet()
            packet.read_from(data)
            network = self.comparison.start_tx(rid, hashlib.sha256(data).digest(), packet.calculate_packet_hash().hex())
        result = None
        send_exception = False
        try:
            result = await radio.send(data)
        except BaseException:
            send_exception = True
            raise
        finally:
            failed = result is False or (isinstance(result, dict) and result.get('ok') is False
                                         and not result.get('tx_outcome_uncertain'))
            if group:
                group.finish(rid, result, send_exception)
            if failed and claim is not None and group is None:
                self._network_tx_seen.pop(claim, None)
            if network is not None:
                self.comparison.end_tx(
                    network, hashlib.sha256(data).digest(), failed=failed,
                    outcome=("exception" if send_exception else
                             "uncertain" if isinstance(result, dict) and result.get("tx_outcome_uncertain") else
                             "confirmed" if result is True or isinstance(result, dict) and result.get("ok") is True else
                             "not_confirmed"))
        if isinstance(result, dict):
            # Do not overwrite if the radio already stamped a more specific id.
            result.setdefault("radio_id", rid)
            return result
        if result is False or result is None:
            # Preserve failure semantics used by Dispatcher (None/False = TX fail).
            return result
        if result is True:
            return {"ok": True, "radio_id": rid}
        # Unknown non-dict success payload: leave unchanged.
        return result

    def get_last_rssi(self) -> int:
        if self._last_rx_radio_id and self._last_rx_radio_id in self._radios:
            radio = self._radios[self._last_rx_radio_id]
            if hasattr(radio, "get_last_rssi"):
                try:
                    return int(radio.get_last_rssi())
                except Exception:
                    pass
        default = self.radio
        if default is not None and hasattr(default, "get_last_rssi"):
            try:
                return int(default.get_last_rssi())
            except Exception:
                pass
        return int(self._last_rssi)

    def get_last_snr(self) -> float:
        if self._last_rx_radio_id and self._last_rx_radio_id in self._radios:
            radio = self._radios[self._last_rx_radio_id]
            if hasattr(radio, "get_last_snr"):
                try:
                    return float(radio.get_last_snr())
                except Exception:
                    pass
        default = self.radio
        if default is not None and hasattr(default, "get_last_snr"):
            try:
                return float(default.get_last_snr())
            except Exception:
                pass
        return float(self._last_snr)

    def check_radio_health(self) -> bool:
        """Run each registered radio's health check.

        Dispatcher sees the Fabric as one radio, so its periodic health check
        must reach every physical endpoint. Besides link recovery, modem
        health checks refresh cached metrics such as noise floor and CRC-error
        counters.
        """
        if not self._radios:
            return False

        healthy = True
        for radio_id, radio in list(self._radios.items()):
            check = getattr(radio, "check_radio_health", None)
            if not callable(check):
                continue
            try:
                if not check():
                    healthy = False
            except Exception as exc:
                healthy = False
                logger.warning("Radio %s health check failed: %s", radio_id, exc)
        return healthy

    def __getattr__(self, name: str) -> Any:
        """Attribute pass-through for radio settings used by Dispatcher."""
        if name.startswith("_"):
            raise AttributeError(name)
        radio = self.radio
        if radio is None:
            raise AttributeError(name)
        return getattr(radio, name)
