"""Local RF observations combined by network, without granting routing authority.

Subscribers must enqueue work without blocking. Returning False rejects admission;
the next physical copy retries the original first reception for that subscriber.
The dispatcher still receives every eligible physical edge separately, because
path variants and receiver forwarding permissions cannot be discarded here.
"""

from __future__ import annotations

from collections import Counter, OrderedDict
from dataclasses import dataclass, field
import hashlib
import inspect
from threading import RLock
import time
from types import MappingProxyType
from typing import Callable
import uuid

from ..protocol.packet import Packet
from ..protocol.constants import MAX_PATH_SIZE


@dataclass(frozen=True)
class PhysicalObservation:
    data: bytes
    wire_hash: str
    radio_id: str
    rssi: float | None
    snr: float | None
    at: float
    monotonic_at: float


@dataclass(frozen=True)
class LogicalReception:
    event_id: str
    stream_id: str
    network: str
    packet_hash: str
    first: PhysicalObservation
    observation: PhysicalObservation
    observations: tuple[PhysicalObservation, ...]
    kind: str
    observation_count: int
    # Explicitly never remote MQTT, a local TX loopback or authorization to send.
    source: str = "local_rf"


@dataclass
class _Entry:
    event_id: str
    sequence: int
    first: PhysicalObservation
    observations: list[PhysicalObservation]
    count: int = 1
    delivered: set[int] = field(default_factory=set)


class LogicalReceiver:
    """One first-arrival stream per active network; isolated streams for shadows.

    Expiry is measured from first admission, never extended by duplicate traffic.
    At capacity a new identity is rejected rather than evicting an unexpired one
    and re-triggering consumers. Per-event retained history is bounded; update
    subscribers still receive each subsequent observation while they keep up.
    Copy/variant compares the wire image with the frozen first image, not
    uniqueness among prior variants. No history is replayed to a new subscriber.
    """

    def __init__(
        self,
        topology,
        *,
        ttl=300,
        capacity=10000,
        history_capacity=32,
        subscriber_capacity=64,
        clock=time.monotonic,
        wall_clock=time.time,
    ):
        if (
            not 1 <= ttl <= 3600
            or not 1 <= capacity <= 100000
            or not 1 <= history_capacity <= 256
            or not 1 <= subscriber_capacity <= 256
        ):
            raise ValueError("Invalid logical receiver bounds")
        groups = {}
        networks = {}
        radios = {}
        for rid, (network, role) in topology.members.items():
            stream = f"shadow:{rid}" if role == "shadow" else network
            if stream in networks and networks[stream] != network:
                raise ValueError("Logical receiver stream collision")
            # A topology network must not alias an isolated shadow stream.
            if role == "shadow" and stream in topology.networks:
                raise ValueError("Network name collides with a shadow stream")
            networks[stream] = network
            groups.setdefault(stream, set()).add(rid)
            radios[rid] = stream
        self.streams = MappingProxyType({k: frozenset(v) for k, v in groups.items()})
        self._networks, self._radios = networks, radios
        self.ttl, self.capacity = ttl, capacity
        self.history_capacity, self.subscriber_capacity = (
            history_capacity,
            subscriber_capacity,
        )
        self.clock, self.wall_clock = clock, wall_clock
        self._lock = RLock()
        self._entries = OrderedDict()
        self._subscribers = {}
        self._sequence = self._token = 0
        self._session = uuid.uuid4().hex
        self._counts = Counter()
        self._closed = False

    def members(self, stream_id):
        try:
            return self.streams[stream_id]
        except KeyError:
            raise ValueError(f"Unknown logical receive stream: {stream_id!r}") from None

    def network(self, stream_id):
        self.members(stream_id)
        return self._networks[stream_id]

    def stream_for_radio(self, radio_id):
        try:
            return self._radios[radio_id]
        except KeyError:
            raise ValueError(f"Unknown physical radio: {radio_id!r}") from None

    def subscribe(self, stream_id, callback: Callable, *, updates=False):
        self.members(stream_id)
        if not callable(callback) or type(updates) is not bool:
            raise ValueError("Invalid logical receive subscriber")
        with self._lock:
            if self._closed:
                raise RuntimeError("Logical receiver is closed")
            if len(self._subscribers) >= self.subscriber_capacity:
                raise ValueError("Logical receiver subscriber capacity reached")
            self._token += 1
            self._subscribers[self._token] = (
                stream_id,
                callback,
                updates,
                self._sequence,
            )
            return self._token

    def unsubscribe(self, token):
        with self._lock:
            self._subscribers.pop(token, None)
            for entry in self._entries.values():
                entry.delivered.discard(token)

    def _event(self, entry, stream, digest, observation, kind):
        return LogicalReception(
            entry.event_id,
            stream,
            self._networks[stream],
            digest,
            entry.first,
            observation,
            tuple(entry.observations),
            kind,
            entry.count,
        )

    def observe(self, data, rssi=None, snr=None, *, rx_radio_id, own_tx_match=False):
        """Admit a physical RX and return its immutable event, or None on refusal.

        Callback admission is serialized with observation admission. Callbacks
        may only do bounded, nonblocking work. A failed callback cannot suppress
        another subscriber or alter dispatcher/forwarding delivery.
        """
        stream = self.stream_for_radio(rx_radio_id)
        with self._lock:
            if self._closed:
                self._counts["closed_drops"] += 1
                return None
            self._counts["physical_observations"] += 1
            if own_tx_match:
                # Do not consume an identity: a genuine incoming packet with the
                # same payload may still arrive later and deserve observation.
                self._counts["own_tx_suppressed"] += 1
                return None
            try:
                raw = bytes(data)
                if not 1 <= len(raw) <= 512:
                    raise ValueError("Invalid physical frame length")
                packet = Packet()
                packet.read_from(raw)
                if (
                    not packet.payload
                    or len(packet.path) > MAX_PATH_SIZE
                    or packet.get_path_hash_size() > 3
                ):
                    raise ValueError("Invalid packet shape")
                digest = packet.calculate_packet_hash().hex()
            except Exception:
                self._counts["invalid"] += 1
                return None
            now, wall = self.clock(), self.wall_clock()
            while (
                self._entries
                and now - next(iter(self._entries.values())).first.monotonic_at
                >= self.ttl
            ):
                self._entries.popitem(last=False)
                self._counts["expired"] += 1
            observation = PhysicalObservation(
                raw, hashlib.sha256(raw).hexdigest(), rx_radio_id, rssi, snr, wall, now
            )
            key = (stream, digest)
            entry = self._entries.get(key)
            if entry is None:
                if len(self._entries) >= self.capacity:
                    self._counts["capacity_drops"] += 1
                    return None
                self._sequence += 1
                entry = _Entry(
                    f"{self._session}:{self._sequence}",
                    self._sequence,
                    observation,
                    [observation],
                )
                self._entries[key] = entry
                kind = "first"
                self._counts["unique_events"] += 1
            else:
                entry.count += 1
                kind = (
                    "copy"
                    if observation.wire_hash == entry.first.wire_hash
                    else "variant"
                )
                self._counts[kind + "_observations"] += 1
                if len(entry.observations) < self.history_capacity:
                    entry.observations.append(observation)
                else:
                    self._counts["history_not_retained"] += 1
            event = self._event(entry, stream, digest, observation, kind)
            for token, (target, callback, updates, after_sequence) in tuple(
                self._subscribers.items()
            ):
                if (
                    token not in self._subscribers
                    or target != stream
                    or entry.sequence <= after_sequence
                ):
                    continue
                acknowledged = token in entry.delivered
                if acknowledged and not updates:
                    continue
                offered = (
                    event
                    if acknowledged
                    else self._event(entry, stream, digest, entry.first, "first")
                )
                try:
                    accepted = callback(offered)
                    if inspect.isawaitable(accepted):
                        if inspect.iscoroutine(accepted):
                            accepted.close()
                        raise TypeError("Logical subscriber must be synchronous")
                    if accepted is False:
                        self._counts["subscriber_refusals"] += 1
                        continue
                    if not acknowledged:
                        if token in self._subscribers:
                            entry.delivered.add(token)
                        self._counts["subscriber_first_deliveries"] += 1
                    else:
                        self._counts["subscriber_update_deliveries"] += 1
                except Exception:
                    self._counts["subscriber_errors"] += 1
            return event

    def snapshot(self):
        with self._lock:
            return {
                **self._counts,
                "cache_size": len(self._entries),
                "subscriber_count": len(self._subscribers),
                "closed": self._closed,
                "streams": {k: sorted(v) for k, v in self.streams.items()},
            }

    def close(self, *, require_unsubscribed=False):
        with self._lock:
            if require_unsubscribed and self._subscribers:
                raise RuntimeError(
                    "Logical receiver has subscribers; topology changes require restart"
                )
            self._closed = True
            self._subscribers.clear()
            self._entries.clear()
