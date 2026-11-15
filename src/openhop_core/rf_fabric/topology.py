"""Explicit two-network routing and bounded receive-only comparison.

No hardware calls live here. A shadow reception never enters the dispatcher.
Network names are opaque identifiers, not inferred from radio order or frequency.
"""

from collections import Counter, OrderedDict
from threading import RLock
import time


class NetworkTopology:
    def __init__(self, entries, settings, default_radio):
        if not isinstance(settings, dict) or set(settings) - {
            "networks",
            "enable_receiver_forwarding",
            "repeat_receiver_on_network",
        }:
            raise ValueError("Invalid fabric.topology fields")
        self.networks = settings.get("networks")
        if not isinstance(self.networks, dict) or len(self.networks) != 2:
            raise ValueError(
                "topology requires exactly two named networks with TX radio IDs"
            )
        if any(
            not isinstance(n, str) or not n.strip() or not isinstance(r, str)
            for n, r in self.networks.items()
        ):
            raise ValueError("Invalid topology network or TX radio ID")
        self.networks = dict(self.networks)
        self.members = {}
        for entry in entries:
            rid = entry.get("id") or entry.get("radio_id")
            network, role = entry.get("network"), entry.get("role")
            if not rid or rid in self.members or network not in self.networks:
                raise ValueError(
                    "Every topology radio needs a unique ID and known network"
                )
            if role not in ("transceiver", "shadow", "receiver"):
                raise ValueError("Every topology radio needs an explicit role")
            self.members[rid] = (network, role)
        for name, rid in self.networks.items():
            if self.members.get(rid) != (name, "transceiver"):
                raise ValueError("Each network TX must name its transceiver")
        if set(
            r for r, (_, role) in self.members.items() if role == "transceiver"
        ) != set(self.networks.values()):
            raise ValueError("Exactly one transceiver is allowed per network")
        if default_radio not in self.networks.values():
            raise ValueError("Default radio must be a designated network transmitter")
        self.default_radio = default_radio
        self.tx_mode = "sticky"
        self.repeat_on_ingress = False
        for key in ("enable_receiver_forwarding", "repeat_receiver_on_network"):
            if type(settings.get(key, False)) is not bool:
                raise ValueError(f"{key} must be a boolean")
        if any(
            role == "receiver" for _, role in self.members.values()
        ) and not settings.get("enable_receiver_forwarding", False):
            raise ValueError(
                "Receiver forwarding requires explicit enable_receiver_forwarding after comparison"
            )
        self.repeat_receiver = settings.get("repeat_receiver_on_network", False)
        if self.repeat_receiver and not settings.get(
            "enable_receiver_forwarding", False
        ):
            raise ValueError(
                "Same-network receiver repetition requires receiver forwarding"
            )

    def network(self, rid):
        if rid not in self.members:
            raise ValueError(f"Unknown topology ingress {rid!r}")
        return self.members[rid][0]

    @property
    def has_receivers(self):
        return any(role == "receiver" for _, role in self.members.values())

    def forwarding_class(self, rid):
        self.network(rid)
        return self.members[rid][1]

    def is_shadow(self, rid):
        self.network(rid)
        return self.members[rid][1] == "shadow"

    def home(self, rid):
        return self.networks[self.network(rid)]

    def destinations(self, rid):
        if self.is_shadow(rid):
            return ()
        home = self.home(rid)
        other = next(tx for tx in self.networks.values() if tx != home)
        same = self.members[rid][1] == "transceiver" or self.repeat_receiver
        return (other, home) if same else (other,)

    def relay_destinations(self, rid):
        allowed = self.destinations(rid)
        if not allowed:
            return ()
        if self.members[rid][1] == "receiver":
            return allowed
        if self.tx_mode == "bridge":
            return allowed if self.repeat_on_ingress else allowed[:1]
        if self.tx_mode == "sticky":
            return (self.home(rid),)
        return (self.default_radio,)

    def reply_destination(self, request_radio):
        if self.is_shadow(request_radio):
            raise ValueError("Shadow receivers cannot cause replies")
        return self.home(request_radio)

    def check_tx(self, target, ingress=None, *, reply_to_radio_id=None):
        if target not in self.networks.values():
            raise ValueError("TX is forbidden on a receive-only radio")
        if reply_to_radio_id is not None:
            # A newly generated local reply is distinct from retransmission.
            if ingress is not None or target != self.reply_destination(
                reply_to_radio_id
            ):
                raise ValueError("A local reply must return on its request's network")
        elif ingress is not None and target not in self.destinations(ingress):
            raise ValueError("TX destination is forbidden for this ingress")


class ReceptionComparison:
    """Rolling exact counts within TTL and capacity; no plaintext or channel keys.

    TX overlap means a reception during a physical send on its network. An
    echo match means the exact frame sent on that network in the last two seconds.
    Evictions are reported so a saturated comparison cannot look complete.
    """

    def __init__(self, topology, *, ttl=300, capacity=10000, clock=time.monotonic):
        self.topology, self.ttl, self.capacity, self.clock = (
            topology,
            ttl,
            capacity,
            clock,
        )
        self.rows = OrderedDict()
        self.tx = OrderedDict()
        self.inflight = Counter()
        self.inflight_frames = Counter()
        self.counters = Counter()
        self.lock = RLock()
        from .capture import ReceptionCapture
        self.capture = ReceptionCapture.from_environment()

    def _expire(self, rows, now):
        while rows and now - next(iter(rows.values()))[0] > self.ttl:
            rows.popitem(last=False)
        while len(rows) > self.capacity:
            rows.popitem(last=False)
            self.counters["capacity_evictions"] += 1

    def receive(self, rid, identity, frame=None):
        network = self.topology.network(rid)
        with self.lock:
            now = self.clock()
            self._expire(self.rows, now)
            self._expire(self.tx, now)
            self.counters[("received", rid)] += 1
            if self.inflight[network]:
                self.counters[("tx_overlap", rid)] += 1
            key = (network, identity)
            sent = self.tx.get((network, frame))
            own = bool(sent is not None and (
                self.inflight_frames[(network, frame)] or now - sent[0] <= 2
            ))
            if self.capture:
                self.capture.emit("rx", network, radio_id=rid, packet_hash=identity,
                                  frame_hash=frame.hex() if frame else None,
                                  own_tx_match=own, tx_overlap=bool(self.inflight[network]))
            if own:
                self.counters[("own_tx_match", rid)] += 1
                return
            if key not in self.rows:
                self.rows[key] = (now, set())
            self.rows[key][1].add(rid)
            self._expire(self.rows, now)

    def start_tx(self, rid, identity, payload_identity=None):
        with self.lock:
            network = self.topology.network(rid)
            self.inflight[network] += 1
            self.inflight_frames[(network, identity)] += 1
            key = (network, identity)
            self.tx.pop(key, None)
            self.tx[key] = (self.clock(), None)
            self._expire(self.tx, self.clock())
            if self.capture:
                self.capture.emit("tx_start", network, radio_id=rid, frame_hash=identity.hex(), packet_hash=payload_identity)
            return network

    def end_tx(self, network, frame=None, failed=False, outcome="unknown"):
        with self.lock:
            if self.capture:
                self.capture.emit("tx_end", network, frame_hash=frame.hex() if frame else None, failed=failed, outcome=outcome)
            self.inflight[network] -= 1
            key = (network, frame)
            self.inflight_frames[key] -= 1
            if not self.inflight_frames[key]:
                del self.inflight_frames[key]
            if failed:
                self.tx.pop(key, None)
            elif key in self.tx:
                self.tx[key] = (self.clock(), None)

    def snapshot(self):
        with self.lock:
            self._expire(self.rows, self.clock())
            receivers = {}
            for rid, (network, role) in self.topology.members.items():
                sets = [
                    ears for (net, _), (_, ears) in self.rows.items() if net == network
                ]
                receivers[rid] = {
                    "network": network,
                    "role": role,
                    "unique_packets": sum(rid in ears for ears in sets),
                    "exclusive_packets": sum(ears == {rid} for ears in sets),
                    **{
                        name: self.counters[(name, rid)]
                        for name in ("received", "tx_overlap", "own_tx_match")
                    },
                }
            return {
                "capture": self.capture.snapshot() if self.capture else None,
                "window_seconds": self.ttl,
                "capacity": self.capacity,
                "capacity_evictions": self.counters["capacity_evictions"],
                "receivers": receivers,
                "networks": {
                    n: sum(net == n for net, _ in self.rows)
                    for n in self.topology.networks
                },
            }
