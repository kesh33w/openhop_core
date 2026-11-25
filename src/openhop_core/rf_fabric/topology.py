"""Explicit multi-network routing and bounded receive-only comparison.

A topology names two or more separate meshes, each with one designated TX
radio. Traffic heard on one network may be sent on its own network and on
every other network; which packets actually cross, and where, is decided by
the caller (for example the repeater's channel-bridge mappings).

No hardware calls live here. A shadow reception never enters the dispatcher.
Network names are opaque identifiers, not inferred from radio order or frequency.
"""

import time
from collections import Counter, OrderedDict
from threading import RLock

# Which sends a TX group applies to.
TX_GROUP_SCOPES = ("all", "crossings")

# Upper bound for the pause between a TX group's legs, in milliseconds. Longer
# pauses only delay the second copy past any use and hold the group open.
MAX_TX_GROUP_GAP_MS = 10000

# Leg order of a TX group: as configured, or shuffled for every send.
TX_GROUP_ORDERS = ("fixed", "random")


class NetworkTopology:
    def __init__(self, entries, settings, default_radio):
        if not isinstance(settings, dict) or set(settings) - {
            "networks",
            "enable_receiver_forwarding",
            "repeat_receiver_on_network",
            "tx_groups",
            "tx_group_scopes",
            "tx_group_gaps_ms",
            "tx_group_orders",
        }:
            raise ValueError("Invalid fabric.topology fields")
        self.networks = settings.get("networks")
        if not isinstance(self.networks, dict) or len(self.networks) < 2:
            raise ValueError("topology requires two or more named networks with TX radio IDs")
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
                raise ValueError("Every topology radio needs a unique ID and known network")
            if role not in ("transceiver", "shadow", "receiver"):
                raise ValueError("Every topology radio needs an explicit role")
            self.members[rid] = (network, role)
        for name, rid in self.networks.items():
            if self.members.get(rid) != (name, "transceiver"):
                raise ValueError("Each network TX must name its transceiver")
        groups = settings.get("tx_groups", {})
        if not isinstance(groups, dict):
            raise ValueError("tx_groups must map logical primary IDs to ordered radio lists")
        self.tx_groups = {}
        for primary, targets in groups.items():
            if primary not in self.networks.values():
                raise ValueError("TX group must name a logical network primary")
            if (
                not isinstance(targets, (list, tuple))
                or not targets
                or any(not isinstance(rid, str) for rid in targets)
                or len(set(targets)) != len(targets)
                or primary not in targets
            ):
                raise ValueError("TX group must contain unique radio IDs including its primary")
            network = self.members[primary][0]
            if any(rid not in self.members or self.members[rid][0] != network for rid in targets):
                raise ValueError("TX group members must belong to the same network")
            if any(self.members[rid][1] == "shadow" for rid in targets):
                raise ValueError("Shadow radios are receive-only and cannot be in a TX group")
            self.tx_groups[primary] = tuple(targets)
        scopes = settings.get("tx_group_scopes", {})
        if not isinstance(scopes, dict):
            raise ValueError("tx_group_scopes must map TX group primaries to all or crossings")
        self.tx_group_scopes = {}
        for primary, scope in scopes.items():
            if primary not in self.tx_groups:
                raise ValueError("tx_group_scopes may only name a configured TX group")
            if scope not in TX_GROUP_SCOPES:
                raise ValueError("TX group scope must be 'all' or 'crossings'")
            self.tx_group_scopes[primary] = scope
        gaps = settings.get("tx_group_gaps_ms", {})
        if not isinstance(gaps, dict):
            raise ValueError("tx_group_gaps_ms must map TX group primaries to [min, max]")
        self.tx_group_gaps_ms = {}
        for primary, gap in gaps.items():
            if len(self.tx_groups.get(primary, ())) < 2:
                raise ValueError("tx_group_gaps_ms may only name a TX group of two or more radios")
            if (
                not isinstance(gap, (list, tuple))
                or len(gap) != 2
                or any(type(v) not in (int, float) for v in gap)
                or not 0 <= gap[0] <= gap[1] <= MAX_TX_GROUP_GAP_MS
            ):
                raise ValueError(
                    f"TX group gap must be [min, max] ms with 0 <= min <= max <= "
                    f"{MAX_TX_GROUP_GAP_MS}"
                )
            self.tx_group_gaps_ms[primary] = (float(gap[0]), float(gap[1]))
        orders = settings.get("tx_group_orders", {})
        if not isinstance(orders, dict):
            raise ValueError("tx_group_orders must map TX group primaries to fixed or random")
        self.tx_group_orders = {}
        for primary, order in orders.items():
            if len(self.tx_groups.get(primary, ())) < 2:
                raise ValueError("tx_group_orders may only name a TX group of two or more radios")
            if order not in TX_GROUP_ORDERS:
                raise ValueError("TX group order must be 'fixed' or 'random'")
            self.tx_group_orders[primary] = order
        permitted = set(self.networks.values())
        for targets in self.tx_groups.values():
            permitted.update(targets)
        if not {r for r, (_, role) in self.members.items() if role == "transceiver"}.issubset(
            permitted
        ):
            raise ValueError("Extra transceivers require an explicit TX group")
        if default_radio not in self.networks.values():
            raise ValueError("Default radio must be a designated network transmitter")
        self.default_radio = default_radio
        self.tx_mode = "sticky"
        self.repeat_on_ingress = False
        for key in ("enable_receiver_forwarding", "repeat_receiver_on_network"):
            if type(settings.get(key, False)) is not bool:
                raise ValueError(f"{key} must be a boolean")
        if any(role == "receiver" for _, role in self.members.values()) and not settings.get(
            "enable_receiver_forwarding", False
        ):
            raise ValueError(
                "Receiver forwarding requires explicit enable_receiver_forwarding after comparison"
            )
        self.repeat_receiver = settings.get("repeat_receiver_on_network", False)
        if self.repeat_receiver and not settings.get("enable_receiver_forwarding", False):
            raise ValueError("Same-network receiver repetition requires receiver forwarding")

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
        """Permitted logical egresses for traffic heard on ``rid``.

        Every other network's TX, in configured order, then the home TX when
        same-network repeats are allowed. With two networks this is
        ``(other, home)`` or ``(other,)``.
        """
        if self.is_shadow(rid):
            return ()
        home = self.home(rid)
        others = tuple(tx for tx in self.networks.values() if tx != home)
        same = self.members[rid][1] == "transceiver" or self.repeat_receiver
        return (*others, home) if same else others

    def relay_destinations(self, rid):
        allowed = self.destinations(rid)
        if not allowed:
            return ()
        if self.members[rid][1] == "receiver":
            return allowed
        if self.tx_mode == "bridge":
            if self.repeat_on_ingress:
                return allowed
            return tuple(tx for tx in allowed if tx != self.home(rid))
        if self.tx_mode == "sticky":
            return (self.home(rid),)
        return (self.default_radio,)

    def reply_destination(self, request_radio):
        if self.is_shadow(request_radio):
            raise ValueError("Shadow receivers cannot cause replies")
        return self.home(request_radio)

    def tx_targets(self, logical_primary):
        """Ordered opt-in physical egresses; logical routing identity is unchanged."""
        if logical_primary not in self.networks.values():
            raise ValueError("TX expansion requires a logical network primary")
        return self.tx_groups.get(logical_primary, (logical_primary,))

    def tx_group_scope(self, logical_primary):
        """``all`` (every send uses the group) or ``crossings`` (bridged sends only)."""
        return self.tx_group_scopes.get(logical_primary, "all")

    def tx_group_gap_ms(self, logical_primary):
        """``(min, max)`` pause in ms before each leg after the first; (0, 0) by default.

        The sender picks a random value in the range for each pause so the extra
        copy does not land at a fixed offset behind the first one, where it would
        collide with neighbours rebroadcasting that first copy.
        """
        return self.tx_group_gaps_ms.get(logical_primary, (0.0, 0.0))

    def tx_group_order(self, logical_primary):
        """``fixed`` (configured order, the default) or ``random`` (shuffled per send).

        Random order lets each radio go first about half the time, so which copy
        neighbours relay can be compared fairly between the radios.
        """
        return self.tx_group_orders.get(logical_primary, "fixed")

    def tx_targets_for(self, logical_primary, *, crossing=False):
        """Physical egresses for one send.

        A group scoped to ``crossings`` sends bridged packets through every
        member, in order, and everything else through its primary alone.
        """
        targets = self.tx_targets(logical_primary)
        if not crossing and self.tx_group_scope(logical_primary) == "crossings":
            return (logical_primary,)
        return targets

    def check_tx(self, target, ingress=None, *, reply_to_radio_id=None, group_primary=None):
        if group_primary is not None:
            if target not in self.tx_targets(group_primary):
                raise ValueError("Physical target is not a member of the active TX group")
            # Startup rejects this too; checked again so a shadow can never key up.
            if self.members.get(target, (None, None))[1] == "shadow":
                raise ValueError("TX is forbidden on a shadow radio")
            target = group_primary
        if target not in self.networks.values():
            raise ValueError("TX is forbidden on a receive-only radio")
        if reply_to_radio_id is not None:
            # A newly generated local reply is distinct from retransmission.
            if ingress is not None or target != self.reply_destination(reply_to_radio_id):
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
            own = bool(
                sent is not None and (self.inflight_frames[(network, frame)] or now - sent[0] <= 2)
            )
            if self.capture:
                self.capture.emit(
                    "rx",
                    network,
                    radio_id=rid,
                    packet_hash=identity,
                    frame_hash=frame.hex() if frame else None,
                    own_tx_match=own,
                    tx_overlap=bool(self.inflight[network]),
                )
            if own:
                self.counters[("own_tx_match", rid)] += 1
                return True
            if key not in self.rows:
                self.rows[key] = (now, set())
            self.rows[key][1].add(rid)
            self._expire(self.rows, now)
            return False

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
                self.capture.emit(
                    "tx_start",
                    network,
                    radio_id=rid,
                    frame_hash=identity.hex(),
                    packet_hash=payload_identity,
                )
            return network

    def end_tx(self, network, frame=None, failed=False, outcome="unknown"):
        with self.lock:
            if self.capture:
                self.capture.emit(
                    "tx_end",
                    network,
                    frame_hash=frame.hex() if frame else None,
                    failed=failed,
                    outcome=outcome,
                )
            self.inflight[network] -= 1
            key = (network, frame)
            self.inflight_frames[key] -= 1
            if not self.inflight_frames[key]:
                del self.inflight_frames[key]
            if failed:
                self.tx.pop(key, None)
            elif key in self.tx:
                # Refreshed entries move to the back: _expire stops at the first
                # unexpired entry, so a stale head would block all later expiry.
                self.tx[key] = (self.clock(), None)
                self.tx.move_to_end(key)

    def snapshot(self):
        with self.lock:
            self._expire(self.rows, self.clock())
            receivers = {}
            for rid, (network, role) in self.topology.members.items():
                sets = [ears for (net, _), (_, ears) in self.rows.items() if net == network]
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
                    n: sum(net == n for net, _ in self.rows) for n in self.topology.networks
                },
            }
