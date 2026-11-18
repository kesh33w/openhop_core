"""Reception comparison bookkeeping across reconfiguration and expiry."""
from openhop_core.rf_fabric.topology import NetworkTopology, ReceptionComparison
from openhop_core.rf_fabric import RFFabric


def topology():
    return NetworkTopology(
        [{"id": "a", "network": "A", "role": "transceiver"},
         {"id": "b", "network": "B", "role": "transceiver"}],
        {"networks": {"A": "a", "B": "b"}}, "a")


class _Radio:
    def set_rx_callback(self, cb):
        pass


def test_reconfiguring_topology_closes_previous_capture():
    fabric = RFFabric()
    for rid in ("a", "b"):
        fabric.register_radio(_Radio(), radio_id=rid)
    fabric.configure_topology(topology())
    closed = []
    fabric.comparison.capture = type("Capture", (), {"close": lambda self: closed.append(True)})()
    fabric.configure_topology(topology())
    assert closed == [True]


def test_refreshed_tx_entry_does_not_block_expiry():
    now = [0.0]
    comparison = ReceptionComparison(topology(), ttl=10, clock=lambda: now[0])
    first = comparison.start_tx("a", b"first")
    comparison.start_tx("a", b"second")
    now[0] = 9.0
    comparison.end_tx(first, b"first")       # refreshes the oldest entry
    now[0] = 12.0
    comparison._expire(comparison.tx, now[0])
    assert ("A", b"second") not in comparison.tx  # expired despite refreshed head
    assert ("A", b"first") in comparison.tx
