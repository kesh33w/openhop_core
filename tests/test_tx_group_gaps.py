"""TX group gaps (pause between legs) and receive-only checks on group members."""

import pytest

from openhop_core.rf_fabric.topology import NetworkTopology

RADIOS = [
    {"id": "host-rf", "network": "SMF", "role": "transceiver"},
    {"id": "bay", "network": "SFO", "role": "transceiver"},
    {"id": "rak910", "network": "SFO", "role": "receiver"},
    {"id": "v3910", "network": "SFO", "role": "shadow"},
]


def topology(**extra):
    settings = {
        "networks": {"SMF": "host-rf", "SFO": "bay"},
        "tx_groups": {"bay": ["bay", "rak910"]},
        "enable_receiver_forwarding": True,
        **extra,
    }
    return NetworkTopology(RADIOS, settings, "host-rf")


def test_gap_defaults_to_none():
    assert topology().tx_group_gap_ms("bay") == (0.0, 0.0)
    assert topology().tx_group_gap_ms("host-rf") == (0.0, 0.0)


def test_gap_range_is_kept_per_group():
    t = topology(tx_group_gaps_ms={"bay": [300, 1200]})
    assert t.tx_group_gap_ms("bay") == (300.0, 1200.0)
    assert topology(tx_group_gaps_ms={"bay": [500, 500]}).tx_group_gap_ms("bay") == (500.0, 500.0)


@pytest.mark.parametrize(
    "gaps",
    [
        {"bay": [1200, 300]},  # min above max
        {"bay": [-1, 100]},
        {"bay": [0, 10001]},
        {"bay": 500},  # not a range
        {"bay": [True, 5]},
        {"bay": ["1", "2"]},
        {"host-rf": [0, 100]},  # no group on that network
        ["bay"],
    ],
)
def test_invalid_gaps_are_rejected(gaps):
    with pytest.raises(ValueError):
        topology(tx_group_gaps_ms=gaps)


def test_gap_needs_a_group_of_two():
    with pytest.raises(ValueError):
        topology(tx_groups={"bay": ["bay"]}, tx_group_gaps_ms={"bay": [0, 100]})


def test_receiver_may_be_a_backup_transmitter():
    t = topology()
    t.check_tx("rak910", group_primary="bay")


def test_shadow_cannot_join_a_tx_group():
    with pytest.raises(ValueError, match="Shadow"):
        topology(tx_groups={"bay": ["bay", "v3910"]})


def test_shadow_cannot_transmit_even_if_listed_after_startup():
    t = topology()
    # Simulate a group edited in memory past the startup check.
    t.tx_groups["bay"] = ("bay", "v3910")
    with pytest.raises(ValueError, match="shadow"):
        t.check_tx("v3910", group_primary="bay")
