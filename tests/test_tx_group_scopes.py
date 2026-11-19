"""TX groups can apply to every send or to bridged (crossing) sends only."""

import pytest

from openhop_core.rf_fabric.topology import NetworkTopology

RADIOS = [
    {"id": "host-rf", "network": "SMF", "role": "transceiver"},
    {"id": "bay", "network": "SFO", "role": "transceiver"},
    {"id": "rak910", "network": "SFO", "role": "transceiver"},
]


def topology(**extra):
    settings = {
        "networks": {"SMF": "host-rf", "SFO": "bay"},
        "tx_groups": {"bay": ["rak910", "bay"]},
        **extra,
    }
    return NetworkTopology(RADIOS, settings, "host-rf")


def test_default_scope_uses_the_group_for_every_send():
    t = topology()
    assert t.tx_group_scope("bay") == "all"
    assert t.tx_targets_for("bay") == ("rak910", "bay")
    assert t.tx_targets_for("bay", crossing=True) == ("rak910", "bay")


def test_crossings_scope_uses_the_group_only_for_bridged_sends():
    t = topology(tx_group_scopes={"bay": "crossings"})
    assert t.tx_targets_for("bay", crossing=True) == ("rak910", "bay")
    assert t.tx_targets_for("bay") == ("bay",)
    # Leg membership checks still see the whole group.
    assert t.tx_targets("bay") == ("rak910", "bay")
    t.check_tx("rak910", group_primary="bay")
    # Networks without a group are unaffected.
    assert t.tx_targets_for("host-rf", crossing=True) == ("host-rf",)


@pytest.mark.parametrize(
    "scopes",
    [
        {"bay": "bridged"},  # unknown value
        {"host-rf": "crossings"},  # not a configured group
        ["bay"],  # wrong type
    ],
)
def test_invalid_scopes_are_rejected(scopes):
    with pytest.raises(ValueError):
        topology(tx_group_scopes=scopes)
