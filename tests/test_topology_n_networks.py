"""Topologies with more than two networks route like the two-network case."""

import pytest

from openhop_core.rf_fabric.topology import NetworkTopology


def three(**settings):
    return NetworkTopology(
        [
            {"id": "a", "network": "A", "role": "transceiver"},
            {"id": "a_rx", "network": "A", "role": "receiver"},
            {"id": "b", "network": "B", "role": "transceiver"},
            {"id": "c", "network": "C", "role": "transceiver"},
            {"id": "c_shadow", "network": "C", "role": "shadow"},
        ],
        {
            "networks": {"A": "a", "B": "b", "C": "c"},
            "enable_receiver_forwarding": True,
            **settings,
        },
        "a",
    )


def test_one_network_is_rejected():
    with pytest.raises(ValueError, match="two or more named networks"):
        NetworkTopology(
            [{"id": "a", "network": "A", "role": "transceiver"}], {"networks": {"A": "a"}}, "a"
        )


def test_destinations_list_every_other_network_then_home():
    topology = three()
    assert topology.destinations("a") == ("b", "c", "a")
    assert topology.destinations("b") == ("a", "c", "b")
    assert topology.destinations("c") == ("a", "b", "c")
    # Receivers do not repeat on their own network unless configured to.
    assert topology.destinations("a_rx") == ("b", "c")
    assert topology.destinations("c_shadow") == ()


def test_receiver_repeat_adds_home():
    topology = three(repeat_receiver_on_network=True)
    assert topology.destinations("a_rx") == ("b", "c", "a")


def test_check_tx_allows_any_network_and_rejects_receive_only():
    topology = three()
    for ingress in ("a", "b", "c", "a_rx"):
        for target in topology.destinations(ingress):
            topology.check_tx(target, ingress)
    with pytest.raises(ValueError, match="receive-only"):
        topology.check_tx("a_rx", "b")
    with pytest.raises(ValueError, match="forbidden for this ingress"):
        topology.check_tx("c", "c_shadow")


def test_relay_destinations_by_mode():
    topology = three()
    assert topology.relay_destinations("b") == ("b",)  # sticky default
    topology.tx_mode = "bridge"
    assert topology.relay_destinations("b") == ("a", "c")
    topology.repeat_on_ingress = True
    assert topology.relay_destinations("b") == ("a", "c", "b")
    topology.tx_mode = "default"
    assert topology.relay_destinations("b") == ("a",)


def test_two_network_bridge_mode_is_unchanged():
    topology = NetworkTopology(
        [
            {"id": "a", "network": "A", "role": "transceiver"},
            {"id": "b", "network": "B", "role": "transceiver"},
        ],
        {"networks": {"A": "a", "B": "b"}},
        "a",
    )
    topology.tx_mode = "bridge"
    assert topology.relay_destinations("a") == ("b",)
    topology.repeat_on_ingress = True
    assert topology.relay_destinations("a") == ("b", "a")


def test_replies_stay_on_request_network():
    topology = three()
    assert topology.reply_destination("c") == "c"
    topology.check_tx("c", reply_to_radio_id="c")
    with pytest.raises(ValueError, match="request's network"):
        topology.check_tx("a", reply_to_radio_id="c")
