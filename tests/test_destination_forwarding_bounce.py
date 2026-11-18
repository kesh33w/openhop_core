"""A packet crossed into a network must not be forwarded back when re-flooded there."""
import asyncio

import pytest

from openhop_core.node.dispatcher import Dispatcher
from openhop_core.protocol import Packet
from openhop_core.protocol.constants import PAYLOAD_TYPE_GRP_TXT, ROUTE_TYPE_FLOOD
from openhop_core.protocol.identity import LocalIdentity
from openhop_core.rf_fabric import FabricRadio, RFFabric
from openhop_core.rf_fabric.topology import NetworkTopology


class _Radio:
    def __init__(self):
        self.rx_callback = None
        self.sent = []

    def set_rx_callback(self, cb):
        self.rx_callback = cb

    async def send(self, data):
        self.sent.append(data)
        return {"ok": True, "airtime_ms": 1.0}

    def get_last_rssi(self):
        return -80

    def get_last_snr(self):
        return 5.0


def _rig(repeat_on_network=False):
    radios = {k: _Radio() for k in ("atx", "arx", "btx", "brx")}
    fabric = RFFabric()
    for rid, radio in radios.items():
        fabric.register_radio(radio, radio_id=rid)
    fabric.configure_topology(NetworkTopology(
        [{"id": "atx", "network": "A", "role": "transceiver"},
         {"id": "arx", "network": "A", "role": "receiver"},
         {"id": "btx", "network": "B", "role": "transceiver"},
         {"id": "brx", "network": "B", "role": "receiver"}],
        {"networks": {"A": "atx", "B": "btx"}, "enable_receiver_forwarding": True,
         "repeat_receiver_on_network": repeat_on_network},
        "atx",
    ))
    fabric.set_default_radio("atx")
    dispatcher = Dispatcher(FabricRadio(fabric))
    dispatcher.local_identity = LocalIdentity()
    dispatcher.destination_forwarding = True
    dispatcher.set_client_repeat_enabled(True)
    return radios


async def _settle():
    for _ in range(30):
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
@pytest.mark.parametrize("repeat_on_network", [False, True])
async def test_reflooded_crossing_is_not_bounced_back(repeat_on_network):
    radios = _rig(repeat_on_network)
    pkt = Packet()
    pkt.header = (PAYLOAD_TYPE_GRP_TXT << 2) | ROUTE_TYPE_FLOOD
    pkt.payload = bytearray(b"\x11" + b"x" * 30)
    pkt.payload_len = 31
    pkt.path = bytearray(b"\x01")
    pkt.path_len = 1

    radios["brx"].rx_callback(pkt.write_to(), -90, 3.0)
    await _settle()
    assert len(radios["atx"].sent) == 1
    b_sends = len(radios["btx"].sent)
    assert b_sends <= 1  # only a same-network repeat may go back out on B

    # A neighbour on network A re-floods our crossing with one more hop.
    echo = Packet()
    echo.read_from(radios["atx"].sent[-1])
    echo.path = bytearray(bytes(echo.path) + b"\x77")
    echo.path_len = len(echo.path)
    radios["arx"].rx_callback(echo.write_to(), -90, 3.0)
    await _settle()
    assert len(radios["btx"].sent) == b_sends
    assert len(radios["atx"].sent) == 1
