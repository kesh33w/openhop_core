"""TCP modem packets must carry their own RSSI/SNR to callbacks and observers."""

import asyncio
import struct
import threading

import pytest

from openhop_core.hardware.protocol_constants import CMD_RX_PACKET
from openhop_core.hardware.tcp_radio import TCPLoRaRadio
from openhop_core.node.dispatcher import Dispatcher
from openhop_core.rf_fabric import FabricRadio
from tests.test_usb_rx_metrics import _packet


def _receive(radio, data, rssi, snr):
    radio._dispatch_frame(CMD_RX_PACKET, struct.pack("<hhh", rssi, round(snr * 10), rssi) + data)


def _burst(*receptions):
    """Queue the whole burst on a real worker before the loop can run callbacks."""
    worker = threading.Thread(target=lambda: [_receive(*rx) for rx in receptions])
    worker.start()
    worker.join(timeout=1)
    assert not worker.is_alive()


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
async def test_tcp_burst_preserves_each_packets_metrics(wrapped):
    physical = TCPLoRaRadio(host="modem.invalid")
    radio = FabricRadio(radio=physical, radio_id="g3") if wrapped else physical
    dispatcher = Dispatcher(radio)
    raw, parsed = [], []
    complete = asyncio.Event()
    first, second = _packet(b"first"), _packet(b"second")

    async def on_raw(data, rssi, snr):
        raw.append((data, rssi, snr))

    def on_packet(pkt, data):
        parsed.append((data, pkt._rssi, pkt._snr))
        if len(parsed) == 2:
            complete.set()

    dispatcher.add_raw_rx_subscriber(on_raw)
    dispatcher.packet_analysis_callback = on_packet
    try:
        _burst((physical, first, -85, -3.0), (physical, second, -44, 11.2))
        assert physical.get_last_rssi() == -44
        await asyncio.wait_for(complete.wait(), 1)
        expected = [(first, -85, -3.0), (second, -44, 11.2)]
        assert raw == expected
        assert parsed == expected
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_tcp_metrics_reach_fabric_observers():
    """MQTT observers read this edge; a data-only delivery published null RSSI."""
    physical = TCPLoRaRadio(host="modem.invalid")
    radio = FabricRadio(radio=physical, radio_id="g3")
    seen = []
    radio.fabric.add_observation_callback(
        lambda data, rssi, snr, **kw: seen.append((data, rssi, snr, kw.get("rx_radio_id")))
    )
    radio.fabric.set_legacy_rx_callback(lambda *a, **kw: None)
    physical._event_loop = asyncio.get_running_loop()
    data = _packet(b"observed")
    _burst((physical, data, -97, 6.5))
    for _ in range(20):
        if seen:
            break
        await asyncio.sleep(0.01)
    assert seen == [(data, -97, 6.5, "g3")]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "metrics"])
async def test_tcp_rx_callback_compatibility(kind):
    radio = TCPLoRaRadio(host="modem.invalid")
    radio._event_loop = asyncio.get_running_loop()
    got = []
    if kind == "legacy":
        radio.set_rx_callback(lambda data: got.append((data,)))
    else:
        radio.set_rx_callback(lambda data, rssi, snr: got.append((data, rssi, snr)))
    data = _packet(b"compat")
    _burst((radio, data, -60, 2.5))
    for _ in range(20):
        if got:
            break
        await asyncio.sleep(0.01)
    assert got == ([(data,)] if kind == "legacy" else [(data, -60, 2.5)])


def test_tcp_callback_typeerror_is_not_retried_as_legacy():
    radio = TCPLoRaRadio(host="modem.invalid")
    calls = []

    def callback(data, rssi, snr):
        calls.append(data)
        raise TypeError("inside callback")

    radio.set_rx_callback(callback)
    radio._event_loop = None
    _receive(radio, _packet(b"once"), -70, 1.0)
    assert len(calls) == 1
