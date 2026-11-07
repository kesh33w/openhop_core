"""USB packet metadata must survive the worker-to-event-loop handoff."""

import asyncio
import struct
import threading
from unittest.mock import AsyncMock

import pytest

from openhop_core.hardware.protocol_constants import CMD_RX_PACKET
from openhop_core.hardware.usb_radio import USBLoRaRadio
from openhop_core.node.dispatcher import Dispatcher
from openhop_core.protocol import Packet
from openhop_core.protocol.constants import PAYLOAD_TYPE_ADVERT, ROUTE_TYPE_FLOOD
from openhop_core.rf_fabric import FabricRadio


def _packet(payload, path=b""):
    pkt = Packet()
    pkt.header = (PAYLOAD_TYPE_ADVERT << 2) | ROUTE_TYPE_FLOOD
    pkt.payload = bytearray(payload)
    pkt.payload_len = len(payload)
    pkt.path = bytearray(path)
    pkt.path_len = len(path)
    return pkt.write_to()


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
@pytest.mark.parametrize("pause_raw", [False, True])
async def test_usb_burst_preserves_each_packets_metrics(wrapped, pause_raw):
    physical = USBLoRaRadio(port="unused")
    radio = FabricRadio(radio=physical, radio_id="usb") if wrapped else physical
    dispatcher = Dispatcher(radio)
    raw, parsed = [], []
    complete = asyncio.Event()
    first, second = _packet(b"first"), _packet(b"second")

    async def on_raw(data, rssi, snr):
        raw.append((data, rssi, snr))
        if pause_raw:
            await asyncio.sleep(0)

    def on_packet(pkt, data):
        parsed.append((data, pkt._rssi, pkt._snr, getattr(pkt, "_rx_radio_id", None)))
        if len(parsed) == 2:
            complete.set()

    dispatcher.add_raw_rx_subscriber(on_raw)
    dispatcher.packet_analysis_callback = on_packet
    try:
        _burst((physical, first, -85, -3.0), (physical, second, -44, 11.2))
        # Status getters intentionally keep their last-reception semantics.
        assert physical.get_last_rssi() == -44
        assert physical.get_last_snr() == 11.2
        await asyncio.wait_for(complete.wait(), 1)
        expected = [(first, -85, -3.0), (second, -44, 11.2)]
        assert raw == expected
        assert parsed == [(*rx, "usb" if wrapped else None) for rx in expected]
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_two_usb_radios_keep_metrics_and_duplicate_path_observations():
    local, link = USBLoRaRadio(port="local"), USBLoRaRadio(port="link")
    radio = FabricRadio(radios=[(local, "local"), (link, "link")])
    dispatcher = Dispatcher(radio)
    raw, legacy_raw, parsed, ingresses, handled = [], [], [], [], []
    complete = asyncio.Event()
    same, variant = _packet(b"same", b"\x11"), _packet(b"same", b"\x22\x33")
    other = _packet(b"other")
    expected = [
        (same, -85, -3.0, "local"),
        (variant, -74, 1.5, "link"),
        (other, -44, 11.2, "local"),
        (same, -60, 7.8, "link"),
    ]

    async def on_raw(data, rssi, snr, *, rx_radio_id=None):
        raw.append((data, rssi, snr, rx_radio_id))
        await asyncio.sleep(0)

    def on_packet(pkt, data):
        parsed.append((data, pkt._rssi, pkt._snr, pkt._rx_radio_id))
        if len(parsed) == 4:
            complete.set()

    dispatcher.add_raw_rx_subscriber(on_raw)
    dispatcher.add_raw_rx_subscriber(lambda data, rssi, snr: legacy_raw.append((data, rssi, snr)))
    dispatcher.packet_analysis_callback = on_packet
    dispatcher.register_fallback_handler(lambda pkt: handled.append(bytes(pkt.payload)))
    radio.set_ingress_callback(lambda ingress: ingresses.append(ingress.reception))
    local.send, link.send = AsyncMock(), AsyncMock()
    try:
        _burst(
            *[
                (local if rid == "local" else link, data, rssi, snr)
                for data, rssi, snr, rid in expected
            ]
        )
        await asyncio.wait_for(complete.wait(), 1)
        assert raw == expected
        assert legacy_raw == [rx[:3] for rx in expected]
        assert parsed == expected
        assert [(r.data, r.rssi, r.snr, r.radio_id) for r in ingresses] == expected
        assert handled == [b"same", b"other"]  # Path variants still deduplicate once.
        local.send.assert_not_called()
        link.send.assert_not_called()
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "builtin", "metrics", "varargs", "keyword_only"])
async def test_usb_rx_callback_compatibility(kind):
    radio = USBLoRaRadio(port="unused")
    calls = []

    def legacy(data):
        calls.append(data)

    def metrics(data, rssi=None, snr=None):
        calls.append((data, rssi, snr))

    def varargs(*args):
        calls.append(args)

    def keyword_only(data, *, rssi=None, snr=None):
        calls.append((data, rssi, snr))

    callback = {
        "legacy": legacy,
        "builtin": calls.append,
        "metrics": metrics,
        "varargs": varargs,
        "keyword_only": keyword_only,
    }[kind]
    radio.set_rx_callback(callback)
    _burst((radio, b"payload", -85, -3.0))
    await asyncio.sleep(0)
    if kind in ("metrics", "varargs"):
        assert calls == [(b"payload", -85, -3.0)]
    elif kind == "keyword_only":
        assert calls == [(b"payload", None, None)]
    else:
        assert calls == [b"payload"]


@pytest.mark.asyncio
async def test_usb_callback_typeerror_is_not_retried_as_legacy():
    radio = USBLoRaRadio(port="unused")
    calls, errors = [], []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    failure = TypeError("callback implementation failed")

    def callback(data, rssi=None, snr=None):
        calls.append((data, rssi, snr))
        raise failure

    loop.set_exception_handler(lambda loop, context: errors.append(context["exception"]))
    try:
        radio.set_rx_callback(callback)
        _receive(radio, b"payload", -85, -3.0)
        await asyncio.sleep(0)
        assert calls == [(b"payload", -85, -3.0)]
        assert errors == [failure]
    finally:
        loop.set_exception_handler(previous_handler)


@pytest.mark.asyncio
async def test_usb_queued_rx_keeps_original_callback_and_unregister_drops_new_frames():
    radio = USBLoRaRadio(port="unused")
    original, replacement = [], []
    radio.set_rx_callback(lambda data, rssi, snr: original.append((data, rssi, snr)))
    _receive(radio, b"original", -85, -3.0)
    radio.set_rx_callback(lambda data, rssi, snr: replacement.append((data, rssi, snr)))
    _receive(radio, b"replacement", -44, 11.2)
    radio.set_rx_callback(None)
    _receive(radio, b"unregistered", -60, 0.0)
    await asyncio.sleep(0)
    assert original == [(b"original", -85, -3.0)]
    assert replacement == [(b"replacement", -44, 11.2)]
    assert radio._rx_count == 3


def test_usb_rx_without_running_loop_still_drops(caplog):
    radio = USBLoRaRadio(port="unused")
    calls = []
    radio.set_rx_callback(calls.append)
    _receive(radio, b"payload", -85, -3.0)
    assert calls == []
    assert "event loop not running" in caplog.text
