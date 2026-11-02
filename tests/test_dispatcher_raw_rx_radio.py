import pytest
from openhop_core.node.dispatcher import Dispatcher


@pytest.mark.asyncio
async def test_raw_subscribers_receive_captured_radio_without_breaking_legacy_callbacks():
    dispatcher = Dispatcher(radio=None)
    calls = []

    async def aware(data, rssi, snr, *, rx_radio_id=None):
        calls.append(("aware", rx_radio_id, data))

    def legacy(data, rssi, snr):
        calls.append(("legacy", data))

    dispatcher.add_raw_rx_subscriber(aware)
    dispatcher.add_raw_rx_subscriber(legacy)
    await dispatcher._process_received_packet(b"", -90, 7.0, rx_radio_id="side-b")
    assert calls == [("aware", "side-b", b""), ("legacy", b"")]
