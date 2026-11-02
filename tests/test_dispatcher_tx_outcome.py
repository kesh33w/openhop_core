from unittest.mock import AsyncMock

import pytest

from openhop_core.node.dispatcher import Dispatcher
from openhop_core.protocol import Packet
from openhop_core.rf_fabric import FabricRadio


class Radio:
    def __init__(self, result=None, error=None):
        self.send = AsyncMock(return_value=result, side_effect=error)

    def set_rx_callback(self, callback):
        pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result,uncertain",
    [
        (False, False),
        (None, True),
        (0, True),
        (1, True),
        ("queued", True),
        ([], True),
        ({"ok": False}, False),
        ({"ok": True, "tx_outcome_uncertain": True}, True),
        ({"airtime_ms": 10, "tx_outcome_uncertain": True}, True),
    ],
)
async def test_failed_send_never_reports_success(result, uncertain):
    radio = Radio(result)
    dispatcher = Dispatcher(radio=FabricRadio(radios=[(radio, "test")]))
    dispatcher.packet_sent_callback = AsyncMock()
    packet = Packet()
    packet.header = 1 | (5 << 2)
    packet.payload = bytearray(b"test")
    packet.payload_len = 4
    assert not await dispatcher.send_packet(packet, wait_for_ack=False, radio_id="test")
    assert packet._tx_metadata["tx_outcome_uncertain"] is uncertain
    assert radio.send.await_count == 1
    dispatcher.packet_sent_callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_internal_typeerror_does_not_retry_on_default_radio():
    default = Radio({"ok": True})
    broken = Radio(error=TypeError("inside driver after write"))
    dispatcher = Dispatcher(
        radio=FabricRadio(radios=[(default, "default"), (broken, "target")])
    )
    packet = Packet()
    packet.header = 1 | (5 << 2)
    packet.payload = bytearray(b"test")
    packet.payload_len = 4
    assert not await dispatcher.send_packet(
        packet, wait_for_ack=False, radio_id="target"
    )
    assert packet._tx_metadata["tx_outcome_uncertain"]
    assert broken.send.await_count == 1
    assert default.send.await_count == 0


@pytest.mark.asyncio
async def test_uncertain_metadata_does_not_debit_confirmed_airtime():
    from unittest.mock import Mock

    radio = Radio({"ok": True, "airtime_ms": 123, "tx_outcome_uncertain": True})
    dispatcher = Dispatcher(radio=FabricRadio(radios=[(radio, "test")]))
    dispatcher._client_repeat_enabled = True
    dispatcher._debit_tx_budget = Mock()
    dispatcher.packet_sent_callback = AsyncMock()
    packet = Packet()
    packet.header = 1 | (5 << 2)
    packet.payload = bytearray(b"test")
    packet.payload_len = 4
    result = await dispatcher._transmit_locked(packet, False, None, radio_id="test")
    assert result == (False, None, None)
    dispatcher._debit_tx_budget.assert_not_called()
    dispatcher.packet_sent_callback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [True, {}, {"airtime_ms": 12.0}, {"ok": True}])
async def test_supported_legacy_success_shapes_remain_accepted(result):
    radio = Radio(result)
    dispatcher = Dispatcher(radio=FabricRadio(radios=[(radio, "test")]))
    dispatcher.packet_sent_callback = AsyncMock()
    packet = Packet()
    packet.header = 1 | (5 << 2)
    packet.payload = bytearray(b"test")
    packet.payload_len = 4
    assert await dispatcher.send_packet(packet, wait_for_ack=False, radio_id="test")
    dispatcher.packet_sent_callback.assert_awaited_once()
