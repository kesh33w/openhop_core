"""Real driver success metadata must authorize the next ordered leg."""

import time
from unittest.mock import AsyncMock
import pytest
from openhop_core.hardware.usb_radio import USBLoRaRadio
from openhop_core.hardware.tcp_radio import TCPLoRaRadio
from openhop_core.hardware.protocol_constants import CMD_TX_REQUEST, CMD_TX_DONE
from openhop_core.rf_fabric.ordered_tx import OrderedTxContext


@pytest.fixture(params=["usb", "tcp"])
def radio(request):
    r = (
        USBLoRaRadio(port="/dev/null")
        if request.param == "usb"
        else TCPLoRaRadio(host="modem.invalid")
    )
    r._initialized = True
    r.lbt_enabled = False
    return r


async def test_tx_done_allows_next_leg(radio):
    radio._send_command = AsyncMock(return_value=b"\x00" * 4)
    ctx = OrderedTxContext("first", ("first", "second"), time.monotonic() + 30)
    ctx.begin("first", b"packet")
    result = await radio.send(b"packet")
    assert result["ok"] is True
    assert abs(result["tx_completed_at"] - time.time()) < 2
    first = radio._send_command.await_args_list[0]
    assert first.args[0] == CMD_TX_REQUEST
    assert first.kwargs["expect_cmd"] == CMD_TX_DONE
    ctx.finish("first", result)
    ctx.begin("second", b"packet")
    assert not ctx.aborted


async def test_missing_tx_done_still_aborts(radio):
    radio._send_command = AsyncMock(return_value=None)
    ctx = OrderedTxContext("first", ("first", "second"), time.monotonic() + 30)
    ctx.begin("first", b"packet")
    result = await radio.send(b"packet")
    ctx.finish("first", result)
    assert ctx.aborted
    with pytest.raises(ValueError, match="uncertain"):
        ctx.begin("second", b"packet")
