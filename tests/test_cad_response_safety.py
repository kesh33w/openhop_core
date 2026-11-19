"""CAD must be observed, not inferred from transport failure."""

from unittest.mock import AsyncMock, patch
import pytest
from openhop_core.hardware.usb_radio import USBLoRaRadio
from openhop_core.hardware.tcp_radio import TCPLoRaRadio
from openhop_core.hardware.protocol_constants import CMD_TX_REQUEST, ERR_CAD_FAILED


@pytest.fixture(params=["usb", "tcp"])
def radio(request):
    r = (
        USBLoRaRadio(port="/dev/null")
        if request.param == "usb"
        else TCPLoRaRadio(host="modem.invalid")
    )
    r._initialized = True
    return r


# A failed channel check behaves like CAD off for that one packet (upstream
# behaviour): the packet is still transmitted rather than silently dropped.


def _tx_requests(radio):
    return [c for c in radio._send_command.await_args_list if c.args[0] == CMD_TX_REQUEST]


@pytest.mark.parametrize("response", [None, b"", b"\x02", b"\x00\x01"])
async def test_unknown_cad_still_sends(radio, response):
    async def command(cmd, *args, **kwargs):
        radio._last_command_unsent = False
        return b"\x00" * 4 if cmd == CMD_TX_REQUEST else response

    radio._send_command = AsyncMock(side_effect=command)
    result = await radio.send(b"test")
    assert len(_tx_requests(radio)) == 1
    assert result.get("ok", True) is not False


async def test_explicit_cad_error_still_sends(radio):
    async def command(cmd, *args, **kwargs):
        radio._last_command_unsent = False
        if cmd == CMD_TX_REQUEST:
            return b"\x00" * 4
        radio._last_modem_error = ERR_CAD_FAILED
        return None

    radio._send_command = AsyncMock(side_effect=command)
    await radio.send(b"test")
    assert len(_tx_requests(radio)) == 1


async def test_cad_exception_still_sends(radio):
    radio._perform_cad = AsyncMock(side_effect=RuntimeError("transport failed"))
    radio._send_command = AsyncMock(return_value=b"\x00" * 4)
    await radio.send(b"test")
    assert len(_tx_requests(radio)) == 1


async def test_unsubmitted_tx_is_definitely_unsent(radio):
    radio.lbt_enabled = False

    async def command(cmd, *args, **kwargs):
        radio._last_command_unsent = True  # e.g. link down before the write
        return None

    radio._send_command = AsyncMock(side_effect=command)
    result = await radio.send(b"test")
    assert result["ok"] is False and result["tx_outcome_uncertain"] is False
    assert result["reason"] == "not_submitted"


async def test_submitted_tx_without_done_stays_uncertain(radio):
    radio.lbt_enabled = False

    async def command(cmd, *args, **kwargs):
        radio._last_command_unsent = False
        return None

    radio._send_command = AsyncMock(side_effect=command)
    assert await radio.send(b"test") is None


async def test_uninitialized_radio_is_definitely_unsent(radio):
    radio._initialized = False
    result = await radio.send(b"test")
    assert result["ok"] is False and result["tx_outcome_uncertain"] is False


async def test_usb_reconnects_only_after_repeated_cad_timeouts():
    radio = USBLoRaRadio(port="/dev/null")
    radio._invalidate_connection = lambda *a, **k: calls.append(a)
    calls = []

    async def timeout(*args, **kwargs):
        assert kwargs.get("invalidate_on_timeout") is False
        radio._last_command_unsent = False
        return None

    radio._send_command = timeout
    for expected in (0, 0, 1):
        with pytest.raises(RuntimeError):
            await radio._perform_cad()
        assert len(calls) == expected


@pytest.mark.parametrize("payload,busy", [(b"\x00", False), (b"\x01", True)])
async def test_cad_valid_response(radio, payload, busy):
    radio._send_command = AsyncMock(return_value=payload)
    assert await radio._perform_cad() is busy


async def test_partial_final_budget_does_not_send_cad(radio):
    # First CAD reports busy at t=3.8; only .2s left of 4s budget.
    radio._perform_cad = AsyncMock(return_value=True)
    radio._send_command = AsyncMock(return_value=b"\x00" * 4)
    radio._lbt_retry_delay_ms = lambda deadline: 0
    with patch.object(
        __import__(radio.__module__, fromlist=["time"]).time,
        "monotonic",
        side_effect=[0, 0, 3.8, 3.8, 4.1],
    ):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await radio.send(b"test")
    radio._perform_cad.assert_awaited_once_with(timeout=1.0)


def test_radios_report_connection_for_fabric_guard():
    usb = USBLoRaRadio(port="/dev/null")
    tcp = TCPLoRaRadio(host="modem.invalid")
    assert usb.is_connected is False and tcp.is_connected is False
    usb._connected_event.set()
    assert usb.is_connected is True
