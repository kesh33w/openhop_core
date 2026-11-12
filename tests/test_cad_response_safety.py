"""CAD must be observed, not inferred from transport failure."""
from unittest.mock import AsyncMock, patch
import pytest
from openhop_core.hardware.usb_radio import USBLoRaRadio
from openhop_core.hardware.tcp_radio import TCPLoRaRadio
from openhop_core.hardware.protocol_constants import CMD_TX_REQUEST, ERR_CAD_FAILED

@pytest.fixture(params=['usb', 'tcp'])
def radio(request):
    r = USBLoRaRadio(port='/dev/null') if request.param == 'usb' else TCPLoRaRadio(host='modem.invalid')
    r._initialized = True
    return r

@pytest.mark.parametrize('response', [None, b'', b'\x02', b'\x00\x01'])
async def test_unknown_cad_never_sends(radio, response):
    radio._send_command = AsyncMock(return_value=response)
    result = await radio.send(b'test')
    assert result['ok'] is False
    assert result['reason'] == 'cad_unavailable'
    assert result['tx_outcome_uncertain'] is False
    assert all(c.args[0] != CMD_TX_REQUEST for c in radio._send_command.await_args_list)

async def test_explicit_cad_error_never_sends(radio):
    async def failed(*args, **kwargs):
        radio._last_modem_error = ERR_CAD_FAILED
        return None
    radio._send_command = AsyncMock(side_effect=failed)
    result = await radio.send(b'test')
    assert result['reason'] == 'cad_unavailable'
    assert radio._send_command.await_count == 1

async def test_cad_exception_never_sends(radio):
    radio._perform_cad = AsyncMock(side_effect=RuntimeError('transport failed'))
    radio._send_command = AsyncMock()
    result = await radio.send(b'test')
    assert result['reason'] == 'cad_unavailable'
    radio._send_command.assert_not_called()

@pytest.mark.parametrize('payload,busy', [(b'\x00', False), (b'\x01', True)])
async def test_cad_valid_response(radio, payload, busy):
    radio._send_command = AsyncMock(return_value=payload)
    assert await radio._perform_cad() is busy

async def test_partial_final_budget_does_not_send_cad(radio):
    # First CAD reports busy at t=3.8; only .2s left of 4s budget.
    radio._perform_cad = AsyncMock(return_value=True)
    radio._send_command = AsyncMock(return_value=b'\x00' * 4)
    radio._lbt_retry_delay_ms = lambda deadline: 0
    with patch.object(__import__(radio.__module__, fromlist=['time']).time, 'monotonic', side_effect=[0,0,3.8,3.8,4.1]):
        with patch('asyncio.sleep', new_callable=AsyncMock):
            await radio.send(b'test')
    radio._perform_cad.assert_awaited_once_with(timeout=1.0)
