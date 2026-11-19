import asyncio
import struct
from unittest.mock import AsyncMock, Mock
import pytest
from openhop_core.hardware.usb_radio import USBLoRaRadio
from openhop_core.hardware.tcp_radio import TCPLoRaRadio
from openhop_core.hardware.protocol_constants import CMD_TX_REQUEST


@pytest.fixture(params=[USBLoRaRadio, TCPLoRaRadio])
def radio(request):
    r = request.param() if request.param is USBLoRaRadio else request.param(host="modem.invalid")
    r._initialized = True
    return r


def test_default_does_not_change_firmware(radio):
    radio._write_cad_mode_sync = Mock()
    assert radio._restore_cad_mode_sync()
    radio._write_cad_mode_sync.assert_not_called()
    assert radio.lbt_enabled is True


@pytest.mark.parametrize("reply", [None, b"", b"\x01", b"\x00\x00"])
def test_rejected_mode_preserves_host(radio, reply):
    radio._send_command = AsyncMock(return_value=reply)
    with pytest.raises(RuntimeError):
        asyncio.run(radio.set_cad_enabled(False))
    assert radio.lbt_enabled is True
    assert radio._cad_mode_explicit is None


@pytest.mark.parametrize("enabled", [False, True])
def test_mode_controls_actual_send_cad(radio, enabled):
    async def command(cmd, payload, **kwargs):
        if cmd == 0x4A:
            assert payload == b"\x00"
            return b"\x00"
        if cmd == CMD_TX_REQUEST:
            return struct.pack("<I", 1000)
        return b""

    radio._send_command = AsyncMock(side_effect=command)
    radio._perform_cad = AsyncMock(return_value=False)

    async def run():
        await radio.set_cad_enabled(enabled)
        result = await radio.send(b"test")
        assert result["airtime_ms"] == 1.0

    asyncio.run(run())
    assert radio._perform_cad.await_count == int(enabled)
    assert radio._cad_mode_explicit is enabled


def test_enable_restores_host_checks(radio):
    radio._send_command = AsyncMock(return_value=b"\x00")
    asyncio.run(radio.set_cad_enabled(True))
    assert radio.lbt_enabled is True
    assert radio._cad_mode_explicit is True
    radio._send_command.assert_awaited_once_with(0x4A, b"\x00", expect_cmd=0x4B, timeout=3.0)


def test_explicit_mode_requires_reconnect_ack(radio):
    radio._cad_mode_explicit = False
    radio._write_cad_mode_sync = Mock(return_value=False)
    assert radio._restore_cad_mode_sync() is False
    radio._write_cad_mode_sync.return_value = True
    assert radio._restore_cad_mode_sync() is True
    assert radio.lbt_enabled is False


@pytest.mark.parametrize("value", ["false", 0, 1])
def test_boolean_only(radio, value):
    with pytest.raises(ValueError):
        asyncio.run(radio.set_cad_enabled(value))


@pytest.mark.parametrize("kind", [USBLoRaRadio, TCPLoRaRadio])
def test_persisted_off_initializes_and_restores(kind):
    kwargs = {"host": "modem.invalid"} if kind is TCPLoRaRadio else {}
    radio = kind(cad_enabled=False, **kwargs)
    assert radio.lbt_enabled is False
    radio._write_cad_mode_sync = Mock(return_value=True)
    assert radio._restore_cad_mode_sync()
    radio._write_cad_mode_sync.assert_called_once()
