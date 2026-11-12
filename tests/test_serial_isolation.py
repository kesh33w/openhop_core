"""Exercise actual per-radio threads, generation fencing and uncertain TX."""

import asyncio
import struct
import threading
import time
from unittest.mock import Mock

import pytest
import serial

from openhop_core.hardware.usb_radio import USBLoRaRadio
from openhop_core.hardware.protocol_constants import (
    CMD_PING,
    CMD_PONG,
    CMD_TX_REQUEST,
    CMD_TX_DONE,
    CMD_ERROR,
    CMD_TX_FAIL,
    CMD_RX_START,
    CMD_RX_STARTED,
    CMD_RX_PACKET,
    CMD_CONFIG_RESP,
    CMD_SET_CONFIG,
    CMD_SET_CAD_PARAMS,
    CMD_CAD_PARAMS_RESP,
    ERR_CHANNEL_BUSY,
    build_frame,
)


class FakeSerial:
    def __init__(self, radio, gate=None, replies=True, result=None):
        self.radio, self.gate, self.replies, self.result = radio, gate, replies, result
        self.is_open = True
        self.port = radio.port
        self.timeout = 0.1
        self.writes = []
        self.started = threading.Event()
        self.closed = threading.Event()
        self.cancelled = threading.Event()
        self.rx = bytearray()
        self.max_active = self.active = 0
        self.flush_calls = 0

    def write(self, frame):
        assert self.is_open
        self.writes.append(frame)
        self.started.set()
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.gate is not None:
                assert self.gate.wait(3), "test forgot to release writer"
            if isinstance(self.result, Exception):
                raise self.result
            if self.replies:
                if frame[1] == CMD_TX_REQUEST:
                    self.radio._dispatch_frame(CMD_TX_DONE, struct.pack("<I", 1500))
                elif frame[1] == CMD_RX_START:
                    self.radio._dispatch_frame(CMD_RX_STARTED, b"")
                else:
                    self.radio._dispatch_frame(CMD_PONG, b"pong")
            return len(frame) if self.result is None else self.result
        finally:
            self.active -= 1

    def flush(self):
        self.flush_calls += 1
        raise AssertionError("unbounded flush must never be called")

    def cancel_write(self):
        self.cancelled.set()  # Deliberately fails to unblock wedged driver.

    def close(self):
        assert self.active == 0, "close overlapped an in-flight write"
        self.is_open = False
        self.closed.set()

    def reset_input_buffer(self):
        self.rx.clear()

    @property
    def in_waiting(self):
        return len(self.rx)

    def read(self, count):
        result = bytes(self.rx[:count])
        del self.rx[:count]
        if not result:
            time.sleep(0.001)
        return result


def connected(name="fake", **kwargs):
    radio = USBLoRaRadio(name, lbt_enabled=False)
    radio._initialized = True
    radio._connected_event.set()
    radio._event_loop = asyncio.get_running_loop()
    radio._serial = FakeSerial(radio, **kwargs)
    radio._write_timeout = 0.15
    return radio


async def until(predicate, timeout=1):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition timed out"
        await asyncio.sleep(0.002)


async def stop(*radios):
    for r in radios:
        r.cleanup()
    for r in radios:
        await until(lambda: not any(t and t.is_alive() for t in (r._writer_thread, r._rx_thread)))


@pytest.mark.asyncio
async def test_stuck_one_radio_does_not_block_loop_other_rx_or_tx():
    gate = threading.Event()
    bad, good = connected("bad", gate=gate), connected("good")
    received = []
    good.set_rx_callback(lambda data, rssi, snr: received.append((data, rssi, snr)))
    task = asyncio.create_task(bad.send(b"uncertain"))
    try:
        await until(bad._serial.started.is_set)
        beats = []

        async def heartbeat():
            for _ in range(10):
                beats.append(time.monotonic())
                await asyncio.sleep(0.005)

        good._dispatch_frame(CMD_RX_PACKET, struct.pack("<hhh", -70, 20, -71) + b"incoming")
        tx, _ = await asyncio.wait_for(asyncio.gather(good.send(b"healthy"), heartbeat()), 0.25)
        assert tx["airtime_ms"] == 1.5
        assert received == [(b"incoming", -70, 2.0)]
        assert len(beats) == 10
        assert max(b - a for a, b in zip(beats, beats[1:])) < 0.05
        assert await asyncio.wait_for(task, 0.3) is None
        assert bad.get_status()["serial_io"]["write_timeouts"] == 1
        assert len(bad._serial.writes) == 1  # No uncertain replay or RX write.
        assert good._serial.flush_calls == 0
    finally:
        gate.set()
        await stop(bad, good)


@pytest.mark.asyncio
async def test_cancel_does_not_allow_next_write_or_reconnect_until_owner_finishes():
    gate = threading.Event()
    r = connected(gate=gate)
    old = r._serial
    task = asyncio.create_task(r._send_command(CMD_PING, b"old", CMD_PONG))
    await until(old.started.is_set)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await r._send_command(CMD_PING, b"next", CMD_PONG) is None
    fresh = FakeSerial(r)
    r._open_serial_sync = Mock(return_value=fresh)
    r._apply_config_sync = Mock(return_value=True)
    recovered = threading.Event()
    worker = threading.Thread(
        target=lambda: (r._reconnect_serial_sync(), recovered.set()), daemon=True
    )
    worker.start()
    try:
        await until(old.cancelled.is_set)
        await asyncio.sleep(0.04)
        assert not recovered.is_set()
        assert not old.closed.is_set()
        r._open_serial_sync.assert_not_called()
        gate.set()
        await until(recovered.is_set)
        assert old.closed.is_set()
        assert len(old.writes) == 1
        assert await r._send_command(CMD_PING, b"fresh", CMD_PONG) == b"pong"
        assert len(fresh.writes) == 1
    finally:
        gate.set()
        await stop(r)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result", [0, 2, serial.SerialTimeoutException("stalled"), OSError("gone")]
)
async def test_short_write_and_error_retire_session_without_replaying_tx(result):
    r = connected(result=result)
    try:
        assert await r.send(b"test") is None
        assert r._reconnect_requested.is_set()
        assert r._io_stats["write_errors"] == 1
        assert len(r._serial.writes) == 1
        assert not r._response_events and not r._response_data
    finally:
        await stop(r)


@pytest.mark.asyncio
async def test_response_timeout_and_old_session_packets_cannot_satisfy_new_request():
    r = connected(replies=False)
    old_generation = r._generation
    received = []
    r.set_rx_callback(received.append)
    try:
        assert await r._send_command(CMD_PING, b"", CMD_PONG, timeout=0.02) is None
        assert r._io_stats["response_timeouts"] == 1
        assert await r._send_command(CMD_PING, b"", CMD_PONG) is None
        r._serial = FakeSerial(r, replies=False)
        r._reconnect_requested.clear()
        r._connected_event.set()
        t = asyncio.create_task(r._send_command(CMD_PING, b"new", CMD_PONG))
        await until(r._serial.started.is_set)
        r._dispatch_frame(CMD_PONG, b"stale", generation=old_generation)
        r._dispatch_frame(
            CMD_RX_PACKET, struct.pack("<hhh", -1, 0, -1) + b"stale", generation=old_generation
        )
        await asyncio.sleep(0.01)
        assert not t.done() and not received
        r._dispatch_frame(CMD_PONG, b"new", generation=r._generation)
        assert await t == b"new"
    finally:
        await stop(r)


@pytest.mark.asyncio
async def test_terminal_reply_first_wins_even_before_write_returns():
    r = connected(replies=False)

    def write(frame):
        if frame[1] == CMD_TX_REQUEST:
            r._dispatch_frame(CMD_TX_DONE, struct.pack("<I", 1000))
            r._dispatch_frame(CMD_ERROR, bytes([ERR_CHANNEL_BUSY]))
            r._dispatch_frame(CMD_TX_FAIL, b"")
            time.sleep(0.02)
        else:
            r._dispatch_frame(CMD_RX_STARTED, b"")
        return len(frame)

    r._serial.write = Mock(side_effect=write)
    try:
        result = await r.send(b"once")
        assert result["airtime_ms"] == 1
        assert r._tx_count == 1
        assert [c.args[0][1] for c in r._serial.write.call_args_list] == [
            CMD_TX_REQUEST,
            CMD_RX_START,
        ]
    finally:
        await stop(r)


@pytest.mark.asyncio
async def test_command_order_admission_bound_and_queue_wait_timeout():
    gate = threading.Event()
    r = connected(gate=gate)
    r._max_pending_commands = 3
    r._write_timeout = 1
    r._queue_timeout = 0.03
    first = asyncio.create_task(r._send_command(CMD_PING, b"1", CMD_PONG))
    await until(r._serial.started.is_set)
    second = asyncio.create_task(r._send_command(CMD_PING, b"2", CMD_PONG))
    third = asyncio.create_task(r._send_command(CMD_PING, b"3", CMD_PONG))
    await asyncio.sleep(0.005)
    try:
        assert await r._send_command(CMD_PING, b"4", CMD_PONG) is None
        assert await second is None
        assert await third is None
        assert r._io_stats["queue_rejections"] == 3
        gate.set()
        assert await first == b"pong"
        assert r._pending_commands == 0
        assert len(r._serial.writes) == 1
        assert r._serial.max_active == 1
    finally:
        gate.set()
        await stop(r)


@pytest.mark.asyncio
async def test_queued_cancel_does_not_retire_active_command():
    gate = threading.Event()
    r = connected(gate=gate)
    r._write_timeout = 1
    first = asyncio.create_task(r._send_command(CMD_PING, b"1", CMD_PONG))
    await until(r._serial.started.is_set)
    second = asyncio.create_task(r._send_command(CMD_PING, b"2", CMD_PONG))
    await asyncio.sleep(0.005)
    second.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await second
        assert r._connected_event.is_set()
        gate.set()
        assert await first == b"pong"
    finally:
        gate.set()
        await stop(r)


@pytest.mark.asyncio
async def test_cleanup_returns_promptly_while_write_is_stuck_then_closes():
    gate = threading.Event()
    r = connected(gate=gate)
    handle = r._serial
    task = asyncio.create_task(r._send_command(CMD_PING, b"", CMD_PONG))
    await until(handle.started.is_set)
    start = time.monotonic()
    r.cleanup()
    assert time.monotonic() - start < 0.03
    assert await task is None
    assert not handle.closed.is_set()
    assert not r.begin()  # No duplicate writer ownership during cleanup.
    gate.set()
    await until(handle.closed.is_set)
    await stop(r)


@pytest.mark.asyncio
async def test_reconnect_configuration_barrier_consumes_late_tx_and_restores_settings():
    r = connected()
    fresh = FakeSerial(r, replies=False)
    r.set_custom_cad_thresholds = Mock()  # Only set cache, no live command.
    r._custom_cad_peak, r._custom_cad_min, r._custom_cad_symbol_num = 23, 11, 8

    def write(frame):
        fresh.writes.append(frame)
        if frame[1] == CMD_SET_CONFIG:
            fresh.rx.extend(build_frame(CMD_TX_DONE, b"old") + build_frame(CMD_CONFIG_RESP, b"ok"))
        elif frame[1] == CMD_SET_CAD_PARAMS:
            fresh.rx.extend(build_frame(CMD_CAD_PARAMS_RESP, b"ok"))
        return len(frame)

    fresh.write = write
    r._open_serial_sync = Mock(return_value=fresh)
    r._invalidate_connection("test")
    recovered = threading.Event()
    t = threading.Thread(target=lambda: (r._reconnect_serial_sync(), recovered.set()), daemon=True)
    t.start()
    try:
        await until(recovered.is_set)
        assert r._connected_event.is_set()
        assert [f[1] for f in fresh.writes] == [CMD_SET_CONFIG, CMD_SET_CAD_PARAMS]
        assert fresh.writes[-1][4:8] == bytes([3, 23, 11, 0])
        assert not fresh.rx
        assert r._io_stats["reconnects"] == 1
    finally:
        await stop(r)


@pytest.mark.asyncio
async def test_posix_serial_reader_writer_and_reconnect_over_real_ptys():
    """Real pyserial + OS PTYs, including unplug/reopen and config barrier."""
    import os
    import pty
    import select
    from openhop_core.hardware.protocol_constants import PROTO_SYNC

    resources = []
    responders = []
    stop_emulators = threading.Event()

    def modem(label):
        master, slave = pty.openpty()
        resources.extend([master, slave])
        frames = []
        local_stop = threading.Event()

        def serve():
            buf = bytearray()
            while not stop_emulators.is_set() and not local_stop.is_set():
                try:
                    if not select.select([master], [], [], 0.02)[0]:
                        continue
                    buf.extend(os.read(master, 4096))
                    while len(buf) >= 6:
                        if buf[0] != PROTO_SYNC:
                            del buf[0]
                            continue
                        size = 6 + int.from_bytes(buf[2:4], "little")
                        if len(buf) < size:
                            break
                        frame = bytes(buf[:size])
                        del buf[:size]
                        frames.append(frame)
                        if frame[1] == CMD_SET_CONFIG:
                            response = build_frame(CMD_PONG, b"old") + build_frame(
                                CMD_CONFIG_RESP, b"ok"
                            )
                        elif frame[1] == CMD_SET_CAD_PARAMS:
                            response = build_frame(CMD_CAD_PARAMS_RESP, b"ok")
                        else:
                            response = build_frame(CMD_PONG, label)
                        os.write(master, response)
                except OSError:
                    return

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        responders.append(thread)
        return master, os.ttyname(slave), frames, local_stop, thread

    old_master, old_port, _, old_stop, old_thread = modem(b"old-modem")
    _, new_port, frames, _, _ = modem(b"new-modem")
    r = USBLoRaRadio(old_port)
    r._serial = r._open_serial_sync()
    r._initialized = True
    r._event_loop = asyncio.get_running_loop()
    r._connected_event.set()
    r._rx_thread = threading.Thread(target=r._rx_worker, daemon=True)
    r._rx_thread.start()
    received = []
    r.set_rx_callback(received.append)
    try:
        assert await r._send_command(CMD_PING, b"", CMD_PONG) == b"old-modem"
        os.write(
            old_master, build_frame(CMD_RX_PACKET, struct.pack("<hhh", -65, 30, -66) + b"hello")
        )
        await until(lambda: received == [b"hello"])
        r._resolve_serial_port = lambda: new_port
        r._custom_cad_peak, r._custom_cad_min, r._custom_cad_symbol_num = 23, 11, 8
        old_stop.set()
        await until(lambda: not old_thread.is_alive())
        os.close(old_master)
        resources.remove(old_master)
        await until(lambda: r._io_stats["reconnects"] == 1, timeout=2)
        assert await r._send_command(CMD_PING, b"", CMD_PONG) == b"new-modem"
        assert [f[1] for f in frames[:2]] == [CMD_SET_CONFIG, CMD_SET_CAD_PARAMS]
        assert frames[1][4:8] == bytes([3, 23, 11, 0])
    finally:
        await stop(r)
        stop_emulators.set()
        await until(lambda: not any(t.is_alive() for t in responders))
        for fd in resources:
            try:
                os.close(fd)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_reader_cannot_stamp_old_handle_with_new_session_generation():
    r = connected()
    r._serial.rx.extend(
        build_frame(CMD_RX_PACKET, struct.pack("<hhh", -65, 30, -66) + b"old-session")
    )
    received = []
    r.set_rx_callback(received.append)
    sampled, resume = threading.Event(), threading.Event()
    original = r._reconnect_requested

    class PauseFirstCheck:
        paused = False

        def is_set(self):
            result = original.is_set()
            if not self.paused:
                self.paused = True
                sampled.set()
                assert resume.wait(1)
            return result

        def __getattr__(self, name):
            return getattr(original, name)

    r._reconnect_requested = PauseFirstCheck()
    r._reconnect_serial_sync = Mock(return_value=False)
    r._rx_thread = threading.Thread(target=r._rx_worker, daemon=True)
    r._rx_thread.start()
    try:
        await until(sampled.is_set)
        r._invalidate_connection("retire_between_reconnect_check_and_snapshot")
        resume.set()
        await until(lambda: not r._rx_thread.is_alive())
        await asyncio.sleep(0.01)
        assert not received
        assert r._rx_count == 0
    finally:
        resume.set()
        await stop(r)
