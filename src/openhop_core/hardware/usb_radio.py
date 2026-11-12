"""
USB LoRa Radio Driver for openhop_core

Implements the LoRaRadio interface using an openHop Modem connected via
USB-CDC. The modem acts as a "dumb" SX1262 transceiver — all MeshCore
protocol logic runs on the host in openhop_core.

Drop-in replacement for SX1262Radio in openhop_core's hardware layer.

Default sync word is 0x12 (MeshCore private syncword), matching the
firmware default — change only if the deployment uses a custom value.

Usage:
    from openhop_core.hardware.usb_radio import USBLoRaRadio

    radio = USBLoRaRadio(
        port="/dev/ttyACM0",
        frequency=869618000,
        bandwidth=62500,
        spreading_factor=8,
        coding_rate=8,
        tx_power=22,
        sync_word=0x12,
        preamble_length=16,
    )
    radio.begin()
    radio.set_rx_callback(my_callback)
    await radio.send(packet_bytes)
"""

import asyncio
import inspect
import logging
import os
import queue
import random
import struct
import threading
import time
from typing import Callable, Optional

import serial
from serial.tools import list_ports

from .protocol_constants import (
    CMD_CAD_PARAMS_RESP,
    CMD_CAD_REQUEST,
    CMD_CAD_RESP,
    CMD_CONFIG_RESP,
    CMD_ERROR,
    CMD_GET_VERSION,
    CMD_GET_WIFI,
    CMD_NOISE_REQ,
    CMD_NOISE_RESP,
    CMD_PING,
    CMD_PONG,
    CMD_RX_PACKET,
    CMD_RX_START,
    CMD_RX_STARTED,
    CMD_SET_CAD_PARAMS,
    CMD_SET_CONFIG,
    CMD_SET_WIFI,
    CMD_STATUS_REQ,
    CMD_STATUS_RESP,
    CMD_TX_DONE,
    CMD_TX_FAIL,
    CMD_TX_REQUEST,
    CMD_VERSION_RESP,
    CMD_WIFI_RESET,
    CMD_WIFI_STATUS,
    ERR_CHANNEL_BUSY,
    MAX_LORA_PAYLOAD,
    PROTO_SYNC,
    RADIO_CONFIG_FMT,
    STATUS_RESP_FMT,
    STATUS_RESP_SIZE,
    WIFI_MODE_AP_CONFIG,
    WIFI_MODE_OFFLINE,
    WIFI_MODE_STA_CONNECTED,
    WIFI_MODE_STA_CONNECTING,
    build_cad_params_payload,
    build_frame,
    crc16_ccitt,
)

logger = logging.getLogger("USBLoRaRadio")


def _deliver_rx_packet(callback: Callable[..., None], data: bytes, rssi: int, snr: float):
    """Deliver captured metrics when supported, preserving data-only callbacks.

    Check argument binding before invoking: a TypeError from inside a callback
    must not trigger a second delivery. Opaque callbacks retain the legacy API.
    """
    try:
        inspect.signature(callback).bind(data, rssi, snr)
    except (TypeError, ValueError):
        callback(data)
    else:
        callback(data, rssi, snr)


# Import LoRaRadio base conditionally — allows standalone testing
try:
    from openhop_core.hardware.base import LoRaRadio

    _HAS_BASE = True
except ImportError:
    _HAS_BASE = False

# Define the class with or without the ABC base
if _HAS_BASE:

    class _RadioBase(LoRaRadio):
        pass

else:

    class _RadioBase:
        pass


class USBLoRaRadio(_RadioBase):
    """USB LoRa Radio — openhop_core LoRaRadio interface over USB-CDC serial.

    Communicates with an openHop Modem over a USB-CDC serial link.
    Provides the same interface as SX1262Radio for
    transparent integration with openhop_core's Dispatcher and MeshNode.
    """

    def __init__(
        self,
        port: str = "/dev/ttyACM0",
        baudrate: int = 921600,
        frequency: int = 869618000,
        bandwidth: int = 62500,
        spreading_factor: int = 8,
        coding_rate: int = 8,
        tx_power: int = 22,
        sync_word: int = 0x12,
        preamble_length: int = 16,
        lbt_enabled: bool = True,
        lbt_max_attempts: int = 5,
        lbt_max_wait_seconds: float = 4.0,
        lbt_retry_interval_ms: int = 200,
    ):
        self.port = port
        self.baudrate = baudrate

        # Radio config — matches SX1262Radio constructor params
        self.frequency = frequency
        self.bandwidth = bandwidth
        self.spreading_factor = spreading_factor
        self.coding_rate = coding_rate
        self.tx_power = tx_power
        self.sync_word = sync_word
        self.preamble_length = preamble_length

        # LBT (Listen Before Talk) via CAD, bounded in TIME rather than
        # attempts, so an occupation longer than the budget cannot leave two
        # neighbours forcing their TX in lockstep. Defaults match MeshCore
        # (4 s cap, 200 ms retry); the jitter keeps two nodes decorrelated.
        # lbt_max_attempts is accepted for call-site compatibility only.
        self.lbt_enabled = lbt_enabled
        self.lbt_max_attempts = lbt_max_attempts
        self.lbt_max_wait_seconds = max(0.5, float(lbt_max_wait_seconds))
        self.lbt_retry_interval_ms = max(20, int(lbt_retry_interval_ms))
        # Last CMD_ERROR code seen, so send() can tell a firmware-side
        # auto-CAD refusal (ERR_CHANNEL_BUSY) from a real TX failure.
        self._last_modem_error = None  # int error code, or None

        # State
        self._serial: Optional[serial.Serial] = None
        self._initialized = False
        self._rx_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._port_identity: Optional[tuple[Optional[int], Optional[int], Optional[str]]] = None

        # Signal metrics — matches SX1262Radio interface
        self.last_rssi: int = -99
        self.last_snr: float = 0.0
        self.last_signal_rssi: int = -99
        self._noise_floor: float = -99.0

        # RX callback — set by Dispatcher via set_rx_callback()
        self.rx_callback: Optional[Callable[..., None]] = None

        # Response synchronization (command → response matching)
        self._response_events: dict[int, asyncio.Event] = {}
        self._response_data: dict[int, Optional[bytes]] = {}
        self._response_lock = threading.Lock()
        # Only one command may wait for a response at a time. Repeater applies
        # CAD thresholds and symbol count back-to-back, and both commands wait
        # for CMD_CAD_PARAMS_RESP. Without serialization the second waiter
        # replaces the first entry in _response_events and one update times out.
        self._command_lock = asyncio.Lock()
        # Each radio has one bounded daemon writer, never the shared executor.
        # A wedged OS write can disable this radio, but cannot block asyncio,
        # consume unbounded threads, or overlap a new serial session.
        self._write_queue = queue.Queue(maxsize=1)
        self._writer_thread: Optional[threading.Thread] = None
        self._serial_io_lock = threading.Lock()
        self._reconnect_requested = threading.Event()
        self._generation = 0
        self._pending_commands = 0
        self._max_pending_commands = 16
        self._write_timeout = 2.5  # pyserial's own write_timeout is 2 seconds
        self._queue_timeout = 5.0
        self._last_io_health_log = 0.0
        self._io_stats = {
            "commands": 0,
            "write_timeouts": 0,
            "response_timeouts": 0,
            "write_errors": 0,
            "queue_rejections": 0,
            "cancellations": 0,
            "reconnects": 0,
            "last_failure": None,
            "last_command": None,
            "last_write_ms": None,
            "last_response_ms": None,
            "last_rx_at": None,
        }

        # Custom CAD thresholds. Set lazily by set_custom_cad_thresholds()
        # or perform_cad(det_peak=..., det_min=...); kept in attributes from
        # construction so the calibration-sample fast-path doesn't trip on
        # AttributeError before the first call.
        self._custom_cad_peak: Optional[int] = None
        self._custom_cad_min: Optional[int] = None
        self._custom_cad_symbol_num: Optional[int] = None

        # TX lock to serialize transmissions (matches SX1262Radio)
        self._tx_lock = asyncio.Lock()

        # Stats
        self._tx_count = 0
        self._rx_count = 0
        self._crc_errors = 0
        self.crc_error_count = 0

        logger.info(
            f"USBLoRaRadio configured: port={port}, freq={frequency / 1e6:.1f}MHz, "
            f"sf={spreading_factor}, bw={bandwidth / 1000:.0f}kHz, "
            f"power={tx_power}dBm, syncword=0x{sync_word:04X}"
        )

    # ══════════════════════════════════════════════════════════
    # LoRaRadio interface implementation
    # ══════════════════════════════════════════════════════════

    def _lbt_retry_delay_ms(self, deadline: float) -> float:
        """One short jittered LBT retry delay, clamped to the remaining budget."""
        delay_ms = self.lbt_retry_interval_ms * random.uniform(0.5, 1.5)
        remaining_ms = (deadline - time.monotonic()) * 1000.0
        return max(0.0, min(delay_ms, remaining_ms))

    def begin(self) -> bool:
        """Initialize USB serial connection and configure the modem radio."""
        if self._initialized:
            return True
        # Never reuse an instance while its previous I/O owners are exiting.
        if any(t and t.is_alive() for t in (self._rx_thread, self._writer_thread)):
            return False
        self._stop_event.clear()
        self._reconnect_requested.clear()

        try:
            # dsrdtr=True tells pyserial to leave DTR/DSR alone on open
            # rather than pulsing them. On boards with a CP2102 (Heltec
            # V3 et al.), pyserial's default toggles DTR and the CP2102
            # in turn pulls EN low, rebooting the ESP32 every time we
            # (re-)open the port. dsrdtr=True is the workaround; rtscts
            # stays off because the firmware does not implement hardware
            # flow control on the RX pipe.
            self._serial = self._open_serial_sync()

            # Short settle in case the caller just power-cycled the device.
            time.sleep(0.3)
            self._serial.reset_input_buffer()
            logger.info(f"Serial connected to {self.port}")

            # 35 s covers the worst-case WifiManager::begin() STA-timeout if
            # the host opens the port right after a cold boot.
            # Skipping PING here on purpose: SET_CONFIG is itself a
            # liveness check (firmware replies with CONFIG_RESP), and on
            # macOS a PING immediately followed by SET_CONFIG confuses the
            # CDC bulk pipe — CONFIG_RESP never arrives. Keep `_ping_sync`
            # available for ad-hoc health probes, just don't chain it with
            # SET_CONFIG at boot.
            if not self._apply_config_sync():
                logger.error("Failed to configure radio")
                self._serial.close()
                return False

            self._connected_event.set()

            # Start RX background thread
            self._stop_event.clear()
            self._rx_thread = threading.Thread(
                target=self._rx_worker, daemon=True, name="usb-lora-rx"
            )
            self._rx_thread.start()

            self._initialized = True
            logger.info("USBLoRaRadio initialized successfully")
            return True

        except Exception as e:
            self._connected_event.clear()
            logger.error(f"Failed to initialize USBLoRaRadio: {e}")
            if self._serial and self._serial.is_open:
                self._serial.close()
            return False

    async def send(self, data: bytes) -> Optional[dict]:
        """Send a LoRa packet asynchronously with LBT (CAD).

        Returns transmission metadata dict matching SX1262Radio.send():
            {
                "airtime_ms": float,
                "lbt_attempts": int,
                "lbt_backoff_delays_ms": list[int],
                "lbt_channel_busy": bool,
            }
        Returns a dict with ok=False for a definitive busy-channel refusal.
        Returns None when the transmission outcome is unknown.
        """
        if not self._initialized:
            logger.error("Radio not initialized")
            return None

        async with self._tx_lock:
            # Log taxonomy, shared with the SPI radio: [LBT] is the
            # listen-before-talk retry loop, [CAD] a single channel-activity
            # scan, [TX] the transmit path. A nominal TX logs two DEBUG lines
            # ("[LBT] Summary" and "[TX] Done"); contention adds bounded
            # DEBUG retries, anomalies log at WARNING, failed sends at ERROR.
            lbt_backoff_delays: list[int] = []

            # ── Listen Before Talk (CAD) ─────────────────────
            # Bounded in TIME rather than attempts: short jittered retries run
            # for the whole budget. The deadline is shared with the
            # ERR_CHANNEL_BUSY retry below, so firmware-side auto-CAD refusals
            # draw from the same budget.
            cad_timeout = min(1.0, self.lbt_max_wait_seconds)
            lbt_deadline = time.monotonic() + self.lbt_max_wait_seconds
            cad_checks = 0
            modem_refusals = 0
            outcome = "clear" if self.lbt_enabled else "off"
            if self.lbt_enabled:
                while True:
                    remaining = lbt_deadline - time.monotonic()
                    if remaining <= 0:
                        outcome = "forced"
                        logger.warning(
                            f"[LBT] Budget exhausted ({self.lbt_max_wait_seconds:.1f}s) - "
                            "channel still busy, transmitting anyway"
                        )
                        break
                    # A shortened final response wait can retire a healthy modem
                    # session before its CAD response arrives. Keep the normal
                    # response allowance; wait out the remaining busy budget
                    # instead of issuing a command we cannot wait for.
                    if cad_checks and remaining < cad_timeout:
                        await asyncio.sleep(remaining)
                        continue
                    try:
                        cad_checks += 1
                        channel_busy = await self._perform_cad(timeout=cad_timeout)
                    except Exception as e:
                        logger.warning(f"[LBT] Channel check unavailable: {e}; not transmitting")
                        # No TX request has been submitted. Unknown CAD is not
                        # a clear channel, and must not authorize transmission.
                        return {
                            "ok": False,
                            "tx_outcome_uncertain": False,
                            "reason": "cad_unavailable",
                            "airtime_ms": 0.0,
                            "lbt_attempts": len(lbt_backoff_delays),
                            "lbt_backoff_delays_ms": lbt_backoff_delays,
                            "lbt_channel_busy": bool(lbt_backoff_delays),
                        }
                    if not channel_busy:
                        break
                    if time.monotonic() >= lbt_deadline:
                        outcome = "forced"
                        logger.warning(
                            f"[LBT] Budget exhausted ({self.lbt_max_wait_seconds:.1f}s) - "
                            "channel still busy, transmitting anyway"
                        )
                        break
                    delay_ms = self._lbt_retry_delay_ms(lbt_deadline)
                    lbt_backoff_delays.append(round(delay_ms))
                    logger.debug(f"[LBT] Channel busy - retrying in {delay_ms:.0f}ms")
                    await asyncio.sleep(delay_ms / 1000.0)

            # ── Transmit ─────────────────────────────────────
            try:
                while True:
                    self._last_modem_error = None
                    resp = await self._send_command(
                        CMD_TX_REQUEST,
                        data,
                        expect_cmd=CMD_TX_DONE,
                        timeout=10.0,
                    )
                    if resp is not None:
                        break
                    # Firmware-side auto-CAD (CMD_SET_AUTO_CAD) refuses a busy
                    # channel with ERR_CHANNEL_BUSY instead of trampling a
                    # neighbour. That is LBT feedback, not a failure: retry
                    # within the same time budget the host-side loop uses.
                    if (
                        self._last_modem_error == ERR_CHANNEL_BUSY
                        and time.monotonic() < lbt_deadline
                    ):
                        modem_refusals += 1
                        delay_ms = self._lbt_retry_delay_ms(lbt_deadline)
                        lbt_backoff_delays.append(round(delay_ms))
                        logger.debug(
                            f"[LBT] Modem reports channel busy - retrying TX in {delay_ms:.0f}ms"
                        )
                        await asyncio.sleep(delay_ms / 1000.0)
                        continue
                    if self._last_modem_error == ERR_CHANNEL_BUSY:
                        # Unlike the host-side loop there is nothing to force
                        # here: the modem is alive and refusing, not wedged.
                        outcome = "refused"
                        modem_refusals += 1
                        logger.warning("[LBT] Modem refused TX - channel busy for the whole budget")
                    break

                logger.debug(
                    f"[LBT] Summary: outcome={outcome} cad_checks={cad_checks} "
                    f"modem_refusals={modem_refusals} "
                    f"backoff_total={sum(lbt_backoff_delays):.0f}ms"
                )

                if resp is not None:
                    self._tx_count += 1

                    # Parse airtime from TX_DONE payload (uint32 LE, microseconds)
                    airtime_us = 0
                    if len(resp) >= 4:
                        airtime_us = struct.unpack("<I", resp[:4])[0]
                    airtime_ms = airtime_us / 1000.0

                    logger.debug(f"[TX] Done {len(data)}B airtime={airtime_ms:.0f}ms")

                    # Restore RX continuous mode
                    await self._send_command(
                        CMD_RX_START,
                        b"",
                        expect_cmd=CMD_RX_STARTED,
                        timeout=2.0,
                    )

                    return {
                        "airtime_ms": airtime_ms,
                        "lbt_attempts": len(lbt_backoff_delays),
                        "lbt_backoff_delays_ms": lbt_backoff_delays,
                        "lbt_channel_busy": len(lbt_backoff_delays) > 0,
                    }
                else:
                    # ERR_CHANNEL_BUSY is a definitive refusal, not a lost
                    # TX_DONE. Keep that distinction after restoring RX (which
                    # can overwrite _last_modem_error), so the caller may use
                    # its bounded known-unsent retry without replaying an
                    # uncertain transmission.
                    definitely_unsent = outcome == "refused"
                    if definitely_unsent:
                        logger.warning("[TX] Not sent - modem refused busy channel")
                    else:
                        logger.error("[TX] Failed - no TX_DONE response")
                    # Try to restore RX anyway
                    await self._send_command(
                        CMD_RX_START,
                        b"",
                        expect_cmd=CMD_RX_STARTED,
                        timeout=2.0,
                    )
                    if definitely_unsent:
                        return {
                            "ok": False,
                            "tx_outcome_uncertain": False,
                            "reason": "channel_busy",
                            "airtime_ms": 0.0,
                            "lbt_attempts": len(lbt_backoff_delays),
                            "lbt_backoff_delays_ms": lbt_backoff_delays,
                            "lbt_channel_busy": True,
                        }
                    return None

            except Exception as e:
                logger.error(f"[TX] Error: {e}")
                return None

    async def wait_for_rx(self) -> bytes:
        """Not used — packets arrive via set_rx_callback()."""
        raise NotImplementedError(
            "Use set_rx_callback(callback) to receive packets asynchronously."
        )

    def set_event_loop(self, loop) -> None:
        """Set the event loop used for callbacks and live configuration pushes."""
        self._event_loop = loop

    def set_rx_callback(self, callback: Optional[Callable[..., None]]):
        """Set a ``(data, rssi, snr)`` or legacy ``(data)`` callback; None clears it."""
        self.rx_callback = callback
        logger.info("RX callback registered")

        # Capture event loop for thread-safe async event dispatch
        try:
            self._event_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

    def sleep(self):
        """Put radio into low-power mode (not typically used with USB modem)."""
        logger.debug("Sleep not applicable for USB modem")

    def get_last_rssi(self) -> int:
        return self.last_rssi

    def get_last_snr(self) -> float:
        return self.last_snr

    def get_last_signal_rssi(self) -> int:
        return self.last_signal_rssi

    # ══════════════════════════════════════════════════════════
    # Extended interface (matching SX1262Radio extras used by pymc_core)
    # ══════════════════════════════════════════════════════════

    def check_radio_health(self) -> bool:
        """Health check — verify RX thread is alive, restart if dead.

        Called by Dispatcher.run_forever() every 60 seconds.
        Also refreshes cached modem metrics from the modem.
        """
        now = time.monotonic()
        if now - self._last_io_health_log >= 60.0:
            self._last_io_health_log = now
            logger.info(
                "Serial health: port=%s connected=%s generation=%s pending=%s "
                "tx=%s rx=%s write_timeouts=%s response_timeouts=%s write_errors=%s "
                "queue_rejections=%s reconnects=%s last_write_ms=%s last_response_ms=%s",
                self.port,
                self._connected_event.is_set(),
                self._generation,
                self._pending_commands,
                self._tx_count,
                self._rx_count,
                self._io_stats["write_timeouts"],
                self._io_stats["response_timeouts"],
                self._io_stats["write_errors"],
                self._io_stats["queue_rejections"],
                self._io_stats["reconnects"],
                self._io_stats["last_write_ms"],
                self._io_stats["last_response_ms"],
            )
        if not self._initialized or not self._connected_event.is_set():
            return False

        # Check RX thread
        if self._rx_thread is None or not self._rx_thread.is_alive():
            logger.warning("RX thread dead — restarting")
            self._stop_event.clear()
            self._rx_thread = threading.Thread(
                target=self._rx_worker, daemon=True, name="usb-lora-rx"
            )
            self._rx_thread.start()
            return False

        # Schedule modem metric refresh (non-blocking)
        if self._event_loop:
            self._event_loop.call_soon_threadsafe(
                lambda: self._event_loop.create_task(self._refresh_background_metrics())
            )

        return True

    def get_status(self) -> dict:
        """Get radio status dict (matches SX1262Radio.get_status())."""
        connected_event = getattr(self, "_connected_event", None)
        connected = self._initialized and (connected_event is None or connected_event.is_set())
        return {
            "initialized": connected,
            "frequency": self.frequency,
            "tx_power": self.tx_power,
            "spreading_factor": self.spreading_factor,
            "bandwidth": self.bandwidth,
            "coding_rate": self.coding_rate,
            "last_rssi": self.last_rssi,
            "last_snr": self.last_snr,
            "last_signal_rssi": self.last_signal_rssi,
            "hardware_ready": connected,
            "driver": "modem_usb",
            "port": self.port,
            "tx_count": self._tx_count,
            "rx_count": self._rx_count,
            "crc_errors": self._crc_errors,
            "crc_error_count": self.crc_error_count,
            "serial_io": {
                **self._io_stats,
                "generation": self._generation,
                "pending_commands": self._pending_commands,
                "reconnecting": self._reconnect_requested.is_set(),
                "writer_alive": bool(self._writer_thread and self._writer_thread.is_alive()),
            },
        }

    async def get_modem_status(self) -> Optional[dict]:
        """Query detailed status from the modem firmware."""
        resp = await self._send_command(
            CMD_STATUS_REQ,
            b"",
            expect_cmd=CMD_STATUS_RESP,
            timeout=2.0,
        )
        if resp and len(resp) >= STATUS_RESP_SIZE:
            fields = struct.unpack(STATUS_RESP_FMT, resp[:STATUS_RESP_SIZE])
            status = {
                "uptime_sec": fields[0],
                "rx_count": fields[1],
                "tx_count": fields[2],
                "crc_errors": fields[3],
                "last_rssi": fields[4],
                "last_snr": fields[5] / 10.0,
                "noise_floor": fields[6] / 10.0,
                "temp_c": fields[7],
                "radio_state": ["idle/rx", "tx", "error"][min(fields[8], 2)],
            }
            self._crc_errors = status["crc_errors"]
            self.crc_error_count = status["crc_errors"]
            return status
        return None

    async def _refresh_background_metrics(self) -> None:
        try:
            await self.refresh_noise_floor()
        except Exception as e:
            logger.debug(f"Noise-floor refresh failed: {e}")
        try:
            await self.get_modem_status()
        except Exception as e:
            logger.debug(f"Status refresh failed: {e}")

    def get_noise_floor(self) -> Optional[float]:
        """Get current noise floor in dBm.

        Matches SX1262Radio.get_noise_floor() interface.
        The noise floor is continuously sampled by the modem firmware
        using the same algorithm as SX1262Radio._sample_noise_floor():
        - 20 instantaneous RSSI samples during quiet periods
        - 500ms quiet time after last packet before sampling
        - Samples above floor + 10dB threshold rejected
        - Averaged and clamped to -150...-50 dBm

        This is a synchronous method returning the cached value.
        The firmware updates it autonomously. For a fresh read,
        use await get_modem_status() which returns noise_floor.
        """
        if not self._initialized:
            return None
        if self._tx_lock.locked():
            return None
        return self._noise_floor

    async def refresh_noise_floor(self) -> Optional[float]:
        """Query fresh noise floor from modem firmware.

        Returns noise floor in dBm, or None on failure.
        Also updates the cached self._noise_floor value.
        """
        resp = await self._send_command(
            CMD_NOISE_REQ,
            b"",
            expect_cmd=CMD_NOISE_RESP,
            timeout=2.0,
        )
        if resp and len(resp) >= 2:
            nf_x10 = struct.unpack("<h", resp[:2])[0]
            self._noise_floor = nf_x10 / 10.0
            logger.debug(f"Noise floor: {self._noise_floor:.1f} dBm")
            return self._noise_floor
        return None

    async def perform_cad(
        self,
        det_peak: Optional[int] = None,
        det_min: Optional[int] = None,
        timeout: float = 1.0,
        calibration: bool = False,
        cad_symbol_num: Optional[int] = None,
    ) -> bool:
        """Public CAD interface matching SX1262Radio.perform_cad().

        When det_peak/det_min are supplied (used by the repeater CAD
        calibration tool), program the thresholds and requested symbol count
        via CMD_SET_CAD_PARAMS before running the scan.
        """
        if cad_symbol_num is None:
            cad_symbol_num = self._custom_cad_symbol_num or 2
        cad_symbol_num = int(cad_symbol_num)

        if det_peak is not None and det_min is not None:
            new_peak = int(det_peak)
            new_min = int(det_min)
            # Avoid re-sending only when thresholds and symbol count all match.
            if (
                new_peak != self._custom_cad_peak
                or new_min != self._custom_cad_min
                or cad_symbol_num != self._custom_cad_symbol_num
            ):
                payload = build_cad_params_payload(cad_symbol_num, new_peak, new_min)
                await self._send_command(
                    CMD_SET_CAD_PARAMS,
                    payload,
                    expect_cmd=CMD_CAD_PARAMS_RESP,
                    timeout=2.0,
                )
                self._custom_cad_peak = new_peak
                self._custom_cad_min = new_min
                self._custom_cad_symbol_num = cad_symbol_num

        # Calibration tool sends timeout=0.3 which is tight for our stack;
        # raise the floor so we don't lose samples to "no response".
        effective = max(timeout, 0.6)
        return await self._perform_cad(effective)

    # ── Wi-Fi / OTA provisioning (v0.5) ───────────────────────

    async def set_wifi_credentials(
        self,
        ssid: str,
        password: str,
        tcp_port: int = 5055,
        tcp_token: str = "",
    ) -> Optional[dict]:
        """Provision Wi-Fi credentials over USB and reboot into STA mode.

        After this call the modem saves SSID/password/port/token to NVS,
        responds with a WIFI_STATUS frame showing the pending config, then
        reboots. USB link drops during reboot — caller should re-open the
        serial port and call `begin()` again, then verify with
        `get_wifi_status()` that STA came up.

        tcp_token: shared secret. Empty = open LAN (TCP + HTTP OTA). Non-empty
        enforces CMD_AUTH on TCP and Basic-auth (user='heltec') on HTTP /update.

        Returns the pending WIFI_STATUS dict, or None on timeout/error.
        """
        ssid_b = ssid.encode("utf-8")
        pass_b = password.encode("utf-8")
        tok_b = tcp_token.encode("utf-8")
        if len(ssid_b) == 0 or len(ssid_b) > 32:
            raise ValueError("SSID must be 1..32 bytes")
        if len(pass_b) > 64:
            raise ValueError("password must be <=64 bytes")
        if len(tok_b) > 64:
            raise ValueError("token must be <=64 bytes")
        if not (1 <= tcp_port <= 65535):
            raise ValueError("tcp_port out of range")

        payload = (
            bytes([len(ssid_b)])
            + ssid_b
            + bytes([len(pass_b)])
            + pass_b
            + struct.pack("<H", tcp_port)
            + bytes([len(tok_b)])
            + tok_b
        )
        resp = await self._send_command(
            CMD_SET_WIFI, payload, expect_cmd=CMD_WIFI_STATUS, timeout=3.0
        )
        if resp is None:
            return None
        return self._parse_wifi_status(resp)

    async def get_wifi_status(self) -> Optional[dict]:
        """Query current Wi-Fi/OTA status.

        Returns a dict:
            {
              "mode": int,          # 0=off 1=connecting 2=STA 3=AP
              "mode_name": str,
              "ip": str,            # "192.168.1.42" or "0.0.0.0"
              "port": int,          # TCP port
              "ssid": str,
              "hostname": str,      # "pymc-ab12cd" — append ".local" for mDNS
              "mdns": str,          # "pymc-ab12cd.local"
            }
        or None on timeout.
        """
        resp = await self._send_command(CMD_GET_WIFI, b"", expect_cmd=CMD_WIFI_STATUS, timeout=2.0)
        if resp is None:
            return None
        return self._parse_wifi_status(resp)

    async def get_version(self) -> Optional[str]:
        """Return firmware version string (e.g. 'v0.5.3').

        None if the modem doesn't implement CMD_GET_VERSION (pre-v0.5.3 firmware)
        or the request times out.
        """
        resp = await self._send_command(
            CMD_GET_VERSION, b"", expect_cmd=CMD_VERSION_RESP, timeout=2.0
        )
        if resp is None:
            return None
        try:
            return resp.decode("ascii", errors="replace")
        except Exception:
            return None

    async def wifi_reset(self) -> bool:
        """Wipe Wi-Fi NVS and reboot into AP config portal.

        Device will reboot and drop the serial link. USB OTA/provisioning
        remains possible via `set_wifi_credentials()` after re-connecting.
        """
        resp = await self._send_command(CMD_WIFI_RESET, b"", expect_cmd=CMD_WIFI_RESET, timeout=2.0)
        return resp is not None

    @staticmethod
    def _parse_wifi_status(payload: bytes) -> Optional[dict]:
        """Parse WIFI_STATUS payload (see protocol.h for layout)."""
        try:
            i = 0
            mode = payload[i]
            i += 1
            ip = f"{payload[i]}.{payload[i + 1]}.{payload[i + 2]}.{payload[i + 3]}"
            i += 4
            port = payload[i] | (payload[i + 1] << 8)
            i += 2
            slen = payload[i]
            i += 1
            ssid = payload[i : i + slen].decode("utf-8", errors="replace")
            i += slen
            hlen = payload[i]
            i += 1
            host = payload[i : i + hlen].decode("utf-8", errors="replace")
        except (IndexError, UnicodeDecodeError) as e:
            logger.error(f"Malformed WIFI_STATUS: {e}")
            return None

        mode_names = {
            WIFI_MODE_OFFLINE: "offline",
            WIFI_MODE_STA_CONNECTING: "connecting",
            WIFI_MODE_STA_CONNECTED: "sta",
            WIFI_MODE_AP_CONFIG: "ap",
        }
        return {
            "mode": mode,
            "mode_name": mode_names.get(mode, "unknown"),
            "ip": ip,
            "port": port,
            "ssid": ssid,
            "hostname": host,
            "mdns": f"{host}.local" if host else "",
        }

    def cleanup(self):
        """Stop accepting work without joining an OS-blocked thread on asyncio."""
        if self._stop_event.is_set():
            return
        self._initialized = False
        self._invalidate_connection("cleanup")
        self._stop_event.set()
        # The I/O owners close the captured handle after any write finishes.
        # With no owners (e.g. partial begin), close on a daemon, not this loop.
        if not any(t and t.is_alive() for t in (self._rx_thread, self._writer_thread)):
            if self._serial is not None:
                self._writer_thread = threading.Thread(
                    target=self._close_serial_sync, daemon=True, name="usb-lora-close"
                )
                self._writer_thread.start()
        logger.info("USBLoRaRadio cleanup requested: port=%s", self.port)

    def _close_serial_sync(self):
        with self._serial_io_lock:
            if self._serial is not None:
                try:
                    self._serial.close()
                except Exception:
                    logger.debug("Serial close failed: port=%s", self.port, exc_info=True)
                self._serial = None

    def _invalidate_connection(self, reason, generation=None):
        """Retire one session and wake waiters; never perform serial I/O here."""
        with self._response_lock:
            if generation is not None and generation != self._generation:
                return
            self._generation += 1
            self._connected_event.clear()
            self._reconnect_requested.set()
            self._last_modem_error = None
            if reason != "cleanup":
                self._io_stats["last_failure"] = reason
            for cmd, evt in self._response_events.items():
                self._response_data[cmd] = None
                self._wake_event(evt)
        log = logger.debug if reason == "cleanup" else logger.warning
        log(
            "Serial session retired: port=%s generation=%s reason=%s",
            self.port,
            self._generation,
            reason,
        )

    def _wake_event(self, evt):
        if self._event_loop and not self._event_loop.is_closed():
            try:
                self._event_loop.call_soon_threadsafe(evt.set)
            except RuntimeError:
                pass  # Loop may close concurrently with shutdown.

    def _write_worker(self):
        try:
            while not self._stop_event.is_set():
                try:
                    handle, generation, frame, done, result = self._write_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                try:
                    with self._serial_io_lock:
                        if (
                            generation != self._generation
                            or self._stop_event.is_set()
                            or not self._connected_event.is_set()
                        ):
                            raise serial.SerialException("serial session retired before write")
                        started = time.monotonic()
                        # Do not call flush(): POSIX tcdrain has no timeout.
                        # A matching firmware response is the delivery barrier.
                        written = handle.write(frame)
                        if written != len(frame):
                            raise serial.SerialException(
                                f"short serial write: {written}/{len(frame)}"
                            )
                        result["write_ms"] = (time.monotonic() - started) * 1000
                except Exception as exc:
                    result["error"] = exc
                finally:
                    done.set()
                    self._write_queue.task_done()
        finally:
            if self._stop_event.is_set():
                self._close_serial_sync()

    def _ensure_write_worker(self):
        if self._writer_thread is None or not self._writer_thread.is_alive():
            self._writer_thread = threading.Thread(
                target=self._write_worker, daemon=True, name=f"usb-lora-tx:{self.port}"
            )
            self._writer_thread.start()

    # ══════════════════════════════════════════════════════════
    # Private — serial I/O
    # ══════════════════════════════════════════════════════════

    def _capture_port_identity(self, device: str) -> None:
        """Remember stable USB identity so tty renumbering can be followed."""
        real_device = os.path.realpath(device)
        for info in list_ports.comports():
            if os.path.realpath(info.device) == real_device:
                self._port_identity = (info.vid, info.pid, info.serial_number)
                return

    def _resolve_serial_port(self) -> str:
        """Resolve the configured modem after USB re-enumeration."""
        if self._port_identity is not None:
            expected_vid, expected_pid, expected_serial = self._port_identity
            for info in list_ports.comports():
                if info.vid != expected_vid or info.pid != expected_pid:
                    continue
                if expected_serial and info.serial_number != expected_serial:
                    continue
                return info.device
        return self.port

    def _open_serial_sync(self) -> serial.Serial:
        """Open a configured serial handle without starting the RX worker."""
        device = self._resolve_serial_port()
        handle = serial.Serial()
        handle.port = device
        handle.baudrate = self.baudrate
        handle.timeout = 0.1
        handle.write_timeout = 2.0
        handle.dsrdtr = True
        handle.rtscts = False
        handle.open()
        self._capture_port_identity(device)
        return handle

    def _reconnect_serial_sync(self) -> bool:
        """Reopen only after the old writer exits, then establish a response barrier.

        Modem firmware handles commands serially: CONFIG_RESP follows any old
        TX_DONE/error. Consuming that response before publishing the new session
        prevents untagged late replies from satisfying a new command. Never
        replay the command whose outcome was unknown.
        """
        self._connected_event.clear()
        self._reconnect_requested.set()
        handle = self._serial
        # Cancellation is best effort and runs on this radio's RX thread.
        if handle is not None and hasattr(handle, "cancel_write"):
            try:
                handle.cancel_write()
            except Exception:
                pass
        waited = time.monotonic()
        while not self._serial_io_lock.acquire(timeout=0.1):
            if self._stop_event.is_set():
                return False
            if time.monotonic() - waited >= 5.0:
                logger.warning(
                    "Serial recovery waiting for old writer: port=%s generation=%s",
                    self.port,
                    self._generation,
                )
                waited = time.monotonic()
        try:
            if self._serial is not None:
                try:
                    self._serial.close()
                except Exception:
                    pass
            attempt = 0
            while not self._stop_event.is_set():
                attempt += 1
                try:
                    with self._response_lock:
                        self._generation += 1
                    self._serial = self._open_serial_sync()
                    if self._stop_event.wait(0.3):
                        return False
                    self._serial.reset_input_buffer()
                    if not self._apply_config_sync():
                        raise RuntimeError("modem did not acknowledge restored configuration")
                    if self._stop_event.is_set():
                        return False
                    self._reconnect_requested.clear()
                    self._connected_event.set()
                    self._io_stats["reconnects"] += 1
                    logger.info(
                        "USB modem reconnected: port=%s generation=%s attempts=%s",
                        self._serial.port,
                        self._generation,
                        attempt,
                    )
                    return True
                except Exception as exc:
                    self._connected_event.clear()
                    if self._serial is not None:
                        try:
                            self._serial.close()
                        except Exception:
                            pass
                    if attempt == 1 or attempt % 10 == 0:
                        logger.warning(
                            "USB modem reconnect failed: port=%s attempt=%s error=%s",
                            self.port,
                            attempt,
                            exc,
                        )
                    self._stop_event.wait(2.0)
            return False
        finally:
            self._serial_io_lock.release()

    def _ping_sync(self, timeout: float = 3.0) -> bool:
        """Synchronous ping — ad-hoc liveness probe.

        Not used from begin() on purpose: on macOS, chaining PING and
        SET_CONFIG into the same CDC bulk stream causes SET_CONFIG to be
        silently dropped. Keep this for explicit health checks.
        """
        frame = build_frame(CMD_PING)
        self._serial.write(frame)

        resp = self._read_frame_sync(timeout=timeout, expect_cmd=CMD_PONG)
        logger.debug(f"PING _read_frame_sync returned: {resp}")
        if resp and resp[0] == CMD_PONG:
            logger.info("Modem PONG received — alive")
            return True
        return False

    def _apply_config_sync(self) -> bool:
        """Synchronous config push — used during begin()."""
        payload = struct.pack(
            RADIO_CONFIG_FMT,
            self.frequency,
            self.bandwidth,
            self.spreading_factor,
            self.coding_rate,
            self.tx_power,
            self.sync_word,
            self.preamble_length,
        )
        frame = build_frame(CMD_SET_CONFIG, payload)
        self._serial.write(frame)

        # 10 s covers the worst case where a burst of RX_PACKET frames from
        # nearby MeshCore nodes arrives ahead of CONFIG_RESP and the filter
        # has to skip them.
        resp = self._read_frame_sync(timeout=10.0, expect_cmd=CMD_CONFIG_RESP)
        if resp and resp[0] == CMD_CONFIG_RESP:
            logger.info(
                f"Radio configured: {self.frequency / 1e6:.1f}MHz SF{self.spreading_factor} "
                f"BW{self.bandwidth / 1000:.0f}kHz {self.tx_power}dBm "
                f"sync=0x{self.sync_word:04X} pre={self.preamble_length}"
            )
            if not self._apply_cad_config_sync():
                logger.warning("Radio configured but cached CAD settings were not restored")
                return False
            return True
        elif resp and resp[0] == CMD_ERROR:
            err = resp[1][0] if len(resp) > 1 and len(resp[1]) > 0 else 0xFF
            logger.error(f"Config rejected by modem: error 0x{err:02X}")
            return False
        else:
            logger.error("No config response from modem")
            return False

    def _write_cad_frame_sync(self, payload: bytes) -> bool:
        """Push one complete CAD configuration before the RX worker starts."""
        if self._serial is None:
            return False
        self._serial.write(build_frame(CMD_SET_CAD_PARAMS, payload))
        resp = self._read_frame_sync(timeout=3.0, expect_cmd=CMD_CAD_PARAMS_RESP)
        if resp and resp[0] == CMD_CAD_PARAMS_RESP:
            return True
        logger.error("No CAD configuration response from modem")
        return False

    def _apply_cad_config_sync(self) -> bool:
        """Restore cached CAD settings during initial attach or re-attach."""
        if self._custom_cad_peak is None or self._custom_cad_min is None:
            return True
        payload = build_cad_params_payload(
            self._custom_cad_symbol_num or 2,
            self._custom_cad_peak,
            self._custom_cad_min,
        )
        return self._write_cad_frame_sync(payload)

    def _read_frame_sync(
        self,
        timeout: float = 2.0,
        expect_cmd: Optional[int] = None,
    ) -> Optional[tuple]:
        """Read one frame synchronously. Returns (cmd, payload) or None.

        When `expect_cmd` is set, frames with a different cmd byte are
        discarded silently (common case: RX_PACKET from nearby MeshCore
        nodes arriving between our command and its response). The single
        `timeout` applies to the whole wait, not per-frame.
        """
        old_timeout = self._serial.timeout
        # Short per-read timeout keeps us responsive; the overall wait is
        # bounded by `deadline`. Longer timeouts (e.g. 35s) were observed
        # to swallow incoming bytes on macOS + CP2102, so we poll instead.
        self._serial.timeout = 0.2
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline and not self._stop_event.is_set():
                b = self._serial.read(1)
                if len(b) == 0:
                    continue
                if b[0] != PROTO_SYNC:
                    continue

                # A loose 0xAA byte can appear inside an RX_PACKET payload;
                # if we can't complete the frame (short read / bad CRC),
                # go back to scanning for the next SYNC instead of bailing.
                hdr = self._serial.read(3)
                if len(hdr) < 3:
                    continue
                cmd = hdr[0]
                length = struct.unpack("<H", hdr[1:3])[0]
                if length > MAX_LORA_PAYLOAD + 16:
                    continue  # garbage length — must be a fake SYNC

                payload = self._serial.read(length) if length > 0 else b""
                if len(payload) < length:
                    continue

                crc_bytes = self._serial.read(2)
                if len(crc_bytes) < 2:
                    continue

                received_crc = struct.unpack("<H", crc_bytes)[0]
                computed_crc = crc16_ccitt(hdr + payload)
                if received_crc != computed_crc:
                    logger.debug(
                        f"CRC mismatch (likely fake SYNC in payload): "
                        f"recv=0x{received_crc:04X} comp=0x{computed_crc:04X}"
                    )
                    continue

                if expect_cmd is not None and cmd != expect_cmd:
                    if cmd == CMD_ERROR:
                        return (cmd, payload)
                    continue

                return (cmd, payload)
            return None
        finally:
            self._serial.timeout = old_timeout

    # ── RX background thread ─────────────────────────────────

    def _rx_worker(self):
        """Background thread: reads serial, dispatches RX packets and command responses."""
        logger.debug("RX worker thread started")
        buf = bytearray()

        while not self._stop_event.is_set():
            try:
                if self._reconnect_requested.is_set():
                    buf.clear()
                    if not self._reconnect_serial_sync():
                        break
                with self._response_lock:
                    if self._reconnect_requested.is_set() or self._stop_event.is_set():
                        continue
                    handle, generation = self._serial, self._generation
                if not handle or not handle.is_open:
                    time.sleep(0.1)
                    continue

                waiting = handle.in_waiting
                if waiting > 0:
                    buf.extend(handle.read(waiting))
                else:
                    time.sleep(0.001)
                    continue

                # Parse complete frames from buffer
                while len(buf) >= 6:  # Min frame: SYNC+CMD+LEN(2)+CRC(2)
                    # Find sync
                    sync_idx = buf.find(PROTO_SYNC)
                    if sync_idx < 0:
                        buf.clear()
                        break
                    if sync_idx > 0:
                        buf = buf[sync_idx:]

                    if len(buf) < 4:
                        break

                    cmd = buf[1]
                    length = buf[2] | (buf[3] << 8)
                    # Sanity-cap the LEN field. The largest legitimate frame
                    # is RX_PACKET (6 B header + MAX_LORA_PAYLOAD); anything
                    # bigger means we're parsing garbage from a desync.
                    # Drop the SYNC byte we just consumed and resync rather
                    # than waiting indefinitely for a phantom 64 KB frame.
                    if length > MAX_LORA_PAYLOAD + 32:
                        logger.warning(
                            f"RX frame LEN={length} exceeds sanity bound — "
                            f"dropping SYNC byte and resyncing"
                        )
                        buf = buf[1:]
                        continue
                    frame_size = 1 + 1 + 2 + length + 2

                    if len(buf) < frame_size:
                        break  # Incomplete frame, wait for more

                    # Extract frame
                    hdr = bytes(buf[1:4])
                    payload = bytes(buf[4 : 4 + length])
                    crc_recv = buf[4 + length] | (buf[5 + length] << 8)
                    crc_comp = crc16_ccitt(hdr + payload)

                    # Consume frame
                    buf = buf[frame_size:]

                    if crc_recv != crc_comp:
                        logger.warning(f"RX CRC mismatch, cmd=0x{cmd:02X}, dropping")
                        continue

                    self._dispatch_frame(cmd, payload, generation=generation)

            except (serial.SerialException, OSError) as e:
                buf.clear()
                self._invalidate_connection(f"read_error:{type(e).__name__}")
                logger.warning("USB modem disconnected: port=%s error=%s", self.port, e)
                if not self._reconnect_serial_sync():
                    break
            except Exception as e:
                logger.error(f"RX worker error: {e}")
                time.sleep(0.1)

        self._close_serial_sync()
        logger.debug("RX worker thread exiting: port=%s", self.port)

    def _deliver_rx_current(self, generation, callback, data, rssi, snr):
        if generation == self._generation and not self._stop_event.is_set():
            _deliver_rx_packet(callback, data, rssi, snr)

    def _dispatch_frame(self, cmd: int, payload: bytes, generation=None):
        """Route only frames from the current serial session."""
        if generation is None:
            generation = self._generation
        if generation != self._generation or self._stop_event.is_set():
            return

        if cmd == CMD_RX_PACKET:
            # ── LoRa packet received ─────────────────────────
            if len(payload) < 6:
                logger.warning(f"RX_PACKET too short: {len(payload)}B")
                return

            rssi = struct.unpack("<h", payload[0:2])[0]
            snr_x10 = struct.unpack("<h", payload[2:4])[0]
            signal_rssi = struct.unpack("<h", payload[4:6])[0]
            lora_data = payload[6:]

            snr = snr_x10 / 10.0
            self.last_rssi = rssi
            self.last_snr = snr
            self.last_signal_rssi = signal_rssi
            self._rx_count += 1
            self._io_stats["last_rx_at"] = time.time()

            logger.debug(f"RX: {len(lora_data)}B RSSI={rssi}dBm SNR={snr_x10 / 10:.1f}dB")

            # Invoke RX callback on the event loop thread.
            # pymc_core dispatcher._on_packet_received calls asyncio.get_running_loop()
            # inside the callback; if we invoke it from this worker thread, it raises
            # RuntimeError and the packet is silently dropped (dispatcher.py:306-308).
            # Capture callback and frame metrics together. The reader may update
            # last_rssi/last_snr again before this callback (or its tasks) runs.
            callback = self.rx_callback
            if callback:
                if self._event_loop and self._event_loop.is_running():
                    self._event_loop.call_soon_threadsafe(
                        self._deliver_rx_current, generation, callback, lora_data, rssi, snr
                    )
                else:
                    logger.warning("RX packet but event loop not running — dropping")
            else:
                logger.warning("RX packet but no callback registered")

        elif cmd == CMD_ERROR:
            err_code = payload[0] if len(payload) > 0 else 0xFF
            if err_code == ERR_CHANNEL_BUSY:
                # LBT feedback, not an anomaly: the send loop retries it.
                logger.debug(f"Modem error: 0x{err_code:02X} (channel busy)")
            else:
                logger.warning(f"Modem error: 0x{err_code:02X}")
            # Also signal any waiting command in case the error
            # is a response to our command
            with self._response_lock:
                if generation != self._generation:
                    return
                for evt_cmd, evt in list(self._response_events.items()):
                    if evt_cmd not in self._response_data:
                        self._last_modem_error = err_code
                        self._response_data[evt_cmd] = None
                        self._wake_event(evt)

        elif cmd == CMD_TX_FAIL:
            # The TX request reached the radio but the chip never asserted
            # TX_DONE before the firmware's own timeout. Wake up whoever is
            # blocked on CMD_TX_DONE so the caller doesn't sit on the full
            # driver timeout.
            logger.warning("[TX] Modem TX_FAIL - radio did not assert TX_DONE")
            with self._response_lock:
                if generation != self._generation:
                    return
                evt = self._response_events.get(CMD_TX_DONE)
                if evt is not None and CMD_TX_DONE not in self._response_data:
                    self._response_data[CMD_TX_DONE] = None
                    self._wake_event(evt)

        else:
            # ── Command response ─────────────────────────────
            with self._response_lock:
                if generation != self._generation:
                    return
                if cmd in self._response_events and cmd not in self._response_data:
                    self._response_data[cmd] = payload
                    evt = self._response_events[cmd]
                    self._wake_event(evt)

    # ── Async command/response ────────────────────────────────

    async def _send_command(
        self,
        cmd: int,
        payload: bytes,
        expect_cmd: int,
        timeout: float = 5.0,
    ) -> Optional[bytes]:
        """Bound admission, serial write and response separately, per radio."""
        if not self._connected_event.is_set() or self._stop_event.is_set():
            return None
        if self._pending_commands >= self._max_pending_commands:
            self._io_stats["queue_rejections"] += 1
            logger.warning("Serial command queue full: port=%s cmd=0x%02X", self.port, cmd)
            return None
        self._pending_commands += 1
        generation = self._generation
        acquired = False
        submitted = False
        evt = None
        started = time.monotonic()
        try:
            try:
                await asyncio.wait_for(self._command_lock.acquire(), self._queue_timeout)
                acquired = True
            except asyncio.TimeoutError:
                self._io_stats["queue_rejections"] += 1
                logger.warning("Serial command queue timeout: port=%s cmd=0x%02X", self.port, cmd)
                return None
            handle = self._serial
            if (
                generation != self._generation
                or not self._connected_event.is_set()
                or not handle
                or not handle.is_open
                or self._stop_event.is_set()
            ):
                return None
            self._event_loop = asyncio.get_running_loop()
            evt = asyncio.Event()
            with self._response_lock:
                self._response_events[expect_cmd] = evt
                self._response_data.pop(expect_cmd, None)
                self._last_modem_error = None
            done, result = threading.Event(), {}
            self._ensure_write_worker()
            self._write_queue.put_nowait(
                (handle, generation, build_frame(cmd, payload), done, result)
            )
            submitted = True
            self._io_stats["commands"] += 1
            self._io_stats["last_command"] = f"0x{cmd:02X}"
            write_deadline = time.monotonic() + self._write_timeout
            while not done.is_set():
                if generation != self._generation:
                    return None
                if time.monotonic() >= write_deadline:
                    self._io_stats["write_timeouts"] += 1
                    self._invalidate_connection(f"write_timeout:0x{cmd:02X}", generation)
                    return None
                await asyncio.sleep(0.005)
            if generation != self._generation:
                return None
            if "error" in result:
                self._io_stats["write_errors"] += 1
                self._invalidate_connection(f"write_error:0x{cmd:02X}", generation)
                logger.warning(
                    "Serial write failed: port=%s cmd=0x%02X error=%s",
                    self.port,
                    cmd,
                    result["error"],
                )
                return None
            self._io_stats["last_write_ms"] = round(result["write_ms"], 2)
            try:
                await asyncio.wait_for(evt.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                self._io_stats["response_timeouts"] += 1
                self._invalidate_connection(f"response_timeout:0x{cmd:02X}", generation)
                return None
            if generation != self._generation:
                return None
            self._io_stats["last_response_ms"] = round((time.monotonic() - started) * 1000, 2)
            return self._response_data.get(expect_cmd)
        except asyncio.CancelledError:
            if submitted:
                self._io_stats["cancellations"] += 1
                self._invalidate_connection(f"command_cancelled:0x{cmd:02X}", generation)
            raise
        except queue.Full:
            self._io_stats["queue_rejections"] += 1
            return None
        finally:
            if acquired:
                with self._response_lock:
                    if evt is not None and self._response_events.get(expect_cmd) is evt:
                        self._response_events.pop(expect_cmd, None)
                        self._response_data.pop(expect_cmd, None)
                self._command_lock.release()
            self._pending_commands -= 1

    async def _perform_cad(self, timeout: float = 1.0) -> bool:
        """Perform Channel Activity Detection. Returns True if busy."""
        resp = await self._send_command(
            CMD_CAD_REQUEST,
            b"",
            expect_cmd=CMD_CAD_RESP,
            timeout=timeout,
        )
        if resp in (b"\x00", b"\x01"):
            busy = resp[0] != 0
            logger.debug(
                "[CAD] BUSY - channel activity detected"
                if busy
                else "[CAD] CLEAR - no channel activity detected"
            )
            return busy
        raise RuntimeError("CAD response missing or malformed")

    # ── Config setters (for runtime reconfiguration) ──────────

    def _run_async_safe(self, coro, wait_timeout: float = 4.0) -> bool:
        """Schedule a command on the radio event loop from any caller thread."""
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._event_loop:

            async def _bg():
                try:
                    response = await coro
                    logger.info("Async CAD config push result: ok=%s", response is not None)
                except Exception as exc:
                    logger.error("Async CAD config push error: %s", exc, exc_info=True)

            asyncio.ensure_future(_bg())
            return True
        if self._event_loop is None:
            coro.close()
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._event_loop)
            return future.result(timeout=wait_timeout) is not None
        except Exception as exc:
            if "future" in locals():
                future.cancel()
            logger.error("Cross-thread CAD config push error: %s", exc, exc_info=True)
            return False

    def _push_cad_config_live(self) -> bool:
        """Push the complete cached CAD tuple when the modem is connected."""
        if self._custom_cad_peak is None or self._custom_cad_min is None:
            return True
        if not self._initialized:
            return True
        if self._event_loop is None or not self._event_loop.is_running():
            return True
        payload = build_cad_params_payload(
            self._custom_cad_symbol_num or 2,
            self._custom_cad_peak,
            self._custom_cad_min,
        )
        return self._run_async_safe(
            self._send_command(
                CMD_SET_CAD_PARAMS,
                payload,
                expect_cmd=CMD_CAD_PARAMS_RESP,
                timeout=3.0,
            )
        )

    def set_custom_cad_thresholds(self, peak: int, min_val: int) -> bool:
        """Cache and live-apply custom CAD detection thresholds."""
        if not (0 <= int(peak) <= 255) or not (0 <= int(min_val) <= 255):
            raise ValueError("CAD thresholds must be between 0 and 255")
        self._custom_cad_peak = int(peak)
        self._custom_cad_min = int(min_val)
        return self._push_cad_config_live()

    def set_custom_cad_symbol_num(self, cad_symbol_num: int) -> bool:
        """Cache and live-apply a validated CAD symbol count."""
        build_cad_params_payload(cad_symbol_num, 0, 0)
        self._custom_cad_symbol_num = int(cad_symbol_num)
        return self._push_cad_config_live()

    def clear_custom_cad_thresholds(self) -> None:
        """Clear host-side CAD thresholds; firmware resets on modem reboot."""
        self._custom_cad_peak = None
        self._custom_cad_min = None

    def set_frequency(self, frequency: int) -> bool:
        self.frequency = frequency
        return True

    def set_tx_power(self, power: int) -> bool:
        self.tx_power = power
        return True

    def set_spreading_factor(self, sf: int) -> bool:
        self.spreading_factor = sf
        return True

    def set_bandwidth(self, bw: int) -> bool:
        self.bandwidth = bw
        return True

    def __del__(self):
        try:
            self.cleanup()
        except Exception:
            pass
