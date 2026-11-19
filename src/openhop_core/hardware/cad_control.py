"""Explicit CAD mode: host owns LBT; modem auto-CAD must acknowledge OFF.

Leaving the option unset preserves existing host and firmware behavior. An
explicit mode is replayed on reconnect before the radio becomes ready. No RF
packet is sent by this control command.
"""

import logging

log = logging.getLogger(__name__)


class CadControlMixin:
    def _init_cad_control(self, enabled, legacy_lbt):
        if enabled is not None and type(enabled) is not bool:
            raise ValueError("cad.enabled must be a boolean")
        self._cad_mode_explicit = enabled
        self.lbt_enabled = legacy_lbt if enabled is None else enabled

    def _restore_cad_mode_sync(self):
        if self._cad_mode_explicit is None:
            return True
        if not self._write_cad_mode_sync():
            log.error("Modem did not acknowledge auto-CAD OFF; explicit CAD mode not ready")
            return False
        self.lbt_enabled = self._cad_mode_explicit
        return True

    async def set_cad_enabled(self, enabled):
        """Confirm firmware bypass before changing host mode; serialize with TX."""
        if type(enabled) is not bool:
            raise ValueError("enabled must be a boolean")
        async with self._tx_lock:
            if not self._initialized:
                raise RuntimeError("Radio is not connected")
            response = await self._send_command(0x4A, b"\x00", expect_cmd=0x4B, timeout=3.0)
            if response != b"\x00":
                raise RuntimeError("Modem did not acknowledge CAD control; host CAD mode unchanged")
            self._cad_mode_explicit = enabled
            self.lbt_enabled = enabled
            return True
