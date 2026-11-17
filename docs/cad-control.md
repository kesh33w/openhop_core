# Per-radio CAD before transmit

The USB/TCP modem drivers accept optional `cad_enabled: bool | None`.
Unset preserves existing `lbt_enabled` and firmware behavior. Explicit true/false
requires the modem's `CMD_SET_AUTO_CAD` (0x4A) OFF command to return exactly status
0 on response 0x4B. OpenHop then owns pre-transmit CAD: true runs the bounded host
LBT loop; false skips it. This avoids leaving a second firmware CAD gate active
when the UI says off. It does not transmit a test packet.

The mode is restored during reconnect. Unsupported firmware or missing ACK fails
configuration rather than silently claiming bypass. The async setter serializes
with sends and changes host mode only after successful acknowledgement. This is
not a frequency-register readback or proof of over-air delivery.
