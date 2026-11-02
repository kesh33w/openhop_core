"""Use existing signal-free channel frames for non-RF observations."""
from unittest.mock import Mock
import pytest
from openhop_core.companion.frame_server import CompanionFrameServer
from openhop_core.companion.models import QueuedMessage
from openhop_core.companion.constants import RESP_CODE_CHANNEL_MSG_RECV, RESP_CODE_CHANNEL_MSG_RECV_V3


@pytest.mark.parametrize('version', [0, 3, 15])
def test_unknown_signal_uses_legacy_frame_even_for_modern_clients(version):
    server = CompanionFrameServer(Mock(), 'hash', port=0)
    server._app_target_ver = version
    msg = QueuedMessage(sender_key=b'', timestamp=7, text='Peer: message', is_channel=True,
                        channel_idx=2, path_len=1, snr=None, rssi=None)
    frame = server._build_message_frame(msg)
    assert frame[0] == RESP_CODE_CHANNEL_MSG_RECV
    assert frame[1:4] == bytes([2, 1, 0])
    assert frame[8:] == b'Peer: message'


def test_known_zero_snr_is_a_measurement_not_unknown():
    server = CompanionFrameServer(Mock(), 'hash', port=0)
    server._app_target_ver = 3
    msg = QueuedMessage(sender_key=b'', is_channel=True, text='Peer: real RF', snr=0.0, rssi=-90)
    frame = server._build_message_frame(msg)
    assert frame[0] == RESP_CODE_CHANNEL_MSG_RECV_V3 and frame[1] == 0


@pytest.mark.asyncio
async def test_channel_event_keeps_missing_metrics_in_queue():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from openhop_core.companion.base_events import _RxEventsMixin
    from openhop_core.companion.message_queue import MessageQueue
    owner = SimpleNamespace(channels=None, message_queue=MessageQueue(),
                            _seen_grp_txt=Mock(), _fire_callbacks=AsyncMock())
    owner._seen_grp_txt.check_and_add.return_value = False
    await _RxEventsMixin._handle_new_channel_message(owner, {
        'packet_hash': 'fixture', 'full_content': 'Peer: translated', 'path_len': 1,
        'network_info': {'snr': None, 'rssi': None},
    })
    queued = owner._fire_callbacks.call_args.args[1].queue_entry
    assert queued.snr is None and queued.rssi is None
