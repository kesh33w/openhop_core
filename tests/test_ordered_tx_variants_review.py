import copy
import time

import pytest

from openhop_core.protocol import Packet
from openhop_core.protocol.constants import (
    PAYLOAD_TYPE_ADVERT,
    PAYLOAD_TYPE_GRP_TXT,
    PAYLOAD_TYPE_TRACE,
    PH_TYPE_SHIFT,
    ROUTE_TYPE_DIRECT,
    ROUTE_TYPE_FLOOD,
    ROUTE_TYPE_TRANSPORT_FLOOD,
)
from openhop_core.protocol.packet_utils import PathUtils
from openhop_core.rf_fabric.ordered_tx import OrderedTxContext


def packet(width=2):
    p = Packet()
    p.header = ROUTE_TYPE_FLOOD | (PAYLOAD_TYPE_GRP_TXT << PH_TYPE_SHIFT)
    p.path = bytearray(b"\x99" * width + b"\xbd" * width)
    p.path_len = PathUtils.encode_path_len(width, 2)
    p.payload = bytearray(b"abc")
    p.payload_len = 3
    return p


def context(a, b):
    return OrderedTxContext(
        "a",
        ("a", "b"),
        time.monotonic() + 10,
        expected_frames={"a": a.write_to(), "b": b.write_to()},
    )


@pytest.mark.parametrize("width", [1, 2, 3])
def test_only_last_hop_allowed(width):
    a = packet(width)
    b = copy.deepcopy(a)
    b.path[-width:] = b"\x77" * width
    c = context(a, b)
    c.begin("a", a.write_to())
    c.finish("a", True)
    c.begin("b", b.write_to())
    c.finish("b", True)


@pytest.mark.parametrize(
    "attack", ["payload", "priorpath", "header", "pathlen", "direct", "trace", "scope"]
)
def test_malicious_variants_rejected(attack):
    a = packet()
    b = copy.deepcopy(a)
    if attack == "payload":
        b.payload[-1] ^= 1
    elif attack == "priorpath":
        b.path[0] ^= 1
    elif attack == "header":
        b.header = ROUTE_TYPE_FLOOD | (PAYLOAD_TYPE_ADVERT << PH_TYPE_SHIFT)
    elif attack == "pathlen":
        b.path.extend(b"\x77\x77")
        b.path_len = PathUtils.encode_path_len(2, 3)
    elif attack == "direct":
        a.header = b.header = ROUTE_TYPE_DIRECT | (PAYLOAD_TYPE_GRP_TXT << PH_TYPE_SHIFT)
    elif attack == "trace":
        a.header = b.header = ROUTE_TYPE_FLOOD | (PAYLOAD_TYPE_TRACE << PH_TYPE_SHIFT)
    elif attack == "scope":
        a.header = b.header = ROUTE_TYPE_TRANSPORT_FLOOD | (PAYLOAD_TYPE_GRP_TXT << PH_TYPE_SHIFT)
        b.transport_codes = [1, 2]
    with pytest.raises(ValueError):
        context(a, b)


def test_declared_frame_binding_and_snapshot():
    a = packet()
    b = copy.deepcopy(a)
    b.path[-1] ^= 1
    frames = {"a": bytearray(a.write_to()), "b": bytearray(b.write_to())}
    c = OrderedTxContext("a", ("a", "b"), time.monotonic() + 10, expected_frames=frames)
    frames["a"][-1] ^= 1
    with pytest.raises(ValueError):
        c.begin("a", frames["a"])
    with pytest.raises(ValueError):
        c.begin("a", b.write_to())
    c.begin("a", a.write_to())
    with pytest.raises(ValueError):
        c.begin("b", b.write_to())
    c.finish("a", {"ok": False, "tx_outcome_uncertain": True})
    with pytest.raises(ValueError):
        c.begin("b", b.write_to())


def test_targets_exact():
    a = packet()
    with pytest.raises(ValueError):
        OrderedTxContext(
            "a", ("a", "b"), time.monotonic() + 10, expected_frames={"a": a.write_to()}
        )
