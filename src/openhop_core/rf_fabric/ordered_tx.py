"""Task-local permission for intentional physical copies of one logical TX.

Never disables global packet deduplication. The context has a finite lifetime,
accepts one byte string or explicitly declared final-hop flood variants, and
cannot be reused by a detached child task.
"""
from dataclasses import dataclass, field
import time


@dataclass
class OrderedTxContext:
    primary: str
    targets: tuple
    deadline: float
    expected_frames: dict | None = None
    active: bool = True
    data: bytes | None = None
    claim: tuple | None = None
    claimed: bool = False
    preserve_claim: bool = False
    aborted: bool = False
    inflight: bool = False
    completed: set = field(default_factory=set)

    def __post_init__(self):
        if self.expected_frames is None:
            return
        from ..protocol.packet import Packet
        from ..protocol.constants import PAYLOAD_TYPE_TRACE
        if set(self.expected_frames) != set(self.targets):
            raise ValueError("Expected frames must cover ordered targets exactly")
        signatures = []
        for raw in self.expected_frames.values():
            packet = Packet(); packet.read_from(raw)
            width = packet.get_path_hash_size()
            if (not packet.is_route_flood() or packet.get_payload_type() == PAYLOAD_TYPE_TRACE
                    or len(packet.path) < width):
                raise ValueError("Only flood final-hop variants are permitted")
            signatures.append((packet.header, packet.path_len, bytes(packet.path[:-width]),
                               bytes(packet.payload), tuple(packet.transport_codes)))
        if any(sig != signatures[0] for sig in signatures[1:]):
            raise ValueError("Ordered variants may differ only in final path hash")
        self.expected_frames = {k: bytes(v) for k, v in self.expected_frames.items()}

    def validate(self, target):
        if not self.active or time.monotonic() >= self.deadline:
            raise ValueError("Ordered TX context expired or closed")
        if self.aborted:
            raise ValueError("Ordered TX aborted after uncertain result")
        if target not in self.targets:
            raise ValueError("Target outside ordered TX group")

    def begin(self, target, data):
        self.validate(target)
        if self.inflight or target in self.completed:
            raise ValueError("Ordered TX leg already sent or another leg in flight")
        if self.expected_frames is not None:
            if self.expected_frames[target] != bytes(data):
                raise ValueError("Ordered TX frame differs from declared leg")
        elif self.data is not None and self.data != data:
            raise ValueError("Ordered TX group must send identical bytes")
        self.data = bytes(data)
        self.inflight = True

    def finish(self, target, result, exception=False):
        self.inflight = False
        confirmed = result is True or (isinstance(result, dict) and result.get("ok") is True)
        uncertain = exception or (isinstance(result, dict) and bool(result.get("tx_outcome_uncertain")))
        # Unknown vendor return values are not evidence that the transmitter stopped.
        definite_failure = result is False or (isinstance(result, dict) and result.get("ok") is False and not uncertain)
        if confirmed or uncertain or not definite_failure:
            self.completed.add(target)
            self.preserve_claim = True
        if uncertain or not (confirmed or definite_failure):
            self.aborted = True
