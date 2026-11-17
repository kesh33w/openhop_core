"""Task-local permission for intentional physical copies of one logical TX.

Never disables global packet deduplication. The context has a finite lifetime,
accepts exactly one byte string, and cannot be reused by a detached child task.
"""
from dataclasses import dataclass, field
import time


@dataclass
class OrderedTxContext:
    primary: str
    targets: tuple
    deadline: float
    active: bool = True
    data: bytes | None = None
    claim: tuple | None = None
    claimed: bool = False
    preserve_claim: bool = False
    aborted: bool = False
    inflight: bool = False
    completed: set = field(default_factory=set)

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
        if self.data is not None and self.data != data:
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
