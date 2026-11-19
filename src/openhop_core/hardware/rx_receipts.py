"""Reader-ordered fences for an optional local raw-RF cancellation subscriber.

No radio writes. Checkpoints run in the actual reader after parsing the current
buffer, never directly from the event loop that requests a fence.
"""

import asyncio
import threading
from collections import deque


class RxReceiptFence:
    def __init__(self):
        self._lock = threading.Lock()
        self._generation = 0
        self._waiting = deque()

    @property
    def generation(self):
        with self._lock:
            return self._generation

    def invalidate(self):
        with self._lock:
            self._generation += 1

    async def wait(self):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        with self._lock:
            if len(self._waiting) >= 16:
                raise RuntimeError("Reader fence queue full")
            self._waiting.append((loop, future, self._generation))
        try:
            return await asyncio.wait_for(future, 1.5)
        finally:
            with self._lock:
                self._waiting = deque(item for item in self._waiting if item[1] is not future)

    def checkpoint(self):
        """Reader thread only, after all complete frames in its buffer dispatch."""
        if not self._waiting:
            return
        with self._lock:
            waiting, self._waiting = self._waiting, deque()
        for loop, future, generation in waiting:

            def finish(future=future, generation=generation):
                if future.done():
                    return
                if self.generation != generation:
                    future.set_exception(RuntimeError("Reader continuity changed"))
                else:
                    future.set_result(generation)

            try:
                loop.call_soon_threadsafe(finish)
            except RuntimeError:
                self.invalidate()
