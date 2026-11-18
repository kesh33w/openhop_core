"""Optional bounded, observation-only capture. RF callbacks never perform disk I/O."""
import atexit
import json
import os
import queue
import threading
import time
import uuid


class ReceptionCapture:
    def __init__(self, path, deadline, network, *, max_bytes=512*1024*1024, queue_size=4096):
        self.path, self.deadline, self.network = path, deadline, network
        self.max_bytes = max_bytes
        self.queue = queue.Queue(maxsize=queue_size)
        self.lock = threading.Lock()
        self.session = uuid.uuid4().hex
        self.active = True
        self.dropped = self.written = self.errors = self.bytes_written = 0
        self.reason = None
        self.thread = threading.Thread(target=self._run, name='shadow-capture', daemon=True)
        self.stop = threading.Event()
        self.thread.start()
        atexit.register(self.close)

    @classmethod
    def from_environment(cls):
        path = os.environ.get('OPENHOP_RX_CAPTURE_PATH')
        if not path:
            return None
        try:
            deadline = float(os.environ['OPENHOP_RX_CAPTURE_END'])
            if not time.time() < deadline <= time.time()+86400:
                return None
            return cls(path, deadline, os.environ['OPENHOP_RX_CAPTURE_NETWORK'])
        except (ValueError, KeyError, OSError):
            return None

    def emit(self, event, network, **fields):
        if network != self.network:
            return
        with self.lock:
            if time.time() >= self.deadline:
                return
            if not self.active:
                self.dropped += 1
                return
            row = dict(event=event, at=time.time(), monotonic=time.monotonic(), session=self.session, network=network, **fields)
            try:
                self.queue.put_nowait(row)
            except queue.Full:
                self.dropped += 1

    def snapshot(self):
        with self.lock:
            return dict(active=self.active, deadline=self.deadline, network=self.network,
                        queued=self.queue.qsize(), written=self.written, dropped=self.dropped,
                        errors=self.errors, bytes_written=self.bytes_written, reason=self.reason,
                        session=self.session, max_bytes=self.max_bytes)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=2)
        atexit.unregister(self.close)  # Do not keep closed captures alive until exit.

    def _run(self):
        in_hand = False
        try:
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb', buffering=0) as out:
                self.bytes_written = os.fstat(out.fileno()).st_size
                previous_tail_incomplete = bool(self.bytes_written and os.pread(out.fileno(), 1, self.bytes_written-1) != b"\n")
                if previous_tail_incomplete and self.bytes_written < self.max_bytes:
                    if out.write(b"\n") != 1:raise OSError("Incomplete separator write")
                    self.bytes_written += 1
                def write(row):
                    data = (json.dumps(row, separators=(',', ':'))+'\n').encode()
                    if self.bytes_written + len(data) > self.max_bytes - 4096:
                        return False
                    if out.write(data) != len(data):
                        raise OSError("Incomplete capture write")
                    with self.lock:
                        self.bytes_written += len(data)
                        self.written += 1
                    return True
                write(dict(event='start', at=time.time(), session=self.session, network=self.network,
                           deadline=self.deadline, max_bytes=self.max_bytes, previous_tail_incomplete=previous_tail_incomplete))
                last_sync = time.monotonic()
                draining_since = None
                while True:
                    if self.stop.is_set() or time.time() >= self.deadline:
                        with self.lock:
                            self.active = False
                        if draining_since is None:draining_since = time.monotonic()
                        if self.queue.empty() or time.monotonic()-draining_since >= 1:break
                    try:
                        row = self.queue.get(timeout=min(1, max(.001, self.deadline-time.time())))
                    except queue.Empty:
                        row = None
                    if row is not None:
                        in_hand = True
                        if not write(row):
                            with self.lock:
                                self.active = False
                                self.dropped += 1+self.queue.qsize()
                                self.reason = 'byte_limit'
                            in_hand = False
                            break
                        in_hand = False
                    if time.monotonic()-last_sync >= 5:
                        if not write(dict(event='checkpoint', at=time.time(), **self.snapshot())):
                            with self.lock:
                                self.active=False;self.reason='byte_limit';self.dropped+=self.queue.qsize()
                            break
                        os.fsync(out.fileno());last_sync = time.monotonic()
                with self.lock:
                    self.active = False
                    if self.reason is None:
                        self.reason = 'closed' if self.stop.is_set() else 'deadline'
                        self.dropped += self.queue.qsize()+int(in_hand)
                final = dict(event='complete', at=time.time(), **self.snapshot())
                data=(json.dumps(final,separators=(',', ':'))+'\n').encode()
                if self.bytes_written+len(data)<=self.max_bytes:
                    if out.write(data)!=len(data):raise OSError("Incomplete completion write")
                os.fsync(out.fileno())
        except Exception:
            with self.lock:
                self.active = False;self.errors += 1;self.reason = 'io_error'
                self.dropped += self.queue.qsize()+int(in_hand)
