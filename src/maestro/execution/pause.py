"""Cooperative controller pause: reject new work, drain existing requests."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import threading
import time
import uuid

from maestro.types import MaestroError


class PauseRequested(MaestroError):
    pass


class PauseController:
    grace_seconds = 30.0

    def __init__(self, root):
        from maestro.storage.paths import run_paths
        self.path = run_paths(root).local / 'authority' / 'pause-request.json'
        self.legacy_path = Path(root) / 'authority' / 'pause-request.json'
        self._at = None

    def request(self):
        # The local clock starts when this controller first observes a pause.
        # A marker's untrusted wall-clock timestamp never extends the grace.
        if self._at is None:
            self._at = time.monotonic()
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_name('pause-' + uuid.uuid4().hex + '.tmp')
            temporary.write_text(json.dumps({'requested_utc': datetime.now(timezone.utc).isoformat(),
                'action': 'stop_new_dispatch_and_drain', 'grace_seconds': self.grace_seconds}))
            temporary.replace(self.path)

    @property
    def requested(self):
        if self._at is None and (self.path.exists() or self.legacy_path.exists()):
            self._at = time.monotonic()
        return self._at is not None

    def check(self):
        if self.requested:
            raise PauseRequested('Experiment paused; no new model calls, repairs, waves or evaluations')

    def drain_deadline(self, deadline):
        return min(deadline, self._at + self.grace_seconds) if self.requested else deadline

    @contextmanager
    def signals(self):
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        def request(signum, frame):
            # Signal handlers only set a local flag. Normal controller/transport
            # execution writes the durable marker; never acquire SQLite locks here.
            if self._at is None:
                self._at = time.monotonic()
        try:
            for s in previous:
                signal.signal(s, request)
            yield
        finally:
            for s, handler in previous.items():
                signal.signal(s, handler)
