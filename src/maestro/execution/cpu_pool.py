"""A shared CPU permit pool for one MAESTRO controller process.

Solver and verifier launches use the same pool. Independent controllers must
use separate CPU reservations or an external executor with a shared authority;
this object does not coordinate unrelated processes on the same machine.
"""
from collections import deque
from contextlib import contextmanager
import math
from threading import Condition
import time


class CpuPoolUnavailable(RuntimeError):
    pass


class GlobalCpuPool:
    def __init__(self, cpu_workers: int):
        if type(cpu_workers) is not int or cpu_workers < 1:
            raise ValueError("cpu_workers must be a positive integer")
        self.capacity = cpu_workers
        self._available = cpu_workers
        self._condition = Condition()
        self._waiting = deque()
        self._poisoned = None

    def poison(self, reason):
        """Stop further launches when a previous process may still be alive."""
        with self._condition:
            self._poisoned = str(reason)
            self._condition.notify_all()

    @contextmanager
    def slot(self, *, deadline=None):
        if deadline is not None and (type(deadline) not in {int, float} or not math.isfinite(deadline)):
            raise ValueError("A finite monotonic CPU wait deadline is required")
        marker = object()
        with self._condition:
            self._waiting.append(marker)
            try:
                while True:
                    if self._poisoned is not None:
                        raise CpuPoolUnavailable("CPU pool requires executor reconciliation: " + self._poisoned)
                    remaining = None if deadline is None else deadline-time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError("CPU permit was unavailable before the phase deadline")
                    if self._waiting[0] is marker and self._available:
                        self._waiting.popleft()
                        self._available -= 1
                        self._condition.notify_all()
                        break
                    self._condition.wait(timeout=remaining)
            except BaseException:
                self._waiting.remove(marker)
                self._condition.notify_all()
                raise
        try:
            yield
        except BaseException as exc:
            if getattr(exc, "execution_unknown", False):
                self.poison(exc)
            raise
        finally:
            with self._condition:
                self._available += 1
                self._condition.notify_all()
