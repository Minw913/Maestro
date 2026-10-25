import random
import threading
import time
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from typing import override

from maestro.execution.cpu_pool import GlobalCpuPool
from maestro.tasks.slots import held

POOL = 4
PAUSE = 0.05  # long enough that every worker takes its first slot before any takes a second


class SlowPool(GlobalCpuPool):
    """A real pool that pauses after granting each slot and tracks how many are in use."""

    def __init__(self) -> None:
        super().__init__(POOL)
        self.lock: threading.Lock = threading.Lock()
        self.in_use: int = 0
        self.peak: int = 0

    @override
    @contextmanager
    def slot(self, *, deadline: float | None = None) -> Generator[None]:
        with super().slot(deadline=deadline):
            with self.lock:
                self.in_use += 1
                self.peak = max(self.peak, self.in_use)
            time.sleep(PAUSE)
            try:
                yield
            finally:
                with self.lock:
                    self.in_use -= 1


@contextmanager
def unlocked(pool: GlobalCpuPool, count: int) -> Generator[None]:
    """The old pattern: slots taken one at a time with no shared lock."""
    with ExitStack() as stack:
        for _ in range(count):
            stack.enter_context(pool.slot())
        yield


def finishes(take: Callable[[GlobalCpuPool, int], AbstractContextManager[None]], pool: GlobalCpuPool,
             wants: list[int], timeout: float) -> bool:
    """True when every worker gets all its slots and releases them within `timeout`."""
    def work(count: int) -> None:
        with take(pool, count):
            pass

    workers = [threading.Thread(target=work, args=(n,), daemon=True) for n in wants]
    for worker in workers:
        worker.start()
    deadline = time.monotonic() + timeout
    for worker in workers:
        worker.join(max(0.0, deadline - time.monotonic()))
    return not any(worker.is_alive() for worker in workers)


def test_the_unlocked_pattern_deadlocks() -> None:
    # Control: each of POOL workers takes one slot during the pause, then all wait for a second that never frees.
    assert not finishes(unlocked, SlowPool(), [2] * POOL, timeout=POOL * 4 * PAUSE + 1)


def test_held_survives_the_same_interleaving() -> None:
    pool = SlowPool()
    assert finishes(held, pool, [2] * POOL, timeout=POOL * 4 * PAUSE + 5)
    assert pool.in_use == 0 and pool.peak <= POOL


def test_held_under_random_load_never_overcommits() -> None:
    pool, rng = SlowPool(), random.Random(0)
    wants = [rng.randint(1, POOL) for _ in range(40)]
    assert finishes(held, pool, wants, timeout=sum(wants) * PAUSE * 2 + 10)
    assert pool.in_use == 0 and pool.peak <= POOL
