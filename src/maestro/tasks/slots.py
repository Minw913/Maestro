"""Taking several CPU slots for one evaluation without deadlocking concurrent evaluations."""
from __future__ import annotations

import threading
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

    from maestro.execution.cpu_pool import GlobalCpuPool

# One lock for every benchmark: an evaluation takes all its slots before the next starts taking any, so no two
# evaluations can each hold part of what the other is waiting for.
_ACQUIRE = threading.Lock()


@contextmanager
def held(pool: GlobalCpuPool, count: int) -> Generator[None]:
    """`count` slots of `pool`, taken atomically and released together."""
    with ExitStack() as stack:
        with _ACQUIRE:
            for _ in range(count):
                stack.enter_context(pool.slot())
        yield
