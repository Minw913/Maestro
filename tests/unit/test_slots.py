from maestro.execution.cpu_pool import GlobalCpuPool
from maestro.tasks.slots import held


def test_held_takes_and_releases_every_slot() -> None:
    pool = GlobalCpuPool(3)
    with held(pool, 3):
        assert pool._available == 0  # pyright: ignore[reportPrivateUsage]  # the pool exposes no public count
    assert pool._available == 3  # pyright: ignore[reportPrivateUsage]
