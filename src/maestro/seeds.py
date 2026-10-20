"""Independent deterministic random streams; no mutable global RNG."""
from maestro.types import stable_hash


def derive_seed(seed: int, *parts: object) -> int:
    return int(stable_hash([seed, *parts])[:16], 16)
