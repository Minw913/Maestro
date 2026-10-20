"""The manifest-normalized internal score, never official competition results."""


def validate_reference(n: int, reference_size: int) -> int:
    if isinstance(n, bool) or isinstance(reference_size, bool):
        raise ValueError("graph and reference sizes must be integers")
    if not isinstance(n, int) or not isinstance(reference_size, int):
        raise ValueError("graph and reference sizes must be integers")
    upper = min(n, 2 * reference_size)
    if not 0 < reference_size < upper:
        raise ValueError(f"reference requires 0 < k* < min(n, 2k*); n={n}, k*={reference_size}")
    return upper


def instance_score(size: int, n: int, reference_size: int) -> float:
    upper = validate_reference(n, reference_size)
    if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= n:
        raise ValueError("solution size must be an integer in [0,n]")
    fraction = max(0.0, min(1.0, (upper - size) / (upper - reference_size)))
    return fraction * fraction


def aggregate_score(scores: list[float] | tuple[float, ...], *, candidate_valid: bool) -> float:
    if not scores:
        raise ValueError("cannot aggregate an empty panel")
    if any(not 0 <= score <= 1 for score in scores):
        raise ValueError("instance scores must be finite and normalized")
    return sum(scores) / len(scores) if candidate_valid else 0.0
