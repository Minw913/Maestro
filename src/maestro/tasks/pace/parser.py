"""Strict parsers independent of the candidate executable and its claims."""

from dataclasses import dataclass


class ParseError(ValueError):
    """Malformed graph or solution."""


@dataclass(frozen=True)
class Graph:
    n: int
    m: int
    # Index zero is unused; PACE IDs remain one based throughout verification.
    adjacency: tuple[frozenset[int], ...]


def _lines(text: str):
    if "\x00" in text:
        raise ParseError("NUL bytes are not valid PACE text")
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line == "c" or line.startswith(("c ", "c\t", "#")):
            continue
        yield number, line.split()


def _integer(token: str, description: str) -> int:
    try:
        # Reject int()'s accepted underscores and Unicode decimal digits.
        unsigned = token.lstrip("+-")
        if not unsigned or not unsigned.isascii() or not unsigned.isdecimal():
            raise ValueError
        return int(token)
    except ValueError as exc:
        raise ParseError(f"{description}: expected an integer, got {token!r}") from exc


def parse_graph(text: str) -> Graph:
    lines = iter(_lines(text))
    try:
        number, header = next(lines)
    except StopIteration as exc:
        raise ParseError("missing p ds n m header") from exc
    if len(header) != 4 or header[:2] != ["p", "ds"]:
        raise ParseError(f"line {number}: expected p ds n m")
    n, m = (_integer(x, "graph header") for x in header[2:])
    if n < 1 or m < 0 or m > n * (n - 1) // 2:
        raise ParseError("invalid simple graph vertex/edge counts")
    adjacency: list[set[int]] = [set() for _ in range(n + 1)]
    edges: set[tuple[int, int]] = set()
    for number, fields in lines:
        if len(fields) != 2:
            raise ParseError(f"line {number}: expected two edge endpoints")
        a, b = (_integer(x, f"line {number}") for x in fields)
        if not 1 <= a <= n or not 1 <= b <= n or a == b:
            raise ParseError(f"line {number}: endpoint out of range or self loop")
        edge = (min(a, b), max(a, b))
        if edge in edges:
            raise ParseError(f"line {number}: duplicate undirected edge")
        edges.add(edge)
        adjacency[a].add(b)
        adjacency[b].add(a)
    if len(edges) != m:
        raise ParseError(f"header declares {m} edges; found {len(edges)}")
    return Graph(n=n, m=m, adjacency=tuple(frozenset(x) for x in adjacency))


def parse_solution(text: str) -> tuple[int, ...]:
    lines = iter(_lines(text))
    try:
        number, size = next(lines)
    except StopIteration as exc:
        raise ParseError("no solver output") from exc
    if len(size) != 1:
        raise ParseError(f"line {number}: first non-comment line must contain only the size")
    count = _integer(size[0], "reported solution size")
    if count < 0:
        raise ParseError("negative reported solution size")
    ids = tuple(_integer(token, f"line {number}")
                for number, fields in lines for token in fields)
    if len(ids) != count:
        raise ParseError(f"reported {count} vertices; found {len(ids)}")
    return ids
