"""Trusted linear-time dominating-set verification."""

from dataclasses import dataclass

from .parser import Graph, ParseError, parse_solution


@dataclass(frozen=True)
class Verification:
    valid: bool
    size: int | None
    vertices: tuple[int, ...] = ()
    reason: str = ""
    undominated: tuple[int, ...] = ()


def verify_solution(graph: Graph, output: str) -> Verification:
    try:
        vertices = parse_solution(output)
    except ParseError as exc:
        return Verification(False, None, reason=str(exc))
    if len(set(vertices)) != len(vertices):
        return Verification(False, len(vertices), vertices, "duplicate vertex IDs")
    if any(vertex < 1 or vertex > graph.n for vertex in vertices):
        return Verification(False, len(vertices), vertices, "vertex ID out of range")
    dominated = set(vertices)
    for vertex in vertices:
        dominated.update(graph.adjacency[vertex])
    missing = tuple(vertex for vertex in range(1, graph.n + 1) if vertex not in dominated)
    if missing:
        return Verification(False, len(vertices), vertices, "not a dominating set", missing)
    return Verification(True, len(vertices), vertices)


def verify_graph_solution(text: str, output: str) -> tuple[int, Verification]:
    """Validate a graph and a solution without allocating adjacency sets.

    This preserves the strict graph parser's checks, including duplicate edges.
    Selected vertices mark their opposite endpoints while edges are scanned.
    Packed edge identities and byte arrays replace per-vertex Python sets. The
    ordinary parser/verifier remain an independent reference implementation.
    """
    import re
    from .parser import _integer

    if "\x00" in text:
        raise ParseError("NUL bytes are not valid PACE text")
    lines = iter(text.splitlines())
    n = m = None
    for raw in lines:
        line = raw.strip()
        if not line or line == "c" or line.startswith(("c ", "c\t", "#")):
            continue
        header = line.split()
        if len(header) != 4 or header[:2] != ["p", "ds"]:
            raise ParseError("expected p ds n m")
        n, m = (_integer(token, "graph header") for token in header[2:])
        break
    if n is None:
        raise ParseError("missing p ds n m header")
    if n < 1 or m < 0 or m > n * (n - 1) // 2:
        raise ParseError("invalid simple graph vertex/edge counts")

    rejected = None
    try:
        vertices = parse_solution(output)
        if len(set(vertices)) != len(vertices):
            rejected = Verification(False, len(vertices), vertices, "duplicate vertex IDs")
        elif any(vertex < 1 or vertex > n for vertex in vertices):
            rejected = Verification(False, len(vertices), vertices, "vertex ID out of range")
    except ParseError as exc:
        vertices = ()
        rejected = Verification(False, None, reason=str(exc))
    selected, dominated = bytearray(n + 1), bytearray(n + 1)
    if rejected is None:
        for vertex in vertices:
            selected[vertex] = dominated[vertex] = 1

    # ASCII integers match _integer; Unicode whitespace matches str.split().
    match_edge = re.compile(r"([+-]?[0-9]+)\s+([+-]?[0-9]+)\Z").fullmatch
    edges = set()
    add_edge = edges.add
    for raw in lines:
        line = raw.strip()
        if not line or line == "c" or line.startswith(("c ", "c\t", "#")):
            continue
        fields = match_edge(line)
        if fields is None:
            raise ParseError("expected two integer edge endpoints")
        try:
            a, b = int(fields[1]), int(fields[2])
        except ValueError as exc:
            raise ParseError("edge endpoint exceeds the integer parser limit") from exc
        if not 1 <= a <= n or not 1 <= b <= n or a == b:
            raise ParseError("endpoint out of range or self loop")
        edge = a * (n + 1) + b if a < b else b * (n + 1) + a
        if edge in edges:
            raise ParseError("duplicate undirected edge")
        add_edge(edge)
        if selected[a]:
            dominated[b] = 1
        if selected[b]:
            dominated[a] = 1
    if len(edges) != m:
        raise ParseError(f"header declares {m} edges; found {len(edges)}")
    if rejected is not None:
        return n, rejected
    missing = tuple(vertex for vertex in range(1, n + 1) if not dominated[vertex])
    if missing:
        return n, Verification(False, len(vertices), vertices, "not a dominating set", missing)
    return n, Verification(True, len(vertices), vertices)
