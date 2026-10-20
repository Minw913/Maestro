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
