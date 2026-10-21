"""Differential checks against the independent adjacency-based implementation."""
from itertools import combinations

import pytest

from maestro.tasks.pace.parser import ParseError, parse_graph
from maestro.tasks.pace.verifier import verify_graph_solution, verify_solution


def test_every_solution_on_every_labeled_four_vertex_graph():
    possible = list(combinations(range(1, 5), 2))
    for graph_mask in range(1 << len(possible)):
        edges = [edge for i, edge in enumerate(possible) if graph_mask & (1 << i)]
        text = f"p ds 4 {len(edges)}\n" + "".join(f"{u} {v}\n" for u, v in edges)
        graph = parse_graph(text)
        for solution_mask in range(1 << 4):
            vertices = [i + 1 for i in range(4) if solution_mask & (1 << i)]
            output = str(len(vertices)) + "\n" + "\n".join(map(str, vertices))
            n, actual = verify_graph_solution(text, output)
            assert n == 4
            assert actual == verify_solution(graph, output)


@pytest.mark.parametrize("text", [
    "", "p ds 0 0", "p ds 2 1\n1 1", "p ds 2 1\n1 3",
    "p ds 3 2\n1 2\n2 1", "p ds 3 2\n1 2", "p ds 2 0\n1 2",
    "p ds 2 1\n1_0 2", "p ds 2 1\n١ 2", "p ds 2 1\n1 2 3",
    "p ds 2 1\n" + "9" * 5000 + " 2",
    "p ds 2 1\n1 2\x00", "p edge 2 1\n1 2", "p ds 2 1\n++1 2",
])
def test_malformed_graphs_rejected_even_with_malformed_solution(text):
    with pytest.raises(ParseError):
        parse_graph(text)
    with pytest.raises(ParseError):
        verify_graph_solution(text, "bad solution")


@pytest.mark.parametrize("output", [
    "", "-1", "1\n0", "1\n6", "2\n1\n1", "2\n1", "1 1",
    "1\nno", "1\n2\x00", "2\n1 3", "c comment\n1\n+2",
])
def test_solution_boundary_results_equal_reference(output):
    text = "p ds 5 4\n1 2\n2 3\n3 4\n4 5\n"
    assert verify_graph_solution(text, output)[1] == verify_solution(parse_graph(text), output)


@pytest.mark.parametrize("separator", [" ", "\t", "\u2003"])
def test_whitespace_comments_signs_and_reversed_edges_preserved(separator):
    text = f"c note\n# note\np{separator}ds{separator}3{separator}2\n+2{separator}+1\n3{separator}2\n"
    assert verify_graph_solution(text, "1\n2\n")[1] == verify_solution(parse_graph(text), "1\n2\n")
