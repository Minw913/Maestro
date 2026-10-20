import json
import hashlib
from pathlib import Path

import pytest

from maestro.tasks.pace.manifest import EvaluationContract, ManifestError, load_panel, assert_smoke_disjoint
from maestro.tasks.pace.parser import ParseError, parse_graph, parse_solution
from maestro.tasks.pace.scoring import aggregate_score, instance_score, validate_reference
from maestro.tasks.pace.verifier import verify_solution


ROOT = Path(__file__).resolve().parents[2]


def test_isolated_vertex_requires_itself():
    graph = parse_graph("c isolated\np ds 1 0\n")
    assert verify_solution(graph, "c solution\n1\n1\n").valid
    assert not verify_solution(graph, "0\n").valid


@pytest.mark.parametrize("output,reason", [
    ("2\n1\n1\n", "duplicate"), ("1\n0\n", "range"),
    ("1\n6\n", "range"), ("2\n1\n", "reported"),
    ("1 1\n", "size"), ("", "no solver output"),
    ("1\nno\n", "integer"), ("-1\n", "negative"),
    ("1\n2\n", "not a dominating set"),
])
def test_invalid_solutions(output, reason):
    graph = parse_graph((ROOT / "fixtures/toy_graphs/star5.gr").read_text())
    result = verify_solution(graph, output)
    assert not result.valid
    assert reason in result.reason


def test_comments_and_vertex_whitespace():
    assert parse_solution("# explain\nc comment\n2\nc skip\n2 4\n") == (2, 4)
    graph = parse_graph("p ds 5 4\n1 2\n2 3\n3 4\n4 5\n")
    assert verify_solution(graph, "2\n2\n4\n").valid


@pytest.mark.parametrize("text", [
    "", "p edge 2 1\n1 2", "p ds 2 1\n1 3", "p ds 2 1\n1 1",
    "p ds 3 2\n1 2\n2 1", "p ds 3 2\n1 2", "p ds 0 0",
    "p ds 2 1\n1 2 3", "p ds 2 0\n1 2", "p ds 2 1\n1_0 2",
])
def test_malformed_graphs(text):
    with pytest.raises(ParseError):
        parse_graph(text)


def test_score_normalization_and_reference_review():
    assert instance_score(2, 6, 2) == 1.0
    assert instance_score(3, 6, 2) == 0.25
    assert instance_score(4, 6, 2) == 0
    assert instance_score(1, 6, 2) == 1.0
    assert aggregate_score([1, 0], candidate_valid=True) == 0.5
    assert aggregate_score([1, 0], candidate_valid=False) == 0
    for bad in (0, -1, 6, None, True):
        with pytest.raises(ValueError):
            validate_reference(6, bad)
    with pytest.raises(ValueError):
        validate_reference(1, 1)


def panel_file(tmp_path, *, reference=1, checksum=None, split="training", relative="graph.gr"):
    data = b"p ds 3 2\n1 2\n2 3\n"
    (tmp_path / "graph.gr").write_bytes(data)
    raw = {"schema_version": 1, "panel_id": "tiny", "split": split,
           "instances": [{"instance_id": "g1", "path": relative,
                          "sha256": checksum or hashlib.sha256(data).hexdigest(),
                          "reference_size": reference}]}
    source = tmp_path / "panel.json"
    source.write_text(json.dumps(raw), encoding="utf-8")
    return source


def test_manifest_is_frozen_and_checks_reference_checksum_split(tmp_path):
    source = panel_file(tmp_path)
    panel = load_panel(source, expected_split="training")
    assert len(panel.instances) == 1
    assert len(panel.panel_hash) == 64
    with pytest.raises(ManifestError, match="split"):
        load_panel(source, expected_split="heldout")
    source = panel_file(tmp_path, reference=None)
    with pytest.raises(ManifestError, match="reference"):
        load_panel(source)
    source = panel_file(tmp_path, checksum="bad")
    with pytest.raises(ManifestError, match="checksum"):
        load_panel(source)
    source = panel_file(tmp_path, relative="../graph.gr")
    with pytest.raises(ManifestError, match="data root"):
        load_panel(source)


def test_panel_hash_relocation_and_disjoint(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    a = load_panel(panel_file(first))
    b = load_panel(panel_file(second))
    assert a.panel_hash == b.panel_hash
    with pytest.raises(ManifestError, match="disjoint"):
        a.assert_disjoint(b)


def test_standard_contract_and_manifest_identity():
    contract = EvaluationContract.standard()
    assert contract.reserved_cpu_seconds == 620
    assert contract.contract_hash != EvaluationContract.standard(seed=1).contract_hash
    with pytest.raises(ManifestError):
        EvaluationContract.standard(instances=1)
    with pytest.raises(ManifestError):
        EvaluationContract(cpus=2)


def test_disjointness_ignores_graph_comments_and_edge_order(tmp_path):
    panel = load_panel(panel_file(tmp_path))
    same_graph = tmp_path / "same.gr"
    same_graph.write_text("c different comments\np ds 3 2\n3 2\n2 1\n")
    with pytest.raises(ManifestError, match="smoke graph"):
        assert_smoke_disjoint(panel, same_graph)
