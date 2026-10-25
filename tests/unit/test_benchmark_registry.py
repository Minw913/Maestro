import pytest

from maestro.config import RunConfig
from maestro.tasks import fields
from maestro.tasks.base import BENCHMARKS, benchmark_for
from maestro.types import canonical_json, stable_hash


def test_unknown_benchmark_is_rejected() -> None:
    with pytest.raises(ValueError, match="benchmark must be one of"):
        _ = RunConfig(benchmark="nope")
    with pytest.raises(ValueError, match="unknown benchmark"):
        _ = benchmark_for("nope")


def test_every_registered_adapter_names_itself() -> None:
    for name in BENCHMARKS:
        assert benchmark_for(name).name == name


def test_pace_rejects_a_task_section() -> None:
    with pytest.raises(ValueError, match="pace:"):
        _ = RunConfig(task={"problem_id": "x"})


def test_empty_task_section_is_omitted_from_identity() -> None:
    config = RunConfig()
    stored = fields.parse_object(canonical_json(config), "stored config")
    assert "task" not in stored and "task" not in config.to_dict()
    assert stable_hash(stored) == config.hash


def test_pace_refuses_a_smoke_graph_that_is_not_a_path() -> None:
    from maestro.tasks.pace.benchmark import PACE
    config = RunConfig(coral={"smoke_graph": 5}, pace={"panel_manifest": "unused.json"})
    with pytest.raises(fields.FieldError, match="smoke_graph must be a path"):
        _ = PACE.smoke_check(config)
