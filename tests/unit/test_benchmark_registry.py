import json

import pytest

from maestro.config import RunConfig
from maestro.tasks.base import BENCHMARKS, benchmark_for
from maestro.types import canonical_json, stable_hash


def test_unknown_benchmark_is_rejected():
    with pytest.raises(ValueError, match="benchmark must be one of"):
        RunConfig(benchmark="nope")
    with pytest.raises(ValueError, match="unknown benchmark"):
        benchmark_for("nope")


def test_every_registered_adapter_names_itself():
    for name in BENCHMARKS:
        assert benchmark_for(name).name == name


def test_pace_rejects_a_task_section():
    with pytest.raises(ValueError, match="pace:"):
        RunConfig(task={"problem_id": "x"})


def test_empty_task_section_is_omitted_from_identity():
    config = RunConfig()
    stored = json.loads(canonical_json(config))
    assert "task" not in stored and "task" not in config.to_dict()
    assert stable_hash(stored) == config.hash
