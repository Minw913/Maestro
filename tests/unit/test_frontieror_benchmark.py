import hashlib
import json
from pathlib import Path

import pytest

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.config import RunConfig
from maestro.execution.cpu_pool import GlobalCpuPool
from maestro.execution.sandbox import ResourceLimits, RunReceipt, RunRequest
from maestro.tasks.frontieror import benchmark as fo
from maestro.types import EvaluationResult, IntegrityError

INSTANCE = '{"num_jobs": 1}'


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def panel(tmp_path: Path, **changes: object) -> RunConfig:
    (tmp_path / 'instances').mkdir()
    (tmp_path / 'checker').mkdir()
    _ = (tmp_path / 'instances' / 'large_1.json').write_text(INSTANCE)
    _ = (tmp_path / 'instances' / 'tiny.json').write_text('{"num_jobs": 0}')
    _ = (tmp_path / 'checker' / 'feasibility_check.py').write_text('# checker')
    data: dict[str, object] = {
        'schema_version': 1, 'paper_id': 'p', 'direction': 'min', 'frontieror_revision': 'rev',
        'time_limit': 30, 'smoke_time_limit': 5,
        'panel': [{'instance': 'large_1', 'file': 'large_1.json', 'input_sha256': digest(INSTANCE),
                   'reference_objective': 100.0}],
        'heldout': [], 'smoke': {'instance': 'tiny', 'file': 'tiny.json', 'input_sha256': digest('{"num_jobs": 0}')},
        'checker_sha256': digest('# checker'), 'reference_source': 'gurobi', **changes}
    _ = (tmp_path / 'manifest.json').write_text(json.dumps(data))
    return RunConfig(benchmark='frontieror', execution_mode='real', evaluation_instance_count=1,
                     cpu_seconds_per_instance=30,
                     task={'paths': {'manifest': str(tmp_path / 'manifest.json'), 'seed_solver': str(tmp_path)},
                           'contract': {'image': 'sha256:' + 'a' * 64, 'memory_mb': 64, 'solution_bytes': 4096,
                                        'checker_seconds': 10}})


class Runner:
    """Answers the solve run with `solution` and the checker run with `verdict`."""

    def __init__(self, solution: dict[str, object], verdict: dict[str, object], *, timed_out: bool = False):
        self.solution: dict[str, object] = solution
        self.verdict: dict[str, object] = verdict
        self.timed_out: bool = timed_out
        self.limits: list[ResourceLimits] = []

    def run(self, request: RunRequest, limits: ResourceLimits) -> RunReceipt:
        self.limits.append(limits)
        solving = 'code.py' in request.argv[-1]
        if solving and self.timed_out:
            return RunReceipt(124, '', 'killed', limits.cpu_seconds, limits.wall_seconds, timed_out=True)
        return RunReceipt(0, json.dumps(self.solution if solving else self.verdict), '', 0.1, 0.1)


def evaluate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner: Runner) -> EvaluationResult:
    live = fo.FRONTIEROR.live(panel(tmp_path))
    candidate = tmp_path / 'candidate'
    candidate.mkdir()
    _ = (candidate / 'code.py').write_text('print(1)')

    def authorized(ticket: object, *, begin: bool = False, launch: bool = False) -> None:
        del ticket, begin, launch

    monkeypatch.setattr('maestro.execution.authorization.require_execution_authority', authorized)
    ticket: dict[str, object] = {
        'job_id': 'j', 'candidate_hash': candidate_tree_hash(candidate), 'candidate_path': str(candidate),
        'panel_hash': live.panel_hash, 'contract_hash': live.contract_hash, 'purpose': 'MAIN'}
    return live.evaluator(runner=runner, artifact_store=None, cpu_pool=GlobalCpuPool(1)).evaluate(ticket)


def test_instance_score_is_gurobi_normalized_and_clipped() -> None:
    assert fo.instance_score(200, 100, 'min') == 0.5
    assert fo.instance_score(80, 100, 'min') == 1.0
    assert fo.instance_score(50, 100, 'max') == 0.5
    assert fo.instance_score(0, 100, 'max') == 0.0


def test_feasible_answer_scores_against_the_reference(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = evaluate(tmp_path, monkeypatch, Runner({'objective_value': 400}, {'feasible': True}))
    assert result.valid and result.score == pytest.approx(0.25)


def test_infeasible_answer_invalidates_the_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = evaluate(tmp_path, monkeypatch, Runner({'objective_value': 1},
                                                    {'feasible': False, 'violated_constraints': [3]}))
    assert not result.valid and result.score == 0.0
    assert result.instance_results[0]['reason'] == 'infeasible: 3'


def test_overrun_is_killed_at_the_time_limit_without_grace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = Runner({'objective_value': 1}, {'feasible': True}, timed_out=True)
    result = evaluate(tmp_path, monkeypatch, runner)
    assert not result.valid
    assert runner.limits[0].wall_seconds == runner.limits[0].cpu_seconds == 30


def test_time_limit_must_match_the_config(tmp_path: Path) -> None:
    config = panel(tmp_path, time_limit=60)
    with pytest.raises(IntegrityError, match='time_limit'):
        _ = fo.FRONTIEROR.live(config)


def test_real_frontieror_config_names_its_prerequisites() -> None:
    with pytest.raises(ValueError, match='task.paths.manifest'):
        _ = RunConfig(benchmark='frontieror', execution_mode='real')
