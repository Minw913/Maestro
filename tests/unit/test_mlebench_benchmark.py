import json
from pathlib import Path

import pytest

from maestro.config import RunConfig
from maestro.execution.sandbox import ResourceLimits, RunReceipt, RunRequest
from maestro.tasks.mlebench import benchmark as mle
from maestro.types import IntegrityError

REPO = Path(__file__).parents[2]
SPOOKY = REPO / 'data' / 'mlebench' / 'spooky-author-identification'
VENV = REPO / 'repos' / 'mle-bench' / '.venv'


def panel(tmp_path: Path, **changes: object) -> mle.MleBenchManifest:
    for split in mle.SPLITS:
        (tmp_path / split / 'inputs').mkdir(parents=True)
        _ = (tmp_path / split / 'inputs' / 'train.csv').write_text('id,y\n1,0\n')
        (tmp_path / split / 'answers').mkdir()
        _ = (tmp_path / split / 'answers' / 'test.csv').write_text('id,y\n2,1\n')
    _ = (tmp_path / 'leaderboard.csv').write_text('score\n0.5\n')
    data: dict[str, object] = {
        'schema_version': 1, 'competition': 'demo', 'mlebench_revision': 'rev', 'image': 'sha256:' + 'a' * 64,
        'timeout_seconds': 60.0, 'cpus': 2.0, 'memory_mb': 64, 'answers': 'test.csv',
        'hashes': {key: mle.content_hash(tmp_path / key) for key in mle.HASHED}, **changes}
    _ = (tmp_path / 'manifest.json').write_text(json.dumps(data))
    return mle.MleBenchManifest.load(tmp_path / 'manifest.json')


class Runner:
    """Returns a fixed receipt and records the request."""

    def __init__(self, returncode: int, stdout: str = '') -> None:
        self.receipt: RunReceipt = RunReceipt(returncode, stdout, 'boom', 1.0, 1.0)
        self.requests: list[RunRequest] = []

    def run(self, request: RunRequest, limits: ResourceLimits) -> RunReceipt:
        del limits  # the fake candidate ignores containment limits
        self.requests.append(request)
        return self.receipt


def test_manifest_needs_every_hash(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match='hashes must name exactly'):
        _ = panel(tmp_path, hashes={'train/inputs': 'x'})


def test_changed_answers_are_refused_before_running(tmp_path: Path) -> None:
    m = panel(tmp_path)
    _ = (tmp_path / 'train' / 'answers' / 'test.csv').write_text('id,y\n2,0\n')
    runner = Runner(0)
    with pytest.raises(IntegrityError, match='train/answers changed'):
        _ = mle.run_and_grade(tmp_path, 'train', data_dir=tmp_path, manifest=m, runner=runner, pids=8,
                              mlebench_python='unused')
    assert runner.requests == []


def test_a_failing_solution_is_invalid_and_never_graded(tmp_path: Path) -> None:
    m = panel(tmp_path)
    runner = Runner(1)
    grade = mle.run_and_grade(tmp_path, 'train', data_dir=tmp_path, manifest=m, runner=runner, pids=8,
                              mlebench_python='/nonexistent/python')
    assert not grade.valid and grade.beaten == 0.0 and 'solution failed' in grade.summary
    request = runner.requests[0]
    assert request.image == m.image and [mount.target for mount in request.mounts] == ['/candidate', '/input']
    assert all(mount.read_only for mount in request.mounts) and request.mounts[1].source == tmp_path / 'train' / 'inputs'


def test_config_cpu_budget_must_match_the_panel(tmp_path: Path) -> None:
    _ = panel(tmp_path)
    config = RunConfig(benchmark='mlebench', execution_mode='real', evaluation_instance_count=1,
                       cpu_seconds_per_instance=60,
                       task={'paths': {'manifest': str(tmp_path / 'manifest.json'), 'seed_solver': str(tmp_path),
                                       'mlebench_venv': str(tmp_path)},
                             'contract': {'pids': 64}})
    with pytest.raises(IntegrityError, match='cpus x timeout_seconds'):
        _ = mle.MLEBENCH.live(config)


@pytest.mark.skipif(not (SPOOKY / 'manifest.json').is_file() or not (VENV / 'bin' / 'python').is_file(),
                    reason='needs scripts/setup_mlebench.sh spooky-author-identification')
def test_the_real_grader_scores_the_sample_submission() -> None:
    m = mle.MleBenchManifest.load(SPOOKY / 'manifest.json')
    sample = (SPOOKY / 'train' / 'inputs' / 'sample_submission.csv').read_text()
    result = mle.grade_submission(sample, 'train', data_dir=SPOOKY, manifest=m,
                                  mlebench_python=str(VENV / 'bin' / 'python'))
    assert result['lower_is_better'] is True and result['teams'] == 1242
    assert result['score'] == pytest.approx(1.09146) and result['beaten'] == pytest.approx(0.0862, abs=1e-4)
    rejected = mle.grade_submission('id,EAP\n', 'train', data_dir=SPOOKY, manifest=m,
                                    mlebench_python=str(VENV / 'bin' / 'python'))
    assert rejected['score'] is None
