import io
import json
import subprocess
import tarfile
from pathlib import Path

import pytest

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.config import RunConfig
from maestro.execution.sandbox import ResourceLimits, RunReceipt, RunRequest
from maestro.tasks.frontiercs2 import benchmark as fcs
from maestro.types import IntegrityError

POLICY = 'pkg/policy.py'


def manifest(tmp_path: Path, **changes: object) -> fcs.FrontierCSManifest:
    (tmp_path / 'base' / 'pkg').mkdir(parents=True)
    _ = (tmp_path / 'base' / POLICY).write_text('def rewrite(plan):\n    return plan\n')
    _ = (tmp_path / 'base' / 'README.md').write_text('# task\n')
    (tmp_path / 'datasets').mkdir()
    for mode in ('train', 'eval'):
        _ = (tmp_path / 'datasets' / f'{mode}.json').write_text(json.dumps({'mode': mode}))
    import hashlib
    data: dict[str, object] = {
        'schema_version': 1, 'task': 'Database/demo', 'problem_id': 'demo', 'frontiercs_revision': 'rev',
        'verifier_image': 'sha256:' + 'b' * 64, 'agent_image': 'sha256:' + 'c' * 64, 'allow_paths': [POLICY],
        'allow_empty': False, 'max_changed_files': 1, 'max_patch_bytes': 4096, 'timeout_seconds': 900.0,
        'cpus': 2.0, 'memory_mb': 64,
        'datasets': {m: hashlib.sha256((tmp_path / 'datasets' / f'{m}.json').read_bytes()).hexdigest()
                     for m in ('train', 'eval')},
        'base_hash': candidate_tree_hash(tmp_path / 'base'), **changes}
    _ = (tmp_path / 'manifest.json').write_text(json.dumps(data))
    return fcs.FrontierCSManifest.load(tmp_path / 'manifest.json')


def candidate(tmp_path: Path, policy: str, extra: dict[str, str] | None = None) -> Path:
    import shutil
    root = tmp_path / 'candidate'
    _ = shutil.copytree(tmp_path / 'base', root)
    _ = (root / POLICY).write_text(policy)
    for name, text in (extra or {}).items():
        _ = (root / name).write_text(text)
    return root


class Runner:
    """Returns `stdout` as the verifier's output and records the request."""

    def __init__(self, stdout: str, *, timed_out: bool = False):
        self.stdout: str = stdout
        self.timed_out: bool = timed_out
        self.requests: list[RunRequest] = []
        self.payload: bytes = b''

    def run(self, request: RunRequest, limits: ResourceLimits) -> RunReceipt:
        del limits  # the fake verifier ignores containment limits
        self.requests.append(request)
        self.payload = (request.mounts[0].source / 'answer.tar').read_bytes()
        return RunReceipt(0, self.stdout, '', 1.0, 1.0, timed_out=self.timed_out)


def test_patch_matches_the_official_maker_and_applies(tmp_path: Path) -> None:
    m = manifest(tmp_path)
    cand = candidate(tmp_path, 'def rewrite(plan):\n    return plan[::-1]\n')
    patch = fcs.make_patch(tmp_path / 'base', cand, m)
    assert patch.startswith(f'diff --git a/{POLICY} b/{POLICY}')
    target = tmp_path / 'applied'
    _ = subprocess.run(['cp', '-r', str(tmp_path / 'base'), str(target)], check=True)
    _ = subprocess.run(['git', 'apply', '-'], input=patch, text=True, cwd=target, check=True)
    assert (target / POLICY).read_text() == (cand / POLICY).read_text()


def test_contract_violations_are_named(tmp_path: Path) -> None:
    m = manifest(tmp_path)
    cand = candidate(tmp_path, 'def rewrite(plan):\n    return plan\n', {'stray.py': 'x = 1\n'})
    problems = fcs.patch_problems(tmp_path / 'base', cand, m, fcs.make_patch(tmp_path / 'base', cand, m))
    assert any('stray.py' in p for p in problems) and any('empty' in p for p in problems)


def test_problem_md_and_unchanged_files_pass(tmp_path: Path) -> None:
    m = manifest(tmp_path)
    cand = candidate(tmp_path, 'def rewrite(plan):\n    return list(plan)\n', {'PROBLEM.md': '# brief\n'})
    assert fcs.patch_problems(tmp_path / 'base', cand, m, fcs.make_patch(tmp_path / 'base', cand, m)) == []


def test_payload_holds_exactly_the_patch() -> None:
    with tarfile.open(fileobj=io.BytesIO(fcs.payload_tar('diff --git a/x b/x\n')), mode='r:') as archive:
        assert archive.getnames() == ['payload', fcs.PATCH_NAME]
        member = archive.extractfile(fcs.PATCH_NAME)
        assert member is not None and member.read() == b'diff --git a/x b/x\n'


@pytest.mark.parametrize(('stdout', 'valid', 'score'), [
    ('{"status": "passed", "raw_score": 42.0, "summary": "ok", "metrics": {}}', True, 42.0),
    ('{"status": "failed", "summary": "bad patch", "metrics": {}}', False, 0.0),
    ('not json', False, 0.0)])
def test_verdict_follows_the_verifier_result(tmp_path: Path, stdout: str, valid: bool, score: float) -> None:
    m = manifest(tmp_path)
    cand = candidate(tmp_path, 'def rewrite(plan):\n    return plan[:]\n')
    runner = Runner(stdout)
    verdict = fcs.verify(cand, 'train', data_dir=tmp_path, manifest=m, runner=runner, pids=64)
    assert (verdict.valid, verdict.raw_score) == (valid, score)
    assert 'FRONTIER_VERIFICATION_MODE=train' in runner.requests[0].argv[-1]
    assert runner.requests[0].image == m.verifier_image


def test_a_timed_out_verifier_is_invalid(tmp_path: Path) -> None:
    m = manifest(tmp_path)
    runner = Runner('{"status": "passed", "raw_score": 99.0, "summary": "late"}', timed_out=True)
    verdict = fcs.verify(candidate(tmp_path, 'pass\n'), 'train', data_dir=tmp_path, manifest=m, runner=runner, pids=64)
    assert not verdict.valid


def test_a_changed_dataset_selector_is_refused(tmp_path: Path) -> None:
    m = manifest(tmp_path)
    _ = (tmp_path / 'datasets' / 'train.json').write_text('{"mode": "tampered"}')
    with pytest.raises(IntegrityError, match='dataset selector'):
        _ = fcs.verify(candidate(tmp_path, 'pass\n'), 'train', data_dir=tmp_path, manifest=m, runner=Runner('{}'), pids=64)


def test_config_cpu_budget_must_match_the_task(tmp_path: Path) -> None:
    _ = manifest(tmp_path)
    config = RunConfig(benchmark='frontiercs2', execution_mode='real', evaluation_instance_count=1,
                       cpu_seconds_per_instance=60,
                       task={'paths': {'manifest': str(tmp_path / 'manifest.json'), 'seed_solver': str(tmp_path)},
                             'contract': {'pids': 64}})
    with pytest.raises(IntegrityError, match='cpus x timeout_seconds'):
        _ = fcs.FRONTIERCS2.live(config)
