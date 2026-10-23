import hashlib
import json
from contextlib import contextmanager

import pytest

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.config import RunConfig
from maestro.tasks.ale import benchmark as ale
from maestro.types import IntegrityError

INPUTS = ['1\n0 0 1\n', '1\n5 5 4\n']


def manifest(tmp_path, **changes):
    data = {'schema_version': 1, 'problem_id': 'ahc001', 'lite_version': False, 'judge_version': '202301',
            'code_language': 'cpp20', 'score_type': 'maximize', 'time_limit': 5.0, 'memory_limit': 2**30,
            'compile_timeout': 60, 'compile_command': 'g++ -o a.out Main.cpp', 'run_command': './a.out',
            'submission_file': 'Main.cpp', 'ale_revision': 'rev', 'reference_candidate_hash': 'ref',
            'cases': [{'input_sha256': hashlib.sha256(i.encode()).hexdigest(), 'reference_score': 100}
                      for i in INPUTS],
            'smoke_input': 'smoke.txt', **changes}
    (tmp_path / 'smoke.txt').write_text('1\n9 9 1\n')
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(data))
    return path


def config(tmp_path, path):
    return RunConfig(benchmark='ale', execution_mode='real', evaluation_instance_count=len(INPUTS),
                     task={'paths': {'manifest': str(path), 'seed_solver': str(tmp_path),
                                     'ale_checkout': str(tmp_path)},
                           'contract': {'image': 'img@sha256:x', 'evaluation_wall_seconds': 60}})


class Pool:
    taken = 0

    @contextmanager
    def slot(self):
        Pool.taken += 1
        yield


def evaluate(tmp_path, monkeypatch, scores, judges):
    path = manifest(tmp_path)
    live = ale.ALE.live(config(tmp_path, path))
    candidate = tmp_path / 'candidate'
    candidate.mkdir()
    (candidate / 'Main.cpp').write_text('int main(){}')
    reply = {'overall_absolute_score': sum(scores), 'cases': [
        {'absolute_score': s, 'judge_result': j, 'execution_time': 1.0, 'memory_usage': 1, 'message': '',
         'input_sha256': hashlib.sha256(i.encode()).hexdigest()} for s, j, i in zip(scores, judges, INPUTS)]}
    monkeypatch.setattr(ale, 'worker_call', lambda python, request, timeout: reply)
    monkeypatch.setattr('maestro.execution.authorization.require_execution_authority', lambda *a, **k: None)
    ticket = {'job_id': 'j', 'candidate_hash': candidate_tree_hash(candidate), 'candidate_path': str(candidate),
              'panel_hash': live.panel_hash, 'contract_hash': live.contract_hash, 'purpose': 'MAIN',
              'effective_instance_workers': 3}
    return live.evaluator(runner=None, artifact_store=None, cpu_pool=Pool()).evaluate(ticket)


def test_case_score_is_reference_normalized_and_clipped():
    assert ale.case_score(50, 100, 'maximize', True) == 0.5
    assert ale.case_score(200, 100, 'maximize', True) == 1.0
    assert ale.case_score(200, 100, 'minimize', True) == 0.5
    assert ale.case_score(50, 100, 'maximize', False) == 0.0


def test_evaluation_averages_cases_and_holds_one_slot_per_worker(tmp_path, monkeypatch):
    Pool.taken = 0
    result = evaluate(tmp_path, monkeypatch, [50, 150], ['ACCEPTED', 'ACCEPTED'])
    assert result.valid and result.score == pytest.approx(0.75)
    assert Pool.taken == 3


def test_any_rejected_case_invalidates_the_candidate(tmp_path, monkeypatch):
    result = evaluate(tmp_path, monkeypatch, [50, 0], ['ACCEPTED', 'WRONG_ANSWER'])
    assert not result.valid and result.score == 0.0 and result.status == 'INVALID'


def test_manifest_rejects_missing_reference_scores(tmp_path):
    with pytest.raises(ValueError, match='reference_score'):
        ale.AleManifest.load(manifest(tmp_path, cases=[{'input_sha256': 'x', 'reference_score': 0}]))


def test_smoke_input_must_not_be_a_public_case(tmp_path):
    path = manifest(tmp_path)
    (tmp_path / 'smoke.txt').write_text(INPUTS[0])
    with pytest.raises(IntegrityError, match='public panel'):
        ale.ALE.smoke_check(config(tmp_path, path))


def test_real_ale_config_names_its_prerequisites():
    with pytest.raises(ValueError, match='task.paths.manifest'):
        RunConfig(benchmark='ale', execution_mode='real')
    with pytest.raises(ValueError, match='task:'):
        RunConfig(benchmark='ale', pace={'panel_manifest': 'x'})
