"""Offline regressions for archived readers and the model-free setup entry."""
import json
from types import SimpleNamespace

import pytest

from maestro.config import RunConfig
from maestro.replay import verify_ledger
from maestro.reports import report
from maestro.storage import ArtifactStore, Ledger
from maestro.types import EvaluationResult, IntegrityError, MaestroError, canonical_json, stable_hash


@pytest.fixture(params=[False, True])
def archive(tmp_path, request):
    config = RunConfig()
    raw = config.to_dict()
    raw.pop('valuation_discount_factor')
    raw.pop('ucb_exploration_coefficient')
    raw['bid_cap'] = '100'
    raw['coral'] = {'candidate_pipeline': 'postbid_v1'} if request.param else {}
    ArtifactStore(tmp_path / 'authority/cas/sha256')
    with Ledger(tmp_path / 'authority/ledger.sqlite') as ledger:
        ledger.create_episode('archive', 'uniform', config, {})
        with ledger.transaction() as db:
            db.execute('UPDATE episodes SET config_json=?,config_hash=? WHERE id=?',
                       (canonical_json(raw), stable_hash(raw), 'archive'))
    return tmp_path, raw


def test_archived_report_and_replay_keep_original_wire_identity(archive):
    root, raw = archive
    ledger_path = root / 'authority/ledger.sqlite'
    before = ledger_path.read_bytes()
    assert verify_ledger(root)['verified']
    result = report(root)
    assert result['episodes'][0]['effective_rounds'] == raw['rounds']
    assert result['episodes'][0]['config_hash'] == stable_hash(raw)
    assert ledger_path.read_bytes() == before
    with pytest.raises(ValueError, match='Unknown config keys'):
        RunConfig.from_dict(raw)


def test_archived_report_still_rejects_config_tampering(archive):
    root, raw = archive
    with Ledger(root / 'authority/ledger.sqlite') as ledger:
        with ledger.transaction() as db:
            db.execute('UPDATE episodes SET config_json=?',
                       (canonical_json({**raw, 'rounds': raw['rounds'] + 1}),))
    with pytest.raises(IntegrityError, match='tampered'):
        report(root)


def test_no_model_check_reaches_ticketed_setup_without_an_agent(tmp_path, monkeypatch, capsys):
    import check_benchmark
    seed = tmp_path / 'seed'
    seed.mkdir()
    (seed / 'run').write_text('print(1)')
    calls = []
    class Evaluator:
        def preflight(self):
            calls.append('preflight')
        def evaluate(self, ticket):
            assert ticket['purpose'] == 'SETUP'
            calls.append('evaluate')
            return EvaluationResult(ticket['job_id'], ticket['candidate_hash'], 0.5, True)
    live = SimpleNamespace(panel=None, contract=None, panel_hash='panel', contract_hash='contract',
                           evaluator=lambda **kwargs: Evaluator())
    benchmark = SimpleNamespace(live=lambda config: live, image=lambda config: 'offline',
        seed_solver=lambda config: str(seed), smoke_check=lambda config: lambda *args: (True, 'pass'))
    config = RunConfig(execution_mode='real', experiment_stage='warm_start')
    monkeypatch.setattr(check_benchmark, 'load_config', lambda *args, **kwargs: config)
    monkeypatch.setattr(check_benchmark, 'benchmark_for', lambda name: benchmark)
    monkeypatch.setattr('maestro.loop.benchmark_for', lambda name: benchmark)
    check_benchmark.main('unused', str(tmp_path / 'check'))
    assert calls == ['preflight', 'evaluate']
    assert json.loads(capsys.readouterr().out.splitlines()[-1])['formal_F'] == 0.5
    with Ledger(tmp_path / 'check/authority/ledger.sqlite', read_only=True) as ledger:
        assert ledger.connection.execute('SELECT status FROM jobs').fetchone()[0] == 'RESULT_RECORDED'

    from maestro.loop import ExperimentEngine
    backend = check_benchmark.NoModelBackend(object())
    with pytest.raises(MaestroError, match='separated implementation'):
        ExperimentEngine(tmp_path / 'search', config, backend=backend)
    with ExperimentEngine(tmp_path / 'setup-only', config, backend=backend, evaluation_only=True) as engine:
        for action in (engine.bootstrap, lambda: engine.run('unused'), lambda: engine.resume('unused')):
            with pytest.raises(MaestroError, match='Evaluation-only setup cannot'):
                action()
        assert not engine.ledger.connection.execute('SELECT 1 FROM episodes').fetchone()
