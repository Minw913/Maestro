"""Offline coverage for benchmark adapters combined with the prebid Market."""
import json
from types import SimpleNamespace
from pathlib import Path

import pytest

from maestro.cli import main
from maestro.config import RunConfig, load_config
from maestro.loop import ExperimentEngine
from maestro.replay import verify_ledger
from maestro.storage import Ledger
from maestro.tasks.base import BENCHMARKS, benchmark_for
from maestro.types import canonical_json, stable_hash


@pytest.mark.parametrize('benchmark', sorted(BENCHMARKS))
@pytest.mark.parametrize('policy', ['uniform', 'ucb', 'first_price_naive'])
def test_prebid_benchmark_identity_survives_warm_market_and_replay(tmp_path, benchmark, policy):
    config = RunConfig(benchmark=benchmark, rounds=1, warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path, config) as engine:
        warm = engine.bootstrap()
        episode = engine.run(warm['checkpoint'], policy)['episode_id']
        rows = engine.ledger.connection.execute(
            'SELECT payload FROM candidates').fetchall()
        assert rows
        for row in rows:
            candidate = json.loads(row[0])
            body = engine.artifacts.get_json(candidate['candidate_hash'])
            assert body['benchmark'] == benchmark
        assert engine.ledger.connection.execute(
            'SELECT count(*) FROM eval_tickets WHERE episode_id=?', (episode,)).fetchone()[0] == config.slots
    assert verify_ledger(tmp_path)['verified']


def test_ale_presets_load_with_current_market_contract():
    repo = Path(__file__).parents[2]
    for path in ('configs/mock_smoke_ale.yaml', 'configs/experiments/ale_local.yaml'):
        config = load_config(repo / path, stage='market')
        assert config.benchmark == 'ale'
        assert config.coral['candidate_pipeline'] == 'prebid_v2'
        assert 'bid_cap' not in config.to_dict()


@pytest.mark.parametrize('benchmark', ['ale', 'frontiercs2', 'mlebench'])
def test_final_eval_dispatches_adapter_without_constructing_search(tmp_path, monkeypatch, benchmark):
    paths = dict(manifest='unused', seed_solver='unused', ale_checkout='unused', mlebench_venv='unused')
    config = RunConfig(benchmark=benchmark, execution_mode='real', task={
        'paths': paths, 'contract': {'image': 'unused', 'evaluation_wall_seconds': 1, 'pids': 8}})
    # Include a retired scheduling field: final evaluation must use only the
    # frozen task settings and leave the historical configuration untouched.
    raw = {**config.to_dict(), 'bid_cap': '100', 'coral': {'candidate_pipeline': 'postbid_v1'}}
    with Ledger(tmp_path / 'authority' / 'ledger.sqlite') as ledger:
        ledger.create_episode('source', 'uniform', config, {})
        with ledger.transaction() as db:
            db.execute("UPDATE episodes SET status='COMPLETED',config_json=?,config_hash=? WHERE id='source'",
                       (canonical_json(raw), stable_hash(raw)))
    seen = []
    def final(adapter, context, episode, manifest):
        assert adapter.name == benchmark and context.config.task == config.task
        assert context.config.execution_mode == 'real'
        assert episode == 'source' and manifest == 'private-panel'
        seen.append(episode)
        return {'ok': True}
    adapter_type = type(benchmark_for(benchmark))
    evaluator = SimpleNamespace(preflight=lambda: seen.append('preflight'))
    live = SimpleNamespace(evaluator=lambda **kwargs: evaluator)
    monkeypatch.setattr(adapter_type, 'live', lambda *args: live)
    monkeypatch.setattr(adapter_type, 'image', lambda *args: 'offline')
    monkeypatch.setattr(adapter_type, 'evaluate_final', final)
    def forbidden(*args, **kwargs):
        pytest.fail('final evaluation constructed a search backend')
    monkeypatch.setattr('maestro.loop.ExperimentEngine', forbidden)
    assert main(['--root', str(tmp_path), 'final-eval', '--episode', 'source', '--manifest', 'private-panel']) == 0
    assert seen == ['preflight', 'source']
    with Ledger(tmp_path / 'authority' / 'ledger.sqlite', read_only=True) as ledger:
        stored = ledger.connection.execute("SELECT config_json FROM episodes WHERE id='source'").fetchone()[0]
        assert json.loads(stored) == raw
