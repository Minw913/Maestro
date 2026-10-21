import json
import sqlite3
from dataclasses import replace

import pytest
from maestro.config import RunConfig, load_config
from maestro.cli import build_parser, _config
from maestro.storage import Ledger
from maestro.types import IntegrityError, stable_hash
from maestro.loop import ExperimentEngine


@pytest.mark.parametrize('key', ['mode', 'panel_size', 'instance_cpu_seconds', 'initialization_mode'])
def test_old_fields_rejected_even_with_new_fields(key):
    with pytest.raises(ValueError, match='Unknown config keys'):
        RunConfig.from_dict({**RunConfig().to_dict(), key: 'obsolete'})


@pytest.mark.parametrize('values', [{'execution_mode': 'pace'}, {'benchmark': 'other'}])
def test_invalid_execution_contract(values):
    with pytest.raises(ValueError):
        RunConfig(**values)


def test_budget_and_serialization():
    c = RunConfig(evaluation_instance_count=7, cpu_seconds_per_instance=11)
    assert c.reserved_cpu_seconds == 97
    assert RunConfig.from_dict(c.to_dict()) == c
    assert c.effective_instance_workers(1, evaluation_instance_count=2) == 1


def test_cli_override_and_help(tmp_path):
    path = tmp_path/'config.json'
    path.write_text(json.dumps({'execution_mode':'mock', 'benchmark':'pace'}))
    args = build_parser().parse_args(['run','--bootstrap','unused','--config',str(path),
        '--execution-mode','real','--benchmark','pace'])
    assert _config(args).execution_mode == 'real'
    assert _config(args).benchmark == 'pace'
    with pytest.raises(SystemExit):
        build_parser().parse_args(['bootstrap', '--mode', 'pace'])


@pytest.mark.parametrize('suffix,text', [('.json','{"execution_mode":"mock","execution_mode":"real"}'),
    ('.yaml','execution_mode: mock\nexecution_mode: real\n')])
def test_duplicate_fields_rejected(tmp_path, suffix, text):
    path = tmp_path/('config'+suffix); path.write_text(text)
    with pytest.raises(ValueError, match='Duplicate config key'):
        load_config(path)


def test_old_ledger_rejected_without_mutation(tmp_path):
    path = tmp_path/'old.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)')
        db.execute("INSERT INTO metadata VALUES('schema_version','1')")
    before = path.read_bytes()
    with pytest.raises(IntegrityError, match='old experiments'):
        Ledger(path)
    assert path.read_bytes() == before


def test_old_checkpoint_rejected(tmp_path):
    payload = {'schema_version':1, 'config':{'mode':'mock'}}
    path = tmp_path/'old.json'
    path.write_text(json.dumps({'payload':payload, 'artifact_hash':stable_hash(payload)}))
    with ExperimentEngine(tmp_path/'run', RunConfig()) as engine:
        with pytest.raises(IntegrityError, match='old experiments'):
            engine.run(path)


def test_resume_execution_change_rejected(tmp_path):
    with ExperimentEngine(tmp_path, RunConfig()) as engine:
        engine.ledger.create_episode('e','first_price',engine.config,{})
        engine.config = replace(engine.config, execution_mode='real')
        with pytest.raises(IntegrityError, match='Resume config hash mismatch'):
            engine.resume('e')


def test_cli_resume_rejects_override_before_backend_start(tmp_path, monkeypatch):
    from maestro.cli import main
    with Ledger(tmp_path/'authority'/'ledger.sqlite') as ledger:
        ledger.create_episode('e', 'first_price', RunConfig(), {})
    def forbidden(*args, **kwargs):
        pytest.fail('Resume mismatch must be rejected before backend construction')
    monkeypatch.setattr('maestro.loop.ExperimentEngine', forbidden)
    assert main(['--root', str(tmp_path), 'resume', '--episode', 'e',
                 '--execution-mode', 'real', '--benchmark', 'pace']) == 2


def test_benchmark_override_validation():
    with pytest.raises(SystemExit):
        build_parser().parse_args(['resume', '--episode', 'e', '--benchmark', 'unsupported'])


@pytest.mark.parametrize('name', ['mock_smoke', 'experiments/pace_remote'])
def test_shipped_presets_use_new_interface(name):
    from pathlib import Path
    config = load_config(Path(__file__).resolve().parents[2]/'configs'/f'{name}.yaml')
    assert config.benchmark == 'pace'
    assert config.execution_mode == ('mock' if name == 'mock_smoke' else 'real')
    assert config.reserved_cpu_seconds == 620


def test_preparation_defaults_and_independent_override(tmp_path):
    defaults = RunConfig()
    assert (defaults.preparation_calls, defaults.preparation_window_seconds) == (4, 300)
    path = tmp_path/'preparation.yaml'
    path.write_text('preparation_calls: 6\npreparation_window_seconds: 450\n')
    config = load_config(path)
    assert (config.preparation_calls, config.preparation_window_seconds) == (6, 450)
    assert RunConfig.from_dict(config.to_dict()) == config
    assert config.hash != defaults.hash
    assert (config.planning_calls, config.generation_calls, config.runtime_repair_calls) == (1, 1, 3)
    assert (config.planning_window_seconds, config.generation_window_seconds) == (120, 300)
    assert config.reserved_cpu_seconds == defaults.reserved_cpu_seconds


@pytest.mark.parametrize('value', [0, -1, 1.5, True, '4', None])
def test_preparation_request_count_validation(value):
    with pytest.raises(ValueError, match='preparation_calls'):
        RunConfig(preparation_calls=value)


@pytest.mark.parametrize('value', [0, -1, float('inf'), float('nan'), True, '300', None])
def test_preparation_window_validation(value):
    with pytest.raises(ValueError, match='preparation_window_seconds'):
        RunConfig(preparation_window_seconds=value)


def test_resume_rejects_changed_preparation_budget(tmp_path):
    with ExperimentEngine(tmp_path, RunConfig()) as engine:
        engine.ledger.create_episode('e', 'first_price', engine.config, {})
        engine.config = replace(engine.config, preparation_calls=5)
        with pytest.raises(IntegrityError, match='Resume config hash mismatch'):
            engine.resume('e')


@pytest.mark.parametrize('key,value', [
    ('preparation_cpu_seconds', 5),
    ('exploration_calls', 4),
    ('exploration_window_seconds', 300),
])
def test_superseded_preparation_fields_rejected(tmp_path, key, value):
    path = tmp_path/'obsolete.json'
    path.write_text(json.dumps({**RunConfig().to_dict(), key: value}))
    with pytest.raises(ValueError, match='Unknown config keys'):
        load_config(path)
