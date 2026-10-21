"""Layer composition and frozen stage identities; no inference or PACE execution."""
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from maestro.cli import _config, build_parser
from maestro.config import RunConfig, load_config
from maestro.loop import ExperimentEngine
from maestro.types import IntegrityError


@pytest.fixture
def manifest(tmp_path):
    source=Path(__file__).resolve().parents[2]/'configs'
    target=tmp_path/'configs'
    shutil.copytree(source,target)
    path=target/'experiments/pace_remote.yaml'
    edit(path,execution_mode='mock')
    edit(target/'benchmarks/pace/warm_start.yaml',warm_opportunities_per_agent=1,planning_window_seconds=90)
    edit(target/'benchmarks/pace/market.yaml',rounds=1,planning_window_seconds=150)
    return path


def edit(path,**changes):
    value=yaml.safe_load(path.read_text());value.update(changes)
    path.write_text(yaml.safe_dump(value))


def fragment(manifest,name):
    return manifest.parent.parent/name


def test_composition_and_roundtrip(manifest,monkeypatch,tmp_path):
    monkeypatch.chdir(tmp_path)
    warm=load_config(manifest)
    market=load_config(manifest,stage='market')
    assert warm.experiment_stage=='warm_start'
    assert (warm.planning_window_seconds,market.planning_window_seconds)==(90,150)
    assert warm.for_stage('market')==market
    assert warm.hash!=market.hash
    assert warm.shared_config_hash==market.shared_config_hash
    assert len(warm.config_sources)==6
    assert Path(warm.pace['panel_manifest'])==manifest.parent.parent.parent/'data/training/panel.json'
    assert warm.coral['backend_factory']=='maestro.backends.coral.native_backend:native_factory'
    assert warm.reserved_cpu_seconds==620
    assert RunConfig.from_dict(json.loads(json.dumps(warm.to_dict())))==warm
    market.validate_warm_source(warm)


@pytest.mark.parametrize('name,changes',[
    ('runtimes/coral_codex.yaml',{'cpu_workers':20}),
    ('deployments/remote.yaml',{'planning_calls':1}),
    ('benchmarks/pace/benchmark.yaml',{'benchmark':'other'}),
    ('benchmarks/pace/warm_start.yaml',{'rounds':2}),
    ('benchmarks/pace/market.yaml',{'generation_calls':0}),
    ('runtimes/coral_codex.yaml',{'includes':['other.yaml']}),
    ('benchmarks/pace/market.yaml',{'kind':'warm_start'}),
])
def test_bad_fragments_rejected_even_inactive(manifest,name,changes):
    edit(fragment(manifest,name),**changes)
    with pytest.raises(ValueError):
        load_config(manifest)


def test_contract_conflict(manifest):
    edit(fragment(manifest,'benchmarks/pace/benchmark.yaml'),cpu_seconds_per_instance=99)
    with pytest.raises(ValueError,match='contract conflict'):
        load_config(manifest)


def test_invalid_path_interpolation(manifest):
    path=fragment(manifest,'deployments/remote.yaml')
    value=yaml.safe_load(path.read_text());value['coral']['checkout_path']='$HOME/coral'
    path.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError,match='interpolation'):
        load_config(manifest)


@pytest.mark.parametrize('reference',['missing.yaml','pace_remote.yaml'])
def test_missing_or_cyclic_reference(manifest,reference):
    edit(manifest,runtime=reference)
    with pytest.raises((ValueError,FileNotFoundError)):
        load_config(manifest)


def test_source_and_active_drift_rejected(manifest):
    warm=load_config(manifest)
    market=warm.for_stage('market')
    with pytest.raises(IntegrityError,match='frozen stage'):
        replace(market,planning_window_seconds=200).validate_warm_source(warm)
    with pytest.raises(IntegrityError,match='shared configuration'):
        replace(market,seed=7).validate_warm_source(warm)
    path=fragment(manifest,'benchmarks/pace/market.yaml')
    path.write_text(path.read_text()+'\n# changed source\n')
    with pytest.raises(IntegrityError,match='shared configuration'):
        load_config(manifest,stage='market').validate_warm_source(warm)
    with pytest.raises(IntegrityError,match='layout mismatch'):
        RunConfig().validate_warm_source(warm)


def test_cli_stage_and_overrides(manifest):
    parser=build_parser()
    warm=_config(parser.parse_args(['bootstrap','--config',str(manifest)]))
    market=_config(parser.parse_args(['run','--bootstrap','unused','--config',str(manifest),'--seed','7','--execution-mode','real','--benchmark','pace']))
    assert warm.experiment_stage=='warm_start'
    assert market.experiment_stage=='market'
    assert market.seed==7 and market.execution_mode=='real'


def test_mock_stage_transition_and_resume(manifest,tmp_path):
    root=tmp_path/'artifacts'
    warm=load_config(manifest)
    with ExperimentEngine(root,warm) as engine:
        result=engine.bootstrap()
    args=build_parser().parse_args(['--root',str(root),'run','--bootstrap',result['checkpoint']])
    market=_config(args)
    assert market==warm.for_stage('market')
    with ExperimentEngine(root,market) as engine:
        engine.run(result['checkpoint'],episode_id='market-test')
    args=build_parser().parse_args(['--root',str(root),'resume','--episode','market-test'])
    assert _config(args)==market
    assert (root/'authority/config.warm_start.resolved.json').exists()
    assert (root/'authority/config.market.resolved.json').exists()
    from maestro.replay import verify_ledger
    verify_ledger(root)


def test_cli_drift_rejected_before_backend(manifest,tmp_path,monkeypatch):
    from maestro.cli import main
    warm=load_config(manifest)
    root=tmp_path/'run'
    with ExperimentEngine(root,warm) as engine:
        checkpoint=engine.bootstrap()['checkpoint']
    edit(fragment(manifest,'benchmarks/pace/market.yaml'),rounds=2)
    def forbidden(*args,**kwargs):
        pytest.fail('Drift must be rejected before backend creation')
    monkeypatch.setattr('maestro.loop.ExperimentEngine',forbidden)
    assert main(['--root',str(root),'run','--bootstrap',checkpoint,'--config',str(manifest)])==2


def test_cli_resume_rejects_changed_manifest_before_backend(manifest,tmp_path,monkeypatch):
    from maestro.cli import main
    from maestro.storage import Ledger
    root=tmp_path/'resume'
    config=load_config(manifest,stage='market')
    with Ledger(root/'authority/ledger.sqlite') as ledger:
        ledger.create_episode('e','first_price',config,{})
    edit(fragment(manifest,'benchmarks/pace/market.yaml'),planning_window_seconds=180)
    def forbidden(*args,**kwargs):
        pytest.fail('Resume drift must be rejected before backend creation')
    monkeypatch.setattr('maestro.loop.ExperimentEngine',forbidden)
    assert main(['--root',str(root),'resume','--episode','e','--config',str(manifest)])==2


def test_duplicate_fragment_keys_rejected(manifest):
    path=fragment(manifest,'benchmarks/pace/market.yaml')
    path.write_text(path.read_text()+'rounds: 2\n')
    with pytest.raises(ValueError,match='Duplicate config key'):
        load_config(manifest)


def test_container_argv_not_resolved(manifest):
    path=fragment(manifest,'benchmarks/pace/benchmark.yaml')
    value=yaml.safe_load(path.read_text())
    value['pace']['contract']['solver_argv']=['/candidate/run','--input','/input/graph.gr']
    path.write_text(yaml.safe_dump(value))
    assert load_config(manifest).pace['contract']['solver_argv']==['/candidate/run','--input','/input/graph.gr']


def test_wrong_stage_cannot_bootstrap(manifest,tmp_path):
    with ExperimentEngine(tmp_path/'wrong',load_config(manifest,stage='market')) as engine:
        with pytest.raises(IntegrityError,match='frozen warm_start'):
            engine.bootstrap()
