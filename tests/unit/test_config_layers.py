"""Layer composition and frozen stage identities; no inference or PACE execution."""
import json
import shutil
from dataclasses import replace
from decimal import Decimal
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
    assert RunConfig.from_dict(json.loads(json.dumps(warm.to_dict()))).to_dict()==warm.to_dict()
    assert 'initial_wealth' not in warm.to_dict()
    assert all('initial_wealth' not in stage for stage in warm.to_dict()['stage_configs'].values())
    market.validate_warm_source(warm)


def test_layered_market_round_extension_accepts_only_horizon_and_source_digest(manifest):
    from maestro.market.round_extension import validate_extension
    previous=load_config(manifest,stage='market')
    edit(fragment(manifest,'benchmarks/pace/market.yaml'),rounds=3)
    extended=load_config(manifest,stage='market')
    assert extended.rounds==3 and previous.rounds==1
    assert validate_extension(previous,extended)==Decimal(0)
    with pytest.raises(IntegrityError,match='beyond Market rounds'):
        validate_extension(previous,replace(extended,slots=1))


def test_warm_checkpoint_can_start_shorter_market_with_only_rounds_changed(manifest,tmp_path):
    from maestro.replay import verify_ledger
    market_file=fragment(manifest,'benchmarks/pace/market.yaml')
    edit(market_file,rounds=20)
    warm=load_config(manifest,stage='warm_start')
    with ExperimentEngine(tmp_path/'short-market',warm) as engine:
        checkpoint=engine.bootstrap()['checkpoint']
    edit(market_file,rounds=10)
    market=load_config(manifest,stage='market').with_market_credits(0)
    market.validate_warm_source(warm)
    with ExperimentEngine(tmp_path/'short-market',market) as engine:
        result=engine.run(checkpoint,'uniform',episode_id='short-market')
        assert result['status']=='COMPLETED' and result['rounds']==10
    assert verify_ledger(tmp_path/'short-market')['verified']
    for target in (12,14,16):
        edit(market_file,rounds=target)
        extended=load_config(manifest,stage='market').with_market_credits(0)
        with ExperimentEngine(tmp_path/'short-market',extended) as engine:
            assert engine.resume('short-market')['rounds']==target
        assert verify_ledger(tmp_path/'short-market')['verified']


def test_prebid_pipeline_is_explicit_frozen_and_cannot_reuse_legacy_warm():
    configs=Path(__file__).resolve().parents[2]/'configs/experiments'
    old=load_config(configs/'pace_remote_deepseek.yaml')
    new=load_config(configs/'pace_remote_deepseek_prebid.yaml')
    market=new.for_stage('market')
    assert old.coral['candidate_pipeline']=='prebid_v2'
    assert new.coral['candidate_pipeline']=='prebid_v2'
    assert new.hash!=old.hash  # source-file identities still distinguish aliases
    assert (new.generation_calls,new.runtime_repair_calls,new.generation_window_seconds)==(1,3,360)
    market.validate_warm_source(new)
    retired=replace(old,coral={**old.coral,'candidate_pipeline':'postbid_v1'})
    with pytest.raises(IntegrityError):market.validate_warm_source(retired)
    with pytest.raises(ValueError):replace(new,coral={**new.coral,'candidate_pipeline':'unknown'})


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
    edit(path,rounds=2)
    load_config(manifest,stage='market').validate_warm_source(warm)
    edit(path,planning_window_seconds=151)
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


def test_cli_round_horizon_override_preserves_warm_stage(manifest):
    parser=build_parser()
    warm=_config(parser.parse_args(['bootstrap','--config',str(manifest)]))
    market=_config(parser.parse_args(['run','--bootstrap','unused','--config',str(manifest),
                                      '--rounds','10','--credits','0']))
    assert market.rounds==market.stage_configs['market']['rounds']==10
    assert market.stage_configs['warm_start']==warm.stage_configs['warm_start']
    assert market.config_sources==warm.config_sources
    market.validate_warm_source(warm)
    with pytest.raises(ValueError,match='positive integer'):
        _config(parser.parse_args(['run','--bootstrap','unused','--config',str(manifest),'--rounds','0']))


def test_credits_override_freezes_market_stage_before_bootstrap(manifest):
    parser=build_parser()
    warm=_config(parser.parse_args(['bootstrap','--config',str(manifest)]))
    market=_config(parser.parse_args(['run','--bootstrap','unused','--config',str(manifest),'--credits','0']))
    with pytest.raises(SystemExit):parser.parse_args(['bootstrap','--config',str(manifest),'--credits','0'])
    assert market.initial_wealth==0
    assert market.effective_initial_wealth==market.rounds*125
    market.validate_warm_source(warm)
    edit(fragment(manifest,'benchmarks/pace/market.yaml'),initial_wealth='7')
    load_config(manifest,stage='market').validate_warm_source(warm)
    with pytest.raises(ValueError):
        _config(parser.parse_args(['run','--bootstrap','unused','--config',str(manifest),'--credits','-1']))


def test_zero_credits_checkpoint_run_and_resume_guard(manifest,tmp_path):
    parser=build_parser();root=tmp_path/'ample'
    warm=_config(parser.parse_args(['bootstrap','--config',str(manifest)]))
    with ExperimentEngine(root,warm) as engine:
        checkpoint=engine.bootstrap()['checkpoint']
    with pytest.raises(ValueError,match='Select Market credits'):
        _config(parser.parse_args(['run','--bootstrap',checkpoint]))
    market=_config(parser.parse_args(['run','--bootstrap',checkpoint,'--credits','0']))
    assert market.initial_wealth==0
    market.validate_warm_source(warm)
    with ExperimentEngine(root,market) as engine:
        assert engine.run(checkpoint,'first_price_naive',episode_id='ample-market')['status']=='COMPLETED'
    finite=load_config(manifest,stage='market')
    finite.validate_warm_source(warm)
    with ExperimentEngine(root,finite) as engine:
        assert engine.run(checkpoint,'first_price_naive',episode_id='finite-market')['status']=='COMPLETED'
    import sqlite3
    with sqlite3.connect(root/'authority/ledger.sqlite') as db:
        assert db.execute("SELECT count(*) FROM wallet_entries WHERE episode_id=?",
            (json.loads(Path(checkpoint).read_text())['payload']['warm_id'],)).fetchone()[0]==0
        assert {row[0] for row in db.execute("SELECT DISTINCT wealth_before FROM settlements WHERE episode_id=?",
            (json.loads(Path(checkpoint).read_text())['payload']['warm_id'],))}=={'NOT_APPLICABLE'}
        assert {row[0] for row in db.execute("SELECT episode_id FROM arm_clones")}=={'ample-market','finite-market'}
        assert {row[0] for row in db.execute("SELECT amount FROM wallet_entries WHERE episode_id='ample-market' AND kind='INITIAL'")}=={'125.000000'}
        assert {row[0] for row in db.execute("SELECT amount FROM wallet_entries WHERE episode_id='finite-market' AND kind='INITIAL'")}=={'5'}
    frozen=json.loads(Path(checkpoint).read_text())['payload']
    assert 'initial_wealth' not in frozen['config']
    assert all('initial_wealth' not in stage for stage in frozen['config']['stage_configs'].values())
    assert all('wealth' not in state for state in frozen['states'].values())
    from maestro.replay import verify_ledger
    assert verify_ledger(root)['verified']
    from maestro.search_tree import load_search_tree
    tree=load_search_tree(root,'first_price_naive',episode='ample-market')
    assert tree['metadata']['boundary']['warm_has_wallet'] is False
    assert _config(parser.parse_args(['--root',str(root),'resume','--episode','ample-market'])).hash==market.hash
    changed=_config(parser.parse_args(['--root',str(root),'resume','--episode','ample-market','--credits','1']))
    assert changed.hash!=market.hash
    from maestro.cli import main
    assert main(['--root',str(root),'resume','--episode','ample-market','--credits','1'])==2


def test_zero_credits_covers_larger_warm_reward_units(manifest):
    edit(fragment(manifest,'benchmarks/pace/market.yaml'),reward_scale='10')
    market=load_config(manifest,stage='market').with_market_credits(0)
    assert market.effective_initial_wealth==market.rounds*Decimal('125.000000')


def test_mock_stage_transition_and_resume(manifest,tmp_path):
    root=tmp_path/'artifacts'
    warm=load_config(manifest)
    with ExperimentEngine(root,warm) as engine:
        result=engine.bootstrap()
    args=build_parser().parse_args(['--root',str(root),'run','--bootstrap',result['checkpoint'],
                                    '--config',str(manifest)])
    market=_config(args)
    assert market==warm.for_stage('market')
    with ExperimentEngine(root,market) as engine:
        engine.run(result['checkpoint'],episode_id='market-test')
    args=build_parser().parse_args(['--root',str(root),'resume','--episode','market-test'])
    assert _config(args).to_dict()==market.to_dict()
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
    edit(fragment(manifest,'benchmarks/pace/market.yaml'),rounds=2,planning_window_seconds=151)
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
        ledger.create_episode('e','first_price_naive',config,{})
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
