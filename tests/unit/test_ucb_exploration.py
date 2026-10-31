"""Exploration can be removed without changing historical contracts or replay."""
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path

import pytest

from maestro.cli import _config,build_parser,main
from maestro.config import RunConfig,load_config
from maestro.market.policies import rank
from maestro.types import AgentState,HistoryRecord,Submission,canonical_json,stable_hash,IntegrityError


def arms():
    states={
        'experienced':AgentState('experienced','a',.5,ucb_n=100,ucb_total=Decimal(100)),
        'sparse':AgentState('sparse','b',.5,ucb_n=1,ucb_total=Decimal('.9')),
        'cold':AgentState('cold','c',.5,ucb_n=0,ucb_total=Decimal(0))}
    for agent,state in states.items():
        if state.ucb_n:
            state.history=[HistoryRecord(str(j),agent,.5,.5,.5,True,'other','other',
                credit=state.ucb_total/state.ucb_n) for j in range(state.ucb_n)]
    submissions=[Submission(a,'plan-'+a,Decimal(0),Decimal(0)) for a in states]
    return states,submissions


def test_zero_exploration_uses_only_mean_and_has_no_infinite_cold_start():
    states,submissions=arms()
    assert rank('ucb',submissions,states,0,0,ucb_exploration_coefficient=1)[0]=='cold'
    assert rank('ucb',submissions,states,0,0,ucb_exploration_coefficient=0)==['experienced','sparse','cold']
    assert rank('ucb',submissions[::-1],states,0,0,ucb_exploration_coefficient=0)==['experienced','sparse','cold']
    states['cold'].ucb_n=1
    states['cold'].history=[HistoryRecord('cold-1','cold',.5,.5,.5,True,'other','other')]
    assert rank('ucb',submissions,states,0,0,ucb_exploration_coefficient=.01)[0]=='experienced'
    assert rank('ucb',submissions,states,0,0,ucb_exploration_coefficient=.1)[0]=='sparse'
    assert rank('ucb',submissions,states,0,0)==rank('ucb',submissions,states,0,0,ucb_exploration_coefficient=0)


@pytest.mark.parametrize('value',[-1,True,'0',None,float('nan'),float('inf')])
def test_coefficient_is_finite_nonnegative_numeric(value):
    with pytest.raises(ValueError,match='ucb_exploration_coefficient'):
        RunConfig(ucb_exploration_coefficient=value)
    states,submissions=arms()
    with pytest.raises(ValueError,match='ucb_exploration_coefficient'):
        rank('ucb',submissions,states,0,0,ucb_exploration_coefficient=value)


def test_default_wire_hash_and_nested_checkpoints_keep_legacy_identity():
    assert RunConfig().ucb_exploration_coefficient==0
    legacy=RunConfig(ucb_exploration_coefficient=1).to_dict()
    assert 'ucb_exploration_coefficient' not in legacy
    default=RunConfig.from_dict(legacy)
    assert default.ucb_exploration_coefficient==1
    assert default.hash==stable_hash(legacy)
    assert json.loads(canonical_json({'config':default}))=={'config':legacy}
    off=replace(default,ucb_exploration_coefficient=0)
    assert off.to_dict()['ucb_exploration_coefficient']==0
    assert off.hash!=default.hash and off.shared_config_hash!=default.shared_config_hash
    assert RunConfig.from_dict(json.loads(canonical_json(off)))==off
    with pytest.raises(IntegrityError):off.validate_warm_source(default)


def test_experiment_and_cli_setting_are_frozen_across_both_stages(tmp_path):
    configs=Path(__file__).resolve().parents[2]/'configs/experiments'
    for name in ['pace_remote_deepseek.yaml','pace_remote_deepseek_prebid.yaml']:
        warm=load_config(configs/name)
        assert warm.ucb_exploration_coefficient==warm.for_stage('market').ucb_exploration_coefficient==0
    current=load_config(configs/'pace_remote.yaml')
    assert current.ucb_exploration_coefficient==0
    old=replace(current,ucb_exploration_coefficient=1)
    args=build_parser().parse_args(['bootstrap','--config',str(configs/'pace_remote.yaml'),'--ucb-exploration-coefficient','0'])
    warm=_config(args);assert warm.ucb_exploration_coefficient==0
    path=tmp_path/'checkpoint.json';path.write_text(json.dumps({'payload':{'config':warm.to_dict()}}))
    args=build_parser().parse_args(['run','--bootstrap',str(path),'--policy','ucb','--credits','5'])
    market=_config(args)
    assert market.ucb_exploration_coefficient==0
    market.validate_warm_source(warm)
    with pytest.raises(IntegrityError):market.validate_warm_source(old)


def test_new_flat_document_defaults_to_zero_but_legacy_wire_still_uses_one(tmp_path):
    path=tmp_path/'new.yaml';path.write_text('execution_mode: mock\n')
    assert load_config(path).ucb_exploration_coefficient==0
    args=build_parser().parse_args(['bootstrap','--config',str(path)])
    assert _config(args).ucb_exploration_coefficient==0
    legacy=RunConfig(ucb_exploration_coefficient=1).to_dict()
    checkpoint=tmp_path/'old.json';checkpoint.write_text(json.dumps({'payload':{'config':legacy}}))
    args=build_parser().parse_args(['run','--bootstrap',str(checkpoint),'--policy','ucb'])
    assert _config(args).ucb_exploration_coefficient==1


def test_changing_coefficient_on_resume_is_rejected_before_backend(tmp_path,monkeypatch):
    from maestro.storage import Ledger
    with Ledger(tmp_path/'authority/ledger.sqlite') as ledger:
        ledger.create_episode('ucb-off','ucb',RunConfig(ucb_exploration_coefficient=0),{})
    def forbidden(*args,**kwargs):pytest.fail('Changed frozen policy must fail before backend construction')
    monkeypatch.setattr('maestro.loop.ExperimentEngine',forbidden)
    assert main(['--root',str(tmp_path),'resume','--episode','ucb-off','--ucb-exploration-coefficient','1'])==2


def test_zero_exploration_flows_through_engine_replay_and_report(tmp_path):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    from maestro.reports import report
    config=RunConfig(rounds=3,warm_opportunities_per_agent=1,ucb_exploration_coefficient=0)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],'ucb',episode_id='ucb-off')
    assert verify_ledger(tmp_path)['verified']
    result=report(tmp_path,'ucb-off')
    assert result['episodes'][0]['ucb_exploration_coefficient']==0
