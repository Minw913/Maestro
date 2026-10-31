"""Actual settled credits, own-attempt age, shared bidding/ranking and replay."""
from dataclasses import replace
from decimal import Decimal
import json

import pytest

from maestro.agents.valuation import estimate_value
from maestro.config import RunConfig, load_config
from maestro.market.policies import rank
from maestro.types import AgentState, HistoryRecord, Submission, BootstrapIncomplete


def records(credits, agent='a'):
    return [HistoryRecord(f'{agent}-{i}',agent,.5,.9,.6,True,
        'local_search' if i%2 else 'other','other',credit=Decimal(str(r)))
        for i,r in enumerate(credits)]


def test_discounted_actual_rewards_include_all_categories_and_zero_failures():
    history=records([4,0,2])
    history[1]=replace(history[1],valid=False,status='TIMEOUT',score=0)
    value,ids=estimate_value(history,gamma=.8)
    assert value==Decimal('1.868852')
    assert ids==['a-0','a-1','a-2']
    assert estimate_value(history,gamma=1)[0]==Decimal('2.000000')
    # Parent/champion, taxonomy and gains cannot rebase actual historical credits.
    changed=[replace(r,parent_score=.1,champion_score=.2,score=.99,target='restart') for r in history]
    assert estimate_value(changed,gamma=.8)==(value,ids)
    assert estimate_value(history[::-1],gamma=.8)[0]!=value


def test_only_own_new_settled_attempt_ages_history():
    history=records([4,0,2])
    state=AgentState('a','parent',.6,history=history,ucb_n=3)
    before=estimate_value(state.history,gamma=.8)
    for _ in range(10):
        assert estimate_value(state.history,gamma=.8)==before
    restored=records([4,0])+records([2])[0:1]
    assert estimate_value(restored,gamma=.8)[0]==before[0]
    # New zero outcome is evidence and lowers the estimate.
    assert estimate_value(history+records([0]),gamma=.8)[0]<before[0]


@pytest.mark.parametrize('gamma',[0,-.1,1.1,True,'0.9',None,float('nan'),float('inf')])
def test_invalid_discounts_rejected(gamma):
    with pytest.raises(ValueError,match='valuation_discount_factor'):
        estimate_value(records([1]),gamma=gamma)
    if gamma is not None:  # None only reserved for historical serialized contracts.
        with pytest.raises(ValueError,match='valuation_discount_factor'):
            RunConfig(valuation_discount_factor=gamma)


def test_unknown_is_not_silently_zero_and_empty_history_has_no_invented_prior():
    with pytest.raises(BootstrapIncomplete):estimate_value([])
    with pytest.raises(ValueError,match='settled'):
        estimate_value([replace(records([1])[0],status='UNKNOWN')])


def test_ucb_uses_same_discounted_value_as_first_price_naive():
    states={a:AgentState(a,'p',.5,history=records(rs,a),ucb_n=2,
        ucb_total=Decimal('999')) for a,rs in {'a':[10,0],'b':[0,8]}.items()}
    submissions=[Submission(a,'plan-'+a,*([estimate_value(s.history,gamma=.5)[0]]*2))
        for a,s in states.items()]
    # b wins despite a's larger lifetime mean (5 versus 4).
    assert rank('first_price_naive',submissions,states,0,0)==['b','a']
    assert rank('ucb',submissions,states,0,0,valuation_discount_factor=.5)==['b','a']
    assert rank('ucb',submissions,states,999,0,valuation_discount_factor=.5)==['b','a']


def test_config_serialization_loading_and_frozen_identity(tmp_path):
    current=RunConfig(valuation_discount_factor=.8)
    assert RunConfig.from_dict(current.to_dict()).hash==current.hash
    assert replace(current,valuation_discount_factor=.9).hash!=current.hash
    legacy=current.to_dict();legacy.pop('valuation_discount_factor')
    restored=RunConfig.from_dict(legacy)
    assert restored.valuation_discount_factor is None
    assert 'valuation_discount_factor' not in restored.to_dict()
    path=tmp_path/'config.json';path.write_text('{}')
    assert load_config(path).valuation_discount_factor==.8
    path.write_text('{"valuation_discount_factor": null}')
    with pytest.raises(ValueError):load_config(path)


@pytest.mark.parametrize('policy',['first_price_naive','ucb'])
def test_engine_freezes_actual_discounted_values_and_replays(tmp_path,policy):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    from maestro.storage.ledger import Ledger
    config=RunConfig(rounds=3,warm_opportunities_per_agent=2,valuation_discount_factor=.8)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],policy,episode_id='arm')
    import sqlite3
    db=sqlite3.connect(tmp_path/'authority/ledger.sqlite')
    for r,raw in db.execute("SELECT round_index,payload FROM round_submissions WHERE episode_id='arm'"):
        submission=json.loads(raw)
        snapshot=json.loads(db.execute("SELECT snapshot_json FROM rounds WHERE episode_id='arm' AND round_index=?",(r,)).fetchone()[0])
        from maestro.types import state_from_dict
        state=state_from_dict(snapshot['states'][submission['agent_id']])
        value,ids=estimate_value(state.history,gamma=.8)
        assert Decimal(submission['value'])==value
        assert submission['sample_ids']==ids
    db.close()
    assert verify_ledger(tmp_path)['verified']


def test_layered_discount_is_shared_and_wrong_layer_is_rejected(tmp_path):
    from pathlib import Path
    import shutil
    source=Path(__file__).resolve().parents[2]/'configs'
    shutil.copytree(source,tmp_path/'configs')
    path=tmp_path/'configs/experiments/pace_remote.yaml'
    text=path.read_text().replace('valuation_discount_factor: 0.8','valuation_discount_factor: 0.6')
    path.write_text(text)
    config=load_config(path)
    assert config.valuation_discount_factor==config.for_stage('market').valuation_discount_factor==.6
    assert RunConfig.from_dict(config.to_dict()).hash==config.hash
    bad=tmp_path/'configs/benchmarks/pace/market.yaml'
    bad.write_text(bad.read_text()+'\nvaluation_discount_factor: 0.7\n')
    with pytest.raises(ValueError,match='Unknown or misplaced'):
        load_config(path)


def test_resume_rejects_changed_discount_before_backend(tmp_path,monkeypatch):
    from maestro.storage.ledger import Ledger
    from maestro.cli import main
    with Ledger(tmp_path/'authority/ledger.sqlite') as ledger:
        ledger.create_episode('frozen','first_price_naive',RunConfig(valuation_discount_factor=.8),{})
    config=tmp_path/'changed.json'
    config.write_text(json.dumps(RunConfig(valuation_discount_factor=.9).to_dict()))
    def forbidden(*args,**kwargs):pytest.fail('Must reject mismatch before backend startup')
    monkeypatch.setattr('maestro.loop.ExperimentEngine',forbidden)
    assert main(['--root',str(tmp_path),'resume','--episode','frozen','--config',str(config)])==2


@pytest.mark.parametrize('policy',['first_price_naive','ucb'])
def test_frozen_legacy_contract_remains_replayable(tmp_path,policy):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    config=RunConfig(rounds=2,warm_opportunities_per_agent=1,
        valuation_discount_factor=None,ucb_exploration_coefficient=1)
    assert 'valuation_discount_factor' not in config.to_dict()
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],policy,episode_id='legacy')
    assert verify_ledger(tmp_path)['verified']


def test_bid_cap_removed_and_scarce_wealth_allowed():
    from maestro.agents.bidding import cooperative_bid
    assert cooperative_bid(Decimal('250.125'))==Decimal('250.125000')
    assert 'bid_cap' not in RunConfig().to_dict()
    with pytest.raises(ValueError,match='Unknown config keys'):
        RunConfig.from_dict({'bid_cap':'100'})
    assert RunConfig(initial_wealth=5).initial_wealth==5
    with pytest.raises(ValueError,match='nonnegative'):
        RunConfig(initial_wealth=-1)
