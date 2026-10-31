from decimal import Decimal
import json
import sqlite3

import pytest

from maestro.agents.bidding import cooperative_bid
from maestro.config import RunConfig,load_config


def test_available_wealth_clips_bid_without_changing_value():
    assert cooperative_bid(8,available_wealth=3)==3
    assert cooperative_bid(2,available_wealth=3)==2
    assert cooperative_bid(8,available_wealth=0)==0
    for invalid in [-1,float('nan')]:
        with pytest.raises(ValueError):cooperative_bid(8,available_wealth=invalid)


@pytest.mark.parametrize('policy',['first_price_naive','ucb','uniform'])
@pytest.mark.parametrize('wealth',[Decimal('.01')])
def test_scarce_wallet_runs_without_debt_and_replays(tmp_path,policy,wealth):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    config=RunConfig(rounds=3,warm_opportunities_per_agent=1,initial_wealth=wealth)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],policy,episode_id='scarce')
    db=sqlite3.connect(tmp_path/'authority/ledger.sqlite')
    clipped=False
    for r,raw in db.execute("SELECT round_index,payload FROM round_submissions WHERE episode_id='scarce'"):
        sub=json.loads(raw);value=Decimal(sub['value']);bid=Decimal(sub['bid'])
        snapshot=json.loads(db.execute("SELECT snapshot_json FROM rounds WHERE episode_id='scarce' AND round_index=?",(r,)).fetchone()[0])
        balance=Decimal(snapshot['states'][sub['agent_id']]['wealth'])
        assert bid==(min(value,balance) if policy=='first_price_naive' else value)
        clipped |= bid<value
    if policy=='first_price_naive':assert clipped
    for payment,before,after in db.execute("SELECT payment,wealth_before,wealth_after FROM settlements WHERE episode_id='scarce'"):
        assert Decimal(after)>=0
        assert Decimal(payment)<=Decimal(before)
        if policy!='first_price_naive':assert Decimal(payment)==0
    db.close()
    assert verify_ledger(tmp_path)['verified']


def test_existing_holds_reduce_available_bid(tmp_path):
    from maestro.loop import ExperimentEngine
    from maestro.types import AgentState
    with ExperimentEngine(tmp_path,RunConfig(initial_wealth=5)) as engine:
        engine.ledger.create_episode('scarce','first_price_naive',engine.config,{})
        with engine.ledger.transaction() as db:
            db.execute("INSERT INTO holds VALUES(?,?,?,?,?,?)",('held','scarce',0,'a','2','HELD'))
        state=AgentState('a','parent',.5,wealth=Decimal(5))
        assert engine._funded_bid('scarce','a',state,Decimal(8),'first_price_naive')==3
        assert engine._funded_bid('scarce','a',state,Decimal(8),'ucb')==8


def test_zero_credits_provision_sufficient_wallet_for_entire_market(tmp_path):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    config=RunConfig(rounds=3,warm_opportunities_per_agent=1,initial_wealth=0)
    assert config.effective_initial_wealth==Decimal('375.000000')
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],'first_price_naive',episode_id='ample')
        from maestro.types import AgentState
        funded=AgentState('agent-1','seed',.5,wealth=config.effective_initial_wealth)
        assert engine._funded_bid('ample','agent-1',funded,Decimal(100),'adjusted_valuation')==Decimal(100)
    db=sqlite3.connect(tmp_path/'authority/ledger.sqlite')
    assert {Decimal(row[0]) for row in db.execute(
        "SELECT amount FROM wallet_entries WHERE episode_id='ample' AND kind='INITIAL'")}=={config.effective_initial_wealth}
    for (raw,) in db.execute("SELECT payload FROM round_submissions WHERE episode_id='ample'"):
        sub=json.loads(raw)
        assert Decimal(sub['bid'])==Decimal(sub['value'])
    assert all(Decimal(row[0])>=0 for row in db.execute(
        "SELECT wealth_after FROM settlements WHERE episode_id='ample'"))
    db.close()
    assert verify_ledger(tmp_path)['verified']


def test_legacy_zero_wallet_cannot_silently_resume_as_sufficient(tmp_path):
    from maestro.loop import ExperimentEngine
    from maestro.types import AgentState,IntegrityError
    config=RunConfig(initial_wealth=0)
    with ExperimentEngine(tmp_path,config) as engine:
        engine.ledger.create_episode('old','first_price_naive',config,
            {'agent-1':AgentState('agent-1','seed',.5,wealth=Decimal(0))})
        with pytest.raises(IntegrityError,match='Legacy zero-wallet'):
            engine.resume('old')


def test_pace_market_presets_use_five_credits():
    from pathlib import Path
    configs=Path(__file__).resolve().parents[2]/'configs'
    for path in (configs/'experiments').glob('*.yaml'):
        config=load_config(path,stage='market')
        if config.benchmark=='pace':
            assert config.initial_wealth==5
