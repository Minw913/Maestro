import json
from pathlib import Path
from decimal import Decimal
import pytest

from maestro.market.regimes import REGIMES,REGISTRY_REVISION,resolve_policy,describe,contract
from maestro.config import RunConfig
from maestro.cli import build_parser,main


def test_stable_ids_aliases_and_defensive_descriptions():
    assert REGISTRY_REVISION==5
    assert [(r['id'],r['policy']) for r in REGIMES]==[('R1','uniform'),('R2','ucb'),('R3','first_price_naive'),('R4','adjusted_valuation')]
    for number,policy in [(1,'uniform'),(2,'ucb'),(3,'first_price_naive'),(4,'adjusted_valuation')]:
        for value in [number,str(number),'R'+str(number),'r'+str(number),policy]:
            assert resolve_policy(value)==policy
    for bad in [0,5,'warm','first_price','agent_adjusted',True,None]:
        with pytest.raises(ValueError):resolve_policy(bad)


def test_settings_changes_keep_id_but_change_contract_hash():
    from dataclasses import replace
    config=RunConfig(initial_wealth=5,valuation_discount_factor=.8)
    base=contract(3,config)
    for other in [replace(config,initial_wealth=3),replace(config,valuation_discount_factor=.7)]:
        changed=contract(3,other)
        assert changed['content_hash']!=base['content_hash']
        assert changed['contract']['regime']['id']=='R3'
    ucb=contract(2,replace(config,ucb_exploration_coefficient=1))
    assert ucb['contract']['settings']['ucb_exploration_coefficient']==1
    assert ucb['contract']['ucb_exploration_active']
    assert not contract(2,config)['contract']['ucb_exploration_active']
    r4=contract(4,config)
    assert r4['contract']['requires_prebid'] is True
    assert r4['contract']['valuation']=='adjusted_valuation_expected_credit_with_discounted_baseline'
    assert contract(4,replace(config,valuation_calls=3))['content_hash']!=r4['content_hash']


def test_cli_lists_without_backend(tmp_path,monkeypatch,capsys):
    def forbidden(*args,**kwargs):pytest.fail('Listing must not construct an engine')
    monkeypatch.setattr('maestro.loop.ExperimentEngine',forbidden)
    path=Path(__file__).resolve().parents[2]/'configs/experiments/pace_remote_deepseek.yaml'
    assert main(['--root',str(tmp_path/'absent'),'regimes','--config',str(path)])==0
    result=json.loads(capsys.readouterr().out)
    assert result['registry_revision']==5
    assert [r['policy'] for r in result['market_regimes']]==['uniform','ucb','first_price_naive','adjusted_valuation']
    assert len(result['contracts'])==4
    assert result['contracts'][0]['contract']['settings']['initial_wealth']=='5'
    assert not (tmp_path/'absent').exists()


def test_cli_selector_conflicts_and_invalid_id():
    parser=build_parser()
    assert parser.parse_args(['run','--bootstrap','x','--regime','1']).regime=='uniform'
    assert parser.parse_args(['run','--bootstrap','x','--regime','2']).regime=='ucb'
    assert parser.parse_args(['run','--bootstrap','x','--regime','3']).regime=='first_price_naive'
    assert parser.parse_args(['run','--bootstrap','x','--regime','4']).regime=='adjusted_valuation'
    assert parser.parse_args(['run','--bootstrap','x','--policy','first_price_naive']).policy=='first_price_naive'
    assert parser.parse_args(['run','--bootstrap','x','--policy','adjusted_valuation']).policy=='adjusted_valuation'
    with pytest.raises(SystemExit):parser.parse_args(['run','--bootstrap','x','--policy','first_price'])
    with pytest.raises(SystemExit):parser.parse_args(['run','--bootstrap','x','--policy','agent_adjusted'])
    with pytest.raises(SystemExit):parser.parse_args(['run','--bootstrap','x','--regime','2','--policy','ucb'])
    with pytest.raises(SystemExit):parser.parse_args(['run','--bootstrap','x','--regime','5'])


@pytest.mark.parametrize('number',[1,2,3])
def test_run_freezes_contract_and_report_replay(tmp_path,number):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    from maestro.reports import report
    config=RunConfig(rounds=2,warm_opportunities_per_agent=1,initial_wealth=5)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        result=engine.run(warm['checkpoint'],number,episode_id='market')
    assert result['status']=='COMPLETED'
    frozen=json.loads((tmp_path/'arms/market/regime-contract.json').read_text())
    assert frozen['contract']['regime']['number']==number
    assert verify_ledger(tmp_path)['verified']
    row=report(tmp_path,'market')['episodes'][0]
    assert row['regime_contract']['content_hash']==frozen['content_hash']


def test_resume_rejects_changed_regime_before_backend(tmp_path,monkeypatch):
    from maestro.storage import Ledger
    with Ledger(tmp_path/'authority/ledger.sqlite') as ledger:
        ledger.create_episode('r1','first_price_naive',RunConfig(),{})
    def forbidden(*args,**kwargs):pytest.fail('Mismatch must fail before engine startup')
    monkeypatch.setattr('maestro.loop.ExperimentEngine',forbidden)
    assert main(['--root',str(tmp_path),'resume','--episode','r1','--regime','1'])==2


@pytest.mark.parametrize('revision',[1,2,3,4])
@pytest.mark.parametrize('policy',['first_price','ucb','uniform'])
def test_legacy_contract_ids_replay_with_their_frozen_revision(tmp_path,monkeypatch,revision,policy):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    from maestro.reports import report
    from maestro.types import stable_hash
    def historical_contract(value,config):
        current=contract('first_price_naive' if value=='first_price' else value,config)
        body=current['contract']
        body['registry_revision']=revision
        body['regime']=describe(policy,registry_revision=revision)
        return {'contract':body,'content_hash':stable_hash(body)}
    monkeypatch.setattr('maestro.market.regimes.contract',historical_contract)
    if policy=='first_price':
        from maestro.market.regimes import resolve_policy as current_resolve
        monkeypatch.setattr('maestro.market.regimes.resolve_policy',
                            lambda value, **kwargs: 'first_price' if value=='first_price' else current_resolve(value,**kwargs))
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1,initial_wealth=5)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap();engine.run(warm['checkpoint'],policy,episode_id='historical')
    path=tmp_path/'arms/historical/regime-contract.json';before=path.read_bytes()
    assert verify_ledger(tmp_path)['verified']
    frozen=report(tmp_path,'historical')['episodes'][0]['regime_contract']
    expected={'first_price':'R1' if revision<4 else 'R3',
              'ucb':'R2','uniform':'R3' if revision<4 else 'R1'}[policy]
    assert frozen['contract']['registry_revision']==revision
    assert frozen['contract']['regime']['id']==expected
    assert path.read_bytes()==before


@pytest.mark.parametrize('revision',[0,6,True,None])
def test_registry_rejects_unrecognized_historical_revisions(revision):
    with pytest.raises(ValueError):describe('first_price',registry_revision=revision)


def test_revision_four_policy_names_remain_readable_without_new_run_aliases():
    assert describe('first_price',registry_revision=4)['id']=='R3'
    assert describe('agent_adjusted',registry_revision=4)['id']=='R4'
    assert describe('adjusted_valuation')['id']=='R4'
    with pytest.raises(ValueError):resolve_policy('agent_adjusted')


def test_r2_bandit_display_name_retains_internal_policy():
    from maestro.market.regimes import describe
    assert describe(2)["name"]=="Bandit-based allocation"
    assert resolve_policy("R2")=="ucb"


@pytest.mark.parametrize('changes',[
    {'valuation_calls':0},{'valuation_calls':5},{'valuation_calls':True},
    {'valuation_window_seconds':0},{'valuation_window_seconds':float('inf')},
])
def test_r4_request_and_window_bounds(changes):
    with pytest.raises(ValueError):RunConfig(**changes)


def test_r4_rejects_ineligible_run_before_checkpoint_or_model(tmp_path):
    from maestro.loop import ExperimentEngine
    with ExperimentEngine(tmp_path,RunConfig()) as engine:
        with pytest.raises(ValueError,match='real prebid_v2'):
            engine.run(tmp_path/'no-checkpoint',4)
