"""Observable whole-system invariants, including failures and common prefixes."""
from dataclasses import replace
from decimal import Decimal
import json
import pytest

from maestro.config import RunConfig
from maestro.loop import ExperimentEngine
from maestro.replay import verify_ledger
from maestro.reports import report
from maestro.types import AuthorizationError,IntegrityError,MaestroError,BootstrapIncomplete
from maestro.market.credit import money


def rows(engine,sql,*params):
    return engine.ledger.connection.execute(sql,params).fetchall()


def test_warm_agent_context_hides_champion_until_market(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=2)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        warm_contexts=[json.loads(row['payload']) for row in rows(engine,'SELECT payload FROM context_manifests')]
        assert warm_contexts
        assert all(context['board']=={} for context in warm_contexts)
        assert all('champion_score' not in record and 'credit' not in record
                   for context in warm_contexts for record in context.get('history',[]))
        checkpoint=json.loads((tmp_path/'bootstrap'/warm['warm_id']/'checkpoint.json').read_text())['payload']
        assert checkpoint['board']['champion_score'] >= 0
        assert any(record['champion_score'] is not None for state in checkpoint['states'].values()
                   for record in state['history'])
        engine.run(warm['checkpoint'],'first_price_naive')
        contexts=[json.loads(row['payload']) for row in rows(engine,'SELECT payload FROM context_manifests')]
        assert any('champion_score' in context['board'] for context in contexts)
    assert verify_ledger(tmp_path)['verified']


def test_full_paired_v1_and_replay(tmp_path):
    config=RunConfig()
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        assert warm['warm_waves']==5
        assert set(warm['opportunities'].values())=={5}
        initial=json.loads((tmp_path/'bootstrap'/warm['warm_id']/'checkpoint.json').read_text())['payload']
        assert all(s['ucb_n']==5 for s in initial['states'].values())
        runs=[engine.run(warm['checkpoint'],p) for p in ('uniform','ucb','first_price_naive')]
        for run in runs:
            e=run['episode_id']
            settled=rows(engine,'SELECT * FROM settlements WHERE episode_id=?',e)
            assert len(settled)==80
            assert len(rows(engine,"SELECT * FROM rounds WHERE episode_id=? AND status='SETTLED'",e))==20
            assert sum(a['x'] for a in rows(engine,'SELECT * FROM final_allocations WHERE episode_id=?',e))==40
            for a in rows(engine,'SELECT * FROM agents WHERE episode_id=?',e):
                state=json.loads(a['state_json'])
                assert state['ucb_n']==len(state['history'])>=5
                assert money(state['wealth'])==money(engine.ledger.balance(e,a['agent_id']))
            assert rows(engine,'SELECT warm_id FROM arm_clones WHERE episode_id=?',e)[0][0]==warm['warm_id']
        uniform=rows(engine,'SELECT agent_id,sum(x) n FROM final_allocations WHERE episode_id=? GROUP BY agent_id',runs[0]['episode_id'])
        assert {r['n'] for r in uniform}=={10}
    verified=verify_ledger(tmp_path)
    assert verified['verified'] and verified['settlements']==260
    output=report(tmp_path)
    assert len(output['episodes'])==4
    assert all(r['outcome_label']=='simulated' for r in output['episodes'])


def test_completed_market_extends_from_checkpoint_without_changing_prior_contract(tmp_path):
    config=RunConfig(rounds=2,warm_opportunities_per_agent=1,initial_wealth=Decimal(0))
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        finished=engine.run(warm['checkpoint'],'uniform',episode_id='boundary-test')
        assert finished['status']=='COMPLETED' and finished['rounds']==2
        original=rows(engine,'SELECT config_json,config_hash FROM episodes WHERE id=?','boundary-test')[0]
        original_contract=rows(engine,"SELECT payload FROM events WHERE episode_id=? AND kind='REGIME_CONTRACT_FROZEN'",'boundary-test')[0][0]
    assert verify_ledger(tmp_path)['verified']
    with ExperimentEngine(tmp_path,replace(config,rounds=4)) as engine:
        finished=engine.resume('boundary-test')
        assert finished['status']=='COMPLETED'
        assert [r['round_index'] for r in rows(engine,'SELECT round_index FROM rounds WHERE episode_id=? ORDER BY round_index','boundary-test')]==[0,1,2,3]
        assert tuple(rows(engine,'SELECT config_json,config_hash FROM episodes WHERE id=?','boundary-test')[0])==tuple(original)
        assert rows(engine,"SELECT payload FROM events WHERE episode_id=? AND kind='REGIME_CONTRACT_FROZEN'",'boundary-test')[0][0]==original_contract
        extension=json.loads(rows(engine,"SELECT payload FROM events WHERE episode_id=? AND kind='MARKET_ROUNDS_EXTENDED'",'boundary-test')[0][0])
        assert extension['previous_rounds']==2 and extension['rounds']==4
        assert Decimal(extension['funding_per_agent'])==Decimal('250')
    assert verify_ledger(tmp_path)['verified']
    from maestro.search_tree import load_search_tree
    tree=load_search_tree(tmp_path,'uniform',episode='boundary-test')
    assert tree['metadata']['status']=='COMPLETED'
    assert len([row for row in tree['rounds'] if row['stage']=='market'])==4
    summary=report(tmp_path,episode_id='boundary-test')['episodes'][0]
    assert (summary['initial_rounds'],summary['effective_rounds'])==(2,4)


def test_market_extension_rejects_other_setting_changes(tmp_path):
    config=RunConfig(rounds=2,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],'uniform',episode_id='boundary-test')
    with ExperimentEngine(tmp_path,replace(config,rounds=4,slots=1)) as engine:
        with pytest.raises(IntegrityError,match='beyond Market rounds'):
            engine.resume('boundary-test')
    assert verify_ledger(tmp_path)['verified']


def test_market_extension_survives_exit_before_next_round(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1,initial_wealth=Decimal(0))
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],'uniform',episode_id='extension-crash')
    extended=replace(config,rounds=2)
    with ExperimentEngine(tmp_path,extended) as engine:
        def interrupted(*args,**kwargs):
            raise RuntimeError('stop before the first extended round')
        engine._round=interrupted
        with pytest.raises(RuntimeError,match='stop before'):
            engine.resume('extension-crash')
        assert len(rows(engine,"SELECT * FROM events WHERE episode_id=? AND kind='MARKET_ROUNDS_EXTENDED'",'extension-crash'))==1
    assert verify_ledger(tmp_path)['verified']
    with ExperimentEngine(tmp_path,extended) as engine:
        assert engine.resume('extension-crash')['status']=='COMPLETED'
        assert len(rows(engine,"SELECT * FROM events WHERE episode_id=? AND kind='MARKET_ROUNDS_EXTENDED'",'extension-crash'))==1
    assert verify_ledger(tmp_path)['verified']


def test_cli_resume_uses_larger_rounds_from_config_file(tmp_path,capsys):
    from maestro.cli import main
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1,initial_wealth=Decimal(0))
    root=tmp_path/'run'
    with ExperimentEngine(root,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],'uniform',episode_id='cli-extension')
    config_path=tmp_path/'extended.json'
    config_path.write_text(json.dumps({'rounds':2,'warm_opportunities_per_agent':1,
                                       'initial_wealth':0,'execution_mode':'mock','benchmark':'pace'}))
    assert main(['--root',str(root),'resume','--episode','cli-extension','--config',str(config_path)])==0
    capsys.readouterr()
    assert verify_ledger(root)['verified']
    assert main(['--root',str(root),'resume','--episode','cli-extension'])==0
    assert verify_ledger(root)['verified']


def test_market_horizon_can_extend_more_than_once(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1,initial_wealth=Decimal(0))
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],'uniform',episode_id='many-extensions')
    for target in (3,4,5):
        with ExperimentEngine(tmp_path,replace(config,rounds=target)) as engine:
            assert engine.resume('many-extensions')['rounds']==target
        assert verify_ledger(tmp_path)['verified']
    with ExperimentEngine(tmp_path,replace(config,rounds=5)) as engine:
        events=[json.loads(row['payload']) for row in rows(engine,
            "SELECT payload FROM events WHERE episode_id=? AND kind='MARKET_ROUNDS_EXTENDED' ORDER BY id",
            'many-extensions')]
        assert [(row['previous_rounds'],row['rounds']) for row in events]==[(1,3),(3,4),(4,5)]
        assert [Decimal(row['funding_per_agent']) for row in events]==[Decimal(250),Decimal(125),Decimal(125)]


def test_precheck_replacement_payment_and_no_fake_observation(tmp_path):
    config=RunConfig(rounds=2,warm_opportunities_per_agent=2)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
    broken=replace(config,failures={'agent-1':'precheck','agent-2':'precheck'})
    with ExperimentEngine(tmp_path,broken) as engine:
        run=engine.run(warm['checkpoint'],'first_price_naive')
        e=run['episode_id']
        for failed in ('agent-1','agent-2'):
            for allocation in rows(engine,'SELECT * FROM final_allocations WHERE episode_id=? AND agent_id=?',e,failed):
                assert allocation['x']==0 and Decimal(allocation['payment'])==0
            assert not rows(engine,'SELECT * FROM eval_tickets WHERE episode_id=? AND agent_id=?',e,failed)
        assert len(rows(engine,'SELECT * FROM prechecks WHERE episode_id=? AND passed=0',e))>0
        assert len(rows(engine,'SELECT * FROM resource_usage WHERE episode_id=? AND phase=?',e,'generation'))>0
        for r in range(2):
            assert sum(x['x'] for x in rows(engine,'SELECT * FROM final_allocations WHERE episode_id=? AND round_index=?',e,r))==2
        for invitation in rows(engine,'SELECT * FROM invitations WHERE episode_id=?',e):
            assert len(rows(engine,'SELECT * FROM invitations WHERE episode_id=? AND round_index=? AND agent_id=?',e,invitation['round_index'],invitation['agent_id']))==1
    assert verify_ledger(tmp_path)['verified']


def test_repair_stays_in_one_invitation(tmp_path):
    # One frozen initial program; only a runtime failure authorizes a repair.
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1,generation_calls=3)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
    repair=replace(config,failures={a:['runtime',None] for a in ('agent-1','agent-2','agent-3','agent-4')})
    with ExperimentEngine(tmp_path,repair) as engine:
        run=engine.run(warm['checkpoint'],'uniform')
        e=run['episode_id']
        assert len(rows(engine,'SELECT * FROM invitations WHERE episode_id=?',e))==2
        assert len(rows(engine,'SELECT * FROM prechecks WHERE episode_id=?',e))==4
        assert len(rows(engine,'SELECT * FROM eval_tickets WHERE episode_id=?',e))==2


def test_formal_failure_consumes_slot_and_no_backfill(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
    failed=replace(config,failures={f'agent-{i}:0':{'formal':'invalid'} for i in range(1,5)})
    with ExperimentEngine(tmp_path,failed) as engine:
        run=engine.run(warm['checkpoint'],'first_price_naive')
        e=run['episode_id']
        assert len(rows(engine,'SELECT * FROM invitations WHERE episode_id=?',e))==2
        paid=rows(engine,'SELECT * FROM settlements WHERE episode_id=? AND payment!=?',e,'0')
        assert all(Decimal(s['reward'])==0 for s in paid)
        assert sum(a['x'] for a in rows(engine,'SELECT * FROM final_allocations WHERE episode_id=?',e))==2
    assert verify_ledger(tmp_path)['verified']


def test_deadline_leaves_unused_slots_and_eligible_pending_reuse(tmp_path):
    config=RunConfig(rounds=5,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
    blocked=replace(config,failures={'deadline_rounds':list(range(5))})
    with ExperimentEngine(tmp_path,blocked) as engine:
        run=engine.run(warm['checkpoint'],'first_price_naive')
        e=run['episode_id']
        assert not rows(engine,'SELECT * FROM eval_tickets WHERE episode_id=?',e)
        submissions=rows(engine,'SELECT * FROM round_submissions WHERE episode_id=? ORDER BY round_index',e)
        assert any(json.loads(s['payload'])['reused'] for s in submissions)
        for a in ('agent-1','agent-2','agent-3','agent-4'):
            plans=rows(engine,'SELECT plan_version_id FROM round_submissions WHERE episode_id=? AND agent_id=? ORDER BY round_index',e,a)
            assert plans[0][0]==plans[1][0]==plans[2][0]
            assert plans[3][0]!=plans[0][0]
        assert len(rows(engine,"SELECT * FROM resource_usage WHERE episode_id=? AND phase='planning'",e))==8
    assert verify_ledger(tmp_path)['verified']


def test_missing_ticket_never_invokes_evaluator_and_completed_idempotence(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        before=len(engine.evaluator.calls)
        with pytest.raises(AuthorizationError):
            engine.dispatcher.dispatch('fake','fake')
        assert len(engine.evaluator.calls)==before
        warm=engine.bootstrap()
        run=engine.run(warm['checkpoint'])
        t=rows(engine,'SELECT * FROM eval_tickets WHERE episode_id=?',run['episode_id'])[0]
        before=len(engine.evaluator.calls)
        completed=engine.dispatcher.dispatch(t['id'],t['job_id'])
        assert completed.job_id==t['job_id']
        assert len(engine.evaluator.calls)==before


def test_checkpoint_tamper_fails_and_resume_no_extra_payment(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        run=engine.run(warm['checkpoint'])
        e=run['episode_id']
        before=len(rows(engine,'SELECT * FROM wallet_entries WHERE episode_id=?',e))
        assert engine.resume(e)['status']=='COMPLETED'
        assert len(rows(engine,'SELECT * FROM wallet_entries WHERE episode_id=?',e))==before
        path=tmp_path/'bad.json'
        envelope=json.loads(open(warm['checkpoint'],encoding='utf-8').read())
        envelope['payload']['states']['agent-1']['parent_score']=0.999
        path.write_text(json.dumps(envelope),encoding='utf-8')
        with pytest.raises(IntegrityError):
            engine.run(path)


def test_real_mode_has_no_silent_mock_fallback(tmp_path):
    with pytest.raises(MaestroError,match='panel_manifest'):
        ExperimentEngine(tmp_path,RunConfig(execution_mode='real'))


def test_exact_duplicate_keeps_priority_copy_without_extra_credit(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
    duplicate=replace(config,failures={'duplicate_candidate_rounds':[0]})
    with ExperimentEngine(tmp_path,duplicate) as engine:
        run=engine.run(warm['checkpoint'],'uniform')
        e=run['episode_id']
        assert len(rows(engine,'SELECT * FROM invitations WHERE episode_id=?',e))==4
        admitted=rows(engine,'SELECT * FROM final_allocations WHERE episode_id=? AND x=1',e)
        assert len(admitted)==1
        order=json.loads(rows(engine,'SELECT payload FROM priority_orders WHERE episode_id=?',e)[0][0])['order']
        assert admitted[0]['agent_id']==order[0]
        assert len(rows(engine,'SELECT * FROM eval_tickets WHERE episode_id=?',e))==1


def test_warm_no_history_is_incomplete_instead_of_inventing_prior(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1,failures={'warm':{'agent-1':'precheck'}})
    with ExperimentEngine(tmp_path,config) as engine:
        with pytest.raises(BootstrapIncomplete):
            engine.bootstrap()
        assert rows(engine,"SELECT status FROM episodes WHERE policy='warm'")[0][0]=='BOOTSTRAP_INCOMPLETE'
        assert not rows(engine,'SELECT * FROM warm_checkpoints')
