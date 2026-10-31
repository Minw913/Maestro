"""Controller-only fixtures; no provider, native process, Docker or experiment."""
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
import threading
import time

import pytest
from maestro.config import RunConfig
from maestro.loop import ExperimentEngine
from maestro.types import AgentState,HistoryRecord,MaestroError,stable_hash
from maestro.agents.planner import mock_plan
from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.execution.valuation_phase import context_bundle,run_valuations,resume_generation_deadline,retire_failed_valuations
from maestro.llm import InferenceRequestUnknown
from maestro.backends.coral.valuation import ValuationOutputLimit,ValuationRepairFailed


@pytest.fixture
def view(tmp_path):
    engine=ExperimentEngine(tmp_path/'fixture',RunConfig(rounds=1))
    root=tmp_path/'source';root.mkdir();(root/'run').write_text('print(1)\n')
    digest=candidate_tree_hash(root)
    notes=tmp_path/'notes';notes.mkdir();(notes/'technical.txt').write_text('Use an adjacency list.')
    (notes/'sensitive.txt').write_text('wallet=500; bid above the competitor')
    state=AgentState('a',digest,.7,wealth=Decimal(5),history=[
        HistoryRecord('old','a',.6,.8,.7,True,'other','other',credit=Decimal(2))])
    backend=SimpleNamespace(_parents={digest:root},_workspace=lambda s:SimpleNamespace(private=notes))
    engine.backend=backend
    proposal={'candidate_hash':digest,'candidate_path':str(root)}
    yield engine,state,mock_plan(state,0,0),proposal,notes
    engine.backend=None;engine.close()


def answer():
    return {'baseline_assessment':'reasonable','scenarios':[
        {'kind':'no_positive_reward','probability':.3,'predicted_score':.7},
        {'kind':'catchup','probability':.5,'predicted_score':.75},
        {'kind':'frontier','probability':.2,'predicted_score':.83}],
        'evidence':[{'path':'solver/run','observation':'Inspected immutable source'}],
        'rationale':'Assessment fixture','uncertainties':[]}


def test_context_is_complete_numeric_history_and_has_no_market_data(view):
    engine,state,plan,proposal,notes=view
    files=context_bundle(engine,state,plan,proposal,.8,Decimal(2))
    text=' '.join(files.values())
    assert 'wallet' not in text and 'competitor' not in text
    assert 'adjacency list' in text and 'print(1)' in text
    assert 'public/board.json' not in files
    trajectory=json.loads(files['public/trajectory.json'])
    assert trajectory[0]['actual_credit']=='2' and trajectory[0]['improvement']==pytest.approx(.1)
    assert len(trajectory)==len(state.history)
    assert json.loads(files['public/contract.json'])['scenario_response_rules']['unavailable_classes']==[]
    tied=json.loads(context_bundle(engine,state,plan,proposal,.7,Decimal(2))['public/contract.json'])
    assert tied['scenario_response_rules']['unavailable_classes']==['catchup']
    ceiling=replace(state,parent_score=1)
    ceiling_rules=json.loads(context_bundle(engine,ceiling,plan,proposal,1,Decimal(2))['public/contract.json'])
    assert ceiling_rules['scenario_response_rules']['unavailable_classes']==['catchup','frontier']
    bad=replace(state,history=[replace(state.history[0],agent_id='peer')])
    with pytest.raises(Exception,match='Peer history'):context_bundle(engine,bad,plan,proposal,.8,Decimal(2))


@pytest.mark.parametrize('mode',['accepted','rounded','invalid','invalid_evidence','timeout','output_limit','repair_failed','unknown'])
def test_parallel_barrier_fallback_and_stop(view,mode):
    engine,state,_,proposal,_=view
    if mode=='rounded':state=replace(state,parent_score=.7009497696956891)
    states={f'a{i}':replace(state,agent_id=f'a{i}',history=[replace(state.history[0],agent_id=f'a{i}')]) for i in range(4)}
    plans={a:mock_plan(s,0,0) for a,s in states.items()};proposals={a:proposal for a in states}
    engine.ledger.create_episode('phase','adjusted_valuation',engine.config,states)
    engine.ledger.open_round('phase',0,{})
    barrier=threading.Barrier(4);deadlines=[];lock=threading.Lock()
    def assess(a,r,files,*,deadline):
        with lock:deadlines.append(deadline)
        barrier.wait(timeout=5)
        if mode=='unknown':raise InferenceRequestUnknown('fixture',request_id=a,scope_id=a,context={})
        if mode=='timeout':raise TimeoutError('known clean timeout')
        if mode=='output_limit':raise ValuationOutputLimit('terminal output limit')
        if mode=='repair_failed':raise ValuationRepairFailed('final regeneration failed')
        from maestro.agents.valuation_reasoning import ValuationReadBroker
        broker=ValuationReadBroker(files,deadline)
        paths=['public/contract.json','public/trajectory.json','solver/run']
        for path in paths[:-1] if mode=='invalid_evidence' else paths:
            broker('maestro_tool',{'action':'read','path':path})
        response=answer()
        if mode=='rounded':response['scenarios'][0]['predicted_score']=.701
        return {'text':json.dumps(response) if mode in ('accepted','rounded','invalid_evidence') else '{}',
                'usage':{'calls':1,'known_input_tokens':5,'known_output_tokens':2,'usage_complete':True},'read_trace':broker.trace}
    engine.backend.value_with_receipt=assess
    # A prior generation deadline can be nearly exhausted. Valuation still
    # receives its own common window and does not dispatch against that clock.
    generation_deadline=time.monotonic()+.001
    opened_before=time.monotonic()
    if mode=='unknown':
        with pytest.raises(MaestroError,match='unresolved'):run_valuations(engine,'phase',0,states,plans,proposals,.8)
        assert engine.ledger.connection.execute("SELECT status FROM episodes WHERE id='phase'").fetchone()[0]=='INCOMPLETE'
        assert engine.ledger.connection.execute('SELECT count(*) FROM round_submissions').fetchone()[0]==0
    else:
        out=run_valuations(engine,'phase',0,states,plans,proposals,.8)
        assert all(v['status']==('ACCEPTED' if mode in ('accepted','rounded') else
                                  'FAILED' if mode=='repair_failed' else 'FALLBACK') for v in out.values())
        if mode!='rounded':
            assert all(Decimal(v['value'])==(Decimal('1.725') if mode=='accepted' else 2) for v in out.values())
        else:
            assert all(v['assessment']['scenarios'][0]['predicted_score']==state.parent_score
                       and len(v['boundary_adjustments'])==1 for v in out.values())
        if mode=='output_limit':assert all(v['reason']=='ValuationOutputLimit' for v in out.values())
        if mode=='repair_failed':
            retire_failed_valuations(engine,'phase',0,states,proposals,out)
            assert proposals=={} and all(s.needs_refresh for s in states.values())
            assert engine.ledger.connection.execute("SELECT count(*) FROM events WHERE kind='VALUATION_AGENT_FAILED'").fetchone()[0]==4
    assert len(deadlines)==4 and len(set(deadlines))==1
    assert deadlines[0]>generation_deadline
    assert opened_before+engine.config.valuation_window_seconds<=deadlines[0]
    assert engine.ledger.connection.execute("SELECT count(*) FROM events WHERE kind='VALUATION_RESULT'").fetchone()[0]==4


def test_valuation_pauses_only_the_generation_wall_clock():
    # 300 s elapsed in valuation while 60 s of generation time was left.
    assert resume_generation_deadline(360,300,600)==660
    assert resume_generation_deadline(360,300,600)-600==60
    with pytest.raises(ValueError,match='backwards'):
        resume_generation_deadline(360,600,300)
