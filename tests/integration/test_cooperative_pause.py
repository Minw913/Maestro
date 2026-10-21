"""Controller pause boundaries, real authority ledger, no model/evaluator calls."""
import time

import pytest

from maestro.config import RunConfig
from maestro.execution.pause import PauseRequested
from maestro.loop import ExperimentEngine
from maestro.tasks.pace.runtime_failure import RuntimeFailure, RuntimePrecheckFailure
from maestro.types import AgentState, Plan


def test_pause_prevents_first_wave_and_seed_evaluation(tmp_path):
    engine = ExperimentEngine(tmp_path, RunConfig(rounds=1))
    try:
        engine.pause.request()
        with pytest.raises(PauseRequested):engine.bootstrap()
        assert not engine.ledger.connection.execute('select * from episodes').fetchall()
        assert not engine.ledger.connection.execute('select * from jobs').fetchall()
    finally:engine.close()


def test_pause_between_failed_precheck_and_repair_prevents_new_model_call(tmp_path):
    engine = ExperimentEngine(tmp_path, RunConfig(rounds=1))
    class Backend:
        def __init__(self):self.initial=0;self.repairs=0;self.last_generation_usage={}
        def generate(self,*args,**kwargs):
            self.initial+=1;self.last_generation_usage={'calls':1}
            return {'candidate_hash':'failed'}
        def precheck(self,candidate):
            engine.pause.request()
            return RuntimePrecheckFailure('runtime',RuntimeFailure('failed','NameError','bad',(('run',1),)))
        def repair_runtime(self,*args,**kwargs):
            self.repairs+=1;pytest.fail('no new repair after pause')
        def close(self):pass
    backend=Backend();engine.backend=backend
    state=AgentState('a','parent',.5)
    plan=Plan('p','v','a','parent','local_search','parameter_change','x','x','x')
    engine.ledger.create_episode('e','warm',engine.config,{'a':state})
    engine.ledger.open_round('e',0,{})
    try:
        with pytest.raises(PauseRequested):
            engine._generate_once_runtime_only('e',0,'a',state,plan,'WARM',time.monotonic()+5)
        assert backend.initial==1 and backend.repairs==0
        assert engine.ledger.connection.execute('select status from episodes').fetchone()[0]=='INCOMPLETE'
    finally:engine.close()


def test_pause_drains_four_preparations_before_any_planning_or_generation(tmp_path):
    from dataclasses import replace
    import threading
    engine=ExperimentEngine(tmp_path,RunConfig(rounds=1))
    engine.config=replace(engine.config,execution_mode='real')
    class Backend:
        def __init__(self):self.barrier=threading.Barrier(4)
        def prepare_with_receipt(self,*args,**kwargs):
            self.barrier.wait(timeout=2)
            engine.pause.request()
            return {'summary':'offline evidence','accepted_monotonic':time.monotonic()}, {'calls':1}
        def plan_with_receipt(self,*args,**kwargs):pytest.fail('planning after pause')
        def generate(self,*args,**kwargs):pytest.fail('generation after pause')
        def close(self):pass
    engine.backend=Backend()
    states={f'a{i}':AgentState(f'a{i}','parent',.5) for i in range(4)}
    engine.ledger.create_episode('e','warm',engine.config,states)
    try:
        with pytest.raises(PauseRequested):engine._round('e',0,states,'uniform',purpose='WARM',warm_invite=list(states))
        assert engine.ledger.connection.execute("select count(*) from resource_usage where phase='preparation'").fetchone()[0]==4
        assert not engine.ledger.connection.execute('select * from jobs').fetchall()
        assert not engine.ledger.connection.execute('select * from edit_grants').fetchall()
    finally:engine.close()


def test_deepseek_known_rejection_stops_episode_before_another_wave(tmp_path):
    from types import SimpleNamespace
    from maestro.backends.coral.deepseek_lifecycle import RequestJournal
    from maestro.types import MaestroError
    engine=ExperimentEngine(tmp_path,RunConfig(rounds=1))
    states={'a':AgentState('a','parent',.5)}
    engine.ledger.create_episode('e','warm',engine.config,states)
    engine.ledger.open_round('e',0,{})
    engine.budgets.create('e:0:a:PREPARATION',max_calls=1,max_request_bytes=100,
                          max_output_tokens=20,deadline=time.time()+10)
    reservation=engine.budgets.reserve('e:0:a:PREPARATION','request',request_bytes=1,output_tokens=20)
    journal=RequestJournal(engine.budgets,reservation,'0'*64)
    journal.emit('REJECTED',http_status=429)
    engine.budgets.finish(reservation)
    engine.backend=SimpleNamespace(budget_store=engine.budgets,close=lambda:None)
    try:
        with pytest.raises(MaestroError,match='Provider rejected'):engine._assert_provider_available('e',0)
        assert engine.ledger.connection.execute('select status from episodes').fetchone()[0]=='STOPPED_INFRASTRUCTURE'
    finally:engine.close()
