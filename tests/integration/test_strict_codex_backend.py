"""Coral state + strict native adapter with injected model protocol only.

HTTP attempts traverse the real durable quota proxy. There is no external model,
container execution, warm bootstrap, or benchmark in this integration fixture.
"""
from dataclasses import replace
import http.client
import json
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

from maestro.backends.codex_session.autonomous_backend import StrictCodexBackend, CodexPhaseUnknown
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.execution.budgets import BudgetStore
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.llm import InferenceRequestUnknown, PlanningResponseError
from maestro.types import AgentState, stable_hash


def post(base, model):
    url = urlsplit(base)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
    connection.request('POST', url.path+'/responses', json.dumps({'model':model,'input':[], 'tools':[]}),
        {'Authorization':'Bearer synthetic-test-credential','Content-Type':'application/json'})
    response = connection.getresponse()
    status = response.status
    response.read()
    connection.close()
    if status != 200:
        raise RuntimeError('Injected native turn was declined')


class Response:
    status = 200
    headers = {'Content-Type':'text/event-stream'}
    def iter_bytes(self, deadline):
        terminal = {'type':'response.completed','response':{'id':'resp_unit','status':'completed',
            'output':[], 'usage':{'input_tokens':10,'output_tokens':5}}}
        yield b'data: '+json.dumps(terminal).encode()+b'\n\n'
    def close(self):
        pass


class Native:
    model = 'test-model'
    attempts = 2
    fail_upstream = False
    def run_phase(self, prompt, *, base_url, deadline, tools, output_schema=None, session_id=None):
        if output_schema is not None:
            post(base_url,self.model)
            raw = json.dumps({'action':'revise','plan':{'target':'local_search','mod_type':'parameter_change',
                'diagnosis':'Bounded own-history test','change':'Update one search setting',
                'expected_effect':'Improve the fixed-panel result','evidence_run_ids':getattr(self,'evidence',[])},'lessons':[]})
        else:
            post(base_url,self.model)
            tools('maestro_tool',{'action':'write','path':'solver/solver.py','content':'print(2)\n'})
            tools('maestro_tool',{'action':'write','path':'private/notes.md','content':'Own bounded lesson'})
            for _ in range(self.attempts-1):
                post(base_url,self.model)
            raw = 'Implemented admitted change'
        return SimpleNamespace(text=raw,thread_id='private-thread',response_hash=stable_hash(raw))
    def close(self):
        pass


@pytest.fixture
def setup(tmp_path):
    seed = tmp_path/'seed'
    seed.mkdir()
    (seed/'solver.py').write_text('print(1)\n')
    store = BudgetStore(tmp_path/'requests.sqlite')
    native = Native()
    forwarded = []
    def upstream(path, headers, body, timeout):
        forwarded.append(path)
        if native.fail_upstream:
            raise OSError('synthetic lost receipt')
        return Response()
    def proxy(*args,**kwargs):
        return CodexQuotaProxy(*args,upstream=upstream,**kwargs)
    config = RunConfig(rounds=1,warm_opportunities_per_agent=1)
    backend = StrictCodexBackend(native,store,WorkspaceStore(tmp_path/'workspaces',tmp_path/'cas'),
        precheck=lambda candidate:(True,'injected'),seed_solver=seed,candidate_runner=object(),image='',
        config=config,proxy_factory=proxy,simulation=True)
    backend.begin_episode('unit')
    candidate = backend.prepare_seed()
    backend.register_parent(candidate['candidate_hash'],candidate['candidate_path'])
    state = AgentState('a',candidate['candidate_hash'],0.5)
    yield backend,state,native,forwarded
    backend.close()


def grant(backend,state,plan,authority,revision=0):
    return authority.issue(episode_id=backend.episode_id,round_index=0,agent_id=state.agent_id,
        plan_version_id=plan.version_id,parent_hash=state.parent_hash,epoch=revision,expires_at=time.time()+30)


def planned(backend,state):
    return backend.plan_with_receipt(state,0,{'board':{}},deadline=time.monotonic()+20)


def test_planning_is_readonly_and_has_one_actual_dispatch(setup):
    backend,state,_,forwarded = setup
    decision,receipt,context = planned(backend,state)
    assert decision.plan and receipt.actual_requests == 1 and len(forwarded) == 1
    assert context['agent_id']=='a'
    assert state.pending_plan is None
    assert not backend._agents
    assert backend.budget_store.usage('unit:0:a:PLAN_RO')['calls']==1


def test_real_proxy_metered_generation_preserves_code_and_private_memory(setup):
    backend,state,_,forwarded = setup
    plan = planned(backend,state)[0].plan
    authority = GrantAuthority()
    result = backend.generate(state,plan,0,deadline=time.monotonic()+20,
        grant=grant(backend,state,plan,authority),grant_authority=authority)
    assert (backend._agents['a'].solver/'solver.py').read_text()=='print(2)\n'
    assert (backend._agents['a'].private/'notes.md').read_text()=='Own bounded lesson'
    assert result['candidate_hash'] != state.parent_hash
    assert backend.last_generation_usage['calls']==2 and len(forwarded)==3
    assert backend.last_generation_usage['known_output_tokens']==10
    assert backend.last_generation_usage['budget_unit']=='provider_request'
    memory = backend.export_warm_memory()
    assert (backend.workspaces.artifact_root/memory['a']['candidate_hash']/'notes.md').is_file()


def test_total_generation_quota_survives_repairs_not_three_calls_per_revision(setup):
    backend,state,native,forwarded = setup
    plan = planned(backend,state)[0].plan
    authority = GrantAuthority()
    common = time.monotonic()+20
    backend.generate(state,plan,0,deadline=common,
        grant=grant(backend,state,plan,authority),grant_authority=authority)
    result = backend.generate(state,plan,0,revision=1,deadline=common,
        grant=grant(backend,state,plan,authority,1),grant_authority=authority)
    assert result['candidate_hash']
    assert backend.last_generation_usage['calls']==1
    assert len(forwarded)==4  # One planning + three generation HTTP dispatches.
    account=f'unit:0:a:GENERATION:{plan.version_id}'
    assert backend.budget_store.usage(account)['calls']==3
    assert backend.budget_store.usage(account)['unresolved_requests']==0
    assert len(backend._brokers)==1


def test_no_grant_zero_generation_requests(setup):
    backend,state,_,forwarded = setup
    plan=planned(backend,state)[0].plan
    with pytest.raises(GrantError):
        backend.generate(state,plan,0,deadline=time.monotonic()+20)
    assert len(forwarded)==1


def test_unresolved_dispatch_blocks_next_phase_and_survives_backend_restart(setup,tmp_path):
    backend,state,native,forwarded = setup
    native.fail_upstream=True
    with pytest.raises(InferenceRequestUnknown):
        planned(backend,state)
    assert len(forwarded)==1
    with pytest.raises(CodexPhaseUnknown):
        backend.begin_episode('another')
    with pytest.raises(CodexPhaseUnknown):
        backend.preflight()


def test_repeated_planning_account_cannot_reset_one_call_budget(setup):
    backend,state,_,forwarded=setup
    planned(backend,state)
    with pytest.raises(PlanningResponseError):
        planned(backend,state)
    assert len(forwarded)==1


def test_private_memory_frozen_for_next_episode_without_session_leak(setup):
    backend,state,_,_=setup
    plan=planned(backend,state)[0].plan
    authority=GrantAuthority()
    backend.generate(state,plan,0,deadline=time.monotonic()+20,
        grant=grant(backend,state,plan,authority),grant_authority=authority)
    manifest=backend.export_warm_memory()
    backend.restore_warm_memory(manifest)
    backend.begin_episode('main-arm')
    workspace=backend._workspace(state)
    assert (workspace.private/'notes.md').read_text()=='Own bounded lesson'
    assert backend._session_ids=={} and backend._brokers=={}


def test_simulation_cannot_claim_real_pace_qualification(setup):
    backend,_,_,_=setup
    with pytest.raises(RuntimeError,match='Simulated'):
        backend.preflight(replace(backend.config,mode='pace'))


def test_restart_cannot_reopen_generation_with_a_fresh_tool_cpu_budget(setup):
    backend,state,_,forwarded=setup
    plan=planned(backend,state)[0].plan
    authority=GrantAuthority()
    common=time.monotonic()+20
    backend.generate(state,plan,0,deadline=common,
        grant=grant(backend,state,plan,authority),grant_authority=authority)
    # This is the only RAM state absent after reconstructing a backend. The
    # durable completed scope must prevent a fresh 30-second broker allocation.
    backend._brokers.clear()
    with pytest.raises(CodexPhaseUnknown,match='fresh tool budget'):
        backend.generate(state,plan,0,revision=1,deadline=common,
            grant=grant(backend,state,plan,authority,1),grant_authority=authority)
    assert len(forwarded)==3


def test_uncertain_precheck_stops_scope_without_extra_generation(setup):
    from maestro.execution.sandbox import ExecutionQuiescenceUnknown
    backend,state,_,forwarded=setup
    plan=planned(backend,state)[0].plan
    authority=GrantAuthority()
    candidate=backend.generate(state,plan,0,deadline=time.monotonic()+20,
        grant=grant(backend,state,plan,authority),grant_authority=authority)
    def uncertain(candidate):
        raise ExecutionQuiescenceUnknown('Synthetic unconfirmed toy container')
    backend._precheck=uncertain
    with pytest.raises(CodexPhaseUnknown):
        backend.precheck(candidate)
    with pytest.raises(CodexPhaseUnknown):
        backend.begin_episode('later')
    assert len(forwarded)==3


def test_toy_precheck_shares_debug_cpu_capacity_and_wait_deadline(setup):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ResourceLimits

    backend,_,_,forwarded=setup
    backend.tool_cpu_pool=GlobalCpuPool(1)
    launches=[]
    started=threading.Event()
    class ToyRunner:
        def run(self,request,limits):
            launches.append(limits)
            started.set()
            return "toy receipt"
    backend.candidate_runner=ToyRunner()
    limits=ResourceLimits(cpu_seconds=1,wall_seconds=3,memory_mb=128,pids=8,output_bytes=65536,cpus=1)
    with ThreadPoolExecutor(max_workers=1) as workers:
        # A peer's debug shell already owns the machine's sole CPU permit.
        with backend.tool_cpu_pool.slot():
            blocked=workers.submit(backend._run_fixed_precheck,object(),limits,
                deadline=time.monotonic()+.05)
            with pytest.raises(TimeoutError,match="CPU permit"):
                blocked.result(timeout=1)
            assert not started.is_set() and launches==[]
        # An expired waiter must leave neither a queued ticket nor a lost permit.
        deadline=time.monotonic()+.5
        assert workers.submit(backend._run_fixed_precheck,object(),limits,deadline=deadline).result(timeout=1)=="toy receipt"
    assert len(launches)==1
    assert 0<launches[0].wall_seconds<=.5
    assert launches[0].cpu_seconds==1 and launches[0].cpus==1
    assert forwarded==[]


def test_strict_requests_flow_through_first_price_ticket_and_settlement(setup,tmp_path):
    from decimal import Decimal
    from maestro.loop import ExperimentEngine
    from maestro.types import HistoryRecord
    from maestro.execution.authorization import require_execution_authority
    from maestro.backends.mock import MockEvaluator
    backend,state,native,_=setup
    native.evidence=['warm-owned']
    state.history.append(HistoryRecord('warm-owned','a',.15,.15,.2,True,
        'local_search','parameter_change',5,'COMPLETED','past-simulated-hash'))
    state.ucb_n=1
    state.ucb_total=Decimal(5)
    class TicketedScorer(MockEvaluator):
        def evaluate(self,ticket):
            require_execution_authority(ticket,begin=True)
            return super().evaluate(ticket)
    config=RunConfig(n_agents=1,slots=1,rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path/'controller',config,evaluator=TicketedScorer()) as engine:
        # Enter the live adapter callback path only; scoring stays an explicitly
        # simulated fixture. No model, benchmark or warm run is launched.
        engine.config=replace(config,mode='pace')
        engine.backend=backend
        engine.ledger.create_episode('unit','first_price',engine.config,{'a':state})
        parent=backend.prepare_seed()
        engine.ledger.record_candidate(parent['candidate_hash'],parent)
        states={'a':state}
        engine._round('unit',0,states,'first_price')
        ticket=engine.ledger.connection.execute('SELECT * FROM eval_tickets').fetchone()
        settled=engine.ledger.connection.execute('SELECT * FROM settlements').fetchone()
        assert ticket['status']=='SETTLED' and Decimal(settled['payment'])>0
        assert states['a'].wealth==Decimal(settled['wealth_before'])-Decimal(settled['payment'])+Decimal(settled['reward'])
        assert engine.evaluator.calls==[ticket['job_id']]
        rows=engine.ledger.connection.execute("SELECT llm_calls,payload FROM resource_usage WHERE phase IN('planning','generation')").fetchall()
        assert sorted(row['llm_calls'] for row in rows)==[1,2]
        assert all(not json.loads(row['payload']).get('request_count_unknown',False) for row in rows)
