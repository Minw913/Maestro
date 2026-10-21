"""Coral stage adapter against the real quota gate and injected provider only."""
from dataclasses import replace
import http.client
import json
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

from maestro.backends.coral.native_backend import NativeCoralBackend
from maestro.backends.coral.native_client import MCP_TOOL
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.config import RunConfig
from maestro.execution.budgets import BudgetStore
from maestro.execution.grants import GrantAuthority
from maestro.types import AgentState, stable_hash


class Response:
    status=200
    headers={'Content-Type':'text/event-stream'}
    def iter_bytes(self,deadline):
        terminal={'type':'response.completed','response':{'id':'resp_offline', 'status':'completed',
            'output':[], 'usage':{'input_tokens':10,'output_tokens':5}}}
        yield b'data: '+json.dumps(terminal).encode()+b'\n\n'
    def close(self):pass


class Native:
    model='test-model'
    supports_resume=False
    preparation_requests=2
    generation_requests=1
    seen=[]
    def request(self,base_url):
        url=urlsplit(base_url)
        connection=http.client.HTTPConnection(url.hostname,url.port,timeout=5)
        connection.request('POST',url.path+'/responses',json.dumps({'model':self.model,'input':[]}),
            {'Authorization':'Bearer synthetic-credential','Content-Type':'application/json'})
        response=connection.getresponse();body=response.read();connection.close()
        if response.status!=200:raise RuntimeError('Quota declined')
    def run_phase(self,prompt,*,base_url,deadline,tools=None,output_schema=None,session_id=None):
        self.seen.append((prompt,deadline,tools is not None))
        if prompt.startswith('PREPARATION:'):
            for _ in range(self.preparation_requests):self.request(base_url)
            assert tools('maestro_tool',{'action':'read','path':'solver/run'})
            for action in [{'action':'write','path':'solver/run','content':'bad'},
                           {'action':'write','path':'private/note','content':'bad'},
                           {'action':'shell','command':'echo denied'}]:
                with pytest.raises(PermissionError):tools('maestro_tool',action)
            text='Read solver/run and private/. Try a different tie break.'
        elif output_schema is not None:
            self.request(base_url)
            assert 'Read solver/run' in prompt and 'parent_source' not in prompt
            text=json.dumps({'action':'revise','plan':{'target':'tie_break','mod_type':'rule_replacement',
                'diagnosis':'inspect frozen seed','change':'replace tie break','expected_effect':'improve',
                'evidence_run_ids':[]},'lessons':[]})
        else:
            for _ in range(self.generation_requests):self.request(base_url)
            assert '"plan"' in prompt and 'replace tie break' in prompt
            denied=tools('maestro_tool',{'action':'write','path':'history/index.json','content':'bad'})
            assert denied['error']=='PermissionError'
            tools('maestro_tool',{'action':'write','path':'solver/run','content':'changed'})
            tools('maestro_tool',{'action':'write','path':'private/note','content':'persisted'})
            text='implemented'
        return SimpleNamespace(text=text,response_hash=stable_hash(text),thread_id='')
    def close(self):pass


@pytest.fixture
def backend(tmp_path):
    seed=tmp_path/'seed';seed.mkdir();(seed/'run').write_text('seed')
    store=BudgetStore(tmp_path/'requests.sqlite')
    forwarded=[]
    def upstream(path,headers,body,timeout):
        value=json.loads(body);forwarded.append(value)
        declared=value.get('tools',[])
        assert not declared or (declared[0]['name']=='mcp__maestro'
                                and declared[0]['tools'][0]['name']==MCP_TOOL)
        return Response()
    native=Native();native.seen=[]
    def proxy(*args,**kwargs):return CodexQuotaProxy(*args,upstream=upstream,**kwargs)
    backend=NativeCoralBackend(native,store,WorkspaceStore(tmp_path/'workspaces',tmp_path/'cas'),
        precheck=lambda candidate:(True,'offline'),seed_solver=seed,candidate_runner=object(),image='',
        config=RunConfig(rounds=1,generation_calls=3),proxy_factory=proxy,simulation=True)
    backend.supports_runtime_only_repair = False  # Historical tool-loop fixture, not the new policy.
    backend.begin_episode('offline')
    candidate=backend.prepare_seed();backend.register_parent(candidate['candidate_hash'],candidate['candidate_path'])
    state=AgentState('agent',candidate['candidate_hash'],.5)
    yield backend,state,native,forwarded
    backend.close()


def prepare_and_plan(backend,state):
    evidence,usage=backend.prepare_with_receipt(state,0,{'board':{}},deadline=time.monotonic()+20)
    decision,receipt,context=backend.plan_with_receipt(state,0,{'board':{}},deadline=time.monotonic()+20)
    return decision.plan,evidence,usage,receipt


def test_native_stages_have_separate_quotas_and_edit_grant(backend):
    owner,state,native,forwarded=backend
    plan,evidence,usage,receipt=prepare_and_plan(owner,state)
    assert usage['calls']==2 and receipt.actual_requests==1
    workspace=owner._workspace(state)
    assert (workspace.solver/'run').read_text()=='seed'
    assert not (workspace.private/'note').exists()
    authority=GrantAuthority()
    grant=authority.issue(episode_id=owner.episode_id,round_index=0,agent_id=state.agent_id,
        plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=0,expires_at=time.time()+20)
    result=owner.generate(state,plan,0,deadline=time.monotonic()+20,grant=grant,grant_authority=authority)
    assert (workspace.private/'note').read_text()=='persisted'
    assert owner.budget_store.usage('offline:0:agent:PREPARATION')['calls']==2
    assert owner.budget_store.usage('offline:0:agent:PLAN_RO')['calls']==1
    assert owner.last_generation_usage['calls']==1
    assert len(forwarded)==4
    with pytest.raises(PermissionError):
        owner.prepare_with_receipt(state,0,{},deadline=time.monotonic()+20)


def test_no_planning_without_same_round_preparation(backend):
    owner,state,_,forwarded=backend
    with pytest.raises(PermissionError):owner.plan_with_receipt(state,0,{},deadline=time.monotonic()+20)
    assert not forwarded


def test_planning_exposes_matching_failed_and_zero_gain_evidence(backend):
    from maestro.types import HistoryRecord
    owner,state,native,_=backend
    state.history=[
        HistoryRecord('zero',state.agent_id,.5,.5,.5,True,'tie_break','rule_replacement'),
        HistoryRecord('failed',state.agent_id,.5,.5,0,False,'tie_break','rule_replacement',status='INVALID'),
        HistoryRecord('other',state.agent_id,.5,.5,.5,True,'initialization','new_component')]
    original=native.run_phase
    def run_phase(prompt,**options):
        response=original(prompt,**options)
        if prompt.startswith('PLAN_RO:'):
            context=json.loads(prompt.split('\n',1)[1])
            assert context['own_evidence_index']==[
                {'experiment_id':r.experiment_id,'target':r.target,'mod_type':r.mod_type,
                 'valid':r.valid,'status':r.status} for r in state.history]
            assert 'including invalid, failed and zero-improvement outcomes' in prompt
            payload=json.loads(response.text)
            payload['plan']['evidence_run_ids']=['zero','failed']
            response.text=json.dumps(payload);response.response_hash=stable_hash(response.text)
        return response
    native.run_phase=run_phase
    plan,_,_,_=prepare_and_plan(owner,state)
    assert plan.evidence_run_ids==('zero','failed')


def test_preparation_quota_cannot_borrow_from_plan_or_generation(backend):
    owner,state,native,forwarded=backend
    native.preparation_requests=5
    with pytest.raises(RuntimeError):
        owner.prepare_with_receipt(state,0,{},deadline=time.monotonic()+20)
    assert len(forwarded)==4
    assert owner.budget_store.usage('offline:0:agent:PREPARATION')['calls']==4
    with pytest.raises(PermissionError):owner.plan_with_receipt(state,0,{},deadline=time.monotonic()+20)


def test_generation_repairs_share_three_real_requests(backend):
    owner,state,native,forwarded=backend
    plan,*_=prepare_and_plan(owner,state)
    authority=GrantAuthority()
    for revision in range(3):
        grant=authority.issue(episode_id=owner.episode_id,round_index=0,agent_id=state.agent_id,
            plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=revision,expires_at=time.time()+20)
        owner.generate(state,plan,0,revision=revision,deadline=time.monotonic()+20,
                       grant=grant,grant_authority=authority)
    assert owner.budget_store.usage(f'offline:0:agent:GENERATION:{plan.version_id}')['calls']==3


def test_peer_unknown_blocks_further_native_dispatch_and_tools(backend):
    owner,state,native,forwarded=backend
    owner.budget_store.connection.execute("INSERT INTO codex_phase_scopes VALUES('old-peer','UNKNOWN')")
    from maestro.backends.codex_session.autonomous_backend import CodexPhaseUnknown
    with pytest.raises(CodexPhaseUnknown):
        owner.prepare_with_receipt(state,0,{},deadline=time.monotonic()+20)
    assert not forwarded
    assert owner.budget_store.connection.execute(
        "SELECT status FROM codex_phase_scopes WHERE account='old-peer'").fetchone()[0]=='UNKNOWN'
