"""DeepSeek adapter with real request gate, broker and ledger; synthetic provider."""
import http.client
import json
import time
from types import SimpleNamespace
from dataclasses import replace
from urllib.parse import urlsplit

import pytest

from maestro.backends.coral.deepseek_backend import NativeDeepSeekBackend
from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy,DEEPSEEK_TOOL
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.execution.budgets import BudgetStore
from maestro.execution.grants import GrantAuthority
from maestro.types import AgentState,stable_hash


class Native:
    model='offline-deepseek'
    supports_resume=False
    def request(self,url,tools=False):
        url=urlsplit(url);connection=http.client.HTTPConnection(url.hostname,url.port,timeout=2)
        connection.request('POST',url.path+'/messages',json.dumps({'model':self.model,'messages':[],'max_tokens':32,
            'tools':[{'name':DEEPSEEK_TOOL}] if tools else []}))
        response=connection.getresponse();response.read();connection.close()
        if response.status!=200:raise RuntimeError('gate refused')
    def run_phase(self,prompt,*,base_url,deadline,tools=None,output_schema=None,session_id=None):
        self.request(base_url,tools=tools is not None)
        if prompt.startswith('PREPARATION:'):
            assert tools('maestro_tool',{'action':'read','path':'solver/run'})
            assert tools('maestro_tool',{'action':'search','path':'private','query':'tie'})
            with pytest.raises(PermissionError):tools('maestro_tool',{'action':'write','path':'solver/run','content':'denied'})
            text='Read solver/run and private/note: inspect tie breaks'
        elif prompt.startswith('PLAN_RO:'):
            assert tools is None and 'Read solver/run' in prompt
            text=json.dumps({'action':'revise','plan':{'target':'tie_break','mod_type':'rule_replacement',
                'diagnosis':'inspect seed','change':'change tie break','expected_effect':'reduce set',
                'evidence_run_ids':[]},'lessons':[]})
        else:
            assert tools is None and output_schema is not None and 'change tie break' in prompt
            text=json.dumps({'files':[{'path':'run','content':'#!/bin/sh\necho changed\n','executable':True}]})
        return SimpleNamespace(text=text,thread_id='',response_hash=stable_hash(text))
    def close(self):pass


def test_separate_deepseek_stages_own_memory_and_edit_grant(tmp_path):
    seed=tmp_path/'seed';seed.mkdir();(seed/'run').write_text('seed')
    store=BudgetStore(tmp_path/'requests.sqlite');calls=[]
    def upstream(payload,deadline):
        calls.append(payload)
        value={'type':'message','id':f'msg_{len(calls)}','stop_reason':'end_turn',
            'content':[{'type':'text','text':'provider-terminal'}],'usage':{'input_tokens':10,'output_tokens':3}}
        return json.dumps(value).encode(),'application/json'
    def proxy(*args,**kwargs):return DeepSeekQuotaProxy(*args,upstream=upstream,**kwargs)
    backend=NativeDeepSeekBackend(Native(),store,WorkspaceStore(tmp_path/'workspaces',tmp_path/'cas'),
        precheck=lambda candidate:(True,'offline'),seed_solver=seed,candidate_runner=object(),image='',
        config=RunConfig(rounds=1),proxy_factory=proxy,simulation=True)
    try:
        backend.begin_episode('offline')
        candidate=backend.prepare_seed();backend.register_parent(candidate['candidate_hash'],candidate['candidate_path'])
        state=AgentState('agent',candidate['candidate_hash'],.5)
        workspace=backend._workspace(state);(workspace.private/'note').write_text('tie break evidence')
        evidence,usage=backend.prepare_with_receipt(state,0,{},deadline=time.monotonic()+10)
        assert usage['calls']==1 and evidence['read_trace']
        decision,receipt,_=backend.plan_with_receipt(state,0,{},deadline=time.monotonic()+10)
        assert receipt.actual_requests==1 and receipt.provider_request_id=='msg_2'
        with pytest.raises(PermissionError):backend.generate(state,decision.plan,0,deadline=time.monotonic()+10)
        authority=GrantAuthority()
        grant=authority.issue(episode_id='offline',round_index=0,agent_id='agent',
            plan_version_id=decision.plan.version_id,parent_hash=state.parent_hash,epoch=0,expires_at=time.time()+10)
        result=backend.generate(state,decision.plan,0,deadline=time.monotonic()+10,grant=grant,grant_authority=authority)
        assert 'changed' in (workspace.solver/'run').read_text()
        assert result['candidate_hash']!=state.parent_hash and len(calls)==3
        assert backend.last_generation_usage['usage_complete']
        assert all(payload['model']=='offline-deepseek' for payload in calls)
        with pytest.raises(PermissionError):backend.prepare_with_receipt(state,0,{},deadline=time.monotonic()+10)
    finally:backend.close()


@pytest.mark.parametrize('cap', [2, 4])
def test_preparation_reports_real_remaining_requests_and_reserves_summary(tmp_path, cap):
    """Multiple broker reads in a response share one actual-request charge."""
    feedback = []
    class BudgetAware(Native):
        def run_phase(self, prompt, *, base_url, deadline, tools=None, **kwargs):
            assert f'at most {cap} actual model requests' in prompt
            assert f'at most {cap - 1} tool-calling model responses' in prompt
            assert 'final evidence summary with NO tool calls' in prompt
            for index in range(cap - 1):
                self.request(base_url, tools=True)
                for _ in range(2):
                    result = tools('maestro_tool', {'action': 'read', 'path': 'solver/run'})
                    budget = result['preparation_budget']
                    assert budget['consumed_requests'] == index + 1
                    assert budget['remaining_requests'] == cap - index - 1
                    assert budget['next_response_must_be_final_summary'] == (index == cap - 2)
                    feedback.append(budget)
            self.request(base_url, tools=True)  # Final response contains no tool use.
            return SimpleNamespace(text='Evidence: solver/run; no further reads needed.')
    seed = tmp_path / 'seed'; seed.mkdir(); (seed / 'run').write_text('seed')
    store = BudgetStore(tmp_path / 'requests.sqlite')
    dispatched = []
    def upstream(payload, deadline):
        dispatched.append(payload)
        return json.dumps({'type': 'message', 'id': f'msg_{len(dispatched)}',
            'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'terminal'}],
            'usage': {'input_tokens': 10, 'output_tokens': 3}}).encode(), 'application/json'
    def proxy(*args, **kwargs):
        return DeepSeekQuotaProxy(*args, upstream=upstream, **kwargs)
    backend = NativeDeepSeekBackend(BudgetAware(), store,
        WorkspaceStore(tmp_path / 'workspaces', tmp_path / 'cas'),
        precheck=lambda candidate: (True, 'offline'), seed_solver=seed,
        candidate_runner=object(), image='',
        config=replace(RunConfig(rounds=1), preparation_calls=cap),
        proxy_factory=proxy, simulation=True)
    try:
        backend.begin_episode('offline')
        candidate = backend.prepare_seed()
        backend.register_parent(candidate['candidate_hash'], candidate['candidate_path'])
        evidence, usage = backend.prepare_with_receipt(
            AgentState('agent', candidate['candidate_hash'], .5), 0, {}, deadline=time.monotonic()+10)
        assert usage['calls'] == len(dispatched) == cap
        assert len(evidence['read_trace']) == 2 * (cap - 1)
        assert evidence['status'] == 'PREPARED' and usage['usage_complete']
        assert feedback[-1]['remaining_requests'] == 1
    finally:
        backend.close()
