"""Review regression tests for independent worker accounting and budget labels."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time

import pytest

from maestro.agents.planner import PlanDecision, mock_plan
from maestro.agents.planning_batch import parallel_plans
from maestro.backends.coral.backend import CoralBackend
from maestro.config import RunConfig
from maestro.types import AgentState


def test_generation_receipts_do_not_cross_threads():
    driver = object.__new__(CoralBackend)
    driver._generation_usage = threading.local()
    barrier = threading.Barrier(2)
    def run(agent):
        driver.last_generation_usage = {'agent': agent}
        barrier.wait(timeout=2)
        return driver.last_generation_usage
    with ThreadPoolExecutor(max_workers=2) as pool:
        a, b = list(pool.map(run, ('a', 'b')))
    assert a == {'agent': 'a'} and b == {'agent': 'b'}
    assert driver.last_generation_usage == {}


def test_session_planning_requires_explicit_budget_unit():
    calls = []
    class SessionBackend:
        planning_capabilities = {'single_request': False, 'single_session_turn': True,
            'hard_request_timeout': True, 'mutates_input_state': False}
        def plan_with_receipt(self, state, round_index, context, *, deadline):
            calls.append(state.agent_id)
            return (PlanDecision('revise', mock_plan(state, round_index, 0)),
                {'actual_requests': None, 'session_turns': 1, 'request_count_unknown': True},
                {'agent_id': state.agent_id})
    states = {'a': AgentState('a','p',.5)}
    strict = parallel_plans(states, {'a': {}}, SessionBackend(), 0, time.monotonic()+2)
    assert not calls and strict['a'].error.startswith('BACKEND_UNBOUNDED')
    pilot = parallel_plans(states, {'a': {}}, SessionBackend(), 0, time.monotonic()+2,
                           budget_unit='session_turn')
    assert calls == ['a'] and pilot['a'].status == 'PLANNED'


def test_session_turn_cannot_pretend_to_be_metered_request():
    class BadSession:
        planning_capabilities = {'single_session_turn': True,
            'hard_request_timeout': True, 'mutates_input_state': False}
        def plan_with_receipt(self, state, round_index, context, *, deadline):
            return (PlanDecision('revise', mock_plan(state, round_index, 0)),
                {'actual_requests': 1, 'session_turns': 1, 'request_count_unknown': False},
                {'agent_id': state.agent_id})
    results = parallel_plans({'a': AgentState('a','p',.5)}, {'a': {}}, BadSession(),
        0, time.monotonic()+2, budget_unit='session_turn')
    assert results['a'].status == 'ABSTAIN' and 'unmetered' in results['a'].error


def test_session_pilot_is_a_separate_live_profile():
    with pytest.raises(ValueError, match='real pace'):
        RunConfig(profile='codex_session_pilot')
    config = RunConfig(mode='pace', profile='codex_session_pilot')
    assert config.profile != RunConfig().profile


def test_unknown_session_planning_remains_visible_in_resource_costs(tmp_path):
    from dataclasses import replace
    import json
    from maestro.llm import InferenceRequestUnknown
    from maestro.loop import ExperimentEngine
    class Store:
        def usage(self, scope):
            return {'calls': 0, 'session_turns': 1, 'request_count_unknown': True,
                    'usage_complete': False}
    class Backend:
        planning_capabilities = {'single_session_turn': True,
            'hard_request_timeout': True, 'mutates_input_state': False}
        turn_store = Store()
        def plan_with_receipt(self, state, round_index, context, *, deadline):
            raise InferenceRequestUnknown('remote session unconfirmed',
                request_id='r', scope_id='s', context=context)
        def close(self):
            pass
    with ExperimentEngine(tmp_path,RunConfig(n_agents=1,slots=1,rounds=1,warm_opportunities_per_agent=1)) as engine:
        engine.config=replace(engine.config,mode='pace',profile='codex_session_pilot')
        engine.backend=Backend()
        states={'a':AgentState('a','p',.5)}
        engine.ledger.create_episode('pilot','first_price',engine.config,states)
        from maestro.types import MaestroError
        with pytest.raises(MaestroError,match='Unresolved planning'):
            engine._round('pilot',0,states,'first_price')
        usage=engine.ledger.connection.execute("SELECT * FROM resource_usage WHERE phase='planning'").fetchone()
        payload=json.loads(usage['payload'])
        assert usage['llm_calls']==0  # Only the confirmed lower bound.
        assert payload['session_turns']==1 and payload['request_count_unknown'] is True
        assert payload['usage_complete'] is False


def test_codex_check_uses_session_client_without_requesting_api_keys(tmp_path,monkeypatch,capsys):
    import json
    from types import SimpleNamespace
    from maestro import cli
    import maestro.backends.codex_session as session
    class Client:
        def __init__(self,**kwargs):
            assert kwargs['model'] is None
        def login_status(self):
            return {'authenticated':True,'auth_method':'chatgpt','external_api_key_required':False}
        def require_chatgpt_login(self):
            return self.login_status()
        def structured_proposal(self,*args,**kwargs):
            return SimpleNamespace(value={'ok':True},completed_turns=1,
                actual_inference_requests=None,input_tokens=10,output_tokens=3)
    monkeypatch.setattr(session,'CodexSessionClient',Client)
    assert cli.main(['--root',str(tmp_path),'codex-check','--probe'])==0
    output=json.loads(capsys.readouterr().out)
    assert output['external_api_key_required'] is False
    assert output['live_probe']['ok'] is True
    assert output['live_probe']['actual_inference_requests'] is None
