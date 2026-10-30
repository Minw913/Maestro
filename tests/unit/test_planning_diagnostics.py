import json

from maestro.agents.planning_diagnostics import planning_rejection_diagnostic, safe_planning_diagnostic
from maestro.backends.codex_session.client import _PLAN_SCHEMA
from maestro.agents.planner import mock_plan, validate_plan
from maestro.types import AgentState
from dataclasses import replace
import pytest


def test_plan_schema_allows_bounded_free_categories():
    properties = _PLAN_SCHEMA['properties']['plan']['anyOf'][1]['properties']
    for field in ('target','mod_type'):
        assert properties[field] == {'type':'string','minLength':1,'maxLength':64}
    plan=replace(mock_plan(AgentState('a','p',.5),0,1),
                 target='spectral_pruning',mod_type='hybrid_rewrite')
    assert validate_plan(plan,'p',[]) == plan
    for field in ('target','mod_type'):
        for bad in ('', 'x'*65, 'bad\nlabel', ' padded '):
            with pytest.raises(ValueError,match=f'invalid {field}'):
                validate_plan(replace(plan,**{field:bad}),'p',[])


def test_validation_diagnostics_are_allowlisted_and_data_free():
    assert planning_rejection_diagnostic(
        ValueError('matching own evidence was omitted, including failures'), 'validation'
    )['code']=='INCOMPLETE_EVIDENCE'
    assert planning_rejection_diagnostic(
        ValueError('planner cannot override parent_hash'), 'validation'
    )['code']=='IDENTITY_OVERRIDE_PARENT_HASH'
    assert planning_rejection_diagnostic(ValueError('invalid target'), 'validation')['code']=='INVALID_TARGET'
    assert planning_rejection_diagnostic(ValueError('invalid mod_type'), 'validation')['code']=='INVALID_MOD_TYPE'
    secret='DO_NOT_STORE_PROVIDER_BODY'
    diagnostic=planning_rejection_diagnostic(ValueError(secret), 'validation')
    assert diagnostic['code']=='UNCLASSIFIED_VALIDATION_ERROR'
    assert secret not in json.dumps(diagnostic)
    assert planning_rejection_diagnostic(
        json.JSONDecodeError(secret, secret, 0), 'validation'
    )['code']=='JSON_DECODE_ERROR'


def test_request_diagnostics_do_not_include_transport_exception_text():
    secret='DO_NOT_STORE_ENDPOINT_OR_TOKEN'
    diagnostic=planning_rejection_diagnostic(TimeoutError(secret), 'request')
    assert diagnostic=={'category':'REQUEST_EXECUTION','code':'DEADLINE_OR_TIMEOUT',
                        'exception_type':'TimeoutError'}
    assert secret not in json.dumps(diagnostic)
    assert planning_rejection_diagnostic(ValueError(secret), 'deadline')['code'] == 'PLANNING_DEADLINE_EXCEEDED'
    assert planning_rejection_diagnostic(ValueError(secret), 'request_count')['code'] == 'REQUEST_COUNT_MISMATCH'
    assert safe_planning_diagnostic({'category':'PLAN_VALIDATION','code':secret,
        'exception_type':'ValueError'}) is None
    assert safe_planning_diagnostic(diagnostic)==diagnostic
