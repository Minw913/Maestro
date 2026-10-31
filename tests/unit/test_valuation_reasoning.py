from decimal import Decimal
import copy
import json
import time
import pytest

from maestro.agents.valuation_reasoning import (ValuationReadBroker,expected_credit,decode,screened,VALUATION_PROMPT,SCHEMA,STRATEGY,outcome_class_contract,canonicalize_rounded_no_reward)


def assessment():
    return {'baseline_assessment':'reasonable','scenarios':[
        {'kind':'no_positive_reward','probability':.3,'predicted_score':.7},
        {'kind':'catchup','probability':.5,'predicted_score':.75},
        {'kind':'frontier','probability':.2,'predicted_score':.83}],
        'evidence':[{'path':'solver/run','observation':'The implemented change removes repeated scans.'}],
        'rationale':'Bounded local improvement is more likely than a breakthrough.','uncertainties':['Runtime risk']}


def test_credit_mixture_and_unavailable_classes():
    assert expected_credit(assessment(),.7,.8,scale=100,catchup=.25,paths={'solver/run'})==Decimal('1.725000')
    value=assessment();value['scenarios'][1].update(probability=0,predicted_score=.8)
    value['scenarios'][0].update(probability=.8,predicted_score=.8)
    value['scenarios'][2].update(predicted_score=.85)
    assert expected_credit(value,.8,.8,scale=100,catchup=.25,paths={'solver/run'})==1
    # At the score ceiling all mass can be on the no-improvement outcome.
    for s in value['scenarios']:s.update(probability=int(s['kind']=='no_positive_reward'),predicted_score=1)
    assert expected_credit(value,1,1,scale=100,catchup=.25,paths={'solver/run'})==0


def test_empty_class_boundaries_are_explicit_and_still_strict():
    equal=outcome_class_contract(.8,.8)
    assert equal['unavailable_classes']==['catchup']
    assert outcome_class_contract(1,1)['unavailable_classes']==['catchup','frontier']
    assert outcome_class_contract(.7,.8)['unavailable_classes']==[]
    assert 'probability to exactly 0' in VALUATION_PROMPT

    value=assessment()
    value['scenarios'][0].update(probability=.8,predicted_score=.8)
    value['scenarios'][1].update(probability=0,predicted_score=.1)
    value['scenarios'][2].update(probability=.2,predicted_score=.85)
    assert expected_credit(value,.8,.8,scale=100,catchup=.25,paths={'solver/run'})==1
    value['scenarios'][1]['probability']=.1
    value['scenarios'][0]['probability']=.7
    with pytest.raises(ValueError,match='catchup predicted_score'):
        expected_credit(value,.8,.8,scale=100,catchup=.25,paths={'solver/run'})

    value['scenarios'][1].update(probability=0,predicted_score=.8)
    value['scenarios'][2].update(probability=.3,predicted_score=.8)
    with pytest.raises(ValueError,match='frontier predicted_score'):
        expected_credit(value,.8,.8,scale=100,catchup=.25,paths={'solver/run'})
    value['scenarios'][0].update(probability=.7,predicted_score=1)
    value['scenarios'][2].update(probability=.3,predicted_score=1)
    with pytest.raises(ValueError,match='frontier predicted_score'):
        expected_credit(value,1,1,scale=100,catchup=.25,paths={'solver/run'})


def test_three_decimal_parent_rounding_is_recorded_and_never_earns_credit():
    parent=.8389497696956891
    value=assessment()
    value['scenarios']=[
        {'kind':'no_positive_reward','probability':.65,'predicted_score':.839},
        {'kind':'catchup','probability':.33,'predicted_score':.846},
        {'kind':'frontier','probability':.02,'predicted_score':.935}]
    with pytest.raises(ValueError,match='no_positive_reward predicted_score'):
        expected_credit(value,parent,.9327561082565461,scale=100,catchup=.25,paths={'solver/run'})
    corrected,changes=canonicalize_rounded_no_reward(value,parent)
    assert corrected['scenarios'][0]['predicted_score']==parent
    assert changes==[{'kind':'no_positive_reward','original_predicted_score':.839,
                      'canonical_predicted_score':parent,'reason':'rounded_parent_score'}]
    assert value['scenarios'][0]['predicted_score']==.839  # Preserve the raw answer.
    assert expected_credit(corrected,parent,.9327561082565461,
        scale=100,catchup=.25,paths={'solver/run'})>0
    assert canonicalize_rounded_no_reward(corrected,parent)[1]==[]


@pytest.mark.parametrize('score',[.84,.8395,.9,1])
def test_other_no_reward_overshoots_remain_invalid(score):
    parent=.8389497696956891
    value=assessment();value['scenarios'][0]['predicted_score']=score
    corrected,changes=canonicalize_rounded_no_reward(value,parent)
    assert corrected==value and changes==[]
    with pytest.raises(ValueError,match='no_positive_reward predicted_score'):
        expected_credit(corrected,parent,.9327561082565461,scale=100,catchup=.25,paths={'solver/run'})


def test_empty_private_directory_cannot_be_cited_as_evidence():
    value=assessment()
    value['evidence'].append({'path':'private','observation':'No technical notes were available.'})
    with pytest.raises(ValueError,match='Unknown evidence path'):
        expected_credit(value,.7,.8,scale=100,catchup=.25,
                        paths={'solver/run','public/contract.json'})
    assert 'Directory names, paths returned only by list, and absent files' in VALUATION_PROMPT
    assert 'describe missing notes in uncertainties' in VALUATION_PROMPT


@pytest.mark.parametrize('case',['sum','negative','bool','nan','duplicate','class','path','extra','rationale'])
def test_bad_assessment_rejected(case):
    value=assessment()
    if case=='sum':value['scenarios'][0]['probability']=.9
    if case=='negative':value['scenarios'][0]['probability']=-.1
    if case=='bool':value['scenarios'][0]['probability']=True
    if case=='nan':value['scenarios'][0]['predicted_score']=float('nan')
    if case=='duplicate':value['scenarios'][0]['kind']='frontier'
    if case=='class':value['scenarios'][2]['predicted_score']=.72
    if case=='path':value['evidence'][0]['path']='authority/ledger.sqlite'
    if case=='extra':value['bid']=100
    if case=='rationale':value['rationale']=''
    with pytest.raises(ValueError):expected_credit(value,.7,.8,scale=100,catchup=.25,paths={'solver/run'})


@pytest.mark.parametrize('field,target,legacy_limit',[
    ('observation',200,4096),('rationale',400,8192),('uncertainty',160,2048),
])
def test_prompt_length_targets_do_not_reject_valid_answers(field,target,legacy_limit):
    value=assessment()
    def set_text(text):
        if field=='observation':value['evidence'][0]['observation']=text
        elif field=='rationale':value['rationale']=text
        else:value['uncertainties']=[text]
    set_text('界'*(target+1))
    assert expected_credit(value,.7,.8,scale=100,catchup=.25,paths={'solver/run'})==Decimal('1.725000')
    set_text('界'*(legacy_limit+1))
    with pytest.raises(ValueError):expected_credit(value,.7,.8,scale=100,catchup=.25,paths={'solver/run'})
    assert str(target) in VALUATION_PROMPT
    schema=(SCHEMA['properties']['evidence']['items']['properties']['observation'] if field=='observation'
        else SCHEMA['properties']['rationale'] if field=='rationale'
        else SCHEMA['properties']['uncertainties']['items'])
    assert 'maxLength' not in schema


def test_final_list_limits():
    value=assessment()
    value['evidence']*=5
    assert expected_credit(value,.7,.8,scale=100,catchup=.25,paths={'solver/run'})==Decimal('1.725000')
    value['evidence']*=4
    with pytest.raises(ValueError):expected_credit(value,.7,.8,scale=100,catchup=.25,paths={'solver/run'})
    value['evidence']=value['evidence'][:1]
    value['uncertainties']=['x']*4
    assert expected_credit(value,.7,.8,scale=100,catchup=.25,paths={'solver/run'})==Decimal('1.725000')
    value['uncertainties']=['x']*17
    with pytest.raises(ValueError):expected_credit(value,.7,.8,scale=100,catchup=.25,paths={'solver/run'})


def test_strict_json_and_strategy_free_prompt():
    with pytest.raises(ValueError):decode('{"x":1,"x":2}')
    with pytest.raises(ValueError):decode('{"x":NaN}')
    # The live DSH response had an extra ] after the rationale string. Keep
    # rejecting it rather than silently repairing a terminal model answer.
    with pytest.raises(json.JSONDecodeError):
        decode('{"rationale":"brief reason"],"uncertainties":[]}')
    assert 'The rationale field is one JSON string' in VALUATION_PROMPT
    assert 'never a rounded-up approximation of P' in VALUATION_PROMPT
    assert 'Do not put a closing square bracket after rationale' in VALUATION_PROMPT
    assert 'only a proven malformed or output-capped final answer' in VALUATION_PROMPT
    assert not STRATEGY.search(VALUATION_PROMPT)
    result=screened({'wallet':123,'change':'increase bid to win the auction','diagnosis':'duplicate loop'})
    assert 'wallet' not in result and 'auction' not in str(result)


def test_readonly_scope_traversal_budget_and_close():
    files={'solver/run':'print(1)','public/contract.json':'{}','private/note.txt':'technical fact'}
    broker=ValuationReadBroker(files,time.monotonic()+10)
    files['solver/run']='mutated'
    assert broker('maestro_tool',{'action':'read','path':'solver/run'})['content']=='print(1)'
    assert broker('maestro_tool',{'action':'search','path':'solver/','query':'print'})['matches']
    for action in ['write','shell','remove','mkdir']:
        with pytest.raises(PermissionError):broker('maestro_tool',{'action':action,'path':'solver/run'})
    for path in ['/etc/passwd','../solver/run','public/board.json','private/auth.json','history/../x','solver\\run','authority/ledger.sqlite']:
        with pytest.raises(PermissionError):broker('maestro_tool',{'action':'read','path':path})
    with pytest.raises(PermissionError):broker('maestro_tool',{'action':'batch','operations':[{'action':'write','path':'solver/run'}]})
    broker.close()
    with pytest.raises(TimeoutError):broker('maestro_tool',{'action':'read','path':'solver/run'})
    broker=ValuationReadBroker(files,time.monotonic()+10,max_calls=1)
    broker('maestro_tool',{'action':'list','path':''})
    with pytest.raises(ValueError):broker('maestro_tool',{'action':'list','path':''})


def test_final_evidence_paths_include_only_returned_content():
    files={'public/contract.json':'P=C','solver/run':'print(1)',
           'history/outcome-0000.json':'actual_credit=2','private/note.txt':'technical note'}
    broker=ValuationReadBroker(files,time.monotonic()+10)
    broker('maestro_tool',{'action':'list','path':'private'})
    assert broker.evidence_paths()==()
    broker('maestro_tool',{'action':'read','path':'public/contract.json'})
    broker('maestro_tool',{'action':'search','path':'solver','query':'print'})
    broker('maestro_tool',{'action':'batch','operations':[
        {'action':'read','path':'history/outcome-0000.json'},
        {'action':'list','path':'private'}]})
    assert broker.evidence_paths()==(
        'history/outcome-0000.json','public/contract.json','solver/run')
    assert 'private' not in broker.evidence_paths()
    assert 'private/note.txt' not in broker.evidence_paths()

    large=ValuationReadBroker({'a':'a'*140_000,'b':'b'*140_000},time.monotonic()+10)
    with pytest.raises(ValueError,match='Read output exceeds bound'):
        large('maestro_tool',{'action':'batch','operations':[
            {'action':'read','path':'a'},{'action':'read','path':'b'}]})
    assert large.evidence_paths()==() and large.trace==[]
