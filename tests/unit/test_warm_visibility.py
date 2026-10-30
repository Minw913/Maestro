from decimal import Decimal

from maestro.agents.retrieval import build_context
from maestro.agents.visibility import agent_state_view
from maestro.types import AgentState, HistoryRecord


def test_warm_state_exposes_own_outcome_without_global_baseline():
    record=HistoryRecord('job','a',.4,.9,.5,True,'restart','new_component',Decimal('2.5'))
    state=AgentState('a','parent',.5,history=[record])
    warm=agent_state_view(state,'WARM')
    context=build_context(warm,{})
    assert context['board']=={}
    assert context['history'][0]['score']==.5
    assert 'champion_score' not in context['history'][0]
    assert 'credit' not in context['history'][0]
    assert state.history[0]==record
    market=build_context(agent_state_view(state,'MAIN'),{'champion_score':.9})
    assert market['board']['champion_score']==.9
    assert market['history'][0]['champion_score']==.9
    assert market['history'][0]['credit']=='2.5'
