"""Search-tree truthfulness: adoption, wallet time, selection and read-only roots."""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import sqlite3

import pytest

from maestro.search_tree import load_search_tree, plot_search_tree, resolve_root


def make_ledger(root, regime='first_price_naive'):
    path=root/'authority/ledger.sqlite';path.parent.mkdir(parents=True)
    db=sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE episodes(id,policy,benchmark,config_json,bootstrap_id,status);
        CREATE TABLE rounds(episode_id,round_index,status,snapshot_json);
        CREATE TABLE checkpoints(episode_id,round_index,payload);
        CREATE TABLE priority_orders(episode_id,round_index,payload);
        CREATE TABLE round_submissions(episode_id,round_index,agent_id,payload);
        CREATE TABLE settlements(episode_id,round_index,agent_id,wealth_before,wealth_after,payment,reward,payload);
        CREATE TABLE final_allocations(episode_id,round_index,agent_id,ticket_id,x,payment);
        CREATE TABLE eval_tickets(id,job_id,candidate_hash);
        CREATE TABLE verified_results(job_id,payload);
        CREATE TABLE invitations(episode_id,round_index,agent_id,status);
    ''')
    cfg=json.dumps({'coral':{'model':'test-model'},'seed':7,'rounds':2,'evaluation_instance_count':3})
    db.executemany('INSERT INTO episodes VALUES(?,?,?,?,?,?)',[
        ('warm','warm','pace',cfg,None,'COMPLETED'),('market',regime,'pace',cfg,'warm','COMPLETED')])
    states={a:{'parent_hash':'seed','parent_score':.5,'wealth':'100'} for a in ['agent-1','agent-2']}

    def append(ep,idx,selected):
        before=deepcopy(states)
        champion=max(before.values(),key=lambda s:s['parent_score'])
        snap={'states':before,'board':{'champion_hash':champion['parent_hash'],'champion_score':champion['parent_score']}}
        db.execute('INSERT INTO rounds VALUES(?,?,?,?)',(ep,idx,'SETTLED',json.dumps(snap)))
        db.execute('INSERT INTO priority_orders VALUES(?,?,?)',(ep,idx,json.dumps({'order':list(states)})))
        for a,state in states.items():
            trial=selected.get(a);bid='0' if ep=='warm' else '.2'
            # Missing submissions are legal for an unfunded/abstaining agent.
            if trial:
                db.execute('INSERT INTO round_submissions VALUES(?,?,?,?)',(ep,idx,a,json.dumps({'value':bid,'bid':bid})))
            payment=Decimal(bid) if trial and ep=='market' and regime=='first_price_naive' else Decimal(0)
            reward=Decimal(trial[2]) if trial else Decimal(0)
            job=f'{ep}:{idx}:{a}' if trial else None
            ticket=job+':ticket' if job else None
            if trial:
                h,s,_=trial
                db.execute('INSERT INTO eval_tickets VALUES(?,?,?)',(ticket,job,h))
                result={'candidate_hash':h,'score':s,'valid':True,'status':'COMPLETED'}
                db.execute('INSERT INTO verified_results VALUES(?,?)',(job,json.dumps(result)))
                if s>state['parent_score']:state.update(parent_hash=h,parent_score=s)
            state['wealth']=str(Decimal(state['wealth'])-payment+reward)
            db.execute('INSERT INTO final_allocations VALUES(?,?,?,?,?,?)',(ep,idx,a,ticket,int(bool(trial)),str(payment)))
            sp={'result_job_id':job}
            db.execute('INSERT INTO settlements VALUES(?,?,?,?,?,?,?,?)',
                       (ep,idx,a,before[a]['wealth'],state['wealth'],str(payment),str(reward),json.dumps(sp)))
        champion=max(states.values(),key=lambda s:s['parent_score'])
        cp={'states':states,'board':{'champion_hash':champion['parent_hash'],'champion_score':champion['parent_score']}}
        db.execute('INSERT INTO checkpoints VALUES(?,?,?)',(ep,idx,json.dumps(cp)))
    append('warm',0,{'agent-1':('warm-good',.6,'1'),'agent-2':('warm-rejected',.4,'0')})
    for s in states.values():s['wealth']='42'
    append('market',0,{'agent-2':('market-good',.7,'2')})
    append('market',1,{'agent-2':('market-rejected',.65,'0')})
    db.commit();db.close()
    return path


@pytest.mark.parametrize('regime',['first_price_naive','ucb','uniform'])
def test_exact_state_times_and_rejected_candidate_are_preserved(tmp_path,regime):
    path=make_ledger(tmp_path,regime);before=path.read_bytes()
    data=load_search_tree(tmp_path,regime,benchmark='pace-ds',model='test-model')
    assert path.read_bytes()==before
    assert len(data['rounds'])==3 and len(data['nodes'])==6
    assert data['metadata']['seed']==7
    assert set(data['metadata']['boundary']['market_initial_wealth'].values())=={'42'}
    accepted=data['rounds'][1]['agents'][1]
    assert accepted['accepted'] and accepted['champion'] and accepted['reward']=='2'
    assert accepted['wealth_before']=='42'
    assert Decimal(accepted['wealth_after'])==(Decimal('43.8') if regime=='first_price_naive' else Decimal(44))
    leaf=data['rounds'][2]['agents'][1]
    assert leaf['candidate_hash']=='market-rejected' and leaf['parent_after']=='market-good'
    assert leaf['candidate_score']==.65 and leaf['s_after']==.7 and not leaf['accepted']
    carried=data['rounds'][2]['agents'][0]
    assert carried['valuation'] is carried['bid'] is carried['reward'] is None
    assert not carried['funded'] and carried['parent_before']==carried['parent_after']


def test_selectors_are_exact_and_ambiguous_episodes_are_rejected(tmp_path):
    base=tmp_path/'runs';nested=base/'pace-ds/test-model/first_price_naive/seed7-r1'
    make_ledger(nested)
    kwargs=dict(runs_dir=base,benchmark='pace-ds',model='test-model',regime='first_price_naive',run='seed7-r1')
    assert resolve_root(**kwargs)==nested
    legacy=base/'pace-ds_test-model_seed7-r1';make_ledger(legacy)
    with pytest.raises(ValueError,match='Both nested and legacy'):resolve_root(**kwargs)
    with pytest.raises(ValueError,match='path component'):resolve_root(**{**kwargs,'run':'../other'})
    with pytest.raises(ValueError,match='model selector'):load_search_tree(nested,'first_price_naive',model='wrong')
    with sqlite3.connect(nested/'authority/ledger.sqlite') as db:
        db.execute("INSERT INTO episodes SELECT 'second',policy,benchmark,config_json,bootstrap_id,status FROM episodes WHERE id='market'")
    with pytest.raises(ValueError,match='found 2'):load_search_tree(nested,'first_price_naive')
    assert load_search_tree(nested,'first_price_naive',episode='market')['metadata']['episodes']['market']=='market'


def test_unsettled_round_is_omitted_without_fabricating_a_node(tmp_path):
    path=make_ledger(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE rounds SET status='GENERATING' WHERE episode_id='market' AND round_index=1")
        db.execute("UPDATE episodes SET status='RUNNING' WHERE id='market'")
    data=load_search_tree(tmp_path,'first_price_naive')
    assert data['metadata']['status']=='PARTIAL_SETTLED_PREFIX'
    assert data['metadata']['unsettled_rounds_omitted']['market']==[2]
    assert len(data['rounds'])==2


def test_inconsistent_wealth_is_rejected_before_output(tmp_path):
    path=make_ledger(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE settlements SET wealth_after='999' WHERE episode_id='market' AND round_index=0 AND agent_id='agent-2'")
    with pytest.raises(ValueError,match='wealth differ'):load_search_tree(tmp_path,'first_price_naive')
    assert not (tmp_path/'reports').exists()


def test_cli_selector_aliases_and_real_png_output(tmp_path):
    pytest.importorskip('matplotlib')
    from maestro.cli import main,build_parser
    root=tmp_path/'runs/pace-ds/test-model/uniform/custom';path=make_ledger(root,'uniform')
    before=path.read_bytes()
    for flag in ['--run','--timestamp','--name']:
        args=build_parser().parse_args(['plot-search-tree','--regime','uniform',flag,'custom'])
        assert args.run_label=='custom'
    for command in [['--root',str(root),'plot-search-tree','--regime','uniform'],
                    ['plot-search-tree','--root',str(root),'--regime','uniform']]:
        assert build_parser().parse_args(command).root==str(root)
    code=main(['plot-search-tree','--benchmark','pace-ds','--model','test-model','--regime','uniform',
               '--name','custom','--runs-dir',str(tmp_path/'runs'),'--dpi','50','--detail-rounds','1:2'])
    assert code==0 and path.read_bytes()==before
    out=root/'reports/search-tree/uniform'
    assert (out/'search-tree.png').read_bytes().startswith(b'\x89PNG\r\n\x1a\n')
    assert 'uniform' in (out/'search-tree.svg').read_text().lower()
    audit=json.loads((out/'audit.json').read_text())
    assert audit['nodes']==6 and audit['new_model_requests']==audit['new_candidate_evaluations']==0
    assert audit['formal_evaluations']=={'warm':2,'market':2}
    assert main(['--root',str(root),'plot-search-tree','--regime','uniform','--detail-rounds','1:99'])==2
