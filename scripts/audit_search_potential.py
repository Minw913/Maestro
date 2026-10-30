"""Read-only historical Warm/Market search audit; no current policy imports.

Uses recorded configurations, verified outcomes and original reward arithmetic.
Creates PNG/SVG plots and CSV/JSON/HTML evidence. Never invokes a model/runner.
"""
from __future__ import annotations
import argparse
import ast
from collections import Counter
import csv
from datetime import datetime,timezone
from decimal import Decimal,ROUND_HALF_EVEN
import hashlib
import html
import json
from pathlib import Path
import sqlite3
import statistics

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

POLICIES=('first_price','ucb','uniform')
AGENTS=tuple(f'agent-{i}' for i in range(1,5))
COLORS=('#2563eb','#d97706','#16a34a','#dc2626')
NAMES={'first_price':'First-price','ucb':'UCB','uniform':'Uniform','warm':'Shared Warm Start'}


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()


def D(value):return Decimal(str(value))


def reward(y,p,c,config,valid):
    if not valid:return D(0)
    y,p,c=map(D,(y,p,c))
    return (D(config['reward_scale'])*(max(D(0),y-c)+D(config['catchup_fraction'])*
        max(D(0),min(y,c)-p))).quantize(D('.000001'),rounding=ROUND_HALF_EVEN)


def require(ok,reason):
    if not ok:raise ValueError(reason)


def read_cas(root,h):
    path=root/'authority/cas/sha256'/h[:2]/h
    raw=path.read_bytes();require(hashlib.sha256(raw).hexdigest()==h,'CAS byte identity mismatch')
    return json.loads(raw)


def audit(root):
    path=root/'authority/ledger.sqlite'
    db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
    db.execute('BEGIN')
    rows=[];rounds={};episodes={};checkpoints={};plans=[]
    try:
        previous='0'*64;count=0
        for event in db.execute('SELECT * FROM events ORDER BY id'):
            require(event['previous_hash']==previous and event['hash']==digest([
                previous,event['episode_id'],event['round_index'],event['kind'],event['payload']]),
                'Event chain mismatch')
            previous=event['hash'];count+=1
        warm_ids={r[0] for r in db.execute("SELECT bootstrap_id FROM episodes WHERE policy IN ('first_price','ucb','uniform')")}
        require(len(warm_ids)==1,'Regimes do not share exactly one Warm checkpoint')
        warm_id=next(iter(warm_ids))
        warm_record=db.execute('SELECT payload,content_hash FROM warm_checkpoints WHERE id=?',(warm_id,)).fetchone()
        warm=json.loads(warm_record[0]);require(digest(warm)==warm_record[1],'Warm checkpoint hash mismatch')
        require(read_cas(root,warm_record[1])==warm,'Warm checkpoint CAS mismatch')
        for policy in ('warm',)+POLICIES:
            found=db.execute('SELECT * FROM episodes WHERE policy=?',(policy,)).fetchall()
            require(len(found)==1,'Expected one episode for '+policy)
            ep=found[0];e=ep['id'];cfg=json.loads(ep['config_json'])
            require(ep['status']=='COMPLETED','Incomplete episode '+e)
            require(digest(cfg)==ep['config_hash'],'Configuration hash mismatch')
            saved=db.execute('SELECT * FROM rounds WHERE episode_id=? AND round_index>=0 ORDER BY round_index',(e,)).fetchall()
            require(len(saved)==(5 if policy=='warm' else 20),'Unexpected number of rounds')
            rounds[policy]=[];checkpoints[policy]=[]
            episodes[policy]={'id':e,'config_hash':ep['config_hash'],'model':cfg['coral']['model'],
                'seed':cfg['seed'],'bootstrap_id':ep['bootstrap_id'],'configuration':cfg}
            for rr in saved:
                r=rr['round_index'];snap=json.loads(rr['snapshot_json'])
                require(rr['status']=='SETTLED' and digest(snap)==rr['snapshot_hash'],'Round identity/status mismatch')
                record=db.execute('SELECT payload,content_hash FROM checkpoints WHERE episode_id=? AND round_index=?',(e,r)).fetchone()
                cp=json.loads(record[0]);require(digest(cp)==record[1] and read_cas(root,record[1])==cp,'Checkpoint identity mismatch')
                checkpoints[policy].append(cp)
                rounds[policy].append({'round':r+1,'champion_before':snap['board']['champion_score'],
                    'champion_after':cp['board']['champion_score'],'champion_hash':cp['board']['champion_hash'],
                    'champion_agents':[a for a in AGENTS if cp['states'][a]['parent_hash']==cp['board']['champion_hash']]})
                if policy!='warm' and r==0:
                    for a in AGENTS:
                        initial=snap['states'][a]
                        for key in ('parent_hash','parent_score','history','ucb_n','ucb_total'):
                            require(initial[key]==warm['states'][a][key],'Warm-to-Market state mismatch: '+key)
                for a in AGENTS:
                    before,after=snap['states'][a],cp['states'][a]
                    s=db.execute('SELECT * FROM settlements WHERE episode_id=? AND round_index=? AND agent_id=?',(e,r,a)).fetchone()
                    body=json.loads(s['payload'])
                    allocation=db.execute('SELECT * FROM final_allocations WHERE episode_id=? AND round_index=? AND agent_id=?',(e,r,a)).fetchone()
                    funded=bool(allocation['x']);result=None;candidate=None
                    if funded:
                        v=db.execute('SELECT payload,payload_hash,candidate_hash FROM verified_results WHERE job_id=?',(body['result_job_id'],)).fetchone()
                        result=json.loads(v['payload']);candidate=result['candidate_hash']
                        require(digest(result)==v['payload_hash'],'Verified outcome hash mismatch')
                        expected=reward(result['score'],before['parent_score'],snap['board']['champion_score'],cfg,result['valid'])
                        require(expected==D(s['reward']),'Reward mismatch')
                        expected_score=max(before['parent_score'],result['score']) if result['valid'] else before['parent_score']
                        require(after['parent_score']==expected_score,'Best-parent adoption mismatch')
                    else:
                        require(D(s['reward'])==0 and not body.get('result_job_id'),'Unfunded reward/result invented')
                        require(after['parent_hash']==before['parent_hash'],'Unfunded parent changed')
                    require(after['ucb_n']-before['ucb_n']==int(funded),'Formal history count mismatch')
                    require(len(after['history'])-len(before['history'])==int(funded),'History mismatch')
                    require(D(after['wealth'])==D(before['wealth'])-D(s['payment'])+D(s['reward']),'Wealth mismatch')
                    records=result['instance_results'] if result else []
                    entry={'regime':policy,'episode':e,'round':r+1,'agent':a,
                        'parent_before':before['parent_score'],'parent_after':after['parent_score'],
                        'retained_gain':after['parent_score']-before['parent_score'],
                        'champion_before':snap['board']['champion_score'],'champion_after':cp['board']['champion_score'],
                        'global_champion':a in rounds[policy][-1]['champion_agents'],
                        'reward':float(D(s['reward'])),'payment':float(D(s['payment'])),
                        'wealth_before':float(D(s['wealth_before'])),'wealth_after':float(D(s['wealth_after'])),
                        'formally_evaluated':funded,'candidate_hash':candidate,
                        'candidate_score':result['score'] if result else None,
                        'candidate_valid':result['valid'] if result else None,
                        'candidate_status':result['status'] if result else None,
                        'completed_instances':sum(x['status']=='COMPLETED' for x in records),
                        'timeout_instances':sum(x['status']=='TIMEOUT' for x in records),
                        'failed_instances':sum(x['status'] not in ('COMPLETED','TIMEOUT') for x in records),
                        'all_20_instances_completed':bool(records) and all(x['status']=='COMPLETED' for x in records),
                        'candidate_cpu_seconds':result['cpu_seconds'] if result else None,
                        'candidate_wall_seconds':result['wall_seconds'] if result else None,
                        'job_id':body.get('result_job_id')}
                    rows.append(entry)
                    sub=db.execute('SELECT plan_version_id FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',(e,r,a)).fetchone()
                    if sub:
                        plan=json.loads(db.execute('SELECT payload FROM plan_versions WHERE id=?',(sub[0],)).fetchone()[0])
                        plans.append({'regime':policy,'round':r+1,'agent':a,'plan':plan,
                            'candidate_hash':candidate,'parent_hash':plan['parent_hash'],
                            'parent_score':before['parent_score'],'score':result['score'] if result else None})
            require(sum(x['formally_evaluated'] for x in rows if x['regime']==policy)==(20 if policy=='warm' else 40),'Formal count mismatch')
        champions={}
        for policy in POLICIES:
            h=rounds[policy][-1]['champion_hash']
            v=db.execute('SELECT payload FROM verified_results WHERE candidate_hash=? AND job_id LIKE ? ORDER BY rowid DESC LIMIT 1',(h,episodes[policy]['id']+'%')).fetchone()
            result=json.loads(v[0]);ratios=[x['result_size']/x['reference_size'] for x in result['instance_results']]
            champions[policy]={'hash':h,'score':result['score'],
                'instance_cpu_mean':statistics.mean(x['cpu_seconds'] for x in result['instance_results']),
                'instance_cpu_max':max(x['cpu_seconds'] for x in result['instance_results']),
                'solution_size_over_reference_min':min(ratios),'solution_size_over_reference_median':statistics.median(ratios),
                'solution_size_over_reference_max':max(ratios),
                'instances_matching_best_known':sum(x['result_size']<=x['reference_size'] for x in result['instance_results']),
                'instances':result['instance_results']}
        selected=next(x for x in plans if x['regime']=='warm' and x['round']==2 and x['agent']=='agent-4')
        submission=json.loads(db.execute('SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=1 AND agent_id=?',(warm_id,'agent-4')).fetchone()[0])
        context=read_cas(root,submission['context_hash'])
        snap=json.loads(db.execute('SELECT snapshot_json FROM rounds WHERE episode_id=? AND round_index=1',(warm_id,)).fetchone()[0])
        provenance={'warm_wave2_agent4_context_hash':submission['context_hash'],
            'preparation':context['preparation'],'plan':selected['plan'],
            'own_parent_hash':selected['parent_hash'],'candidate_hash':selected['candidate_hash'],
            'public_champion_hash':snap['board']['champion_hash'],
            'public_champion_score':snap['board']['champion_score'],
            'observed_public_champion_read':any('public/champion/run' in json.dumps(x['arguments'])
                for x in context['preparation']['read_trace'])}
        require(provenance['observed_public_champion_read'],'Missing observed public code read')
        return {'root':str(root),'warm_id':warm_id,'directions':warm['direction_briefs'],
            'opportunities':warm['opportunities'],'rows':rows,'rounds':rounds,'episodes':episodes,
            'champions':champions,'agent4_provenance':provenance,
            'integrity':{'event_count':count,'last_event_hash':previous,'reward_formula_recomputed':True,
                'configuration_snapshot_checkpoint_result_hashes_verified':True,
                'frozen_shared_warm_parent_history_verified':True,
                'ledger_access':'SQLite mode=ro; explicit read transaction; no Ledger/policy/backend imports',
                'formal_evaluations':140,'agent_round_records':len(rows),'model_calls':0,'new_evaluations':0}}
    finally:db.rollback();db.close()


def series(data,policy,agent):
    return sorted([r for r in data['rows'] if r['regime']==policy and r['agent']==agent],key=lambda r:r['round'])


def summarize(data):
    metrics=[];stage_summary={};shapes=[]
    for policy in ('warm',)+POLICIES:
        stages=data['rounds'][policy];global_gain=stages[-1]['champion_after']-stages[0]['champion_before']
        updates=[r['round'] for r in stages if r['champion_after']>r['champion_before']]
        last=updates[-1] if updates else 0
        stage_summary[policy]={'initial_champion_score':stages[0]['champion_before'],
            'final_champion_score':stages[-1]['champion_after'],'global_champion_gain':global_gain,
            'global_improvement_rounds':updates,'last_global_improvement_round':last,
            'rounds_since_last_global_improvement':len(stages)-last,
            'last_5_global_gain':stages[-1]['champion_after']-(stages[-6]['champion_after'] if len(stages)>5 else stages[0]['champion_before']),
            'final_champion_agents':stages[-1]['champion_agents']}
        normalized=[]
        for a in AGENTS:
            s=series(data,policy,a);rewards=np.array([r['reward'] for r in s]);total=float(sum(rewards))
            funded=[r for r in s if r['formally_evaluated']]
            improvements=[r for r in s if r['retained_gain']>1e-12]
            last_r=improvements[-1]['round'] if improvements else 0
            last_index=max([i+1 for i,r in enumerate(funded) if r['retained_gain']>1e-12],default=0)
            m={'regime':policy,'agent':a,'formal_evaluations':len(funded),
                'valid_candidate_flags':sum(bool(r['candidate_valid']) for r in funded),
                'fully_completed_panels':sum(r['all_20_instances_completed'] for r in funded),
                'evaluations_with_timeouts':sum(r['timeout_instances']>0 for r in funded),
                'invalid_candidates':sum(r['candidate_valid'] is False for r in funded),
                'retained_improvements':len(improvements),'initial_score':s[0]['parent_before'],
                'final_score':s[-1]['parent_after'],'score_gain':s[-1]['parent_after']-s[0]['parent_before'],
                'gross_reward':total,'payment':sum(r['payment'] for r in s),
                'last_improvement_round':last_r,'calendar_rounds_since_last_improvement':len(s)-last_r,
                'formal_evaluations_since_last_improvement':len(funded)-last_index,
                'positive_reward_rounds':int(np.count_nonzero(rewards>0)),
                'largest_reward_fraction':float(max(rewards)/total) if total else None,
                'fraction_reward_by_midpoint':float(sum(rewards[:len(s)//2])/total) if total else None,
                'fraction_reward_last_5':float(sum(rewards[-5:])/total) if total else None,
                'reward_center_round':float(np.dot(np.arange(1,len(s)+1),rewards)/total) if total else None,
                'score_gain_last_5':s[-1]['parent_after']-(s[-6]['parent_after'] if len(s)>5 else s[0]['parent_before'])}
            metrics.append(m)
            if total:normalized.append((a,np.r_[0,np.cumsum(rewards)]/total))
        for i,(a,x) in enumerate(normalized):
            for b,y in normalized[i+1:]:
                shapes.append({'regime':policy,'agent_a':a,'agent_b':b,
                    'normalized_reward_curve_rmse':float(np.sqrt(np.mean((x-y)**2))),
                    'normalized_reward_curve_max_difference':float(max(abs(x-y)))})
    data['agent_metrics']=metrics;data['stage_summary']=stage_summary;data['normalized_shape_distances']=shapes


def savefig(fig,path):
    for extension in ('png','svg'):fig.savefig(path.with_suffix('.'+extension),dpi=180,bbox_inches='tight')
    plt.close(fig)


def curves(data,out):
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,
                         'axes.grid':True,'grid.alpha':.18,'figure.facecolor':'white'})
    fig,axes=plt.subplots(2,3,figsize=(17,9))
    for j,policy in enumerate(POLICIES):
        for i,a in enumerate(AGENTS):
            warm=series(data,'warm',a);market=series(data,policy,a)
            full=np.r_[0,np.cumsum([r['reward'] for r in warm+market])]
            axes[0,j].step(range(26),full,where='post',color=COLORS[i],label=a,lw=1.8)
            axes[1,j].step(range(21),np.r_[0,np.cumsum([r['reward'] for r in market])],where='post',color=COLORS[i],lw=1.8)
        axes[0,j].axvspan(0,5,color='#64748b',alpha=.1)
        axes[0,j].axvline(5,color='#64748b',ls='--',lw=1)
        axes[0,j].set(title=NAMES[policy]+' | shared Warm + Market',xlabel='0=seed; 5=Warm end; 25=Market end',ylabel='Cumulative gross reward (credits)')
        axes[1,j].set(title=NAMES[policy]+' | Market increments only',xlabel='Market round',ylabel='Cumulative Market reward (credits)',xlim=(0,20))
    axes[0,0].legend(loc='lower right',ncol=2)
    fig.suptitle('Observed reward growth: staircase curves, no extrapolation\nGross rewards exclude bid payments; the same Warm prefix is shown in each regime.',fontsize=15)
    fig.tight_layout(rect=(0,0,1,.93));savefig(fig,out/'reward-growth')

    fig,axes=plt.subplots(2,3,figsize=(17,9))
    for j,policy in enumerate(POLICIES):
        for i,a in enumerate(AGENTS):
            warm=series(data,'warm',a);market=series(data,policy,a)
            axes[0,j].step(range(26),[warm[0]['parent_before']]+[r['parent_after'] for r in warm+market],
                where='post',color=COLORS[i],label=a,lw=1.8)
            observed=[r for r in market if r['formally_evaluated']]
            axes[1,j].step(range(len(observed)+1),[market[0]['parent_before']]+[r['parent_after'] for r in observed],
                where='post',color=COLORS[i],label=a,lw=1.8,marker='o',ms=3)
        axes[0,j].axvspan(0,5,color='#64748b',alpha=.1);axes[0,j].axvline(5,color='#64748b',ls='--',lw=1)
        axes[0,j].set(title=NAMES[policy],xlabel='Combined round (Warm 1-5, Market 1-20)',ylabel='Retained best-parent training score',ylim=(.78,.975))
        axes[1,j].set(xlabel='Actual Market formal evaluation number',ylabel='Retained best-parent training score',ylim=(.83,.975))
    axes[0,0].legend(loc='lower right',ncol=2)
    fig.suptitle('Performance and evaluation opportunity\nBottom row removes unfunded calendar rounds; unequal curve lengths are unequal observed exposure.',fontsize=15)
    fig.tight_layout(rect=(0,0,1,.93));savefig(fig,out/'performance-and-exposure')

    fig,axes=plt.subplots(1,4,figsize=(19,4.8),sharey=True)
    for ax,policy in zip(axes,('warm',)+POLICIES):
        for i,a in enumerate(AGENTS):
            s=series(data,policy,a);y=np.r_[0,np.cumsum([r['reward'] for r in s])]
            if y[-1]>0:ax.step(range(len(y)),y/y[-1],where='post',color=COLORS[i],label=a,lw=1.8)
        ax.set(title=NAMES[policy],xlabel='Wave' if policy=='warm' else 'Market round',ylim=(-.02,1.05))
    axes[0].set_ylabel('Fraction of this stage\'s final gross reward');axes[0].legend(loc='lower right')
    fig.suptitle('Shape comparison after removing final reward height\nNormalization uses each completed stage total; it is retrospective, not a forecast.',fontsize=14)
    fig.tight_layout(rect=(0,0,1,.89));savefig(fig,out/'normalized-reward-shapes')

    fig,axes=plt.subplots(1,2,figsize=(13,4.8))
    for i,a in enumerate(AGENTS):
        w=series(data,'warm',a)
        axes[0].step(range(6),[w[0]['parent_before']]+[r['parent_after'] for r in w],where='post',color=COLORS[i],label=a,lw=2,marker='o',ms=4)
        axes[1].plot(range(1,6),[r['reward'] for r in w],color=COLORS[i],marker='o',label=a,lw=1.5)
    axes[0].legend();axes[0].set(xlabel='Warm wave',ylabel='Best-parent training score',title='Agent 3 leads W1; Agent 4 overtakes at W2')
    axes[1].set(xlabel='Warm wave',ylabel='Reward in this wave',title='Agent 4 receives most of its Warm reward at W2')
    fig.tight_layout();savefig(fig,out/'warm-direction-breakthrough')


def proposals(seed_hash):
    return {'status':'PROPOSED_ONLY_NOT_CONFIGURED_OR_RUN','start_from_common_seed':seed_hash,
        'note':'Two mechanisms beyond the current four templates. Existing director cycles four templates for N>4; explicit frozen provisioning is needed.',
        'briefs':[
            {'agent_id':'agent-5','direction_id':'adaptive_frequency_tabu',
             'allowed_focus':['local_search','restart'],
             'objective':'Explore frequency-weighted, tabu/age-guided remove/add search with bounded internal infeasible states; always retain a verified feasible incumbent for output.',
             'initial_hypotheses':['Adaptive penalties on repeatedly uncovered vertices and short-term tabu can escape neighborhoods that fixed greedy swaps revisit.'],
             'diversity_rationale':'Dynamic search state and penalties, rather than only deterministic tie order or faster static bookkeeping.'},
            {'agent_id':'agent-6','direction_id':'diverse_solution_pool_recombination',
             'allowed_focus':['restart','local_search'],
             'objective':'Keep a small bounded pool of structurally diverse feasible dominating sets for each input. Recombine two solutions by retaining shared choices and repairing their disagreement region, then apply bounded reduction/local search; preserve the best feasible incumbent and cap pool size and recombination work.',
             'initial_hypotheses':['Combining complementary choices from different good solutions can reach solutions not encountered by independent restarts that discard every nonbest incumbent.'],
             'diversity_rationale':'Within-instance solution diversity and recombination, not just new random tie seeds or more work on one incumbent.'}],
        'overlap_notes':[
            'Old plans already mention short tabu and exact tiny-component routines; these are not claimed to be entirely unexplored.',
            'Agent 5 emphasizes dynamic frequency penalties as the core mechanism, beyond an incidental tabu list.',
            'No path-relinking/crossover/recombination/solution-pool/population/genetic wording was found in the 260 recorded submitted plans. This is a plan-text observation, not a proof that all source behavior is absent.',
            'Bounded exact residual neighborhoods remain an alternative, but existing Agent 3 exact-component expansion attempts demonstrate timeout risk.'],
        'sources':[
            'https://drops.dagstuhl.de/storage/00lipics/lipics-vol358-ipec2025/html/LIPIcs.IPEC.2025.32/LIPIcs.IPEC.2025.32.html',
            'https://www.sciencedirect.com/science/article/pii/037722179500159X']}


def export(data,out):
    out.mkdir(parents=True,exist_ok=True)
    for name,items in [('agent-rounds',data['rows']),('agent-metrics',data['agent_metrics']),
                       ('normalized-shape-distances',data['normalized_shape_distances'])]:
        with (out/(name+'.csv')).open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(items[0]));writer.writeheader();writer.writerows(items)
    for name,value in [('audit',data),('agent4-warm-breakthrough',data['agent4_provenance']),
                       ('proposed-new-directions',proposals(data['directions'][0]['seed_artifact']))]:
        (out/(name+'.json')).write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n')
    curves(data,out)
    body='''<!doctype html><meta charset="utf-8"><title>MAESTRO search-potential audit</title>
<style>body{max-width:1450px;margin:32px auto;font:15px/1.55 system-ui;color:#243447}img{width:100%}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:7px;border-bottom:1px solid #ddd;text-align:right}th:first-child,td:first-child{text-align:left}a{color:#2563eb}pre{white-space:pre-wrap;background:#f1f5f9;padding:16px}</style>
<h1>Historical search potential: shared Warm + three Market regimes</h1>
<p>Read-only audit. One shared Warm checkpoint, not three independent Warm runs. 140 formal candidates and 260 agent-round records. No model calls, new evaluations or policy imports. Old provider UNKNOWN costs remain untouched.</p>
<p>Rewards are cumulative gross credits, not wealth or net profit. A flat unfunded round does not establish algorithmic stagnation. The historical protocol generated only invited candidates; the new pre-bid pipeline and later policy changes are not retroactively applied.</p>'''
    for file,title in [('reward-growth','Cumulative gross rewards'),('performance-and-exposure','Performance and actual evaluation exposure'),
                       ('normalized-reward-shapes','Shape after height normalization'),('warm-direction-breakthrough','Warm Start breakthrough')]:
        body+=f'<h2>{title}</h2><a href="{file}.png"><img src="{file}.png"></a><p><a href="{file}.svg">Vector SVG</a></p>'
    body+='<h2>Per-agent stage evidence</h2><table><tr><th>Regime / agent</th><th>Formal evaluations</th><th>Full panels</th><th>Positive improvements</th><th>Reward</th><th>Score gain</th><th>Last improvement</th><th>Formal nonimprovement streak</th><th>Reward by midpoint</th></tr>'
    for m in data['agent_metrics']:
        body+=f"<tr><td>{m['regime']} / {m['agent']}</td><td>{m['formal_evaluations']}</td><td>{m['fully_completed_panels']}</td><td>{m['retained_improvements']}</td><td>{m['gross_reward']:.6f}</td><td>{m['score_gain']:.8f}</td><td>{m['last_improvement_round']}</td><td>{m['formal_evaluations_since_last_improvement']}</td><td>{m['fraction_reward_by_midpoint']:.1%}</td></tr>"
    body+='''</table><h2>Interpretation limits</h2><p>The curves contain discrete breakthroughs and long flat intervals, not a demonstrated universal logarithmic law. Normalize heights and compare evaluation opportunities before inferring shape. One run per arm cannot separate direction effects, model stochasticity, shared-code transfer and allocation. No extension to 50/100 rounds is extrapolated here. Best-known references are frozen targets, not proofs of optimality, and may require different solve budgets.</p>
<p>Agent 4 explicitly read agent 3's public champion in Warm W2 and combined reduction/repair ideas with CSR and added search. This is evidence of cooperative transfer, not an ablation proving any one component's causal contribution.</p>
<p>Two proposed directions: adaptive frequency/tabu local search and diverse solution-pool recombination. These are new experiment proposals only. Old plans already mention short tabu and exact-component routines, so these are not claimed to be untouched ideas. The pool/recombination wording did not occur in the recorded submitted plans; this is a plan-text observation only.</p>
<p>Six agents times five opportunities means 30 Warm opportunities; with concurrency four the current scheduler needs eight waves. Five synchronized six-agent waves require six-way concurrency and fresh resource qualification. Simply setting n_agents=6 repeats two of the existing four direction templates. At two Market slots, 30/50 rounds give 60/100 formal slots (10/16.7 per agent on average only if full and balanced). A six-agent Warm uses 50% more opportunities than the old four-agent Warm, so population gains would be confounded with extra compute.</p>
<p>Suggested follow-up: a new frozen six-agent experiment with 50 Market rounds and checkpoints at 30/50, rather than promising a 100-round gain. Repeat fresh Warms and rotate direction-to-agent assignments to estimate direction effects. Do not mutate the old four-agent checkpoint. Appending two later entrants with access to the final old champion is a different late-entry experiment, not symmetric initialization.</p>
<p>Algorithm inspiration: <a href="https://drops.dagstuhl.de/storage/00lipics/lipics-vol358-ipec2025/html/LIPIcs.IPEC.2025.32/LIPIcs.IPEC.2025.32.html">PACE 2025 report, section 5.2.2</a> describes adaptive weighting. <a href="https://www.sciencedirect.com/science/article/pii/037722179500159X">Beasley and Chu (1996)</a> describes set-cover crossover with feasibility repair. The proposed bounded implementations are engineering hypotheses, not predicted results from these runs.</p>
<ul><li><a href="audit.json">Complete verified audit and frozen configurations</a></li><li><a href="agent-rounds.csv">260 agent-rounds</a></li><li><a href="agent-metrics.csv">Agent stage metrics</a></li><li><a href="normalized-shape-distances.csv">Normalized shape distances</a></li><li><a href="agent4-warm-breakthrough.json">Agent 4 source-read / plan evidence</a></li><li><a href="proposed-new-directions.json">Two new direction proposals</a></li></ul>'''
    (out/'index.html').write_text(body)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();data=audit(args.root.resolve());summarize(data)
    data['generated_utc']=datetime.now(timezone.utc).isoformat()
    export(data,args.output.resolve())
    print(json.dumps({'report':str(args.output.resolve()/'index.html'),'integrity':data['integrity'],
        'stage_summary':data['stage_summary'],'agent_metrics':data['agent_metrics']},indent=2))


if __name__=='__main__':main()
