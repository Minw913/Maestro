"""Audit historical valuation and reward curves without model/evaluation calls.

python scripts/analyze_valuation.py --root PATH_TO_RUN
"""
from __future__ import annotations

import argparse
import csv
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
import hashlib
import html
import json
import math
from pathlib import Path
import sqlite3
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from maestro.agents.valuation import estimate_legacy_value as estimate_value
from maestro.types import state_from_dict

Q=Decimal('.000001')


def D(x):return Decimal(str(x))


def cash(x):return D(x).quantize(Q,rounding=ROUND_HALF_EVEN)


def credit(y,p,c,scale,beta):
    y,p,c=map(D,(y,p,c))
    return cash(scale*(max(D(0),y-c)+beta*max(D(0),min(y,c)-p)))


def require(ok,message):
    if not ok:raise ValueError(message)


def extract(root):
    path=root/'authority/ledger.sqlite'
    if not path.is_file():raise ValueError('Missing ledger '+str(path))
    db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
    rows,terms,metadata=[],[],{}
    try:
        db.execute('BEGIN')
        for policy in ('first_price','ucb'):
            eps=db.execute('SELECT * FROM episodes WHERE policy=?',(policy,)).fetchall()
            require(len(eps)==1,f'Expected one {policy} episode')
            ep=eps[0];e=ep['id'];cfg=json.loads(ep['config_json'])
            scale,beta=D(cfg['reward_scale']),D(cfg['catchup_fraction'])
            snapshots=db.execute("SELECT * FROM rounds WHERE episode_id=? AND round_index>=0 AND status='SETTLED' ORDER BY round_index",(e,)).fetchall()
            require(len(snapshots)==cfg['rounds'],'Only complete Market episodes are audited by this script')
            warm_id=ep['bootstrap_id']
            coefficient=cfg.get('ucb_exploration_coefficient',1.0)
            metadata[policy]={'episode':e,'warm':warm_id,'config_hash':ep['config_hash'],'seed':cfg['seed'],'reward_scale':str(scale),'catchup_fraction':str(beta),'ucb_exploration_coefficient':coefficient}
            first_warm=json.loads(db.execute('SELECT snapshot_json FROM rounds WHERE episode_id=? AND round_index=0',(warm_id,)).fetchone()[0])
            warm_curves={a:[s['parent_score']] for a,s in first_warm['states'].items()}
            for checkpoint in db.execute('SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index',(warm_id,)):
                state=json.loads(checkpoint[0])['states']
                for a in warm_curves:warm_curves[a].append(state[a]['parent_score'])
            metadata[policy]['warm_parent_score_curves']=warm_curves
            for rr in snapshots:
                idx=rr['round_index'];snap=json.loads(rr['snapshot_json']);c=D(snap['board']['champion_score'])
                cp=json.loads(db.execute('SELECT payload FROM checkpoints WHERE episode_id=? AND round_index=?',(e,idx)).fetchone()[0])
                saved_order=json.loads(db.execute('SELECT payload FROM priority_orders WHERE episode_id=? AND round_index=?',(e,idx)).fetchone()[0])['order']
                total_n=sum(s['ucb_n'] for s in snap['states'].values());round_rows=[]
                for a,s in sorted(snap['states'].items()):
                    history=s['history'];p=D(s['parent_score']);n=s['ucb_n'];total_credit=D(s['ucb_total'])
                    require(n==len(history),'UCB n differs from formal history length')
                    require(total_credit==sum((D(h['credit']) for h in history),D(0)),'UCB total differs from saved historical credits')
                    # Verify origins and cutoff independently of sample_ids.
                    for h in history:
                        origin=db.execute('SELECT t.episode_id,t.round_index,t.agent_id,v.payload FROM eval_tickets t JOIN verified_results v ON v.job_id=t.job_id WHERE t.job_id=?',(h['experiment_id'],)).fetchone()
                        require(origin is not None,'Missing historical result')
                        require(origin['agent_id']==a,'Foreign agent history')
                        require(origin['episode_id']==warm_id or (origin['episode_id']==e and origin['round_index']<idx),'Future/current/other-regime history leakage')
                        raw=json.loads(origin['payload'])
                        require(raw['score']==h['score'] and raw['valid']==h['valid'],'History score differs from verified result')
                        hs=db.execute('SELECT reward,payload FROM settlements WHERE episode_id=? AND round_index=? AND agent_id=?',(origin['episode_id'],origin['round_index'],a)).fetchone()
                        require(D(hs['reward'])==D(h['credit']),'History credit differs from original settlement')
                        require(D(json.loads(hs['payload'])['parent_before'])==D(h['parent_score']),'History baseline differs from original settlement')
                    subrow=db.execute('SELECT * FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',(e,idx,a)).fetchone()
                    require(subrow is not None,'Missing submission; do not fabricate valuation')
                    sub=json.loads(subrow['payload']);plan=json.loads(db.execute('SELECT payload FROM plan_versions WHERE id=?',(subrow['plan_version_id'],)).fetchone()[0])
                    selected=[h for h in history if h['target']==plan['target'] and h['mod_type']==plan['mod_type']]
                    mode='EXACT_TARGET_MOD_TYPE' if selected else 'FALLBACK_ALL_OWN_HISTORY'
                    if not selected:selected=history
                    require(bool(selected),'No formal history')
                    contributions=[];gains=[];effective=[]
                    for h in selected:
                        delta=max(D(0),D(h['score'])-D(h['parent_score'])) if h['valid'] else D(0)
                        hypothetical=min(D(1),p+delta);contribution=credit(hypothetical,p,c,scale,beta)
                        contributions.append(contribution);gains.append(delta);effective.append(hypothetical-p)
                        terms.append({'regime':policy,'round':idx+1,'agent':a,'source_job_id':h['experiment_id'],
                                      'source_parent_score':h['parent_score'],'source_score':h['score'],'source_valid':h['valid'],
                                      'historical_positive_gain':str(delta),'current_parent':str(p),'current_champion':str(c),
                                      'hypothetical_score':str(hypothetical),'recomputed_reward_contribution':str(contribution),
                                      'original_historical_credit':h['credit']})
                    with localcontext() as ctx:
                        ctx.prec=40
                        value=cash(sum(contributions,D(0))/len(contributions));mean_gain=sum(gains,D(0))/len(gains);eff_gain=sum(effective,D(0))/len(effective)
                    production,ids=estimate_value(state_from_dict(s).history,plan['target'],plan['mod_type'],p,c,scale,beta)
                    require(value==D(sub['value'])==production,'Valuation disagreement')
                    require(ids==sub['sample_ids']==[h['experiment_id'] for h in selected],'Sample ID disagreement')
                    require(D(sub['bid'])==cash(min(D(cfg['bid_cap']),value)),'Bid formula disagreement')
                    mu=float(total_credit/n) if n else math.inf
                    bonus=coefficient*math.sqrt(2*math.log(1+total_n)/n) if n else (0.0 if coefficient==0 else math.inf)
                    ucb=mu+bonus
                    st=db.execute('SELECT * FROM settlements WHERE episode_id=? AND round_index=? AND agent_id=?',(e,idx,a)).fetchone()
                    alloc=db.execute('SELECT * FROM final_allocations WHERE episode_id=? AND round_index=? AND agent_id=?',(e,idx,a)).fetchone()
                    funded=bool(alloc['x']);actual=None;candidate_score=None
                    if funded:
                        job=json.loads(st['payload'])['result_job_id'];v=json.loads(db.execute('SELECT payload FROM verified_results WHERE job_id=?',(job,)).fetchone()[0]);candidate_score=v['score']
                        actual=credit(v['score'],p,c,scale,beta) if v['valid'] else cash(0)
                        require(actual==D(st['reward']),'Actual reward differs from independently recomputed reward')
                    else:require(D(st['reward'])==0,'Nonadmitted settlement credit nonzero')
                    after=cp['states'][a]['parent_score']
                    row={'regime':policy,'round':idx+1,'agent':a,'target':plan['target'],'mod_type':plan['mod_type'],
                         'parent_before':float(p),'champion_before':float(c),'parent_after':after,'candidate_score':candidate_score,
                         'history_count':len(history),'sample_mode':mode,'selected_count':len(selected),
                         'zero_gain_count':sum(g==0 for g in gains),'invalid_count':sum(not h['valid'] for h in selected),
                         'selected_job_ids':sub['sample_ids'],'valuation_saved':float(D(sub['value'])),'valuation_recomputed':float(value),
                         'bid':float(D(sub['bid'])),'historical_mean_positive_gain':float(mean_gain),'effective_score_gain_projection':float(eff_gain),
                         'derived_next_parent_projection':float(p+eff_gain),'ucb_n':n,'ucb_total_credit':float(total_credit),
                         'ucb_historical_mean_reward':mu,'ucb_exploration_bonus':bonus,'ucb_priority_index':ucb,
                         'priority_rank':saved_order.index(a)+1 if a in saved_order else None,'eligible':sub.get('eligible',True),
                         'funded':funded,'actual_reward':float(actual) if actual is not None else None,
                         'valuation_error':float(value-actual) if actual is not None else None,
                         'ucb_mean_error':mu-float(actual) if actual is not None else None,
                         'actual_retained_gain':after-float(p) if funded else None,
                         'score_projection_error':float(p+eff_gain)-after if funded else None}
                    rows.append(row);round_rows.append(row)
                def tie(a):return hashlib.sha256(repr((cfg['seed'],'policy-tie',idx,a)).encode()).hexdigest()
                predicted_order=sorted([r for r in round_rows if r['eligible']],key=lambda r:(-r['ucb_priority_index'],tie(r['agent'])) if policy=='ucb' else (-D(str(r['bid'])),tie(r['agent'])))
                require([r['agent'] for r in predicted_order]==saved_order,'Priority order disagreement')
        return rows,terms,metadata
    finally:db.rollback();db.close()


def metrics(rows):
    result=[]
    for policy in ('first_price','ucb'):
        for agent in ['ALL']+sorted({r['agent'] for r in rows}):
            observed=[r for r in rows if r['regime']==policy and r['funded'] and (agent=='ALL' or r['agent']==agent)]
            for forecast in (['valuation_saved'] if policy=='first_price' else ['valuation_saved','ucb_historical_mean_reward']):
                errors=[r[forecast]-r['actual_reward'] for r in observed];n=len(errors)
                result.append({'regime':policy,'agent':agent,'forecast':forecast,'n_observed':n,
                               'mean_prediction':statistics.mean(r[forecast] for r in observed),
                               'mean_actual_reward':statistics.mean(r['actual_reward'] for r in observed),
                               'bias_prediction_minus_reward':statistics.mean(errors),'mae':statistics.mean(map(abs,errors)),
                               'rmse':math.sqrt(statistics.mean(e*e for e in errors)),
                               'overpredictions':sum(e>1e-6 for e in errors),'underpredictions':sum(e< -1e-6 for e in errors),
                               'zero_reward_outcomes':sum(r['actual_reward']==0 for r in observed)})
    return result


COLORS={'first_price':'#14815c','ucb':'#3268b2'}


def plot(rows,out,metadata):
    agents=sorted({r['agent'] for r in rows});policies=['first_price','ucb'];paths=[]
    with plt.rc_context({'font.family':'DejaVu Sans','font.size':10,'svg.fonttype':'none'}):
        for kind in ['performance','valuation-reward']:
            fig,axes=plt.subplots(len(agents),2,figsize=(17,3.0*len(agents)+1.5),squeeze=False)
            for j,p in enumerate(policies):
                for i,a in enumerate(agents):
                    ax=axes[i,j];rr=[r for r in rows if r['regime']==p and r['agent']==a];x=[r['round'] for r in rr];fund=[r for r in rr if r['funded']];unfund=[r for r in rr if not r['funded']]
                    if kind=='performance':
                        ax.step([0]+x,[rr[0]['parent_before']]+[r['parent_after'] for r in rr],where='post',color=COLORS[p],lw=2,label='Retained parent score')
                        ax.plot(x,[r['derived_next_parent_projection'] for r in rr],color='#d98a20',lw=1,alpha=.7,label='History-derived one-step score projection')
                        ax.scatter([r['round'] for r in unfund],[r['derived_next_parent_projection'] for r in unfund],facecolor='white',edgecolor='#d98a20',s=22,zorder=4,label='Unfunded projection (no outcome)')
                        ax.scatter([r['round'] for r in fund],[r['derived_next_parent_projection'] for r in fund],color='#d98a20',s=19,zorder=4)
                        for r in fund:ax.plot([r['round']]*2,[r['parent_after'],r['derived_next_parent_projection']],color='#b34e4e',lw=.7,alpha=.6)
                        ax.set_ylabel('Training score')
                    else:
                        ax.plot(x,[r['valuation_saved'] for r in rr],color='#d98a20',lw=1.6,label='Plan valuation v (both regimes)')
                        if p=='ucb':ax.plot(x,[r['ucb_historical_mean_reward'] for r in rr],color='#3268b2',lw=1.5,ls='--',label='UCB historical mean reward μ')
                        ax.scatter([r['round'] for r in fund],[r['actual_reward'] for r in fund],color='#ba303b',s=27,zorder=5,label='Actual reward (funded only)')
                        for r in fund:ax.plot([r['round']]*2,[r['valuation_saved'],r['actual_reward']],color='#ba303b',lw=.7,alpha=.55)
                        ax.set_ylim(bottom=-.03);ax.set_ylabel('Reward / valuation units')
                    ax.set_title(p.replace('_','-').upper()+' · '+a,loc='left',weight='bold');ax.set_xlim(0,20.5);ax.set_xticks(range(0,21,2));ax.grid(axis='y',alpha=.2);ax.spines[['top','right']].set_visible(False)
                    if i==0:ax.legend(fontsize=8,loc='best',frameon=False)
                    if i==len(agents)-1:ax.set_xlabel('Market round (1-based)')
            title='Parent-score growth and a lagged history projection' if kind=='performance' else 'Predicted valuation versus realized reward'
            fig.suptitle(title,fontsize=19,weight='bold',x=.06,ha='left')
            note=('Orange score projection is a diagnostic E[min(1,p+historical positive gain)], not a controller field or v/100. Hollow points were not evaluated.' if kind=='performance' else 'Unfunded rewards are missing, not zero. UCB uses μ + exploration bonus to rank; v is the same plan-conditional formula used in First-price.')
            fig.text(.06,.025,note,fontsize=10);fig.subplots_adjust(left=.07,right=.98,top=.93,bottom=.07,hspace=.4,wspace=.19)
            for ext in ['png','svg']:pfile=out/(kind+'.'+ext);fig.savefig(pfile,dpi=155);paths.append(pfile)
            plt.close(fig)
        fig,axes=plt.subplots(2,2,figsize=(14,9));
        for ax,a in zip(axes.flat,agents):
            rr=[r for r in rows if r['regime']=='ucb' and r['agent']==a];x=[r['round'] for r in rr];fund=[r for r in rr if r['funded']]
            for key,label,color,style in [('ucb_historical_mean_reward','Historical reward mean μ','#3268b2','-'),('ucb_exploration_bonus','Exploration bonus','#d98a20','--'),('ucb_priority_index','Allocation index U = μ + bonus','#20252c','-')]:ax.plot(x,[r[key] for r in rr],color=color,ls=style,lw=1.6,label=label)
            ax.scatter([r['round'] for r in fund],[r['actual_reward'] for r in fund],color='#ba303b',s=25,zorder=4,label='Actual funded reward')
            ax.set_title(a,loc='left',weight='bold');ax.set_xlim(1,20);ax.set_xticks(range(2,21,2));ax.set_xlabel('Market round');ax.set_ylabel('Reward units / priority-index value');ax.grid(axis='y',alpha=.2);ax.spines[['top','right']].set_visible(False)
        axes[0,0].legend(fontsize=8,frameon=False);fig.suptitle('UCB decomposition: the allocation index is not a point reward forecast',fontsize=16,weight='bold');fig.text(.06,.02,'All histories include the common Warm. Historical mean retains original credits; the bonus is optimistic exploration, not expected reward.',fontsize=10);fig.subplots_adjust(top=.9,bottom=.09,hspace=.32,wspace=.2)
        for ext in ['png','svg']:pfile=out/('ucb-decomposition.'+ext);fig.savefig(pfile,dpi=155);paths.append(pfile)
        plt.close(fig)
        fig,axes=plt.subplots(2,2,figsize=(14,8.5))
        for j,p in enumerate(policies):
            rr=[r for r in rows if r['regime']==p and r['agent']=='agent-4'];x=[r['round'] for r in rr];fund=[r for r in rr if r['funded']]
            ax=axes[0,j]
            ax.step([0]+x,[rr[0]['parent_before']]+[r['parent_after'] for r in rr],where='post',color=COLORS[p],lw=2,label='Actual retained-parent performance')
            ax.plot(x,[r['derived_next_parent_projection'] for r in rr],color='#d98a20',marker='.',ms=4,lw=1.4,label='One-step history projection (diagnostic)')
            for r in fund:ax.plot([r['round']]*2,[r['parent_after'],r['derived_next_parent_projection']],color='#ba303b',lw=.6,alpha=.6)
            ax.set_ylabel('Training score');ax.set_title(p.replace('_','-').upper()+' · agent-4',loc='left',weight='bold');ax.legend(fontsize=8,frameon=False)
            ax=axes[1,j];ax.plot(x,[r['valuation_saved'] for r in rr],color='#d98a20',lw=1.8,label='Actual plan valuation v')
            if p=='ucb':ax.plot(x,[r['ucb_historical_mean_reward'] for r in rr],color='#3268b2',ls='--',lw=1.8,label='Historical reward mean μ')
            ax.scatter([r['round'] for r in fund],[r['actual_reward'] for r in fund],color='#ba303b',s=28,label='Actual reward (funded only)',zorder=5)
            for r in fund:ax.plot([r['round']]*2,[r['valuation_saved'],r['actual_reward']],color='#ba303b',lw=.6,alpha=.6)
            ax.set_xlabel('Market round');ax.set_ylabel('Reward / valuation units');ax.legend(fontsize=8,frameon=False)
        for ax in axes.flat:ax.set_xlim(0,20.5);ax.set_xticks(range(0,21,2));ax.grid(axis='y',alpha=.2);ax.spines[['top','right']].set_visible(False)
        fig.suptitle('Agent-4: observed growth, history projection and reward calibration',fontsize=17,weight='bold');fig.text(.06,.025,'Top orange points are a derived score diagnostic, not v/100. Bottom orange lines are the actual controller valuations. Unfunded rewards are missing.',fontsize=9);fig.subplots_adjust(top=.89,bottom=.09,hspace=.28,wspace=.18)
        for ext in ['png','svg']:pfile=out/('agent4-focus.'+ext);fig.savefig(pfile,dpi=160);paths.append(pfile)
        plt.close(fig)
        fig,axes=plt.subplots(2,2,figsize=(14,8))
        for ax,a in zip(axes.flat,agents):
            warm=metadata['first_price']['warm_parent_score_curves'][a];ax.plot(list(range(1-len(warm),1)),warm,color='#333',marker='.',lw=2,label='Shared Warm best parent')
            ax.axvspan(1-len(warm),0,color='#dce3ed',alpha=.4);ax.axvline(0,color='#999',lw=.8)
            for p in policies:
                rr=[r for r in rows if r['regime']==p and r['agent']==a];ax.step([0]+[r['round'] for r in rr],[rr[0]['parent_before']]+[r['parent_after'] for r in rr],where='post',color=COLORS[p],lw=2,label=p.replace('_','-'))
            ax.set_title(a,loc='left',weight='bold');ax.set_xlabel('Warm prefix (<0), then Market rounds');ax.set_ylabel('Retained-parent training score');ax.grid(axis='y',alpha=.2);ax.spines[['top','right']].set_visible(False)
        axes[0,0].legend(fontsize=8,frameon=False);fig.suptitle('Early Warm gains versus later Market growth',fontsize=17,weight='bold');fig.text(.06,.025,'The historical means retain early large improvements. Scores belong to each agent’s own retained parent, not only the global champion.',fontsize=10);fig.subplots_adjust(top=.9,bottom=.1,hspace=.3,wspace=.18)
        for ext in ['png','svg']:pfile=out/('growth-overview.'+ext);fig.savefig(pfile,dpi=155);paths.append(pfile)
        plt.close(fig)
    return paths


def csv_write(path,rows):
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader()
        for r in rows:w.writerow({k:json.dumps(v) if isinstance(v,list) else v for k,v in r.items()})


def write_html(out,rows,terms):
    groups={}
    for t in terms:groups.setdefault((t['regime'],t['round'],t['agent']),[]).append(t)
    head='''<!doctype html><meta charset="utf-8"><title>Valuation audit</title><style>body{font:14px system-ui;margin:24px;color:#17212b}table{border-collapse:collapse;width:100%;white-space:nowrap}td,th{padding:7px;border-bottom:1px solid #dde3e8;text-align:right}th{position:sticky;top:0;background:#edf2f7}td:first-child{text-align:left}details table{font-size:12px}details{white-space:normal}small{color:#667}select{padding:7px;margin:8px}summary{cursor:pointer}</style><h1>Valuation audit · First-price and UCB</h1><p>All 160 Market agent-rounds. Values use only prior own formal outcomes, including Warm. Expand a row for every historical contribution. Missing rewards are unobserved, not zero.</p><label>Regime <select id="policy"><option value="">All</option><option>first_price</option><option>ucb</option></select></label><label>Agent <select id="agent"><option value="">All</option>'''
    head+=''.join('<option>'+html.escape(a)+'</option>' for a in sorted({r['agent'] for r in rows}))+'</select></label>'
    for name in ['agent4-focus','growth-overview','performance','valuation-reward','ucb-decomposition']:head+=f'<p><a href="{name}.png">{name} PNG</a> · <a href="{name}.svg">SVG</a></p>'
    keys=['regime','round','agent','target','mod_type','parent_before','parent_after','selected_count','sample_mode','valuation_saved','valuation_recomputed','ucb_historical_mean_reward','ucb_exploration_bonus','ucb_priority_index','funded','actual_reward','valuation_error','ucb_mean_error']
    head+='<div style="overflow:auto"><table><thead><tr>'+''.join('<th>'+k+'</th>' for k in ['Calculation details']+keys)+'</tr></thead><tbody>'
    for r in rows:
        tt=groups[(r['regime'],r['round'],r['agent'])];cols=['source_job_id','source_parent_score','source_score','source_valid','historical_positive_gain','hypothetical_score','recomputed_reward_contribution','original_historical_credit']
        detail='<details><summary>'+str(len(tt))+' historical terms</summary><table><tr>'+''.join('<th>'+k+'</th>' for k in cols)+'</tr>'
        for t in tt:detail+='<tr>'+''.join('<td>'+html.escape(str(t[k]))+'</td>' for k in cols)+'</tr>'
        detail+='</table></details>'
        def fmt(v):return '—' if v is None else f'{v:.6f}' if isinstance(v,float) else str(v)
        head+=f'<tr data-policy="{r["regime"]}" data-agent="{r["agent"]}"><td>'+detail+'</td>'+''.join('<td>'+html.escape(fmt(r[k]))+'</td>' for k in keys)+'</tr>'
    head+='''</tbody></table></div><script>function filter(){const p=document.querySelector('#policy').value,a=document.querySelector('#agent').value;document.querySelectorAll('tr[data-policy]').forEach(r=>r.hidden=!!((p&&r.dataset.policy!==p)||(a&&r.dataset.agent!==a)));}document.querySelectorAll('select').forEach(s=>s.onchange=filter);</script>'''
    (out/'audit.html').write_text(head,encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',required=True);parser.add_argument('--output');args=parser.parse_args()
    root=Path(args.root).resolve();out=Path(args.output).resolve() if args.output else root/'reports/valuation-audit';rows,terms,metadata=extract(root);out.mkdir(parents=True,exist_ok=True)
    stats=metrics(rows);csv_write(out/'agent-rounds.csv',rows);csv_write(out/'historical-contributions.csv',terms);csv_write(out/'metrics.csv',stats)
    (out/'raw.json').write_text(json.dumps({'metadata':metadata,'rows':rows,'terms':terms},indent=2)+'\n')
    plot(rows,out,metadata);write_html(out,rows,terms)
    summary={'source_root':str(root),'metadata':metadata,'agent_rounds_reviewed':len(rows),'historical_contributions_reviewed':len(terms),'independent_formula_matches':len(rows),'sample_id_matches':len(rows),'priority_orders_match':40,'metrics':stats,'observed_outcomes':sum(r['funded'] for r in rows),'unfunded_no_outcome':sum(not r['funded'] for r in rows),'new_model_calls':0,'new_candidate_evaluations':0,'limitations':['Forecast errors measured only on funded, selected plans.','UCB index includes an exploration bonus; it is not an unbiased point-reward prediction.','The orange next-score curve is a diagnostic from historical clipped positive gains, not an implemented controller forecast.','One Market realization per regime; no causal policy comparison.']}
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2));print(out)


if __name__=='__main__':main()
