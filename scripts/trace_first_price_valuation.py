"""Trace exact-match membership and historical contributions, without inference.

.venv-remote/bin/python scripts/trace_first_price_valuation.py --root PATH_TO_RUN
"""
from __future__ import annotations

import argparse
import csv
from decimal import Decimal, ROUND_HALF_EVEN
import html
import json
from pathlib import Path
import sqlite3

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from analyze_valuation import extract


def label(job,warm,market):
    episode,idx,agent,suffix=job.rsplit(':',3)
    if episode not in (warm,market):raise ValueError('Foreign history source')
    return ('W' if episode==warm else 'M')+str(int(idx)+1)


def csv_write(path,rows):
    with path.open('w',newline='',encoding='utf-8') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader()
        for row in rows:writer.writerow({k:json.dumps(v) if isinstance(v,(list,dict)) else v for k,v in row.items()})


def trace(root):
    audited,all_terms,meta=extract(root)
    original=[r for r in audited if r['regime']=='first_price'];warm,market=meta['first_price']['warm'],meta['first_price']['episode']
    grouped={}
    for t in all_terms:
        if t['regime']=='first_price':grouped.setdefault((t['round'],t['agent']),[]).append(t)
    rows,terms,previous_used,previous_history=[],[],{},{}
    db=sqlite3.connect((root/'authority/ledger.sqlite').as_uri()+'?mode=ro',uri=True)
    try:
        db.execute('BEGIN')
        for r in original:
            a=r['agent'];snap=json.loads(db.execute('SELECT snapshot_json FROM rounds WHERE episode_id=? AND round_index=?',(market,r['round']-1)).fetchone()[0]);history=snap['states'][a]['history']
            matching=[h for h in history if h['target']==r['target'] and h['mod_type']==r['mod_type']]
            selected=matching or history;tt=grouped[(r['round'],a)]
            assert [h['experiment_id'] for h in selected]==[t['source_job_id'] for t in tt]
            source_ids=[t['source_job_id'] for t in tt];aliases=[label(j,warm,market) for j in source_ids]
            full_history=[h['experiment_id'] for h in history]
            k=sum((Decimal(t['recomputed_reward_contribution']) for t in tt),Decimal(0));n=len(tt)
            kw=sum((Decimal(t['recomputed_reward_contribution']) for t in tt if t['source_job_id'].startswith(warm+':')),Decimal(0))
            top=max(tt,key=lambda t:Decimal(t['recomputed_reward_contribution']))
            assert (k/n).quantize(Decimal('.000001'),rounding=ROUND_HALF_EVEN)==Decimal(str(r['valuation_saved']))
            row={'round':r['round'],'agent':a,'target':r['target'],'mod_type':r['mod_type'],
                 'total_prior_history':len(history),'exact_match_count':len(matching),'used_sample_count':n,
                 'fallback_to_all':not bool(matching),'used_warm_count':sum(j.startswith(warm+':') for j in source_ids),
                 'used_market_count':sum(j.startswith(market+':') for j in source_ids),
                 'positive_gain_count':sum(Decimal(t['historical_positive_gain'])>0 for t in tt),
                 'zero_gain_count':sum(Decimal(t['historical_positive_gain'])==0 for t in tt),
                 'invalid_count':sum(not t['source_valid'] for t in tt),
                 'sample_aliases':aliases,'sample_job_ids':source_ids,
                 'new_formal_history_since_previous_round':[label(j,warm,market) for j in full_history if a in previous_history and j not in previous_history[a]],
                 'samples_added_since_previous_round':[label(j,warm,market) for j in source_ids if a not in previous_used or j not in previous_used[a]],
                 'samples_removed_since_previous_round':[label(j,warm,market) for j in previous_used.get(a,[]) if j not in source_ids],
                 'sum_recomputed_reward_contributions':str(k),'warm_valuation_component':float(kw/n),
                 'market_valuation_component':float((k-kw)/n),'warm_contribution_share':float(kw/k) if k else None,
                 'largest_source':label(top['source_job_id'],warm,market) if k else None,
                 'largest_source_share':float(Decimal(top['recomputed_reward_contribution'])/k) if k else None,
                 'valuation':r['valuation_saved'],'funded':r['funded'],'actual_reward':r['actual_reward']}
            rows.append(row)
            for t in tt:terms.append({**t,'source_alias':label(t['source_job_id'],warm,market),
                                     'exact_category_match':bool(matching),'sample_weight':1/n,
                                     'contribution_to_valuation':float(Decimal(t['recomputed_reward_contribution'])/n)})
            previous_used[a]=source_ids;previous_history[a]=full_history
    finally:db.rollback();db.close()
    return rows,terms,meta['first_price']


def figures(rows,out):
    agents=sorted({r['agent'] for r in rows})
    with plt.rc_context({'font.family':'DejaVu Sans','font.size':10,'svg.fonttype':'none'}):
        fig,axes=plt.subplots(len(agents),2,figsize=(17,3*len(agents)+1.8),squeeze=False)
        for i,a in enumerate(agents):
            rr=[r for r in rows if r['agent']==a];x=[r['round'] for r in rr];ax=axes[i,0]
            warm=[r['used_warm_count'] if not r['fallback_to_all'] else 0 for r in rr]
            market=[r['used_market_count'] if not r['fallback_to_all'] else 0 for r in rr]
            ax.bar(x,warm,color='#929eaf',label='Exact matches: Warm');ax.bar(x,market,bottom=warm,color='#2e8d79',label='Exact matches: Market')
            for r in rr:
                if r['fallback_to_all']:
                    ax.bar([r['round']],[r['used_sample_count']],facecolor='none',edgecolor='#d98a20',hatch='///',lw=1.3)
                    ax.text(r['round'],r['used_sample_count']+.35,'F',ha='center',fontsize=9,color='#b87517',weight='bold')
            ax.set_title(a+' · sample membership',loc='left',weight='bold');ax.set_ylabel('Number of prior formal records');ax.set_ylim(top=max(r['used_sample_count'] for r in rr)+2)
            if i==0:ax.legend(fontsize=8,frameon=False,loc='upper right')
            ax=axes[i,1]
            ax.stackplot(x,[r['warm_valuation_component'] for r in rr],[r['market_valuation_component'] for r in rr],colors=['#929eaf','#2e8d79'],alpha=.8,labels=['Valuation from Warm history','Valuation from Market history'])
            ax.plot(x,[r['valuation'] for r in rr],color='#171b22',marker='.',ms=4,lw=1.2,label='Total valuation v')
            for r in rr:
                if r['fallback_to_all']:ax.scatter([r['round']],[r['valuation']],s=45,facecolor='none',edgecolor='#d98a20',zorder=4)
            ax.set_title(a+' · source of valuation',loc='left',weight='bold');ax.set_ylabel('Valuation / expected reward units')
            if i==0:ax.legend(fontsize=8,frameon=False,loc='upper right')
        for ax in axes.flat:ax.set_xlim(.3,20.7);ax.set_xticks(range(1,21));ax.grid(axis='y',alpha=.18);ax.spines[['top','right']].set_visible(False)
        for ax in axes[-1]:ax.set_xlabel('Market round (before this round’s evaluation)')
        fig.suptitle('First-price valuation provenance: exact matches, fallback and historical influence',fontsize=18,weight='bold',x=.06,ha='left')
        fig.text(.06,.025,'F / hatched bars: zero exact matches; all own history is used instead. Positive and zero/invalid outcomes both count. Current-round results enter only subsequent rounds.',fontsize=9.5)
        fig.subplots_adjust(left=.065,right=.98,top=.93,bottom=.07,hspace=.4,wspace=.17)
        for ext in ['png','svg']:fig.savefig(out/('provenance.'+ext),dpi=155)
        plt.close(fig)


def report_html(rows,terms,out):
    h='''<!doctype html><meta charset="utf-8"><title>First-price valuation provenance</title><style>body{font:14px system-ui;margin:25px;color:#17212b}table{border-collapse:collapse;width:100%;white-space:nowrap}td,th{border-bottom:1px solid #dce2e8;padding:8px;text-align:right}th{background:#edf2f7}thead th{position:sticky;top:0}td:first-child{text-align:left}summary{cursor:pointer}details table{font-size:12px}select{padding:7px}small{color:#667}</style><h1>First-price valuation provenance</h1><p>Exact means the same target AND mod_type in this agent’s own prior formal history. Zero exact matches falls back to ALL own history. Wn = Warm wave n; Mn = Market round n. All round labels are 1-based. A current-round outcome is not included in its own valuation.</p><p><a href="provenance.png">Full provenance chart</a> · <a href="trace.csv">80 agent-rounds CSV</a> · <a href="historical-terms.csv">Historical contributions CSV</a></p><label>Agent <select id="agent"><option value="">All</option>'''
    h+=''.join('<option>'+html.escape(a)+'</option>' for a in sorted({r['agent'] for r in rows}))+'</select></label><div style="overflow:auto"><table><thead><tr>'
    keys=['round','agent','target','mod_type','total_prior_history','exact_match_count','used_sample_count','fallback_to_all','positive_gain_count','zero_gain_count','sample_aliases','new_formal_history_since_previous_round','valuation','warm_contribution_share','largest_source','largest_source_share','funded','actual_reward']
    h+=''.join('<th>'+k+'</th>' for k in ['Expand calculation']+keys)+'</tr></thead><tbody>'
    for r in rows:
        tt=[t for t in terms if t['round']==r['round'] and t['agent']==r['agent']]
        cols=['source_alias','exact_category_match','source_parent_score','source_score','source_valid','historical_positive_gain','hypothetical_score','recomputed_reward_contribution','sample_weight','contribution_to_valuation']
        detail='<details><summary>'+str(len(tt))+' terms</summary><p>v = '+html.escape(r['sum_recomputed_reward_contributions'])+' / '+str(r['used_sample_count'])+' = '+str(r['valuation'])+'</p><p>Added samples: '+html.escape(str(r['samples_added_since_previous_round']))+'; removed: '+html.escape(str(r['samples_removed_since_previous_round']))+'</p><table><tr>'+''.join('<th>'+c+'</th>' for c in cols)+'</tr>'
        for t in tt:detail+='<tr>'+''.join('<td>'+html.escape(str(t[c]))+'</td>' for c in cols)+'</tr>'
        detail+='</table></details>'
        def fmt(v):return '—' if v is None else f'{v:.6f}' if isinstance(v,float) else str(v)
        h+='<tr data-agent="'+r['agent']+'"><td>'+detail+'</td>'+''.join('<td>'+html.escape(fmt(r[k]))+'</td>' for k in keys)+'</tr>'
    h+='''</tbody></table></div><script>document.querySelector('#agent').onchange=function(){const a=this.value;document.querySelectorAll('tr[data-agent]').forEach(r=>r.hidden=!!(a&&r.dataset.agent!==a));}</script>'''
    (out/'trace.html').write_text(h,encoding='utf-8')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',required=True);p.add_argument('--output');args=p.parse_args()
    root=Path(args.root).resolve();out=Path(args.output).resolve() if args.output else root/'reports/first-price-valuation-provenance'
    rows,terms,metadata=trace(root);out.mkdir(parents=True,exist_ok=True)
    csv_write(out/'trace.csv',rows);csv_write(out/'historical-terms.csv',terms)
    (out/'trace.json').write_text(json.dumps({'metadata':metadata,'rows':rows,'terms':terms},indent=2)+'\n')
    figures(rows,out);report_html(rows,terms,out)
    ds=[r for r in rows if r['agent']=='agent-4' and r['target']=='data_structure']
    summary={'agent_rounds':len(rows),'historical_sample_uses':len(terms),'exact_match_agent_rounds':sum(not r['fallback_to_all'] for r in rows),
             'fallbacks':[{'agent':r['agent'],'round':r['round'],'used_sample_count':r['used_sample_count']} for r in rows if r['fallback_to_all']],
             'agent4_data_structure_contribution_sum_range':[str(min(Decimal(r['sum_recomputed_reward_contributions']) for r in ds)),str(max(Decimal(r['sum_recomputed_reward_contributions']) for r in ds))],
             'new_model_calls':0,'new_evaluations':0}
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2));print(out)


if __name__=='__main__':main()
