"""Portable CSV/JSON reports, including common-prefix and main costs."""
from pathlib import Path
from decimal import Decimal
import csv
import json
import math
from maestro.storage import Ledger
from maestro.types import canonical_json


def report(root,episode_id=None):
    root=Path(root)
    output=root/'reports'
    output.mkdir(parents=True,exist_ok=True)
    with Ledger(root/'authority'/'ledger.sqlite') as ledger:
        db=ledger.connection
        episodes=db.execute('SELECT * FROM episodes'+(' WHERE id=?' if episode_id else ''),(episode_id,) if episode_id else ()).fetchall()
        summary=[]
        trajectory=[]
        wallets=[]
        for ep in episodes:
            e=ep['id']
            usage=db.execute('SELECT phase,sum(cpu_seconds) cpu,sum(wall_seconds) wall,sum(llm_calls) calls,sum(tokens) tokens FROM resource_usage WHERE episode_id=? GROUP BY phase',(e,)).fetchall()
            costs={r['phase']:{'cpu_seconds':r['cpu'],'wall_seconds':r['wall'],'llm_calls':r['calls'],'tokens':r['tokens']} for r in usage}
            for phase,data in costs.items():
                raw=db.execute('SELECT payload FROM resource_usage WHERE episode_id=? AND phase=?',(e,phase)).fetchall()
                data['usage_complete']=all(json.loads(v[0]).get('usage_complete',True) for v in raw)
                data['token_measurement']='actual_or_explicit_simulation' if data['usage_complete'] else 'known_lower_bound_with_unresolved_usage'
                payloads=[json.loads(v[0]) for v in raw]
                data['session_turns']=sum(v.get('session_turns',0) or 0 for v in payloads)
                data['model_request_measurement']='confirmed_requests' if data['usage_complete'] else 'confirmed_lower_bound'
            final=db.execute('SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',(e,)).fetchone()
            checkpoint=json.loads(final[0]) if final else {}
            stats=db.execute('SELECT sum(x) admitted,count(*) decisions FROM final_allocations WHERE episode_id=? AND round_index>=0',(e,)).fetchone()
            invites=db.execute('SELECT count(*) FROM invitations WHERE episode_id=? AND round_index>=0',(e,)).fetchone()[0]
            cfg=json.loads(ep['config_json'])
            row={'episode_id':e,'policy':ep['policy'],'execution_mode':ep['execution_mode'],'benchmark':ep['benchmark'],'status':ep['status'],
                'profile':cfg.get('profile','v1_cooperative'),
                'inference_budget_unit':'session_turn' if cfg.get('profile')=='codex_session_pilot' else 'provider_request',
                'warm_id':ep['bootstrap_id'],'final_training_score':checkpoint.get('board',{}).get('champion_score'),
                'admitted_slots':stats['admitted'] or 0,'invitations':invites,
                'generation_nonadmissions':invites-(stats['admitted'] or 0),
                'costs':costs,'heldout_score':None,'heldout_status':'NOT_RUN',
                'reserved_cpu_seconds':(stats['admitted'] or 0)*(cfg['evaluation_instance_count']*cfg['cpu_seconds_per_instance']+cfg['verification_cpu_seconds']),
                'calibration_scope':'funded_only_selection_biased','outcome_label':'simulated' if ep['execution_mode']=='mock' else 'pace_style_internal'}
            errors=[]
            for s in db.execute('SELECT * FROM settlements WHERE episode_id=?',(e,)):
                submission=db.execute('SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',(e,s['round_index'],s['agent_id'])).fetchone()
                if submission and json.loads(s['payload']).get('result_job_id'):
                    errors.append(float(json.loads(submission[0])['value'])-float(s['reward']))
                wallets.append({k:s[k] for k in ('episode_id','round_index','agent_id','payment','reward','wealth_before','wealth_after')})
            row['forecast_mae']=sum(abs(v) for v in errors)/len(errors) if errors else None
            row['forecast_mse']=sum(v*v for v in errors)/len(errors) if errors else None
            heldout_data=None
            for heldout in db.execute("SELECT payload FROM events WHERE kind='HELDOUT_COMPLETED' ORDER BY id DESC"):
                value=json.loads(heldout[0])
                if value.get('source_episode_id')==e:
                    heldout_data=value
                    break
            if heldout_data:
                row['heldout_score']=heldout_data.get('score')
                row['heldout_status']=heldout_data.get('status','COMPLETED')
                row['heldout_record']=heldout_data
                row['costs']['heldout']={'cpu_seconds':heldout_data['cpu_seconds'],'wall_seconds':heldout_data['wall_seconds'],'llm_calls':0,'tokens':0}
            row['comparison_eligible']=ep['policy'] in ('uniform','ucb','first_price') and ep['status']=='COMPLETED' and cfg.get('profile','v1_cooperative')=='v1_cooperative'
            if ep['bootstrap_id']:
                prefix=db.execute('SELECT sum(cpu_seconds) cpu,sum(llm_calls) calls,sum(tokens) tokens FROM resource_usage WHERE episode_id=?',(ep['bootstrap_id'],)).fetchone()
                row['common_prefix_cost']={'cpu_seconds':prefix['cpu'] or 0,'llm_calls':prefix['calls'] or 0,'tokens':prefix['tokens'] or 0}
                prefix_payloads=[json.loads(item[0]) for item in db.execute('SELECT payload FROM resource_usage WHERE episode_id=?',(ep['bootstrap_id'],))]
                complete=all(item.get('usage_complete',True) for item in prefix_payloads)
                row['common_prefix_cost'].update(session_turns=sum(item.get('session_turns',0) or 0 for item in prefix_payloads),
                    usage_complete=complete,model_request_measurement='confirmed_requests' if complete else 'confirmed_lower_bound')
                row['total_cpu_seconds_with_prefix']=sum(v['cpu_seconds'] for v in costs.values())+(prefix['cpu'] or 0)
            summary.append(row)
            for board in db.execute('SELECT round_index,payload FROM public_board_versions WHERE episode_id=? ORDER BY round_index',(e,)):
                b=json.loads(board['payload'])
                trajectory.append({'episode_id':e,'policy':ep['policy'],'round_index':board['round_index'],'champion_score':b['champion_score'],'champion_hash':b['champion_hash']})
        (output/'summary.json').write_text(canonical_json({'episodes':summary,'mock_is_not_real_pace':True}),encoding='utf-8')
        for name,rows in [('trajectory.csv',trajectory),('wallets.csv',wallets)]:
            with (output/name).open('w',newline='',encoding='utf-8') as stream:
                if rows:
                    writer=csv.DictWriter(stream,fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
        _trajectory_svg(output/'trajectory.svg',trajectory)
        export=root/'exports'
        export.mkdir(exist_ok=True)
        with (export/'events.jsonl').open('w',encoding='utf-8') as stream:
            for event in db.execute('SELECT * FROM events ORDER BY id'):
                stream.write(canonical_json(dict(event))+'\n')
        return {'report':str(output/'summary.json'),'episodes':summary}


def _trajectory_svg(path,rows):
    """Simple dependency-free quality curve; source data remain in CSV."""
    from html import escape
    colors={'uniform':'#2563eb','ucb':'#dc2626','first_price':'#16a34a'}
    groups={}
    for row in rows:
        if row['policy'] in colors:
            groups.setdefault((row['episode_id'],row['policy']),[]).append(row)
    max_round=max((r['round_index'] for r in rows),default=1)
    pieces=['<svg xmlns="http://www.w3.org/2000/svg" width="820" height="420" viewBox="0 0 820 420">',
        '<rect width="820" height="420" fill="white"/>',
        '<text x="60" y="25" font-size="16">Verified training champion (see execution_mode and benchmark labels in summary.json)</text>',
        '<path d="M60 50 V360 H770" fill="none" stroke="#333"/>']
    for tick in range(6):
        score=tick/5
        y=360-score*290
        pieces.append(f'<text x="20" y="{y+4}" font-size="12">{score:.1f}</text><path d="M60 {y} H770" stroke="#eee"/>')
    for index,((episode,policy),series) in enumerate(groups.items()):
        points=' '.join(f"{60+710*r['round_index']/max(1,max_round):.2f},{360-290*r['champion_score']:.2f}" for r in series)
        pieces.append(f'<polyline points="{points}" fill="none" stroke="{colors[policy]}" stroke-width="2"><title>{escape(episode)}</title></polyline>')
        pieces.append(f'<text x="{60+(index%3)*225}" y="{390+(index//3)*15}" font-size="12" fill="{colors[policy]}">{escape(policy)}</text>')
    pieces.append('</svg>')
    Path(path).write_text('\n'.join(pieces),encoding='utf-8')
