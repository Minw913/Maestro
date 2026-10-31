"""One-use live R4 generation-deadline qualification; one provider request.

Uses the real PACE seed and native DeepSeek implementation path, then pauses
the generation broker as valuation does and checks the frozen candidate in the
real Docker precheck. No formal evaluation, score, market, or retry is run.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from maestro.backends.coral.deepseek_backend import native_factory
from maestro.backends.coral.deepseek_lifecycle import audit_ledger, ledger_fingerprint
from maestro.config import load_config
from maestro.execution.grants import GrantAuthority
from maestro.types import AgentState, Plan


def utc():
    return datetime.now(timezone.utc).isoformat()


def run(args):
    root=args.root.resolve()
    if root.exists():
        raise FileExistsError('One-use qualification root already exists')
    old=args.quarantined_ledger.resolve()
    prior=audit_ledger(old)
    unknown=[row for row in prior['requests'] if row['state']=='UNKNOWN']
    if (prior['status']!='BLOCKED' or len(unknown)!=1 or prior['unresolved_scopes']
            or any(item['account']!=unknown[0]['account'] for item in prior['active_budgets'])):
        raise RuntimeError('Historical UNKNOWN identity changed; do not dispatch')
    config=load_config(args.config,stage='market')
    if (config.execution_mode,config.benchmark,config.coral.get('runtime'),
            config.coral.get('candidate_pipeline'))!=('real','pace','dsh','prebid_v2'):
        raise ValueError('Real native DeepSeek prebid config required')
    # No transport retry: this diagnostic authorizes exactly one physical request.
    config=replace(config,generation_calls=1,generation_window_seconds=min(180,config.generation_window_seconds),
                   coral={**config.coral,'deepseek_transport_retry':None})
    root.mkdir(parents=True)
    native=root/'runtime'
    (native/'authority').mkdir(parents=True)
    template=json.loads(args.historical_template.read_text())
    entries=list(template['entries'])
    entries.append({'ledger':str(old),'reservation_ids':[unknown[0]['reservation_id']],
                    'ledger_fingerprint':ledger_fingerprint(old),
                    'authorization_scope':'User explicitly requested independent small requalification after the paused R4 Market; local controller inactive; remote cost remains UNKNOWN'})
    exception={'new_run_root':str(native),'user_authorization':
        'User requested requalification on 2026-10-27; one physical request maximum; old UNKNOWN and quota retained',
        'remote_termination_confirmed':False,'retain_historical_budget':True,'entries':entries}
    (native/'authority/deepseek-historical-exceptions.json').write_text(json.dumps(exception,indent=2)+'\n')
    (root/'qualification-config.json').write_text(json.dumps(config.to_dict(),indent=2)+'\n')
    result={'status':'FAILED','started_utc':utc(),'maximum_actual_requests':1,
            'formal_evaluations':0,'market_rounds':0,'historical_unknown_unchanged':False,
            'checks':{}}
    old_fingerprint=ledger_fingerprint(old)
    backend=None;authority=GrantAuthority();grant=None;stage='preflight'
    try:
        backend=native_factory(config,root=native)
        episode='deadline-qualification-'+root.name
        backend.begin_episode(episode)
        parent=backend.prepare_seed()
        backend.register_parent(parent['candidate_hash'],parent['candidate_path'])
        state=AgentState('qualifier',parent['candidate_hash'],0)
        plan=Plan('deadline-plan','deadline-v1','qualifier',state.parent_hash,
                  'data_structure','parameter_change',
                  'Deadline qualification on the real frozen PACE seed.',
                  'Preserve the complete solver. Add exactly one harmless Python comment to run; '
                  'return the complete candidate-relative run file. Do not change solver behavior.',
                  'No quality claim; exercise one native implementation and physical precheck.')
        deadline=time.monotonic()+config.generation_window_seconds
        stage='generation'
        grant=authority.issue(episode_id=episode,round_index=0,agent_id=state.agent_id,
            plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=0,
            expires_at=time.time()+config.generation_window_seconds,purpose='prebid_implementation',
            source_candidate_hash=state.parent_hash)
        candidate=backend.generate(state,plan,0,deadline=deadline,grant=grant,grant_authority=authority)
        generation_usage=dict(backend.last_generation_usage)
        authority.revoke(grant);grant=None
        result['generation_usage']=generation_usage
        result['checks']['real_generation_complete']=True
        stage='valuation-barrier'
        started=time.monotonic()
        paused=backend.pause_generation_brokers(0,{state.agent_id:plan},
            {state.agent_id:candidate},started,deadline)
        # Real monotonic passage; no synthetic clock and no second model call.
        time.sleep(.05)
        finished=time.monotonic()
        resumed=deadline+(finished-started)
        backend.resume_generation_brokers(paused,finished,resumed)
        result['valuation_barrier_seconds']=finished-started
        result['checks']['common_deadline_restored']=True
        stage='postauction-check'
        grant=authority.issue(episode_id=episode,round_index=0,agent_id=state.agent_id,
            plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=0,
            expires_at=time.time()+max(.001,resumed-time.monotonic()),purpose='postauction_check',
            source_candidate_hash=candidate['candidate_hash'])
        backend.activate_frozen_candidate(state,plan,candidate,0,resumed,
            grant=grant,grant_authority=authority)
        result['checks']['frozen_candidate_rebound']=True
        outcome=backend.precheck(candidate)
        result['precheck']={'passed':outcome==(True,'pass'),
                            'reason':outcome[1] if isinstance(outcome,tuple) else type(outcome).__name__,
                            'usage':dict(backend.last_generation_usage)}
        result['checks']['physical_precheck_passed']=result['precheck']['passed']
        authority.revoke(grant);grant=None
    except BaseException as error:
        result['status']='UNKNOWN' if (getattr(error,'inflight_unknown',False)
            or getattr(error,'execution_unknown',False)) else 'FAILED'
        result['failed_stage']=stage
        result['error_type']=type(error).__name__
        result['error_message']=str(error)[:500]
    finally:
        if grant is not None:authority.revoke(grant)
        if backend is not None:
            try:backend.close()
            except BaseException as error:
                result['status']='UNKNOWN';result['cleanup_error_type']=type(error).__name__
        ledger=native/'deepseek-native-authority/requests.sqlite'
        if ledger.exists():
            audit=audit_ledger(ledger)
            result['request_audit']={'status':audit['status'],
                'requests':[{k:v for k,v in row.items() if k in
                    ('reservation_id','state','ledger_status','provider_message_id','last_event')}
                    for row in audit['requests']],
                'unresolved_scopes':len(audit['unresolved_scopes']),
                'active_budgets':len(audit['active_budgets'])}
            if audit['status']!='CLEAR' or len(audit['requests'])!=1:
                result['status']='UNKNOWN' if audit['status']!='CLEAR' else 'FAILED'
        result['historical_unknown_unchanged']=ledger_fingerprint(old)==old_fingerprint
        if not result['historical_unknown_unchanged']:result['status']='UNKNOWN'
        if (result['status']=='FAILED' and result.get('failed_stage')):pass
        elif (result.get('request_audit',{}).get('status')=='CLEAR'
              and len(result['request_audit']['requests'])==1
              and all(result['checks'].get(key) for key in
                  ('real_generation_complete','common_deadline_restored',
                   'frozen_candidate_rebound','physical_precheck_passed'))
              and result['historical_unknown_unchanged']):
            result['status']='PASSED'
        result['finished_utc']=utc()
        (root/'result.json').write_text(json.dumps(result,indent=2,default=str)+'\n')
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',required=True,type=Path)
    parser.add_argument('--config',required=True,type=Path)
    parser.add_argument('--historical-template',required=True,type=Path)
    parser.add_argument('--quarantined-ledger',required=True,type=Path)
    value=run(parser.parse_args())
    print(json.dumps({'status':value['status'],'result':str(Path(parser.parse_args().root)/'result.json'),
        'checks':value['checks']},default=str),flush=True)
    raise SystemExit(0 if value['status']=='PASSED' else 2)
