"""Read-only request-level failure reporting; no prompt/response/credential data."""
from collections import Counter
from datetime import datetime,timezone
import csv
import json
from pathlib import Path
import sqlite3

from .deepseek_lifecycle import audit_ledger


def request_failures(root,episode_ids=None,*,ledger_path=None):
    root=Path(root);path=Path(ledger_path) if ledger_path is not None else root/'deepseek-native-authority/requests.sqlite'
    result={'observed_utc':datetime.now(timezone.utc).isoformat(),'provider':'deepseek',
        'request_ledger':str(path),'available':path.is_file(),'read_only':True,'requests':[]}
    if not path.is_file():return {**result,'counts':{}}
    audit=audit_ledger(path);states={r['reservation_id']:r for r in audit['requests']}
    db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
    try:
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        links={}
        if 'deepseek_request_attempts' in tables:
            links={r['account']:dict(r) for r in db.execute('''SELECT a.*,l.account AS root_account,
                l.status AS logical_status,l.final_reservation FROM deepseek_request_attempts a
                JOIN deepseek_logical_requests l ON l.id=a.logical_id''')}
        abandoned={r[0]:json.loads(r[1]) for r in db.execute('SELECT account,cleanup_json FROM deepseek_abandoned_scopes')} if 'deepseek_abandoned_scopes' in tables else {}
        cleanups={r[0]:json.loads(r[1]) for r in db.execute('SELECT account,cleanup_json FROM deepseek_phase_cleanup')} if 'deepseek_phase_cleanup' in tables else {}
        for row in db.execute('SELECT * FROM inference_reservations ORDER BY rowid'):
            rid=row['reservation_id'];link=links.get(row['account'],{});account=link.get('root_account',row['account'])
            parts=account.split(':');episode=parts[0]
            if episode_ids is not None and episode not in episode_ids:continue
            stage=parts[3] if len(parts)>3 else None
            events=[{**dict(e),'data':json.loads(e['data_json'])} for e in db.execute(
                'SELECT kind,utc,monotonic,data_json FROM deepseek_request_events WHERE reservation_id=? ORDER BY id',(rid,))] if 'deepseek_request_events' in tables else []
            errors=[e for e in events if e['kind']=='ERROR'];rejects=[e for e in events if e['kind']=='REJECTED']
            observed=states.get(rid,{'state':row['status'],'provider_message_id':None,'response_request_id':None})
            state=observed['state']
            if not errors and not rejects and state in {'COMPLETED','NEVER_DISPATCHED'}:continue
            error=errors[-1]['data'] if errors else {};error_utc=errors[-1]['utc'] if errors else None
            if not error and link and 'deepseek_retry_events' in tables:
                fallback=db.execute("SELECT data_json,utc FROM deepseek_retry_events WHERE logical_id=? AND attempt_index=? AND kind='ATTEMPT_FAILED' ORDER BY id DESC LIMIT 1",
                    (link['logical_id'],link['attempt_index'])).fetchone()
                if fallback:error=json.loads(fallback[0]);error_utc=fallback[1]
            header=next((e['data'] for e in events if e['kind']=='HEADERS'),{})
            start=next((e for e in events if e['kind']=='DISPATCH_STARTED'),None)
            closed=next((e for e in reversed(events) if e['kind']=='TRANSPORT_CLOSED'),None)
            replacement=link.get('logical_status')=='COMPLETED' and row['status']=='UNKNOWN'
            result['requests'].append({'reservation_id':rid,'request_id':row['request_id'],
                'episode_id':episode,'round_index':int(parts[1]) if len(parts)>1 and parts[1].lstrip('-').isdigit() else None,
                'wave_number':int(parts[1])+1 if episode.startswith('warm-') and len(parts)>1 and parts[1].isdigit() else None,
                'agent_id':parts[2] if len(parts)>2 else None,'stage':stage,'root_account':account,
                'logical_request_id':link.get('logical_id'),'attempt_index':link.get('attempt_index',0),
                'ledger_status':row['status'],'audit_state':state,'is_unknown':state=='UNKNOWN',
                'http_status':header.get('http_status'),'provider_message_id':observed['provider_message_id'],
                'response_request_id':observed['response_request_id'],
                'first_dispatch_utc':start['utc'] if start else None,
                'error_utc':error_utc,'transport_closed_utc':closed['utc'] if closed else None,
                'elapsed_seconds':closed['monotonic']-start['monotonic'] if start and closed else None,
                'exception_class':error.get('exception_class'),'errno':error.get('exception_errno'),
                'operation':error.get('transport_operation'),'exception_chain':error.get('exception_chain',[]),
                'traceback_available':bool(error.get('exception_chain')),
                'local_abort_reason':error.get('local_abort_reason'),
                'deadline_remaining_seconds':error.get('deadline_remaining_seconds'),
                'transport_closed':closed is not None,
                'phase_cleanup':cleanups.get(account,abandoned.get(account)),
                'outcome':'REPLACEMENT_COMPLETED_COST_UNKNOWN' if replacement else 'PHASE_ABSTAINED_COST_UNKNOWN' if account in abandoned else 'UNRECOVERED',
                'replacement_reservation_id':link.get('final_reservation'),
                'reserved_output_tokens':row['output_tokens'],'actual_output_tokens':row['actual_output_tokens'],
                'remote_termination_confirmed':False if state=='UNKNOWN' else None,
                'events':[{'kind':e['kind'],'utc':e['utc'],'monotonic':e['monotonic'],'data':e['data']} for e in events if e['kind']!='STREAM_EVENT']})
    finally:db.close()
    unknown=[r for r in result['requests'] if r['is_unknown']]
    result['counts']={'unknown_requests':len(unknown),'failed_or_unknown_requests':len(result['requests']),
        'affected_logical_requests':len({r['logical_request_id'] or r['reservation_id'] for r in unknown}),
        'affected_phase_scopes':len({r['root_account'] for r in unknown}),
        'unknown_with_completed_replacement':sum(r['outcome']=='REPLACEMENT_COMPLETED_COST_UNKNOWN' for r in unknown),
        'unknown_with_phase_abstention':sum(r['outcome']=='PHASE_ABSTAINED_COST_UNKNOWN' for r in unknown),
        'unknown_without_traceback':sum(not r['traceback_available'] for r in unknown)}
    result['unknown_by_stage']=dict(Counter(r['stage'] or 'UNKNOWN_STAGE' for r in unknown))
    result['unknown_by_agent']=dict(Counter(r['agent_id'] or 'UNKNOWN_AGENT' for r in unknown))
    result['exceptions']=dict(Counter(r['exception_class'] or 'NO_EXCEPTION_RECORD' for r in result['requests']))
    authority=root/'authority/ledger.sqlite'
    result['controller_phase_failures']=[]
    if authority.is_file():
        core=sqlite3.connect(authority.resolve().as_uri()+'?mode=ro',uri=True);core.row_factory=sqlite3.Row
        try:
            for row in core.execute("SELECT id,episode_id,round_index,kind,payload FROM events WHERE kind IN ('PREPARATION_OUTCOME','PLANNING_OUTCOME','GENERATION_ATTEMPT_FAILED','GENERATION_FAILED','PHASE_CLEANUP_DRAIN','WARM_SEED_FALLBACK') ORDER BY id"):
                if episode_ids is not None and row['episode_id'] not in episode_ids:continue
                payload=json.loads(row['payload'])
                if row['kind']=='PREPARATION_OUTCOME' and payload.get('status')=='PREPARED':continue
                result['controller_phase_failures'].append({k:row[k] for k in ('id','episode_id','round_index','kind')} | {
                    k:payload.get(k) for k in ('agent_id','status','error_type','scope','reason','phase','reclaimed_callbacks','pending_callbacks') if k in payload})
        finally:core.close()
    from maestro.storage.paths import run_paths
    controller=run_paths(root).local/'controller-errors.jsonl'
    result['controller_tracebacks']=[json.loads(line) for line in controller.read_text().splitlines()] if controller.is_file() else []
    return result


def write_failure_report(root,output=None,episode_ids=None,*,ledger_path=None):
    root=Path(root);output=Path(output) if output is not None else root/'reports';output.mkdir(parents=True,exist_ok=True)
    value=request_failures(root,episode_ids,ledger_path=ledger_path)
    (output/'request-failures.json').write_text(json.dumps(value,indent=2)+'\n')
    keys=['episode_id','round_index','wave_number','agent_id','stage','logical_request_id','attempt_index',
        'reservation_id','ledger_status','audit_state','is_unknown','exception_class','errno','operation',
        'http_status','error_utc','elapsed_seconds','transport_closed','traceback_available','outcome','provider_message_id']
    with (output/'request-failures.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=keys,extrasaction='ignore');writer.writeheader();writer.writerows(value['requests'])
    lines=['Request failure audit (read-only). Counts are request-level, not phase-level.',
           json.dumps(value.get('counts',{}),ensure_ascii=False),'']
    for r in value['requests']:
        lines.append(f"{r['episode_id']} round={r['round_index']} {r['agent_id']} {r['stage']} attempt={r['attempt_index']} {r['audit_state']} {r['exception_class']} [{r['reservation_id']}]")
        if not r['traceback_available']:lines.append('  Traceback unavailable in original evidence; not reconstructed.')
        for chain in r['exception_chain']:
            lines.append(f"  {chain['exception_class']} errno={chain['exception_errno']}")
            for frame in chain['frames']:lines.append(f"    {frame['file']}:{frame['line']} in {frame['function']}")
    (output/'request-failures.txt').write_text('\n'.join(lines)+'\n')
    return {'json':str(output/'request-failures.json'),'csv':str(output/'request-failures.csv'),
            'tracebacks':str(output/'request-failures.txt'),'counts':value.get('counts',{})}
