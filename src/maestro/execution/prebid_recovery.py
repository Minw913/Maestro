"""One-time, append-only containment of an interrupted prebid round."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import time

from maestro.backends.coral.deepseek_retry import audit_retry_continuation
from maestro.execution.pause import PauseController
from maestro.market.credit import money
from maestro.replay import verify_ledger
from maestro.storage import Ledger
from maestro.types import IntegrityError, canonical_json, stable_hash


def _command(*args):
    result=subprocess.run(args,capture_output=True,text=True,timeout=15,check=True)
    return result.stdout.strip()


def _frozen_source_matches(db, episode_id, checkpoint, snapshot, round_index):
    if checkpoint['board'] != snapshot['board']:
        return False
    expected = json.loads(json.dumps(checkpoint['states']))
    extensions = []
    for row in db.execute("SELECT payload FROM events WHERE episode_id=? "
                          "AND kind='MARKET_ROUNDS_EXTENDED'", (episode_id,)):
        event = json.loads(row[0])
        if event.get('checkpoint_round_index') == round_index - 1:
            extensions.append(event)
    if len(extensions) > 1:
        return False
    if extensions:
        delta = Decimal(extensions[0]['funding_per_agent'])
        for state in expected.values():
            state['wealth'] = str(money(Decimal(state['wealth']) + delta))
    return expected == snapshot['states']


def _collision_has_no_model_dispatch(db, native, episode_id, round_index, agents):
    outcomes = [json.loads(row[0]) for row in db.execute(
        "SELECT payload FROM events WHERE episode_id=? AND round_index=? "
        "AND kind='PREPARATION_OUTCOME'", (episode_id, round_index))]
    if (len(outcomes) != len(agents) or {item.get('agent_id') for item in outcomes} != set(agents)
            or any(item.get('status') != 'ABSTAIN' or item.get('error_type') != 'FileExistsError'
                   or item.get('request_count_unknown') is not False
                   or item.get('usage', {}).get('calls') != 0
                   or item.get('usage', {}).get('unresolved_requests') != 0
                   for item in outcomes)):
        return False
    conn = sqlite3.connect(native.resolve().as_uri() + '?mode=ro', uri=True)
    try:
        prefix = f'{episode_id}:{round_index}:'
        return all(conn.execute(
            f'SELECT 1 FROM {table} WHERE substr(account,1,length(?))=? LIMIT 1',
            (prefix,prefix)).fetchone() is None
            for table in ('inference_reservations', 'deepseek_logical_requests','codex_phase_scopes'))
    finally:
        conn.close()


def reconcile_prebid(root, episode_id, round_index, *, service):
    root=Path(root).resolve()
    pause=PauseController(root)
    if not pause.path.is_file():
        raise IntegrityError('A durable pause marker is required')
    props=dict(line.split('=',1) for line in _command('systemctl','--user','show',service,
        '-p','ActiveState','-p','MainPID','-p','ControlGroup').splitlines())
    if props.get('ActiveState') not in ('inactive','failed') or props.get('MainPID')!='0' or props.get('ControlGroup'):
        raise IntegrityError('Controller service or cgroup is not fully reclaimed')
    containers=_command('docker','ps','--format','{{.Names}}').splitlines()
    if any('maestro' in name.lower() or episode_id in name for name in containers):
        raise IntegrityError('A MAESTRO container remains active')
    native=root/'deepseek-native-authority'/'requests.sqlite'
    audit=audit_retry_continuation(native)
    if audit['status'] not in ('CLEAR','CONTINUABLE_WITH_UNKNOWN_COST'):
        raise IntegrityError('Native request ledger has unreclaimed or unaccounted scopes')
    verify=verify_ledger(root,episode_id)
    with Ledger(root/'authority'/'ledger.sqlite',read_only=True) as ledger:
        db=ledger.connection
        latest=db.execute('SELECT * FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',(episode_id,)).fetchone()
        row=db.execute('SELECT * FROM rounds WHERE episode_id=? AND round_index=?',(episode_id,round_index)).fetchone()
        if (not latest or latest['round_index']!=round_index-1 or not row
                or row['status'] not in ('IMPLEMENTATION','PREPARATION_FAILED')):
            raise IntegrityError('Round is not a recoverable prebid attempt after a settled checkpoint')
        unfinished=db.execute("SELECT round_index FROM rounds WHERE episode_id=? AND status!='SETTLED'",(episode_id,)).fetchall()
        if [x[0] for x in unfinished]!=[round_index]:
            raise IntegrityError('Other unfinished rounds exist')
        snapshot=json.loads(row['snapshot_json']); checkpoint=json.loads(latest['payload'])
        if (not _frozen_source_matches(db,episode_id,checkpoint,snapshot,round_index)
                or row['snapshot_hash']!=stable_hash(snapshot)):
            raise IntegrityError('Frozen state/board differs from settled checkpoint')
        if (row['status']=='PREPARATION_FAILED'
                and not _collision_has_no_model_dispatch(db,native,episode_id,round_index,
                                                         checkpoint['states'])):
            raise IntegrityError('Preparation collision has dispatched or unaccounted requests')
        for table in ('round_submissions','priority_orders','invitations','eval_tickets','final_allocations','checkpoints'):
            if db.execute(f'SELECT 1 FROM {table} WHERE episode_id=? AND round_index=? LIMIT 1',(episode_id,round_index)).fetchone():
                raise IntegrityError(f'Interrupted round already has {table}')
        old_grants=[json.loads(x[0]) for x in db.execute(
            "SELECT payload FROM edit_grants WHERE json_extract(payload,'$.episode_id')=? "
            "AND json_extract(payload,'$.round_index')=?",(episode_id,round_index))]
        if any(float(grant['expires_at'])>=time.time() for grant in old_grants):
            raise IntegrityError('An EditGrant is still valid')
        for kind in ('PROPOSAL_FROZEN','PLAN_SUBMITTED','ROUND_SETTLED'):
            if db.execute('SELECT 1 FROM events WHERE episode_id=? AND round_index=? AND kind=? LIMIT 1',
                (episode_id,round_index,kind)).fetchone():
                raise IntegrityError(f'Interrupted round already has {kind}')
        anchor=db.execute('SELECT id,hash FROM events ORDER BY id DESC LIMIT 1').fetchone()
    replay={'verified':verify['verified'],'settlements':verify['settlements'],
            'events':len(verify['events']),'artifacts':verify['artifacts']}
    evidence={'utc':datetime.now(timezone.utc).isoformat(),'episode_id':episode_id,'round_index':round_index,
        'interrupted_status':row['status'],
        'service':service,'service_properties':props,'active_docker_names':containers,
        'request_audit_status':audit['status'],'unknown_cost_preserved':not audit['cost_usage_complete'],
        'native_ledger_sha256':hashlib.sha256(native.read_bytes()).hexdigest(),
        'last_settled_checkpoint_hash':latest['content_hash'],'round_snapshot_hash':row['snapshot_hash'],
        'expired_edit_grants':len(old_grants),
        'source_event_id':anchor['id'],'source_event_hash':anchor['hash'],'replay':replay,
        'request_namespace':f'{episode_id}-recovery-r{round_index}-a1'}
    from maestro.storage.paths import run_paths
    output=run_paths(root).local/'authority'/f'prebid-recovery-r{round_index}.json'
    if output.exists():
        raise IntegrityError('Recovery evidence already exists; refusing another attempt')
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(canonical_json(evidence)+'\n',encoding='utf-8')
    with Ledger(root/'authority'/'ledger.sqlite') as ledger:
        with ledger.transaction() as db:
            current=db.execute('SELECT status,snapshot_hash FROM rounds WHERE episode_id=? AND round_index=?',
                (episode_id,round_index)).fetchone()
            last=db.execute('SELECT id,hash FROM events ORDER BY id DESC LIMIT 1').fetchone()
            if (not current or current['status']!=row['status'] or current['snapshot_hash']!=row['snapshot_hash']
                    or tuple(last)!=tuple(anchor)):
                raise IntegrityError('Recovery source changed during containment')
            db.execute("UPDATE rounds SET status='RESTART_AUTHORIZED' WHERE episode_id=? AND round_index=?",
                (episode_id,round_index))
            ledger.event('PREBID_RECOVERY_AUTHORIZED',{'evidence_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
                'request_namespace':evidence['request_namespace'],'old_requests_not_replayed':True},episode_id,round_index)
    pause.path.unlink()
    return {'status':'RESTART_AUTHORIZED','evidence':str(output),'request_namespace':evidence['request_namespace']}
