"""Append-only transport evidence and read-only restart audit. No credentials/body text."""
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import sqlite3
import time

from maestro.execution.budgets import BudgetStore


def exception_details(error):
    """Preserve traceback locations and errno, never messages, source or locals.

    Exception strings and formatted tracebacks can contain credentials, request
    bodies or response text. Frame locations are sufficient to locate a failed
    transport operation without copying any of those values into the ledger.
    """
    chain=[];seen=set();current=error
    while current is not None and id(current) not in seen and len(chain)<4:
        seen.add(id(current));frames=[];tb=current.__traceback__
        while tb is not None:
            frames.append({'file':Path(tb.tb_frame.f_code.co_filename).name,
                           'function':tb.tb_frame.f_code.co_name,'line':tb.tb_lineno})
            tb=tb.tb_next
        number=getattr(current,'errno',None)
        chain.append({'exception_class':type(current).__name__,
                      'exception_errno':number if type(number) is int else None,
                      'frames':frames[-32:]})
        current=(current.__cause__ if current.__cause__ is not None else
                 None if current.__suppress_context__ else current.__context__)
    return {'exception_class':type(error).__name__,
            'exception_errno':chain[0]['exception_errno'],'exception_chain':chain}


class RequestJournal:
    def __init__(self, store, reservation, request_hash):
        self.store, self.reservation = store, reservation
        with store._transaction():
            store.connection.execute('''CREATE TABLE IF NOT EXISTS deepseek_request_events(
                id INTEGER PRIMARY KEY, reservation_id TEXT NOT NULL,
                kind TEXT NOT NULL, utc TEXT NOT NULL, monotonic REAL NOT NULL,
                data_json TEXT NOT NULL,
                FOREIGN KEY(reservation_id) REFERENCES inference_reservations(reservation_id))''')
        self.emit('RESERVED', request_hash=request_hash)

    def emit(self, kind, **data):
        # Only trusted transport code supplies metadata. Never persist headers,
        # URLs, prompts, response text/thinking or arbitrary exception messages.
        allowed = {'request_hash', 'provider_message_id', 'response_request_id',
                   'http_status', 'media_type', 'event_type', 'usage', 'bytes_received',
                   'exception_class', 'exception_errno', 'exception_chain', 'reason',
                   'transport_operation', 'elapsed_seconds', 'deadline_remaining_seconds',
                   'local_abort_requested', 'local_abort_reason', 'local_abort_monotonic',
                   'dispatch_sent', 'peer_ip', 'peer_port', 'local_ip', 'local_port',
                   'tls_version', 'watchdog_reclaimed'}
        if set(data) - allowed:
            raise ValueError('Unsupported transport metadata')
        with self.store._transaction():
            self.store.connection.execute('INSERT INTO deepseek_request_events '
                '(reservation_id,kind,utc,monotonic,data_json) VALUES(?,?,?,?,?)',
                (self.reservation, kind, datetime.now(timezone.utc).isoformat(),
                 time.monotonic(), json.dumps(data, sort_keys=True, allow_nan=False)))


def audit_ledger(path):
    """Read only. Old RESERVED/DISPATCHING rows never imply cancellation."""
    path = Path(path).resolve()
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        rows = []
        for r in db.execute('SELECT * FROM inference_reservations ORDER BY reservation_id'):
            receipt = (db.execute('SELECT * FROM provider_receipts WHERE reservation_id=?',
                                 (r['reservation_id'],)).fetchone() if 'provider_receipts' in tables else None)
            trusted_receipt = bool(receipt and receipt['usage_complete']==1
                and isinstance(receipt['provider_request_id'],str) and receipt['provider_request_id']
                and type(receipt['output_tokens']) is int and 0<=receipt['output_tokens']<=r['output_tokens'])
            events = ([dict(e) for e in db.execute('SELECT * FROM deepseek_request_events '
                'WHERE reservation_id=? ORDER BY id', (r['reservation_id'],))]
                if 'deepseek_request_events' in tables else [])
            terminal = next((json.loads(e['data_json']) for e in reversed(events)
                             if e['kind'] == 'TERMINAL'), None)
            never_sent = any(e['kind'] == 'NEVER_DISPATCHED' for e in events)
            rejected = any(e['kind'] == 'REJECTED' for e in events)
            started = next((json.loads(e['data_json']) for e in reversed(events)
                            if e['kind'] == 'MESSAGE_START'), {})
            headers = next((json.loads(e['data_json']) for e in reversed(events)
                            if e['kind'] == 'HEADERS'), {})
            normalized = BudgetStore.normalize_usage(terminal.get('usage', {}), protocol='/v1/messages') if terminal else {}
            trusted_terminal = bool(terminal and isinstance(terminal.get('provider_message_id'),str)
                                    and terminal['provider_message_id']
                                    and normalized.get('usage_complete')
                                    and normalized['output_tokens'] <= r['output_tokens'])
            if r['status'] == 'COMPLETED' and (trusted_receipt or never_sent or rejected):
                state = 'NEVER_DISPATCHED' if never_sent else 'REJECTED' if rejected else 'COMPLETED'
            elif trusted_terminal or trusted_receipt:
                state = 'TERMINAL_EVIDENCE_REQUIRES_RECONCILIATION'
            elif never_sent:
                state = 'NEVER_DISPATCHED_REQUIRES_RECONCILIATION'
            else:
                state = 'UNKNOWN'
            rows.append({'reservation_id': r['reservation_id'], 'account': r['account'],
                'request_id': r['request_id'], 'ledger_status': r['status'], 'state': state,
                'provider_message_id': (terminal or started).get('provider_message_id', '') or
                    (receipt['provider_request_id'] if receipt else ''),
                'response_request_id': headers.get('response_request_id',''),
                'last_event': events[-1]['kind'] if events else None,
                'transport_closed': any(e['kind'] == 'TRANSPORT_CLOSED' for e in events)})
        scopes = ([dict(r) for r in db.execute("SELECT * FROM codex_phase_scopes WHERE status IN ('UNKNOWN','DISPATCHING')")]
                  if 'codex_phase_scopes' in tables else [])
        budgets = [dict(r) for r in db.execute('SELECT account,active FROM inference_budgets WHERE active>0')]
        blocked = any(r['state'] not in {'COMPLETED','NEVER_DISPATCHED','REJECTED'} for r in rows) or bool(scopes) or bool(budgets)
        return {'ledger': str(path), 'status': 'BLOCKED' if blocked else 'CLEAR',
                'requests': rows, 'unresolved_scopes': scopes,'active_budgets':budgets,
                'read_only': True, 'automatic_reconciliation': False}
    finally:
        db.close()


def ledger_fingerprint(path):
    """Logical snapshot including quota charges; reading does not checkpoint WAL."""
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)
    db.row_factory=sqlite3.Row
    try:
        db.execute('BEGIN')
        snapshot={}
        for row in db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name"):
            name=row['name'];identifier='"'+name.replace('"','""')+'"'
            values=[dict(r) for r in db.execute('SELECT * FROM '+identifier)]
            snapshot[name]={'schema':row['sql'],'rows':sorted(values,key=lambda v:json.dumps(v,sort_keys=True))}
        return hashlib.sha256(json.dumps(snapshot,sort_keys=True,allow_nan=False).encode()).hexdigest()
    finally:db.close()


def _snapshot_terminal_proof(path, pending, entry):
    """A frozen export is not another execution: require its original's proof.

    Both ledgers remain untouched. Matching account/reservation IDs alone is
    insufficient; the original request identity and immutable bounds must match.
    """
    original=Path(entry['snapshot_of']).resolve()
    if original==path.resolve() or not original.is_file():return False
    if ledger_fingerprint(original)!=entry.get('source_ledger_fingerprint'):return False
    original_audit=audit_ledger(original)
    states={r['reservation_id']:r['state'] for r in original_audit['requests']}
    if any(states.get(r['reservation_id']) not in {'COMPLETED','REJECTED','NEVER_DISPATCHED'} for r in pending):
        return False
    snapshot=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)
    source=sqlite3.connect(original.as_uri()+'?mode=ro',uri=True)
    try:
        query=('SELECT account,request_id,request_bytes,output_tokens FROM '
               'inference_reservations WHERE reservation_id=?')
        return all(snapshot.execute(query,(r['reservation_id'],)).fetchone()==
                   source.execute(query,(r['reservation_id'],)).fetchone() for r in pending)
    finally:snapshot.close();source.close()


def _reclaimed_historical_scopes(entry,audit):
    """Bind explicit quarantine to exact native cleanup and supervisor evidence.

    This never settles remote usage or changes a historical scope's state.
    """
    proofs=entry.get('reclaimed_scopes',[])
    if not isinstance(proofs,list):return False
    if {p.get('account') for p in proofs}!={s['account'] for s in audit['unresolved_scopes']}:return False
    if len(proofs)!=len(audit['unresolved_scopes']):return False
    try:
        for proof in proofs:
            documents={}
            for role in ('scope','cleanup','supervisor'):
                file=Path(proof[role+'_path'])
                raw=file.read_bytes()
                if hashlib.sha256(raw).hexdigest()!=proof[role+'_sha256']:return False
                documents[role]=json.loads(raw)
            scope=documents['scope'];cleanup=documents['cleanup'];supervisor=documents['supervisor']
            account=proof['account'].split(':')
            if len(account)!=4:return False
            if account[3]=='PREPARATION':
                identity,_=json.JSONDecoder().raw_decode(scope['prompt'].split('\n',1)[1])
                if (identity.get('episode_id')!=account[0]
                        or identity.get('round_index')!=int(account[1])
                        or identity.get('agent_id')!=account[2]):return False
            elif account[3]=='VALUATION_RO':
                # Valuation's prompt is deliberately generic. Bind its native
                # scope to the agent via the exact prompt header emitted by
                # Coral, and require an audited request under this account.
                if not scope['prompt'].startswith('VALUATION_RO:') or not any(
                    r['account']==proof['account'] for r in audit['requests']):return False
                log=Path(proof['log_path'])
                if (log!=Path(proof['scope_path']).parent/'public/logs'/f'{account[2]}.0.log'
                        or hashlib.sha256(log.read_bytes()).hexdigest()!=proof['log_sha256']):return False
                with log.open() as stream:header=json.loads(stream.readline())
                if (header.get('type')!='coral' or header.get('subtype')!='prompt'
                        or header.get('source')!='maestro:deepseek'
                        or header.get('agent_id')!=account[2]
                        or header.get('prompt')!=scope['prompt']):return False
            else:return False
            if cleanup.get('local_process_group_reclaimed') is not True:return False
            if (supervisor.get('kind')!='systemd_cgroup_empty'
                    or supervisor.get('MainPID')!=0 or supervisor.get('ControlGroup')!=''
                    or supervisor.get('ActiveState') not in ('inactive','failed')
                    or supervisor.get('remaining_native_processes')!=0
                    or supervisor.get('running_rootless_containers')!=0):return False
        return True
    except (KeyError,TypeError,ValueError,OSError):return False


def _terminal_failed_generation(entry,path,audit):
    """Quarantine one terminal request whose native generation yielded no code.

    The old UNKNOWN scope and all charges remain unchanged. This only allows a
    different run to pass the shared restart audit after exact local and remote
    terminal evidence is bound to the frozen source episode.
    """
    if (len(audit['unresolved_scopes'])!=1 or audit['active_budgets']
            or any(r['state'] not in {'COMPLETED','REJECTED','NEVER_DISPATCHED'}
                   for r in audit['requests'])):
        return False
    proof=entry.get('terminal_generation_proof')
    if not isinstance(proof,dict):return False
    account=audit['unresolved_scopes'][0]['account']
    parts=account.split(':')
    if (len(parts)!=5 or parts[3]!='GENERATION' or not parts[4]
            or proof.get('account')!=account
            or audit['unresolved_scopes'][0]['status']!='UNKNOWN'):
        return False
    try:
        round_index=int(parts[1]);agent=parts[2];episode=parts[0]
        if round_index<1 or not agent.startswith('agent-'):return False
        files={}
        for role in ('scope','cleanup','log','supervisor'):
            file=Path(proof[role+'_path']).resolve(strict=True)
            if hashlib.sha256(file.read_bytes()).hexdigest()!=proof[role+'_sha256']:
                return False
            files[role]=file
        phase=files['scope'].parent
        if (files['scope']!=phase/'scope.json' or files['cleanup']!=phase/'cleanup.json'
                or files['log']!=phase/'public/logs'/f'{agent}.0.log'):
            return False
        scope=json.loads(files['scope'].read_text())
        cleanup=json.loads(files['cleanup'].read_text())
        supervisor=json.loads(files['supervisor'].read_text())
        if (not scope.get('prompt','').startswith('ONE INITIAL IMPLEMENTATION ONLY.')
                or any(cleanup.get(k) is not True for k in
                       ('local_process_group_reclaimed','tool_service_reclaimed'))):
            return False
        lines=files['log'].read_text().splitlines()
        if not lines:return False
        header=json.loads(lines[0])
        if (header.get('type')!='coral' or header.get('subtype')!='prompt'
                or header.get('source')!='maestro:deepseek'
                or header.get('agent_id')!=agent or header.get('prompt')!=scope['prompt']
                or '\n'.join(lines[1:]).strip()):
            return False
        if (supervisor.get('kind')!='systemd_cgroup_empty'
                or supervisor.get('MainPID')!=0 or supervisor.get('ControlGroup')!=''
                or supervisor.get('ActiveState') not in ('inactive','failed')
                or supervisor.get('remaining_native_processes')!=0
                or supervisor.get('running_rootless_containers')!=0):
            return False
        old_root=path.resolve().parent.parent
        experiment=old_root/'authority/ledger.sqlite'
        if (proof.get('experiment_ledger_path')!=str(experiment)
                or ledger_fingerprint(experiment)!=proof.get('experiment_ledger_fingerprint')):
            return False
        db=sqlite3.connect(experiment.as_uri()+'?mode=ro',uri=True)
        try:
            status=db.execute('SELECT status FROM rounds WHERE episode_id=? AND round_index=?',
                              (episode,round_index)).fetchone()
            checkpoint=db.execute('SELECT payload FROM checkpoints WHERE episode_id=? AND round_index=?',
                                  (episode,round_index-1)).fetchone()
            tickets=db.execute('SELECT count(*) FROM eval_tickets WHERE episode_id=? AND round_index=?',
                               (episode,round_index)).fetchone()[0]
            submissions=db.execute('SELECT count(*) FROM round_submissions WHERE episode_id=? AND round_index=?',
                                   (episode,round_index)).fetchone()[0]
            stopped=[json.loads(r[0]) for r in db.execute(
                "SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='IMPLEMENTATION_STOPPED'",
                (episode,round_index))]
        finally:db.close()
        if (status!=('IMPLEMENTATION_UNKNOWN',) or not checkpoint or tickets or submissions
                or not any(e.get('agent_id')==agent and e.get('error_type')=='InferenceRequestUnknown'
                           for e in stopped)):
            return False
        parent=json.loads(checkpoint[0])['states'][agent]['parent_hash']
        workspace=phase.parents[2]/'coral-workspaces'/episode/agent/'solver'
        from maestro.backends.coral.workspace import candidate_tree_hash
        if (proof.get('workspace_path')!=str(workspace)
                or proof.get('parent_hash')!=parent
                or candidate_tree_hash(workspace)!=parent):
            return False
        authority=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)
        authority.row_factory=sqlite3.Row
        try:
            stored_cleanup=authority.execute('SELECT cleanup_json FROM deepseek_phase_cleanup '
                                             'WHERE account=?',(account,)).fetchone()
            if not stored_cleanup:return False
            stored_cleanup=json.loads(stored_cleanup[0])
            if (stored_cleanup.get('phase_path')!=str(phase)
                    or stored_cleanup.get('proxy_reclaimed') is not True
                    or any(stored_cleanup.get(k)!=v for k,v in cleanup.items())):
                return False
            logical=authority.execute('SELECT id,status,final_reservation FROM deepseek_logical_requests '
                                      'WHERE account=?',(account,)).fetchall()
            if len(logical)!=1 or logical[0]['status']!='COMPLETED':return False
            reservation=logical[0]['final_reservation']
            if entry.get('reservation_ids')!=[reservation]:return False
            physical=authority.execute('''SELECT r.status,r.output_tokens AS ceiling,
                r.actual_output_tokens,p.output_tokens,p.usage_complete,p.provider_request_id
                FROM inference_reservations r JOIN provider_receipts p USING(reservation_id)
                WHERE r.reservation_id=?''',(reservation,)).fetchone()
            if (physical is None or physical['status']!='COMPLETED'
                    or physical['usage_complete']!=1 or not physical['provider_request_id']
                    or physical['output_tokens']!=physical['actual_output_tokens']
                    or not 0<=physical['output_tokens']<=physical['ceiling']):return False
            attempts=authority.execute('SELECT status FROM deepseek_request_attempts WHERE logical_id=?',
                                       (logical[0]['id'],)).fetchall()
            if len(attempts)!=1 or attempts[0]['status']!='COMPLETED':return False
            events=authority.execute('SELECT kind,monotonic FROM deepseek_request_events '
                                     'WHERE reservation_id=? ORDER BY id',(reservation,)).fetchall()
        finally:authority.close()
        terminal=[e['monotonic'] for e in events if e['kind']=='TERMINAL']
        closed=[e['monotonic'] for e in events if e['kind']=='TRANSPORT_CLOSED']
        return (len(terminal)==1 and bool(closed)
                and cleanup['cleanup_finished_monotonic']<terminal[0]<=closed[-1]
                and terminal[0]<=cleanup['phase_deadline_monotonic'])
    except (KeyError,TypeError,ValueError,OSError,sqlite3.Error):
        return False


def _historical_exception(root,path,audit):
    """An explicit operator artifact may quarantine an exact historical snapshot.

    Never changes its ledger, and can never exempt the current run's requests.
    Candidate tools cannot access authority/. This is not reconciliation.
    """
    if path.resolve()==root/'deepseek-native-authority'/'requests.sqlite':return False
    manifest=root/'authority'/'deepseek-historical-exceptions.json'
    if not manifest.exists():return False
    value=json.loads(manifest.read_text())
    if (value.get('new_run_root')!=str(root) or not value.get('user_authorization')
            or value.get('remote_termination_confirmed') is not False
            or value.get('retain_historical_budget') is not True):
        raise RuntimeError('Invalid historical DeepSeek exception authorization')
    entries=[e for e in value.get('entries',[]) if e.get('ledger')==str(path.resolve())]
    if len(entries)!=1:return False
    entry=entries[0]
    if entry.get('kind')=='terminal_failed_generation':
        return (entry.get('ledger_fingerprint')==ledger_fingerprint(path)
                and _terminal_failed_generation(entry,path,audit))
    pending=[r for r in audit['requests'] if r['state'] not in {'COMPLETED','REJECTED','NEVER_DISPATCHED'}]
    allowed=set(entry.get('reservation_ids',[]))
    accounts={r['account'] for r in pending}
    scopes_match=(all(s['account'] in accounts for s in audit['unresolved_scopes'])
        if 'reclaimed_scopes' not in entry else _reclaimed_historical_scopes(entry,audit))
    matches=(bool(allowed) and {r['reservation_id'] for r in pending}==allowed
        and scopes_match
        and all(b['account'] in accounts for b in audit['active_budgets'])
        and entry.get('ledger_fingerprint')==ledger_fingerprint(path))
    if matches and 'snapshot_of' in entry:
        return _snapshot_terminal_proof(path,pending,entry)
    return matches


def historical_artifacts_root(run_root):
    """Find the workspace artifacts from execution location, not frozen source."""
    root=Path(run_root).resolve()
    ancestors=(root,*root.parents)
    artifact=next((p for p in ancestors if p.name=='artifacts'),None)
    if artifact is None:
        runtimes=[p for p in ancestors if p.name=='runtime']
        artifact=runtimes[-1].parent/'artifacts' if runtimes else root.parent/'artifacts'
    if not artifact.is_dir():
        raise RuntimeError('Cannot identify the shared artifacts root for DeepSeek restart audit')
    return artifact


def check_restart_ledgers(root, *, artifacts_root=None, allow_recovered_transport=False):
    """Check this run and sibling artifact runs; a new directory is no bypass.

    Includes qualification roots that nest their runtime one directory deeper.
    No global home scanning and no changes to historical ledgers.
    """
    root = Path(root).resolve()
    paths = {root / 'deepseek-native-authority' / 'requests.sqlite'}
    parents={root.parent}
    if artifacts_root is not None:
        base=Path(artifacts_root).resolve()
        parents.add(base)
        if base.exists():
            paths.update(p for p in base.rglob('deepseek-native-authority/requests.sqlite')
                if not any(part.startswith('export-staging-') for part in p.parts))
        local=base.parent/'runtime'
        if local.exists():
            paths.update(p for p in local.rglob('deepseek-native-authority/requests.sqlite')
                if not any(part.startswith('export-staging-') for part in p.parts))
        if (base/'runs').exists():parents.add(base/'runs')
    for parent in parents:
        for pattern in ('*/deepseek-native-authority/requests.sqlite',
                        '*/*/deepseek-native-authority/requests.sqlite'):
            paths.update(parent.glob(pattern))
    for path in sorted(paths):
        if not path.exists():continue
        audit=audit_ledger(path)
        if allow_recovered_transport and path.resolve()==root/'deepseek-native-authority'/'requests.sqlite':
            from .deepseek_retry import audit_retry_continuation
            if audit_retry_continuation(path)['status']=='CONTINUABLE_WITH_UNKNOWN_COST':continue
        if audit['status'] != 'CLEAR' and not _historical_exception(root,path,audit):
            raise RuntimeError(f'DeepSeek restart blocked by unresolved evidence: {path}; '
                               'use deepseek-audit, do not reset quotas or change directories')
