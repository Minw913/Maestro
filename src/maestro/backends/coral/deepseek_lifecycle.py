"""Append-only transport evidence and read-only restart audit. No credentials/body text."""
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import sqlite3
import time

from maestro.execution.budgets import BudgetStore


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
                   'exception_class', 'reason'}
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
    pending=[r for r in audit['requests'] if r['state'] not in {'COMPLETED','REJECTED','NEVER_DISPATCHED'}]
    allowed=set(entry.get('reservation_ids',[]))
    accounts={r['account'] for r in pending}
    return (bool(allowed) and {r['reservation_id'] for r in pending}==allowed
        and all(s['account'] in accounts for s in audit['unresolved_scopes'])
        and all(b['account'] in accounts for b in audit['active_budgets'])
        and entry.get('ledger_fingerprint')==ledger_fingerprint(path))


def check_restart_ledgers(root, *, artifacts_root=None):
    """Check this run and sibling artifact runs; a new directory is no bypass.

    Includes qualification roots that nest their runtime one directory deeper.
    No global home scanning and no changes to historical ledgers.
    """
    root = Path(root).resolve()
    paths = {root / 'deepseek-native-authority' / 'requests.sqlite'}
    parents={root.parent}
    if artifacts_root is not None:parents.add(Path(artifacts_root).resolve())
    for parent in parents:
        for pattern in ('*/deepseek-native-authority/requests.sqlite',
                        '*/*/deepseek-native-authority/requests.sqlite'):
            paths.update(parent.glob(pattern))
    for path in sorted(paths):
        if not path.exists():continue
        audit=audit_ledger(path)
        if audit['status'] != 'CLEAR' and not _historical_exception(root,path,audit):
            raise RuntimeError(f'DeepSeek restart blocked by unresolved evidence: {path}; '
                               'use deepseek-audit, do not reset quotas or change directories')
