"""Opt-in, durable network recovery. Unknown attempts are never reconciled.

A phase's logical requests and actual upstream attempts are separate counters.
Every attempt uses the existing physical request gate and its own reservation.
Only a complete retry can authorize continued local execution after a lost stream.
"""
from copy import deepcopy
from dataclasses import dataclass
import json
import math
import threading
import time
import uuid

from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.llm import InferenceRequestUnknown
from .deepseek_gate import DeepSeekQuotaProxy, DeepSeekHTTPError, DeepSeekStreamIncomplete, DEEPSEEK_TOOL


@dataclass(frozen=True)
class RetryPolicy:
    max_retries_per_request: int = 1
    max_extra_requests: int = 10
    backoff_seconds: float = 10.
    continue_after_network_failure: bool = False
    warm_seed_fallback: bool = False

    def __post_init__(self):
        if type(self.continue_after_network_failure) is not bool or type(self.warm_seed_fallback) is not bool:
            raise ValueError('Failure continuation and warm fallback must be booleans')
        if self.warm_seed_fallback and not self.continue_after_network_failure:
            raise ValueError('Warm seed fallback requires explicit failure continuation')
        for name in ('max_retries_per_request','max_extra_requests'):
            if type(getattr(self,name)) is not int or not 1<=getattr(self,name)<=100:
                raise ValueError('Retry counts must be finite positive integers <=100')
        if (isinstance(self.backoff_seconds,bool) or not isinstance(self.backoff_seconds,(int,float))
                or not math.isfinite(self.backoff_seconds) or not 0<self.backoff_seconds<=300):
            raise ValueError('Retry backoff must be finite, positive and <=300 seconds')

    @classmethod
    def parse(cls,value):
        if value is None:return None
        if not isinstance(value,dict) or set(value)-{'max_retries_per_request','max_extra_requests','backoff_seconds',
                'continue_after_network_failure','warm_seed_fallback'}:
            raise ValueError('Invalid DeepSeek transport retry policy')
        return cls(**value)


def retryable(error):
    # Do not retry invalid JSON/schema, empty completed answers, code errors,
    # protocol/permission failures, credentials, balance, or arbitrary 4xx.
    return (isinstance(error,(ConnectionError,TimeoutError,DeepSeekStreamIncomplete))
            or isinstance(error,DeepSeekHTTPError) and error.http_status in {429,500,502,503,504})


class DeepSeekRetryStore(BudgetStore):
    def extra_request_limit(self):
        """Effective operator cap; the original policy and all charges stay frozen."""
        import hashlib
        with self._lock:
            row=self.connection.execute('SELECT payload FROM deepseek_retry_policy WHERE id=1').fetchone()
            if row is None:raise BudgetExceeded('No frozen transport retry policy')
            base=row[0];limit=RetryPolicy.parse(json.loads(base)).max_extra_requests
            exists=self.connection.execute("SELECT 1 FROM sqlite_master WHERE name='deepseek_retry_limit_extensions'").fetchone()
            if not exists:return limit
            used=self.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]
            for entry in self.connection.execute('SELECT * FROM deepseek_retry_limit_extensions ORDER BY id'):
                if (entry['previous_limit']!=limit
                        or not limit<entry['new_limit']<=100
                        or not 0<=entry['requests_used']<=used
                        or not isinstance(entry['authorization'],str) or not entry['authorization'].strip()
                        or entry['base_policy_sha256']!=hashlib.sha256(base.encode()).hexdigest()):
                    raise BudgetExceeded('Invalid operator retry-limit extension chain')
                limit=entry['new_limit']
            return limit

    def operator_extend_retry_limit(self,new_limit,*,expected_used,authorization):
        """Append an explicitly authorized increase while requests are quiescent.

        This operator method is not exposed to agents. It never updates the base
        policy, existing reservations, recovery proofs, receipts or phase quotas.
        The caller must also hold the formal-controller lock and verify execution
        reclamation; the request audit here cannot establish Docker quiescence.
        """
        from datetime import datetime,timezone
        import hashlib
        if (type(new_limit) is not int or not 1<=new_limit<=100
                or type(expected_used) is not int or expected_used<0
                or not isinstance(authorization,str) or not authorization.strip()):
            raise ValueError('Explicit authorization and a finite increasing retry limit required')
        with self._lock:
            database=next(row[2] for row in self.connection.execute('PRAGMA database_list') if row[1]=='main')
        if not database:raise BudgetExceeded('Retry-limit extensions require a durable ledger')
        audit=audit_retry_continuation(database)
        if audit['status'] not in ('CLEAR','CONTINUABLE_WITH_UNKNOWN_COST'):
            raise BudgetExceeded('Cannot extend retry quota with unreclaimed requests or scopes')
        with self._transaction():
            limit=self.extra_request_limit()
            used=self.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]
            if used!=expected_used or new_limit<=limit:
                raise BudgetExceeded('Retry usage changed or limit did not increase; no reset')
            if (self.connection.execute("SELECT 1 FROM inference_reservations WHERE status='RESERVED'").fetchone()
                    or self.connection.execute("SELECT 1 FROM deepseek_logical_requests WHERE status='DISPATCHING'").fetchone()):
                raise BudgetExceeded('Cannot extend retry quota during active dispatch')
            tables={row[0] for row in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if ('codex_phase_scopes' in tables and self.connection.execute(
                    "SELECT 1 FROM codex_phase_scopes WHERE status IN ('DISPATCHING','UNKNOWN')").fetchone()):
                raise BudgetExceeded('Cannot extend retry quota with active native scopes')
            base=self.connection.execute('SELECT payload FROM deepseek_retry_policy WHERE id=1').fetchone()[0]
            self.connection.execute('''CREATE TABLE IF NOT EXISTS deepseek_retry_limit_extensions(
                id INTEGER PRIMARY KEY,previous_limit INTEGER NOT NULL,new_limit INTEGER NOT NULL,
                requests_used INTEGER NOT NULL,authorization TEXT NOT NULL,utc TEXT NOT NULL,
                base_policy_sha256 TEXT NOT NULL)''')
            for action in ('UPDATE','DELETE'):
                self.connection.execute(f'''CREATE TRIGGER IF NOT EXISTS retry_limit_no_{action.lower()}
                    BEFORE {action} ON deepseek_retry_limit_extensions BEGIN
                    SELECT RAISE(ABORT,'retry limit extensions are append-only'); END''')
            utc=datetime.now(timezone.utc).isoformat()
            self.connection.execute('''INSERT INTO deepseek_retry_limit_extensions(
                previous_limit,new_limit,requests_used,authorization,utc,base_policy_sha256)
                VALUES(?,?,?,?,?,?)''',(limit,new_limit,used,authorization,utc,hashlib.sha256(base.encode()).hexdigest()))
        return {'previous_limit':limit,'effective_max_extra_requests':new_limit,
                'extra_requests_used':used,'remaining_extra_requests':new_limit-used,
                'recorded_utc':utc,'prior_charges_retained':True}

    def enable_retries(self,policy):
        if policy is None:self.retry_policy=None;return
        frozen=json.dumps(policy.__dict__,sort_keys=True)
        with self._transaction():
            schema='''
            CREATE TABLE IF NOT EXISTS deepseek_retry_policy(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS deepseek_logical_phases(
              account TEXT PRIMARY KEY,max_calls INTEGER NOT NULL,max_bytes INTEGER NOT NULL,
              max_tokens INTEGER NOT NULL,deadline REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS deepseek_logical_requests(
              id TEXT PRIMARY KEY,account TEXT NOT NULL,status TEXT NOT NULL,
              payload_hash TEXT NOT NULL,final_reservation TEXT);
            CREATE TABLE IF NOT EXISTS deepseek_request_attempts(
              logical_id TEXT NOT NULL,attempt_index INTEGER NOT NULL,account TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL,PRIMARY KEY(logical_id,attempt_index));
            CREATE TABLE IF NOT EXISTS deepseek_recovered_attempts(
              reservation_id TEXT PRIMARY KEY,final_reservation TEXT NOT NULL,
              policy TEXT NOT NULL,recorded_utc REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS deepseek_retry_events(
              id INTEGER PRIMARY KEY,logical_id TEXT NOT NULL,attempt_index INTEGER NOT NULL,
              kind TEXT NOT NULL,utc TEXT NOT NULL,monotonic REAL NOT NULL,data_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS deepseek_abandoned_scopes(
              account TEXT PRIMARY KEY,policy TEXT NOT NULL,cleanup_json TEXT NOT NULL,recorded_utc REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS deepseek_phase_cleanup(account TEXT PRIMARY KEY,cleanup_json TEXT NOT NULL);
            '''
            for statement in schema.split(';'):
                if statement.strip():self.connection.execute(statement)
            row=self.connection.execute('SELECT payload FROM deepseek_retry_policy WHERE id=1').fetchone()
            if row is None:self.connection.execute('INSERT INTO deepseek_retry_policy VALUES(1,?)',(frozen,))
            elif row[0]!=frozen:raise BudgetExceeded('Frozen retry policy cannot change or reset')
        self.retry_policy=policy

    def retry_event(self,logical,index,kind,**data):
        from datetime import datetime,timezone
        with self._transaction():
            self.connection.execute('INSERT INTO deepseek_retry_events(logical_id,attempt_index,kind,utc,monotonic,data_json) VALUES(?,?,?,?,?,?)',
                (logical,index,kind,datetime.now(timezone.utc).isoformat(),time.monotonic(),json.dumps(data,sort_keys=True)))

    def register_phase(self,account,calls,byte_limit,tokens,deadline):
        values=(account,calls,byte_limit,tokens,deadline)
        with self._transaction():
            row=self.connection.execute('SELECT * FROM deepseek_logical_phases WHERE account=?',(account,)).fetchone()
            if row is None:self.connection.execute('INSERT INTO deepseek_logical_phases VALUES(?,?,?,?,?)',values)
            elif tuple(row)!=values:raise BudgetExceeded('Logical phase already exists; no reset')

    def is_logical(self,account):
        with self._lock:
            exists=self.connection.execute("SELECT 1 FROM sqlite_master WHERE name='deepseek_logical_phases'").fetchone()
            return bool(exists and self.connection.execute('SELECT 1 FROM deepseek_logical_phases WHERE account=?',(account,)).fetchone())

    def attempts(self,account):
        with self._lock:
            return [dict(r) for r in self.connection.execute('''SELECT a.*,l.account AS root_account
                FROM deepseek_request_attempts a JOIN deepseek_logical_requests l ON l.id=a.logical_id
                WHERE l.account=? ORDER BY l.rowid,a.attempt_index''',(account,))]

    def usage(self,account):
        if not self.is_logical(account):return super().usage(account)
        records=self.attempts(account);parts=[super(DeepSeekRetryStore,self).usage(r['account']) for r in records]
        with self._lock:
            logical=self.connection.execute('SELECT count(*) FROM deepseek_logical_requests WHERE account=?',(account,)).fetchone()[0]
            phase=self.connection.execute('SELECT max_tokens FROM deepseek_logical_phases WHERE account=?',(account,)).fetchone()
        complete=all(x['usage_complete'] for x in parts)
        result={key:sum(x.get(key,0) for x in parts) for key in ['calls','provider_receipts','known_input_tokens',
            'known_output_tokens','cache_read_input_tokens','cache_creation_input_tokens','reserved_output_tokens',
            'charged_output_tokens','unknown_requests','unresolved_requests']}
        result.update(logical_calls=logical,retry_attempts=sum(r['attempt_index']>0 for r in records),
            usage_complete=complete,input_tokens=result['known_input_tokens'] if complete else None,
            output_tokens=result['known_output_tokens'] if complete else None,max_output_tokens=phase[0],
            continuation_blocking_requests=sum(x['unresolved_requests'] for r,x in zip(records,parts)
                if not self.continuation_authorized(r['account'])))
        return result

    def continuation_authorized(self,attempt_account):
        """Read-only proof of accepted replacement, never remote termination."""
        with self._lock:
            rows=self.connection.execute('SELECT * FROM inference_reservations WHERE account=? AND status!=\'COMPLETED\'',(attempt_account,)).fetchall()
            if not rows:return False
            for row in rows:
                proof=self.connection.execute('''SELECT f.*,p.usage_complete FROM deepseek_recovered_attempts q
                    JOIN inference_reservations f ON f.reservation_id=q.final_reservation
                    JOIN provider_receipts p ON p.reservation_id=f.reservation_id
                    JOIN deepseek_request_attempts a ON a.account=f.account
                    JOIN deepseek_request_attempts old ON old.account=? AND old.logical_id=a.logical_id
                    JOIN deepseek_logical_requests l ON l.id=a.logical_id AND l.final_reservation=f.reservation_id
                    JOIN deepseek_retry_policy policy ON policy.id=1 AND policy.payload=q.policy
                    WHERE q.reservation_id=? AND l.status='COMPLETED' AND p.request_hash=l.payload_hash''',
                    (attempt_account,row['reservation_id'])).fetchone()
                closed=self.connection.execute("SELECT 1 FROM deepseek_request_events WHERE reservation_id=? AND kind='TRANSPORT_CLOSED'",(row['reservation_id'],)).fetchone()
                replaced=bool(proof and proof['status']=='COMPLETED' and proof['usage_complete']==1)
                if row['status']!='UNKNOWN' or not closed or not (replaced or self.abandonment_authorized(attempt_account)):return False
            return True

    def abandonment_authorized(self,attempt_account):
        root=self.root_for_attempt(attempt_account)
        with self._lock:
            row=self.connection.execute('''SELECT a.cleanup_json,p.payload FROM deepseek_abandoned_scopes a
                JOIN deepseek_retry_policy p ON p.id=1 AND p.payload=a.policy WHERE a.account=?''',(root,)).fetchone()
            if not row:return False
            policy=RetryPolicy.parse(json.loads(row['payload']));proof=json.loads(row['cleanup_json'])
            return (policy.continue_after_network_failure and proof.get('local_process_group_reclaimed') is True
                and proof.get('tool_service_reclaimed') is True and proof.get('proxy_reclaimed') is True)

    def abandon_phase(self,account,proof):
        """Quarantine a FAILED local phase. Unknown remote costs remain active."""
        if not self.retry_policy.continue_after_network_failure:return False
        if not all(proof.get(k) is True for k in ('local_process_group_reclaimed','tool_service_reclaimed','proxy_reclaimed')):return False
        with self._lock:
            logical=self.connection.execute("SELECT id FROM deepseek_logical_requests WHERE account=? AND status='FAILED'",(account,)).fetchall()
            if not logical:return False
            attempts=self.attempts(account)
            for attempt in attempts:
                reservations=self.connection.execute('SELECT reservation_id,status FROM inference_reservations WHERE account=?',(attempt['account'],)).fetchall()
                for row in reservations:
                    if row['status']=='RESERVED':return False
                    if not self.connection.execute("SELECT 1 FROM deepseek_request_events WHERE reservation_id=? AND kind='TRANSPORT_CLOSED'",(row['reservation_id'],)).fetchone():return False
            errors=[json.loads(row[0]).get('exception_class') for row in self.connection.execute('''
                SELECT e.data_json FROM deepseek_retry_events e JOIN deepseek_logical_requests l ON l.id=e.logical_id
                WHERE l.account=? AND e.kind='ATTEMPT_FAILED' ''',(account,))]
            network={'ConnectionResetError','ConnectionAbortedError','ConnectionRefusedError','BrokenPipeError',
                     'TimeoutError','DeepSeekStreamIncomplete','DeepSeekHTTPError'}
            if not any(e in network for e in errors):return False
            for attempt in attempts:
                codes=self.connection.execute('''SELECT json_extract(e.data_json,'$.http_status')
                    FROM deepseek_request_events e JOIN inference_reservations r USING(reservation_id)
                    WHERE r.account=? AND e.kind='HEADERS' ''',(attempt['account'],)).fetchall()
                if any(row[0] not in (200,429,500,502,503,504) for row in codes):return False
        with self._transaction():
            self.connection.execute('INSERT OR IGNORE INTO deepseek_abandoned_scopes VALUES(?,?,?,?)',
                (account,json.dumps(self.retry_policy.__dict__,sort_keys=True),json.dumps(proof,sort_keys=True),time.time()))
            self.connection.execute("UPDATE codex_phase_scopes SET status='FAILED_LOCAL_RECLAIMED' WHERE account=?",(account,))
        return True

    def root_for_attempt(self,account):
        with self._lock:
            row=self.connection.execute('''SELECT l.account FROM deepseek_request_attempts a
                JOIN deepseek_logical_requests l ON l.id=a.logical_id WHERE a.account=?''',(account,)).fetchone()
        return row[0] if row else None

    def replacement_complete(self,account):
        with self._lock:
            return bool(self.connection.execute('''SELECT 1 FROM deepseek_request_attempts a
                JOIN deepseek_logical_requests l ON l.id=a.logical_id
                JOIN provider_receipts p ON p.reservation_id=l.final_reservation
                JOIN inference_reservations r ON r.reservation_id=p.reservation_id
                WHERE a.account=? AND a.status='FAILED' AND l.status='COMPLETED'
                  AND p.usage_complete=1 AND r.status='COMPLETED' ''',(account,)).fetchone())


class RetryingDeepSeekProxy(DeepSeekQuotaProxy):
    def __init__(self,*args,retry_policy,**kwargs):
        super().__init__(*args,**kwargs)
        self.policy=retry_policy;self.store.enable_retries(retry_policy)
        self.store.register_phase(self.account,self.store.snapshot(self.account)['max_calls'],
            self.max_bytes,self.token_budget,self.store.snapshot(self.account)['deadline'])
        self._logical_lock=threading.Lock()
        self._attempt_proxy=None

    def complete(self,value):
        if not self._logical_lock.acquire(blocking=False):raise BudgetExceeded('One logical request per phase at a time')
        with self._condition:self._pending+=1
        try:return self._complete_logical(value)
        finally:
            with self._condition:self._pending-=1;self._condition.notify_all()
            self._logical_lock.release()

    def _check_deadline(self):
        if self._closed or time.monotonic()>=self.deadline:raise TimeoutError('Retry common deadline elapsed')
        if self.guard:self.guard()

    def _complete_logical(self,value):
        import hashlib
        self._check_deadline();usage=self.store.usage(self.account)
        maximum=self.store.snapshot(self.account)['max_calls']
        if maximum and usage['logical_calls']>=maximum:raise BudgetExceeded('Logical request quota exhausted')
        final=self.final_response_on_last_request and maximum>0 and usage['logical_calls']==maximum-1
        payload=self._payload(value,final_response=final)
        remaining=self.token_budget-usage['known_output_tokens']
        if remaining<=0:raise BudgetExceeded('Confirmed phase output token quota exhausted')
        payload['max_tokens']=min(payload['max_tokens'],remaining)
        wire=json.dumps(payload,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        logical=uuid.uuid4().hex
        with self.store._transaction():
            self.store.connection.execute('INSERT INTO deepseek_logical_requests VALUES(?,?,\'DISPATCHING\',?,NULL)',
                (logical,self.account,hashlib.sha256(wire).hexdigest()))
        attempts=[]
        try:
            for index in range(self.policy.max_retries_per_request+1):
                if index:
                    with self.store._lock:
                        used=self.store.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]
                    if used>=self.store.extra_request_limit():raise BudgetExceeded('Experiment-wide transport retry quota exhausted')
                    until=time.monotonic()+self.policy.backoff_seconds
                    if until>=self.deadline:raise TimeoutError('No time for retry backoff within common deadline')
                    self.store.retry_event(logical,index,'RETRY_WAIT_STARTED',backoff_seconds=self.policy.backoff_seconds)
                    until=time.monotonic()+self.policy.backoff_seconds
                    if until>=self.deadline:raise TimeoutError('No time for retry backoff within common deadline')
                    while time.monotonic()<until:
                        self._check_deadline();time.sleep(min(.05,max(0,until-time.monotonic())))
                    self.store.retry_event(logical,index,'RETRY_WAIT_FINISHED')
                self._check_deadline()
                account=self.account+':TRANSPORT_ATTEMPT:'+logical+':'+str(index)
                with self.store._transaction():
                    extras=self.store.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]
                    if index and extras>=self.store.extra_request_limit():raise BudgetExceeded('Experiment-wide transport retry quota exhausted')
                    byte_usage=self.store.connection.execute('''SELECT COALESCE(SUM(r.request_bytes),0)
                        FROM inference_reservations r JOIN deepseek_request_attempts a ON a.account=r.account
                        JOIN deepseek_logical_requests l ON l.id=a.logical_id WHERE l.account=?''',(self.account,)).fetchone()[0]
                    if byte_usage+len(wire)>self.max_bytes:raise BudgetExceeded('Phase request byte quota exhausted including retries')
                    self.store.connection.execute('INSERT INTO deepseek_request_attempts VALUES(?,?,?,\'CLAIMED\')',(logical,index,account))
                self.store.retry_event(logical,index,'ATTEMPT_CLAIMED',extra_attempt=index>0)
                class FrozenAttempt(DeepSeekQuotaProxy):
                    def _payload(inner,value,**options):return deepcopy(payload)
                attempt=FrozenAttempt(self.store,account,self.model,deadline=self.deadline,
                    max_calls=1,max_request_bytes=self.max_bytes,max_output_tokens=payload['max_tokens'],
                    per_request_tokens=payload['max_tokens'],reasoning_effort=self.effort,
                    allowed_tool_names=(DEEPSEEK_TOOL,) if self.tools and not final else (),
                    upstream=self.upstream,request_guard=self.guard)
                self._attempt_proxy=attempt;attempts.append(account)
                try:
                    result=attempt.complete(payload)
                    with self.store._transaction():
                        rid=self.store.connection.execute('SELECT reservation_id FROM inference_reservations WHERE account=?',(account,)).fetchone()[0]
                        self.store.connection.execute("UPDATE deepseek_logical_requests SET status='COMPLETED',final_reservation=? WHERE id=?",(rid,logical))
                        for previous in attempts[:-1]:
                            lost=self.store.connection.execute("SELECT reservation_id FROM inference_reservations WHERE account=? AND status='UNKNOWN'",(previous,)).fetchone()
                            if lost:self.store.connection.execute('INSERT INTO deepseek_recovered_attempts VALUES(?,?,?,?)',
                                (lost[0],rid,json.dumps(self.policy.__dict__,sort_keys=True),time.time()))
                        self.store.connection.execute("UPDATE deepseek_request_attempts SET status='COMPLETED' WHERE logical_id=? AND attempt_index=?",(logical,index))
                    self.store.retry_event(logical,index,'LOGICAL_REQUEST_COMPLETED')
                    return result
                except Exception as error:
                    attempt_usage=attempt.usage()
                    self._violations+=attempt_usage['protocol_violations']
                    with self.store._transaction():self.store.connection.execute("UPDATE deepseek_request_attempts SET status='FAILED' WHERE logical_id=? AND attempt_index=?",(logical,index))
                    self.store.retry_event(logical,index,'ATTEMPT_FAILED',exception_class=type(error).__name__)
                    if attempt_usage['provider_receipts'] or not retryable(error) or index>=self.policy.max_retries_per_request:raise
                finally:
                    attempt.close();self._attempt_proxy=None
        except BaseException:
            self._failures+=1
            with self.store._transaction():self.store.connection.execute("UPDATE deepseek_logical_requests SET status='FAILED' WHERE id=?",(logical,))
            raise

    def usage(self):
        return {**self.store.usage(self.account),'protocol_violations':self._violations,
                'terminal_failures':self._failures,'measurement':'deepseek_messages_gate_with_explicit_retry',
                'token_ceiling_enforced':True}

    def assert_resolved(self):
        if self.usage()['continuation_blocking_requests']:
            raise InferenceRequestUnknown('Unrecovered DeepSeek attempt remains unresolved',request_id=self.account,scope_id=self.account,context={})


def audit_retry_continuation(path):
    """Read-only operational proof; cost audit remains BLOCKED for UNKNOWN.

    This does not authorize another experiment or waive unreclaimed processes.
    Callers must independently check their controller/jobs/container reclamation.
    """
    import sqlite3
    import threading
    from pathlib import Path
    from .deepseek_lifecycle import audit_ledger
    audit=audit_ledger(path)
    result={'cost_audit':audit,'status':'BLOCKED','cost_usage_complete':audit['status']=='CLEAR',
            'remote_unknown_terminated':False,'read_only':True}
    if audit['status']=='CLEAR':result['status']='CLEAR';return result
    if audit['unresolved_scopes']:return result
    store=object.__new__(DeepSeekRetryStore)
    store.connection=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)
    store.connection.row_factory=sqlite3.Row;store._lock=threading.RLock()
    try:
        policy_row=store.connection.execute('SELECT payload FROM deepseek_retry_policy WHERE id=1').fetchone()
        if not policy_row:return result
        policy=RetryPolicy.parse(json.loads(policy_row[0]))
        extra=store.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]
        limit=store.extra_request_limit()
        if extra>limit:return result
        pending=[r for r in audit['requests'] if r['state'] not in {'COMPLETED','NEVER_DISPATCHED','REJECTED'}]
        if (not pending or any(r['state']!='UNKNOWN' or not store.continuation_authorized(r['account']) for r in pending)
                or any(not store.continuation_authorized(b['account']) for b in audit['active_budgets'])):return result
        result.update(status='CONTINUABLE_WITH_UNKNOWN_COST',extra_requests=extra,
                      quarantined_unknown_requests=len(pending),policy=policy.__dict__,
                      effective_max_extra_requests=limit,remaining_extra_requests=limit-extra)
        return result
    except (sqlite3.Error,ValueError,BudgetExceeded):return result
    finally:store.connection.close()
