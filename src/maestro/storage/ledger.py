"""Single transactional authority for admission, execution and settlement."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import sqlite3
import threading

from maestro.types import (AuthorizationError, IntegrityError, MaestroError,
    canonical_json, stable_hash, to_jsonable, result_from_dict)

SCHEMA_VERSION = '2'


def validate_round_resources(config, snapshot, ticket=None):
    """Derive admission and worker ceilings from frozen authority records.

    Historical rounds without the explicit capacity field retain their original
    auction-slot limit. New rounds must carry the complete resource binding;
    callers cannot enlarge warm admission or per-candidate instance concurrency.
    This pure check is also used by accounting replay.
    """
    if 'admission_capacity' not in snapshot:
        return {'admission_capacity':config['slots']}

    def positive_int(value, name):
        if type(value) is not int or value < 1:
            raise AuthorizationError(f'Frozen {name} must be a positive integer')
        return value

    cpu_workers = positive_int(config.get('cpu_workers'), 'cpu_workers')
    instance_workers = positive_int(config.get('instance_workers'), 'instance_workers')
    evaluation_instance_count = positive_int(config.get('evaluation_instance_count'), 'evaluation_instance_count')
    purpose = snapshot.get('purpose')
    if purpose == 'MAIN':
        capacity = positive_int(config.get('slots'), 'slots')
    elif purpose == 'WARM':
        population = positive_int(config.get('n_agents'), 'n_agents')
        warm_concurrency = positive_int(config.get('warm_concurrency'), 'warm_concurrency')
        invited = snapshot.get('warm_invite')
        states = snapshot.get('states')
        if (not isinstance(invited,list) or not invited
                or any(not isinstance(agent,str) or not agent for agent in invited)
                or len(set(invited)) != len(invited)
                or not isinstance(states,dict) or any(agent not in states for agent in invited)):
            raise AuthorizationError('Warm admission requires a unique frozen invited population')
        capacity = len(invited)
        if capacity > min(warm_concurrency,population,cpu_workers):
            raise AuthorizationError('Warm wave exceeds its frozen concurrency budget')
        if ticket is not None and ticket.get('agent_id') not in invited:
            raise AuthorizationError('Warm ticket is outside this frozen opportunity wave')
    elif purpose in ('SETUP','HELDOUT'):
        capacity = 1
    else:
        raise AuthorizationError('Unknown frozen execution purpose')

    batch_width = min(capacity,cpu_workers)
    fair_ceiling = (cpu_workers + batch_width - 1) // batch_width
    expected = {'admission_capacity':capacity,'cpu_workers':cpu_workers,
        'instance_workers':instance_workers,'evaluation_batch_width':batch_width,
        'effective_instance_workers':min(instance_workers,evaluation_instance_count,fair_ceiling)}
    for name,value in expected.items():
        if type(snapshot.get(name)) is not int or snapshot[name] != value:
            raise AuthorizationError(f'Round {name} differs from the frozen resource budget')
        if ticket is not None and (type(ticket.get(name)) is not int or ticket[name] != value):
            raise AuthorizationError(f'Ticket {name} differs from the frozen round resource budget')
    if ticket is not None and ticket.get('purpose') != purpose:
        raise AuthorizationError('Ticket purpose does not match the frozen resource budget')
    return expected


class Ledger:
    def __init__(self, path, *, read_only=False):
        self.path = Path(path)
        if read_only:
            self.connection=sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True,check_same_thread=False)
            self.connection.row_factory=sqlite3.Row
            self._lock=threading.RLock()
            row=self.connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
            if row is None or row[0]!=SCHEMA_VERSION:
                self.connection.close()
                raise IntegrityError('Incompatible ledger schema; old experiments are not supported')
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(path), timeout=30, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        tables = {r[0] for r in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if tables:
            row = (self.connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
                   if 'metadata' in tables else None)
            if row is None or row[0] != SCHEMA_VERSION:
                self.connection.close()
                raise IntegrityError('Incompatible ledger schema; old experiments are not supported')
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.execute('PRAGMA busy_timeout=30000')
        self._lock = threading.RLock()
        self.connection.executescript(Path(__file__).with_name('schema.sql').read_text())
        row = self.connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
        if row and row[0] != SCHEMA_VERSION:
            raise IntegrityError('Incompatible ledger schema; explicit migration required')
        self.connection.execute("INSERT OR IGNORE INTO metadata VALUES('schema_version',?)", (SCHEMA_VERSION,))

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @contextmanager
    def transaction(self):
        with self._lock:
            if self.connection.in_transaction:
                yield self.connection
                return
            self.connection.execute('BEGIN IMMEDIATE')
            try:
                yield self.connection
            except BaseException:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()

    def event(self, kind, payload, episode_id=None, round_index=None):
        with self.transaction() as db:
            previous = db.execute('SELECT hash FROM events ORDER BY id DESC LIMIT 1').fetchone()
            previous = previous[0] if previous else '0' * 64
            text = canonical_json(payload)
            digest = stable_hash([previous, episode_id, round_index, kind, text])
            db.execute('INSERT INTO events(episode_id,round_index,kind,payload,previous_hash,hash) VALUES(?,?,?,?,?,?)',
                       (episode_id, round_index, kind, text, previous, digest))

    def create_episode(self, episode_id, policy, config, states, bootstrap_id=None):
        with self.transaction() as db:
            if db.execute('SELECT 1 FROM episodes WHERE id=?', (episode_id,)).fetchone():
                raise IntegrityError(f'Episode already exists: {episode_id}')
            db.execute('INSERT INTO episodes VALUES(?,?,?,?,?,?,?,?,?)',
                (episode_id,policy,config.execution_mode,config.benchmark,canonical_json(config.to_dict()),config.hash,bootstrap_id,'RUNNING',datetime.now(timezone.utc).isoformat()))
            for state in states.values():
                payload=json.loads(canonical_json(state))
                if policy=='warm':payload.pop('wealth',None)
                db.execute('INSERT INTO agents VALUES(?,?,?)', (episode_id,state.agent_id,canonical_json(payload)))
                if policy!='warm':
                    db.execute('INSERT INTO wallet_entries VALUES(?,?,?,?,?,?)',
                        (f'{episode_id}:initial:{state.agent_id}',episode_id,state.agent_id,'INITIAL',str(state.wealth),None))
            self.event('EPISODE_CREATED', {'policy':policy,'config_hash':config.hash,'bootstrap_id':bootstrap_id},episode_id)

    def balance(self, episode_id, agent_id):
        with self._lock:
            amounts=self.connection.execute('SELECT amount FROM wallet_entries WHERE episode_id=? AND agent_id=?',(episode_id,agent_id)).fetchall()
            return sum((Decimal(row[0]) for row in amounts),Decimal(0))

    def register_artifact(self,digest,size):
        with self.transaction() as db:
            db.execute('INSERT OR IGNORE INTO artifact_refs VALUES(?,?,?)',(digest,'blob',size))

    def open_round(self, episode_id, round_index, snapshot):
        text = canonical_json(snapshot)
        with self.transaction() as db:
            db.execute('INSERT INTO rounds VALUES(?,?,?,?,?)',(episode_id,round_index,'PLANNING',text,stable_hash(snapshot)))
            self.event('ROUND_OPENED', {'snapshot_hash':stable_hash(snapshot)},episode_id,round_index)

    def restart_reconciled_round(self, episode_id, round_index, snapshot, request_namespace):
        """Append a new prebid attempt without replacing its frozen round anchor."""
        with self.transaction() as db:
            row=db.execute('SELECT status,snapshot_hash FROM rounds WHERE episode_id=? AND round_index=?',
                (episode_id,round_index)).fetchone()
            if not row or row['status']!='RESTART_AUTHORIZED' or row['snapshot_hash']!=stable_hash(snapshot):
                raise IntegrityError('Recovery snapshot/status differs from the frozen round')
            db.execute("UPDATE rounds SET status='PLANNING' WHERE episode_id=? AND round_index=?",
                (episode_id,round_index))
            self.event('ROUND_RESTARTED',{'snapshot_hash':row['snapshot_hash'],
                'request_namespace':request_namespace},episode_id,round_index)

    def set_round_phase(self, episode_id, round_index, phase):
        with self.transaction() as db:
            current = db.execute('SELECT status FROM rounds WHERE episode_id=? AND round_index=?',(episode_id,round_index)).fetchone()
            if not current or current[0] == 'SETTLED':
                raise AuthorizationError('Cannot transition nonexistent/settled round')
            db.execute('UPDATE rounds SET status=? WHERE episode_id=? AND round_index=?',(phase,episode_id,round_index))
            self.event('ROUND_PHASE', {'phase':phase},episode_id,round_index)

    def submit(self, episode_id, round_index, plan, submission, metadata):
        with self.transaction() as db:
            if submission.agent_id!=plan.agent_id or submission.plan_version_id!=plan.version_id:
                raise AuthorizationError('Submission identity does not match the frozen plan')
            current = db.execute('SELECT status,snapshot_json FROM rounds WHERE episode_id=? AND round_index=?',(episode_id,round_index)).fetchone()
            prebid=bool(current and json.loads(current['snapshot_json']).get('candidate_pipeline')=='prebid_v2')
            if not current or current['status'] != ('BIDDING' if prebid else 'PLANNING'):
                raise AuthorizationError('Planning window closed')
            if prebid:
                frozen=[json.loads(row[0]) for row in db.execute(
                    "SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='PROPOSAL_FROZEN'",
                    (episode_id,round_index)) if json.loads(row[0])['agent_id']==plan.agent_id]
                if (len(frozen)!=1 or frozen[0]['plan_version_id']!=plan.version_id
                        or frozen[0]['parent_hash']!=plan.parent_hash
                        or metadata.get('proposal_hash')!=frozen[0]['candidate_hash']):
                    raise AuthorizationError('Bid must bind the previously frozen candidate')
            consumed=db.execute('SELECT 1 FROM round_submissions s JOIN final_allocations a ON a.episode_id=s.episode_id AND a.round_index=s.round_index AND a.agent_id=s.agent_id WHERE s.plan_version_id=? AND a.x=1 LIMIT 1',(plan.version_id,)).fetchone()
            if consumed:
                raise AuthorizationError('A formally consumed plan version cannot be resubmitted')
            if not all(Decimal(str(v)).is_finite() and Decimal(str(v))>=0 for v in (submission.value,submission.bid)):
                raise IntegrityError('Submission value/bid must be finite nonnegative credits')
            existing = db.execute('SELECT payload FROM plan_versions WHERE id=?',(plan.version_id,)).fetchone()
            text = canonical_json(plan)
            if existing and existing[0] != text:
                raise IntegrityError('Plan version identity reused with changed content')
            db.execute('INSERT OR IGNORE INTO plan_versions VALUES(?,?,?,?,?)',(plan.version_id,plan.agent_id,plan.parent_hash,text,stable_hash(plan)))
            payload = {**to_jsonable(submission),**metadata}
            db.execute('INSERT INTO round_submissions VALUES(?,?,?,?,?)',(episode_id,round_index,plan.agent_id,plan.version_id,canonical_json(payload)))
            for evidence in plan.evidence_run_ids:
                db.execute('INSERT OR IGNORE INTO evidence_links VALUES(?,?)',(plan.version_id,evidence))
            self.event('PLAN_SUBMITTED',payload,episode_id,round_index)

    def invite(self, episode_id, round_index, agent_id, amount, payload=None):
        with self.transaction() as db:
            amount=Decimal(str(amount))
            if not amount.is_finite() or amount<0:
                raise IntegrityError('Invalid hold amount')
            outstanding = sum((Decimal(r[0]) for r in db.execute("SELECT amount FROM holds WHERE episode_id=? AND agent_id=? AND status='HELD'",(episode_id,agent_id))),Decimal(0))
            if self.balance(episode_id,agent_id)-outstanding < amount:
                raise AuthorizationError('Insufficient available wealth')
            hold_id = f'{episode_id}:{round_index}:{agent_id}:hold'
            db.execute('INSERT INTO invitations VALUES(?,?,?,?,?)',(episode_id,round_index,agent_id,'INVITED',canonical_json(payload or {})))
            db.execute('INSERT INTO holds VALUES(?,?,?,?,?,?)',(hold_id,episode_id,round_index,agent_id,str(amount),'HELD'))
            self.event('INVITED',{'agent_id':agent_id,'hold':str(amount)},episode_id,round_index)
            return hold_id

    def release_hold(self, episode_id, round_index, agent_id, reason):
        with self.transaction() as db:
            db.execute("UPDATE holds SET status='RELEASED' WHERE episode_id=? AND round_index=? AND agent_id=? AND status='HELD'",(episode_id,round_index,agent_id))
            db.execute("UPDATE invitations SET status='FAILED' WHERE episode_id=? AND round_index=? AND agent_id=?",(episode_id,round_index,agent_id))
            self.event('HOLD_RELEASED',{'agent_id':agent_id,'reason':reason},episode_id,round_index)

    def record_candidate(self, candidate_hash, payload):
        with self.transaction() as db:
            text = canonical_json(payload)
            old = db.execute('SELECT payload FROM candidates WHERE hash=?',(candidate_hash,)).fetchone()
            if old and old[0] != text:
                raise IntegrityError('Candidate identity collision')
            db.execute('INSERT OR IGNORE INTO candidates VALUES(?,?)',(candidate_hash,text))

    def admit(self, ticket, payment, slots):
        """Atomic final allocation, own-bid debit and durable launch intent."""
        ticket = dict(ticket)
        required = ('ticket_id','job_id','episode_id','round_index','agent_id','candidate_hash','contract_hash','panel_hash','plan_version_id','phase_epoch','purpose')
        if any(k not in ticket for k in required):
            raise IntegrityError('Incomplete ticket binding')
        e,r,a = ticket['episode_id'],ticket['round_index'],ticket['agent_id']
        with self.transaction() as db:
            episode=db.execute('SELECT config_json FROM episodes WHERE id=?',(e,)).fetchone()
            if not episode:
                raise AuthorizationError('Unknown episode')
            config=json.loads(episode[0])
            roundrow=db.execute('SELECT status,snapshot_json FROM rounds WHERE episode_id=? AND round_index=?',(e,r)).fetchone()
            if not roundrow:
                raise AuthorizationError('Unknown admission round')
            snapshot=json.loads(roundrow['snapshot_json'])
            configured_slots=validate_round_resources(config,snapshot,ticket)['admission_capacity']
            if type(slots) is not int or slots!=configured_slots:
                raise AuthorizationError('Caller cannot alter the frozen admission capacity')
            original=db.execute('SELECT payload FROM eval_tickets WHERE id=?',(ticket['ticket_id'],)).fetchone()
            if original:
                allocation=db.execute('SELECT payment FROM final_allocations WHERE ticket_id=?',(ticket['ticket_id'],)).fetchone()
                if original[0]!=canonical_json(ticket) or not allocation or Decimal(allocation[0])!=Decimal(str(payment)):
                    raise IntegrityError('Conflicting retry of final admission')
                return
            if roundrow['status'] != 'ADMISSION':
                raise AuthorizationError('Admission is only allowed after generation barrier')
            count = db.execute('SELECT count(*) FROM final_allocations WHERE episode_id=? AND round_index=? AND x=1',(e,r)).fetchone()[0]
            if count >= configured_slots:
                raise AuthorizationError('Round slot cap exceeded')
            if snapshot.get('purpose')!=ticket['purpose']:
                raise AuthorizationError('Ticket purpose does not match the frozen round')
            for key in ('contract_hash','panel_hash'):
                if snapshot.get(key)!=ticket[key]:
                    raise AuthorizationError('Ticket task contract/panel mismatch')
            if ticket['purpose'] in ('MAIN','WARM'):
                submission=db.execute('SELECT * FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',(e,r,a)).fetchone()
                planrow=db.execute('SELECT payload FROM plan_versions WHERE id=?',(ticket['plan_version_id'],)).fetchone()
                if not submission or submission['plan_version_id']!=ticket['plan_version_id'] or not planrow:
                    raise AuthorizationError('Ticket is not bound to this round submission')
                plan=json.loads(planrow[0])
                parent=snapshot.get('states',{}).get(a)
                if not parent or plan['agent_id']!=a or plan['parent_hash']!=parent['parent_hash'] or ticket.get('parent_hash')!=parent['parent_hash']:
                    raise AuthorizationError('Ticket parent/agent binding mismatch')
                if ticket.get('parent_score')!=parent['parent_score'] or ticket.get('champion_score')!=snapshot['board']['champion_score']:
                    raise AuthorizationError('Ticket baseline mismatch')
                if ticket.get('target')!=plan['target'] or ticket.get('mod_type')!=plan['mod_type']:
                    raise AuthorizationError('Ticket modification labels mismatch')
                policy=db.execute('SELECT policy FROM episodes WHERE id=?',(e,)).fetchone()[0]
                expected_payment=Decimal(json.loads(submission['payload'])['bid']) if policy in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') and ticket['purpose']=='MAIN' else Decimal(0)
                if Decimal(str(payment))!=expected_payment:
                    raise AuthorizationError('Final admission must charge the frozen policy payment')
                precheck=db.execute('SELECT 1 FROM prechecks WHERE episode_id=? AND round_index=? AND agent_id=? AND candidate_hash=? AND passed=1',(e,r,a,ticket['candidate_hash'])).fetchone()
                if not precheck:
                    raise AuthorizationError('Candidate has not passed this invitation precheck')
                if snapshot.get('candidate_pipeline')=='prebid_v2':
                    from maestro.execution.proposals import validate_proposal_binding
                    validate_proposal_binding(db,ticket,json.loads(submission['payload']))
            elif ticket['purpose'] in ('SETUP','HELDOUT'):
                expected=snapshot.get('candidate_hash',snapshot.get('seed',{}).get('candidate_hash'))
                if expected!=ticket['candidate_hash']:
                    raise AuthorizationError('Setup/final candidate identity mismatch')
            else:
                raise AuthorizationError('Unknown execution purpose')
            candidate_row = db.execute('SELECT payload FROM candidates WHERE hash=?',(ticket['candidate_hash'],)).fetchone()
            if not candidate_row:
                raise IntegrityError('Unregistered immutable candidate')
            candidate = json.loads(candidate_row[0])
            if candidate.get('candidate_hash') != ticket['candidate_hash']:
                raise IntegrityError('Registered candidate identity mismatch')
            registered_path, ticket_path = candidate.get('candidate_path'), ticket.get('candidate_path')
            if (registered_path is None) != (ticket_path is None):
                raise AuthorizationError('Ticket candidate path differs from its immutable registration')
            if registered_path is not None:
                if Path(registered_path).resolve() != Path(ticket_path).resolve():
                    raise AuthorizationError('Ticket candidate path differs from its immutable registration')
            elif json.loads(episode[0]).get('execution_mode') == 'real':
                raise AuthorizationError('Formal PACE candidate has no registered immutable path')
            held = db.execute("SELECT amount FROM holds WHERE episode_id=? AND round_index=? AND agent_id=? AND status='HELD'",(e,r,a)).fetchone()
            if not held or Decimal(held[0]) != Decimal(str(payment)):
                raise AuthorizationError('Admission payment is not the frozen hold')
            if self.balance(e,a) < Decimal(str(payment)):
                raise AuthorizationError('Insufficient balance')
            db.execute('INSERT INTO final_allocations VALUES(?,?,?,?,?,?,?)',(e,r,a,ticket['ticket_id'],1,str(payment),'ADMITTED'))
            db.execute('INSERT INTO eval_tickets VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(ticket['ticket_id'],ticket['job_id'],e,r,a,ticket['candidate_hash'],ticket['contract_hash'],ticket['panel_hash'],ticket['phase_epoch'],'ADMITTED',0,canonical_json(ticket)))
            db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)',(ticket['job_id'],ticket['ticket_id'],'ADMITTED',0,None))
            db.execute('INSERT INTO dispatch_outbox VALUES(?,?)',(ticket['job_id'],'PENDING'))
            db.execute("UPDATE holds SET status='DEBITED' WHERE episode_id=? AND round_index=? AND agent_id=?",(e,r,a))
            if ticket['purpose']=='MAIN':
                db.execute('INSERT INTO wallet_entries VALUES(?,?,?,?,?,?)',(f"{ticket['job_id']}:payment",e,a,'PAYMENT',str(-Decimal(str(payment))),r))
            self.event('ADMITTED',{**ticket,'payment':str(payment)},e,r)

    def ticket(self, ticket_id):
        with self._lock:
            row = self.connection.execute('SELECT * FROM eval_tickets WHERE id=?',(ticket_id,)).fetchone()
        if row is None:
            raise AuthorizationError('Unknown evaluation ticket')
        payload = json.loads(row['payload'])
        for key in ('status','phase_epoch','fence','job_id','candidate_hash','contract_hash','panel_hash'):
            payload[key] = row[key]
        return payload

    def claim_ticket(self, ticket_id, job_id, expected_epoch=0):
        with self.transaction() as db:
            ticket = self.ticket(ticket_id)
            if ticket['job_id'] != job_id or ticket['phase_epoch'] != expected_epoch or ticket['status'] != 'ADMITTED':
                raise AuthorizationError('Ticket used, revoked, stale or bound to another job')
            phase = db.execute('SELECT status FROM rounds WHERE episode_id=? AND round_index=?',(ticket['episode_id'],ticket['round_index'])).fetchone()
            if not phase or phase[0] != 'EVALUATING':
                raise AuthorizationError('Formal evaluation phase is not open')
            fence = ticket['fence'] + 1
            db.execute("UPDATE eval_tickets SET status='CLAIMED',fence=? WHERE id=?",(fence,ticket_id))
            db.execute("UPDATE jobs SET status='CLAIMED',fence=? WHERE id=?",(fence,job_id))
            db.execute("UPDATE dispatch_outbox SET status='CLAIMED' WHERE job_id=?",(job_id,))
            self.event('CLAIMED',{'ticket_id':ticket_id,'job_id':job_id,'fence':fence},ticket['episode_id'],ticket['round_index'])
            return {**ticket,'status':'CLAIMED','fence':fence}

    def mark_running(self, job_id, fence, receipt):
        with self.transaction() as db:
            job = db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
            if not job or job['fence'] != fence or job['status'] not in ('CLAIMED', 'LAUNCHING'):
                raise AuthorizationError('Stale execution claim')
            if not isinstance(receipt, dict) or not receipt:
                raise AuthorizationError('Physical execution requires an executor receipt')
            db.execute("UPDATE jobs SET status='RUNNING',receipt=? WHERE id=?",(canonical_json(receipt),job_id))
            db.execute("UPDATE eval_tickets SET status='RUNNING' WHERE id=?",(job['ticket_id'],))

    def validate_execution_claim(self, claimed_ticket):
        with self.transaction() as db:
            current = self.ticket(claimed_ticket['ticket_id'])
            if (any(claimed_ticket.get(key) != value for key, value in current.items() if key != 'status')
                    or current['status'] not in ('CLAIMED', 'LAUNCHING', 'RUNNING')):
                raise AuthorizationError('Formal launch has no active execution claim')
            phase = db.execute('SELECT status FROM rounds WHERE episode_id=? AND round_index=?',
                               (current['episode_id'], current['round_index'])).fetchone()
            if not phase or phase[0] != 'EVALUATING':
                raise AuthorizationError('Formal evaluation phase is not open')

    def mark_launch_intent(self, job_id, fence):
        """Conservative dispatch boundary: an OS start may happen after commit.

        A crash after this write is ambiguous and cannot be refunded merely by
        asserting a boolean proof. No physical-start receipt is fabricated.
        """
        with self.transaction() as db:
            job = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not job or job['fence'] != fence or job['status'] not in ('CLAIMED','LAUNCHING','RUNNING'):
                raise AuthorizationError('Stale or inactive execution claim')
            self.validate_execution_claim(self.ticket(job['ticket_id']))
            if job['status'] in ('LAUNCHING','RUNNING'):
                return
            intent = {'launch_attempted': True, 'job_id': job_id, 'fence': fence}
            db.execute("UPDATE jobs SET status='LAUNCHING',receipt=? WHERE id=?", (canonical_json(intent),job_id))
            db.execute("UPDATE eval_tickets SET status='LAUNCHING' WHERE id=?", (job['ticket_id'],))
            self.event('EXECUTION_LAUNCH_INTENT',intent)

    def record_result(self, result, fence):
        with self.transaction() as db:
            old = db.execute('SELECT payload_hash,fence FROM verified_results WHERE job_id=?',(result.job_id,)).fetchone()
            digest = stable_hash(result)
            if old:
                if old[0] != digest or old['fence'] != fence:
                    raise IntegrityError('Conflicting duplicate result')
                return
            job = db.execute('SELECT * FROM jobs WHERE id=?',(result.job_id,)).fetchone()
            if not job or job['fence'] != fence or job['status'] not in ('CLAIMED','LAUNCHING','RUNNING'):
                raise AuthorizationError('Result from stale or unknown job')
            ticket = self.ticket(job['ticket_id'])
            if result.candidate_hash != ticket['candidate_hash']:
                raise IntegrityError('Result candidate binding mismatch')
            import math
            terminal={'COMPLETED','COMPLETED_WITH_TIMEOUTS','INVALID','CRASH','TIMEOUT','INFRA_FAILURE','FAILED'}
            if result.status not in terminal or (result.valid and result.status not in {'COMPLETED','COMPLETED_WITH_TIMEOUTS'}):
                raise IntegrityError('Nonterminal or contradictory evaluator status')
            if type(result.valid) is not bool or (not result.valid and result.score!=0):
                raise IntegrityError('Invalid results must have aggregate zero')
            if not 0 <= result.score <= 1 or not all(math.isfinite(n) and n>=0 for n in (result.cpu_seconds,result.wall_seconds)):
                raise IntegrityError('Invalid evaluator metrics')
            db.execute('INSERT INTO verified_results VALUES(?,?,?,?,?)',(result.job_id,result.candidate_hash,canonical_json(result),digest,fence))
            for index, record in enumerate(result.instance_results):
                db.execute('INSERT INTO instance_results VALUES(?,?,?)',(result.job_id,index,canonical_json(record)))
            db.execute("UPDATE jobs SET status='RESULT_RECORDED' WHERE id=?",(result.job_id,))
            db.execute("UPDATE eval_tickets SET status='RESULT_RECORDED' WHERE id=?",(job['ticket_id'],))
            db.execute("UPDATE dispatch_outbox SET status='DONE' WHERE job_id=?",(result.job_id,))
            self.event('RESULT_RECORDED',{'job_id':result.job_id,'result_hash':digest},ticket['episode_id'],ticket['round_index'])

    def existing_result(self, job_id):
        with self._lock:
            row = self.connection.execute('SELECT payload FROM verified_results WHERE job_id=?',(job_id,)).fetchone()
        return result_from_dict(json.loads(row[0])) if row else None

    def mark_unknown(self, job_id, reason):
        with self.transaction() as db:
            row = db.execute('SELECT ticket_id,status FROM jobs WHERE id=?',(job_id,)).fetchone()
            if not row:
                raise AuthorizationError('Unknown job')
            if row['status'] in ('RESULT_RECORDED','SETTLED','NOT_STARTED'):
                raise AuthorizationError('Cannot overwrite a terminal execution state')
            db.execute("UPDATE jobs SET status='UNKNOWN' WHERE id=?",(job_id,))
            db.execute("UPDATE eval_tickets SET status='UNKNOWN' WHERE id=?",(row[0],))
            self.event('EXECUTION_UNKNOWN',{'job_id':job_id,'reason':reason})

    def void_never_started(self, ticket_id, proof):
        if (not isinstance(proof, dict) or proof.get('confirmed_not_started') is not True
                or not isinstance(proof.get('executor_receipt'), str) or not proof['executor_receipt'].strip()):
            raise AuthorizationError('Executor proof required')
        with self.transaction() as db:
            t = self.ticket(ticket_id)
            if t['status'] == 'NOT_STARTED':
                return
            job = db.execute('SELECT status,receipt FROM jobs WHERE id=?', (t['job_id'],)).fetchone()
            if (t['status'] not in ('ADMITTED','CLAIMED','UNKNOWN') or not job
                    or job['receipt'] is not None):
                raise AuthorizationError('Cannot refund a started formal job')
            entry = db.execute('SELECT amount FROM wallet_entries WHERE operation_key=?',(t['job_id']+':payment',)).fetchone()
            if entry:
                db.execute('INSERT INTO wallet_entries VALUES(?,?,?,?,?,?)',(t['job_id']+':compensation',t['episode_id'],t['agent_id'],'COMPENSATION',str(-Decimal(entry[0])),t['round_index']))
            elif t['purpose']!='WARM':
                raise IntegrityError('Market admission payment entry missing')
            db.execute("UPDATE final_allocations SET x=0,payment='0',status='NOT_STARTED' WHERE ticket_id=?",(ticket_id,))
            db.execute("UPDATE eval_tickets SET status='NOT_STARTED' WHERE id=?",(ticket_id,))
            db.execute("UPDATE jobs SET status='NOT_STARTED' WHERE id=?",(t['job_id'],))
            db.execute("UPDATE dispatch_outbox SET status='DONE' WHERE job_id=?",(t['job_id'],))
            self.event('NOT_STARTED_COMPENSATED' if entry else 'NOT_STARTED_VOIDED',
                {'ticket_id':ticket_id,'proof':proof},t['episode_id'],t['round_index'])

    def usage(self, episode_id, round_index, agent_id, phase, cpu=0, wall=0, calls=0, tokens=0, payload=None):
        with self.transaction() as db:
            db.execute('INSERT INTO resource_usage(episode_id,round_index,agent_id,phase,cpu_seconds,wall_seconds,llm_calls,tokens,payload) VALUES(?,?,?,?,?,?,?,?,?)',
                (episode_id,round_index,agent_id,phase,cpu,wall,calls,tokens,canonical_json(payload or {})))

    def verify_event_chain(self):
        previous = '0'*64
        count = 0
        for row in self.connection.execute('SELECT * FROM events ORDER BY id'):
            expected = stable_hash([previous,row['episode_id'],row['round_index'],row['kind'],row['payload']])
            if row['previous_hash'] != previous or row['hash'] != expected:
                raise IntegrityError(f'Event chain mismatch at {row["id"]}')
            previous = expected
            count += 1
        return count
