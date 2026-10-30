"""Pre-bid implementation, immutable proposals and post-auction lineage.

Versioned in coral.candidate_pipeline so old episode identities remain intact.
Authority records use the existing append-only event chain; no ledger migration.
"""
from concurrent.futures import ThreadPoolExecutor, wait
from copy import deepcopy
import json
from pathlib import Path
import time
import traceback

from maestro.types import AuthorizationError, IntegrityError, MaestroError, canonical_json


def validate_proposal_binding(db, ticket, submission):
    """Bind the bid to h0 and admission to its controller-recorded repair chain."""
    e,r,a = (ticket[k] for k in ('episode_id','round_index','agent_id'))
    events = [(row['kind'],json.loads(row['payload'])) for row in db.execute(
        'SELECT kind,payload FROM events WHERE episode_id=? AND round_index=? ORDER BY id',(e,r))]
    proposals = [p for k,p in events if k=='PROPOSAL_FROZEN' and p['agent_id']==a]
    if len(proposals)!=1:
        raise AuthorizationError('Exactly one frozen pre-bid proposal is required')
    proposal=proposals[0]
    if (proposal['plan_version_id']!=ticket['plan_version_id']
            or proposal['parent_hash']!=ticket['parent_hash']
            or submission.get('proposal_hash')!=proposal['candidate_hash']
            or ticket.get('proposal_hash')!=proposal['candidate_hash']):
        raise AuthorizationError('Ticket/bid differs from its frozen proposal')
    chain=[proposal['candidate_hash']]
    for kind,payload in events:
        if kind!='RUNTIME_REPAIR_FROZEN' or payload['agent_id']!=a:continue
        if (payload['plan_version_id']!=ticket['plan_version_id']
                or payload['source_candidate_hash']!=chain[-1]
                or payload['repair_index']!=len(chain)-1):
            raise AuthorizationError('Broken runtime repair lineage')
        evidence=db.execute('SELECT payload FROM prechecks WHERE episode_id=? AND round_index=? '
            'AND agent_id=? AND candidate_hash=? AND passed=0',
            (e,r,a,chain[-1])).fetchall()
        if not any(json.loads(row[0]).get('runtime_repair_eligible') for row in evidence):
            raise AuthorizationError('Runtime repair has no controller-recorded runtime failure')
        grant=db.execute('SELECT payload FROM edit_grants WHERE id=?',(payload['grant_id'],)).fetchone()
        grant=json.loads(grant[0]) if grant else {}
        if any(grant.get(k)!=v for k,v in {'episode_id':e,'round_index':r,'agent_id':a,
                'plan_version_id':ticket['plan_version_id'],'purpose':'runtime_repair',
                'source_candidate_hash':chain[-1],'epoch':len(chain)}.items()):
            raise AuthorizationError('Runtime repair grant source/owner mismatch')
        chain.append(payload['candidate_hash'])
    if ticket.get('repair_chain')!=chain or ticket['candidate_hash']!=chain[-1]:
        raise AuthorizationError('Admission is not the final frozen repair of the bid proposal')
    config=json.loads(db.execute('SELECT config_json FROM episodes WHERE id=?',(e,)).fetchone()[0])
    if len(chain)>1+config.get('runtime_repair_calls',3):
        raise AuthorizationError('Repair lineage exceeds the frozen request cap')


def validate_prebid_round(db,e,r):
    """Check event ordering for losing as well as admitted proposals."""
    events=[(row['kind'],json.loads(row['payload'])) for row in db.execute(
        'SELECT kind,payload FROM events WHERE episode_id=? AND round_index=? ORDER BY id',(e,r))]
    bidding=False;auction=False;proposals={}
    for kind,payload in events:
        if kind=='ROUND_PHASE' and payload['phase']=='BIDDING':bidding=True
        if kind=='PROPOSAL_FROZEN':
            a=payload['agent_id']
            if bidding or a in proposals:raise AuthorizationError('Proposal frozen after bidding or more than once')
            proposals[a]=payload
            if not db.execute('SELECT 1 FROM candidates WHERE hash=?',(payload['candidate_hash'],)).fetchone():
                raise AuthorizationError('Frozen proposal has no registered source')
        if kind=='PLAN_SUBMITTED':
            a=payload['agent_id']
            if (not bidding or auction or a not in proposals
                    or payload.get('proposal_hash')!=proposals[a]['candidate_hash']
                    or payload['plan_version_id']!=proposals[a]['plan_version_id']):
                raise AuthorizationError('Bid not bound to a proposal before allocation')
        if kind=='INVITED':
            auction=True
            if not bidding:raise AuthorizationError('Allocation preceded pre-bid implementation barrier')


class PrebidPipeline:
    @property
    def prebid_pipeline(self):
        return self.config.coral.get('candidate_pipeline','postbid_v1')=='prebid_v2'

    def _register_source_candidate(self,candidate):
        if candidate.get('candidate_path'):
            from maestro.backends.coral.workspace import candidate_tree_hash
            if candidate_tree_hash(Path(candidate['candidate_path']))!=candidate['candidate_hash']:
                raise IntegrityError('Candidate changed before immutable publication')
        with self.ledger._lock:
            old=self.ledger.connection.execute('SELECT payload FROM candidates WHERE hash=?',
                                               (candidate['candidate_hash'],)).fetchone()
            if old:
                canonical=json.loads(old[0])
                if candidate.get('candidate_path'):
                    candidate={**candidate,'candidate_path':canonical['candidate_path']}
            else:self.ledger.record_candidate(candidate['candidate_hash'],candidate)
        return candidate

    def _implement_proposal(self,e,r,a,state,plan,purpose,deadline):
        """No precheck, hold, payment, formal score or repair before bidding."""
        self._check_pause()
        if time.monotonic()>=deadline:return None
        if self.config.execution_mode=='mock':
            return self._implement_mock_proposal(e,r,a,state,plan,purpose,deadline)
        if not getattr(self.backend,'supports_runtime_only_repair',False):
            raise MaestroError('prebid_v2 requires separated implementation/runtime repair support')
        grant=self.grants.issue(episode_id=e,round_index=r,agent_id=a,
            plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=0,
            expires_at=time.time()+max(.001,deadline-time.monotonic()),
            purpose='prebid_implementation',source_candidate_hash=plan.parent_hash)
        with self.ledger.transaction() as db:
            db.execute('INSERT INTO edit_grants VALUES(?,?)',(grant.grant_id,canonical_json(grant)))
        started=time.monotonic()
        self.backend.last_generation_usage={}
        try:
            self.grants.verify(grant,agent_id=a,plan_version_id=plan.version_id,
                parent_hash=plan.parent_hash,epoch=0)
            candidate=self.backend.generate(state,plan,r,revision=0,deadline=deadline,
                grant=grant,grant_authority=self.grants)
            if time.monotonic()>=deadline:return None
            return self._register_source_candidate(candidate)
        except (AuthorizationError,IntegrityError):raise
        except Exception as error:
            if (getattr(error,'inflight_unknown',False) or getattr(error,'execution_unknown',False)
                    or getattr(error,'local_quiescent',None) is False):raise
            self.ledger.event('IMPLEMENTATION_FAILED',{'agent_id':a,
                'error':str(error),'error_type':type(error).__name__,
                'traceback':traceback.format_exc()},e,r)
            return None
        finally:
            self.grants.revoke(grant)
            usage=getattr(self.backend,'last_generation_usage',{})
            self.ledger.usage(e,r,a,'generation',float(usage.get('cpu_seconds') or 0),
                float(usage.get('wall_seconds') or time.monotonic()-started),
                calls=int(usage.get('calls') or 0),
                tokens=int(usage.get('known_input_tokens') or 0)+int(usage.get('known_output_tokens') or 0),
                payload={'usage':usage,'simulated':False,'stage':'PREBID_IMPLEMENTATION',
                    'grant_id':grant.grant_id,'usage_complete':bool(usage.get('usage_complete',False)),
                    'request_count_unknown':usage.get('calls') is None or bool(usage.get('continuation_blocking_requests',usage.get('unresolved_requests',0))),
                    'cost_usage_unknown':not bool(usage.get('usage_complete',False))})

    def _implement_all(self,e,r,states,plans,purpose,deadline):
        pool=ThreadPoolExecutor(max_workers=self.config.n_agents,thread_name_prefix='maestro-prebid')
        futures={pool.submit(self._implement_proposal,e,r,a,deepcopy(states[a]),p,purpose,deadline):a
                 for a,p in plans.items()}
        self._generation_futures.update(futures)
        try:
            done,pending=wait(futures,timeout=max(0,deadline-time.monotonic()))
            # Drain only for cleanup. This never extends candidate acceptance.
            if pending:
                late,pending=wait(pending,timeout=5.)
                done.update(late)
            if pending:
                self.ledger.set_round_phase(e,r,'IMPLEMENTATION_UNKNOWN')
                with self.ledger.transaction() as db:
                    db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                self.ledger.event('IMPLEMENTATION_STOPPED',{'reason':'CALLBACK_NOT_RECLAIMED',
                    'agents':[futures[f] for f in pending]},e,r)
                raise MaestroError('Pre-bid implementation callbacks unreclaimed; reconcile before continuing')
            def unresolved(error):
                return bool(getattr(error,'inflight_unknown',False) or getattr(error,'execution_unknown',False)
                            or getattr(error,'local_quiescent',None) is False)
            errors=[(a,f.exception()) for f,a in futures.items() if f.exception() is not None]
            if errors:
                unknown=any(unresolved(error) for _,error in errors)
                self.ledger.set_round_phase(e,r,'IMPLEMENTATION_UNKNOWN' if unknown else 'IMPLEMENTATION_FAILED')
                with self.ledger.transaction() as db:
                    db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                for a,error in errors:
                    self.ledger.event('IMPLEMENTATION_STOPPED',{'agent_id':a,
                        'error_type':type(error).__name__,'unknown':unresolved(error),
                        'traceback':''.join(traceback.format_exception(error))},e,r)
                raise next((error for _,error in errors if unresolved(error)),errors[0][1])
            if time.monotonic()>=deadline:
                # Cleanup grace proves quiescence; it never permits a late
                # implementation barrier, bid or candidate acceptance.
                for a in plans:states[a].needs_refresh=True
                self.ledger.event('IMPLEMENTATION_WINDOW_EXPIRED',{
                    'reclaimed_callbacks':len(done),'accepted_proposals':0,
                    'deadline_extended':False},e,r)
                return {}
            proposals={}
            # Publish in agent order after every callback has reclaimed its
            # native processes and revoked its grant, never by completion speed.
            for future,a in futures.items():
                candidate=future.result()
                if candidate is None:
                    states[a].needs_refresh=True
                    continue
                proposals[a]=candidate
                self.ledger.event('PROPOSAL_FROZEN',{'agent_id':a,
                    'plan_version_id':plans[a].version_id,'parent_hash':plans[a].parent_hash,
                    'candidate_hash':candidate['candidate_hash'],'pipeline':'prebid_v2',
                    'tested':False,'monotonic':time.monotonic()},e,r)
            return proposals
        finally:
            pool.shutdown(wait=False,cancel_futures=True)

    def _check_frozen_proposal(self,e,r,a,state,plan,purpose,deadline,candidate):
        if self.config.execution_mode=='mock':
            return self._check_mock_proposal(e,r,a,state,plan,purpose,deadline,candidate)
        return self._generate_once_runtime_only(e,r,a,state,plan,purpose,deadline,
                                               initial_candidate=candidate)

    def _implement_mock_proposal(self,e,r,a,state,plan,purpose,deadline):
        """Explicit simulated implementation; no testing or repair before bids."""
        from maestro.execution.grants import GrantError
        account=f'{e}:{r}:{a}:generation'
        self.budgets.create(account,max_calls=self.config.generation_calls,
            max_request_bytes=1024*1024,max_output_tokens=32000,
            deadline=time.time()+max(.001,deadline-time.monotonic()))
        grant=self.grants.issue(episode_id=e,round_index=r,agent_id=a,plan_version_id=plan.version_id,
            parent_hash=plan.parent_hash,epoch=0,expires_at=time.time()+max(.001,deadline-time.monotonic()),
            purpose='prebid_implementation',source_candidate_hash=plan.parent_hash)
        with self.ledger.transaction() as db:
            db.execute('INSERT INTO edit_grants VALUES(?,?)',(grant.grant_id,canonical_json(grant)))
        try:
            self.grants.verify(grant,agent_id=a,plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=0)
            if time.monotonic()>=deadline:return None
            reservation=self.budgets.reserve(account,'initial',request_bytes=512,output_tokens=4096)
            body={'execution_mode':'mock','benchmark':'pace','agent_id':a,'round_index':r,'purpose':purpose,
                  'parent_hash':plan.parent_hash,'target':plan.target,'mod_type':plan.mod_type,
                  'change':plan.change,'revision':0,'seed':self.config.seed}
            if purpose=='MAIN' and r in self.config.failures.get('duplicate_candidate_rounds',[]):
                body={'execution_mode':'mock','benchmark':'pace','forced_duplicate_round':r,'seed':self.config.seed}
            candidate={'candidate_hash':self.artifacts.put_json(body),'source':body,'kind':'mock'}
            self.budgets.finish(reservation,actual_output_tokens=256)
            self.ledger.usage(e,r,a,'generation',.02,.025,calls=1,tokens=256,
                payload={'simulated':True,'stage':'PREBID_IMPLEMENTATION','usage_complete':True})
            return self._register_source_candidate(candidate)
        except GrantError:
            if time.monotonic()>=deadline:return None
            raise
        finally:self.grants.revoke(grant)

    def _check_mock_proposal(self,e,r,a,state,plan,purpose,deadline,candidate):
        from maestro.execution.generation import precheck_mock
        initial=candidate['candidate_hash'];chain=[initial]
        failures=self.config.failures if purpose=='MAIN' else self.config.failures.get('warm',{})
        for attempt in range(1+self.config.runtime_repair_calls):
            if time.monotonic()>=deadline:return None
            if attempt:
                account=f'{e}:{r}:{a}:runtime-repair'
                if attempt==1:
                    self.budgets.create(account,max_calls=self.config.runtime_repair_calls,
                        max_request_bytes=1024*1024,max_output_tokens=32000,
                        deadline=time.time()+max(.001,deadline-time.monotonic()))
                reservation=self.budgets.reserve(account,str(attempt),request_bytes=512,output_tokens=4096)
                grant=self.grants.issue(episode_id=e,round_index=r,agent_id=a,plan_version_id=plan.version_id,
                    parent_hash=plan.parent_hash,epoch=attempt,expires_at=time.time()+max(.001,deadline-time.monotonic()),
                    purpose='runtime_repair',source_candidate_hash=chain[-1])
                with self.ledger.transaction() as db:
                    db.execute('INSERT INTO edit_grants VALUES(?,?)',(grant.grant_id,canonical_json(grant)))
                try:
                    self.grants.verify(grant,agent_id=a,plan_version_id=plan.version_id,
                        parent_hash=plan.parent_hash,epoch=attempt)
                    body={**candidate['source'],'revision':attempt}
                    candidate=self._register_source_candidate({'candidate_hash':self.artifacts.put_json(body),'source':body,'kind':'mock'})
                    self.ledger.event('RUNTIME_REPAIR_FROZEN',{'agent_id':a,'plan_version_id':plan.version_id,
                        'source_candidate_hash':chain[-1],'candidate_hash':candidate['candidate_hash'],
                        'repair_index':attempt-1,'grant_id':grant.grant_id,'simulated':True},e,r)
                    chain.append(candidate['candidate_hash'])
                    self.budgets.finish(reservation,actual_output_tokens=256)
                    self.ledger.usage(e,r,a,'runtime_repair',.02,.025,calls=1,tokens=256,payload={'simulated':True,'usage_complete':True})
                finally:self.grants.revoke(grant)
            passed,reason=precheck_mock(a,r,failures,attempt)
            with self.ledger.transaction() as db:
                db.execute('INSERT INTO prechecks VALUES(?,?,?,?,?,?,?)',(f'{e}:{r}:{a}:{attempt}',e,r,a,
                    candidate['candidate_hash'],int(passed),canonical_json({'reason':reason,'revision':attempt,
                    'simulated':True,'runtime_repair_eligible':reason=='runtime'})))
            if time.monotonic()>=deadline:return None
            if passed:return {**candidate,'proposal_hash':initial,'repair_chain':chain}
            if reason!='runtime':return None
        return None
