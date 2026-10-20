"""End-to-end warm start and synchronized V1 market episodes.

SQLite owns all admission and settlement decisions. Evaluators only receive
trusted, claimed tickets. Mock and real execution are explicitly different modes.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from collections import Counter
import importlib
import json
import time
import uuid

from maestro.config import RunConfig, CORAL_REVISION
from maestro.types import (AgentState, HistoryRecord, Submission, MaestroError,
    BootstrapIncomplete, IntegrityError, AuthorizationError, canonical_json,
    stable_hash, to_jsonable, state_from_dict)
from maestro.seeds import derive_seed
from maestro.storage import Ledger, ArtifactStore
from maestro.market.credit import reward, money
from maestro.market.policies import rank
from maestro.agents.valuation import estimate_value
from maestro.agents.bidding import cooperative_bid
from maestro.agents.planner import needs_refresh, mock_plan, validate_plan, PlanDecision, apply_decision
from maestro.agents.retrieval import build_context
from maestro.bootstrap.director import make_direction_briefs
from maestro.bootstrap.warm_start import balanced_schedule, validate_warm_completion
from maestro.execution.dispatcher import Dispatcher
from maestro.execution.generation import precheck_mock
from maestro.execution.grants import GrantAuthority
from maestro.execution.budgets import BudgetStore
from maestro.execution.cpu_pool import GlobalCpuPool
from maestro.backends.mock import MockEvaluator


def _new_id(prefix):
    return prefix + '-' + uuid.uuid4().hex[:16]


def _receipt_value(receipt,name,default=None):
    return receipt.get(name,default) if isinstance(receipt,dict) else getattr(receipt,name,default)


class ExperimentEngine:
    def __init__(self, root, config: RunConfig, *, backend=None, evaluator=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.config = config
        self.cpu_pool = GlobalCpuPool(config.cpu_workers)
        self.ledger = Ledger(self.root / 'authority' / 'ledger.sqlite')
        self.artifacts = ArtifactStore(self.root / 'authority' / 'cas' / 'sha256',self.ledger.register_artifact)
        self.budgets = BudgetStore(self.ledger.path)
        self.grants = GrantAuthority()
        self.backend = backend
        self.evaluator = evaluator
        self._generation_futures = set()
        self._closed = False
        self._live_contract = None
        self._live_panel = None
        if config.mode == 'mock':
            if backend is not None:
                raise ValueError('Mock mode cannot silently use a live backend')
            self.evaluator = evaluator or MockEvaluator(seed=config.seed)
            self.panel_hash = stable_hash({'mode':'mock','instances':config.panel_size,'seed':config.seed})
            self.contract_hash = stable_hash({'mode':'mock','reserved_cpu':config.reserved_cpu_seconds,'panel':self.panel_hash})
        else:
            try:
                self._prepare_live()
            except BaseException:
                self.budgets.close()
                self.ledger.close()
                raise
        self.dispatcher = Dispatcher(self.ledger, self.evaluator)
        (self.root / 'authority' / 'config.resolved.json').write_text(canonical_json(config),encoding='utf-8')

    def _prepare_live(self):
        """Every production prerequisite is explicit; there is no mock fallback."""
        from maestro.tasks.pace.manifest import load_panel, EvaluationContract
        from maestro.tasks.pace.runner import PaceEvaluator
        manifest = self.config.pace.get('panel_manifest')
        if not manifest:
            raise MaestroError('pace mode requires pace.panel_manifest, seed_solver, pinned runner and Coral backend')
        self._live_panel = load_panel(manifest,expected_split='training')
        contract_settings = dict(self.config.pace.get('contract',{}))
        for name in ('cpu_workers','instance_workers'):
            value = getattr(self.config,name)
            if name in contract_settings and contract_settings[name] != value:
                raise IntegrityError(f'Top-level {name} and PACE contract disagree')
            contract_settings[name] = value
        self._live_contract = EvaluationContract.standard(**contract_settings)
        if self.config.panel_size != self._live_contract.instances:
            raise IntegrityError('Config and PACE panel contract disagree')
        self.panel_hash = self._live_panel.panel_hash
        self.contract_hash = self._live_contract.contract_hash
        if self.backend is None:
            factory = self.config.coral.get('backend_factory')
            if not factory or ':' not in factory:
                raise MaestroError('pace mode requires a trusted coral.backend_factory module:callable or injected CoralBackend')
            module,name = factory.split(':',1)
            self.backend = getattr(importlib.import_module(module),name)(self.config,self.root)
        self.backend.preflight(self.config)
        if self.evaluator is None:
            runner = getattr(self.backend,'candidate_runner',None)
            self.evaluator = PaceEvaluator(self._live_panel,self._live_contract,
                runner=runner,artifact_store=self.artifacts.put,
                cpu_workers=self.config.cpu_workers,
                instance_workers=self.config.instance_workers,cpu_pool=self.cpu_pool)
        if isinstance(self.evaluator,MockEvaluator):
            raise MaestroError('MockEvaluator is forbidden in pace mode')
        if hasattr(self.evaluator,'preflight'):
            self.evaluator.preflight()
        if not self.config.pace.get('seed_solver'):
            raise MaestroError('pace.seed_solver is required')

    def close(self, *, timeout=30.0):
        if self._closed:
            return
        # A native phase can stop before its outer generation callback records
        # usage/revokes the grant. Keep both authority connections open until
        # those callbacks finish; cancelling a Future cannot stop a running one.
        _, pending = wait(self._generation_futures, timeout=max(0.0, timeout))
        if pending:
            raise MaestroError('Generation cleanup unresolved; preserve authority connections until containment reconciliation')
        self._generation_futures.clear()
        if self.backend is not None and hasattr(self.backend,'close'):
            self.backend.close()
        self.budgets.close()
        self.ledger.close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self,*args):
        self.close()

    def _board(self, states, round_index, results=()):
        champion = max(states.values(),key=lambda s:(s.parent_score,s.parent_hash))
        summaries={}
        for state in states.values():
            for record in state.history:
                summaries[record.experiment_id]={'experiment_id':record.experiment_id,
                    'candidate_hash':record.candidate_hash,'agent_id':record.agent_id,
                    'score':record.score,'valid':record.valid,'status':record.status,'verified':True}
        for result in results:
            summaries[result['experiment_id']]=result
        return {'version':round_index+1,'round_index':round_index,
            'champion_hash':champion.parent_hash,'champion_score':champion.parent_score,
            'champion_artifact':champion.parent_hash,'panel_hash':self.panel_hash,
            'contract_hash':self.contract_hash,'results':list(summaries.values())}

    def _state_payload(self,states):
        return {a:to_jsonable(s) for a,s in states.items()}

    def bootstrap(self):
        c = self.config
        warm_id = _new_id('warm')
        agent_ids = [f'agent-{i+1}' for i in range(c.n_agents)]
        if c.mode == 'mock':
            seed_hash = self.artifacts.put_json({'mode':'mock','solver':'common-seed-v1','seed':c.seed})
            seed_candidate = {'candidate_hash':seed_hash,'kind':'mock','seed':True}
        else:
            seed_candidate = self.backend.prepare_seed(c.pace['seed_solver'])
            seed_hash = seed_candidate['candidate_hash']
        states = {a:AgentState(a,seed_hash,0.0,c.initial_wealth) for a in agent_ids}
        self.ledger.create_episode(warm_id,'warm',c,states)
        if self.backend and hasattr(self.backend,'begin_episode'):
            self.backend.begin_episode(warm_id,states)
        if self.backend and hasattr(self.backend,'register_parent'):
            self.backend.register_parent(seed_hash,seed_candidate['candidate_path'])
        self.ledger.record_candidate(seed_hash,seed_candidate)
        initial_score = self._evaluate_seed(warm_id,states,seed_candidate)
        for state in states.values():
            state.parent_score = initial_score
        director_provider = getattr(self.backend,'director_provider',None) if self.backend else None
        briefs = make_direction_briefs(agent_ids,c.seed,provider=director_provider,
            context={'seed_artifact':seed_hash,'contract_hash':self.contract_hash})
        director_hash = self.artifacts.put_json(briefs)
        self.ledger.event('DIRECTOR_COMPLETED',{'artifact_hash':director_hash,'source':briefs[0]['source']},warm_id)
        director_receipt=getattr(self.backend,'last_director_receipt',None) if self.backend else None
        if director_receipt:
            actual=_receipt_value(director_receipt,'actual_requests')
            self.ledger.usage(warm_id,-1,None,'director',calls=actual or 0,
                tokens=(_receipt_value(director_receipt,'input_tokens') or 0)+(_receipt_value(director_receipt,'output_tokens') or 0),
                payload={'receipt':to_jsonable(director_receipt),'simulated':False,
                    'session_turns':_receipt_value(director_receipt,'session_turns',0),
                    'request_count_unknown':actual is None,'usage_complete':actual is not None})
        if self.backend and hasattr(self.backend,'set_directions'):
            self.backend.set_directions(briefs)
        opportunities = Counter({a:0 for a in agent_ids})
        schedule = balanced_schedule(agent_ids,c.warm_batch_width,c.warm_opportunities_per_agent,c.seed)
        for index,wave in enumerate(schedule):
            for agent in wave:
                opportunities[agent] += 1
            self._round(warm_id,index,states,'uniform',purpose='WARM',warm_invite=wave)
        try:
            validate_warm_completion(states,opportunities,c.warm_opportunities_per_agent)
        except BootstrapIncomplete:
            with self.ledger.transaction() as db:
                db.execute("UPDATE episodes SET status='BOOTSTRAP_INCOMPLETE' WHERE id=?",(warm_id,))
                self.ledger.event('BOOTSTRAP_INCOMPLETE',{'opportunities':dict(opportunities)},warm_id)
            raise
        payload = {'warm_id':warm_id,'schema_version':1,'config':c.to_dict(),
            'config_hash':c.hash,'contract_hash':self.contract_hash,'panel_hash':self.panel_hash,
            'coral_revision':CORAL_REVISION,'states':self._state_payload(states),
            'board':self._board(states,len(schedule)-1),'direction_briefs':briefs,
            'opportunities':dict(opportunities),'warm_waves':len(schedule)}
        if self.backend and hasattr(self.backend,'export_warm_memory'):
            payload['private_memory_manifest']=self.backend.export_warm_memory()
        digest = self.artifacts.put_json(payload)
        with self.ledger.transaction() as db:
            db.execute('INSERT INTO warm_checkpoints VALUES(?,?,?)',(warm_id,canonical_json(payload),digest))
            db.execute("UPDATE episodes SET status='COMPLETED' WHERE id=?",(warm_id,))
            self.ledger.event('WARM_FROZEN',{'warm_id':warm_id,'content_hash':digest},warm_id)
        folder = self.root / 'bootstrap' / warm_id
        folder.mkdir(parents=True,exist_ok=True)
        checkpoint = folder / 'checkpoint.json'
        checkpoint.write_text(canonical_json({'artifact_hash':digest,'payload':payload}),encoding='utf-8')
        return {'warm_id':warm_id,'checkpoint':str(checkpoint),'content_hash':digest,
            'warm_waves':len(schedule),'opportunities':dict(opportunities)}

    def _evaluate_seed(self, episode_id, states, candidate):
        """Setup is a real authorized evaluation, separate from gain history."""
        a = next(iter(states))
        evaluation_settings = self._evaluation_settings(1)
        self.ledger.open_round(episode_id,-1,{'purpose':'SETUP','seed':candidate,'contract_hash':self.contract_hash,'panel_hash':self.panel_hash,
            **evaluation_settings})
        self.ledger.set_round_phase(episode_id,-1,'GENERATION')
        self.ledger.invite(episode_id,-1,a,Decimal(0))
        self.ledger.set_round_phase(episode_id,-1,'ADMISSION')
        ticket = self._ticket(episode_id,-1,a,candidate,None,0.2,0.2,'SETUP',evaluation_settings)
        self.ledger.admit(ticket,Decimal(0),1)
        self.ledger.set_round_phase(episode_id,-1,'EVALUATING')
        result = self.dispatcher.dispatch(ticket['ticket_id'],ticket['job_id'])
        if not result.valid:
            raise BootstrapIncomplete('Initial solver failed the formal setup contract')
        with self.ledger.transaction() as db:
            db.execute("UPDATE rounds SET status='SETTLED' WHERE episode_id=? AND round_index=-1",(episode_id,))
            self.ledger.event('SETUP_COMPLETED',{'candidate_hash':candidate['candidate_hash'],'score':result.score},episode_id,-1)
        self.ledger.usage(episode_id,-1,a,'setup',result.cpu_seconds,result.wall_seconds)
        return result.score

    def run(self, checkpoint, policy='first_price', *, episode_id=None):
        if policy not in ('first_price','uniform','ucb'):
            raise ValueError('Unknown V1 policy')
        envelope = json.loads(Path(checkpoint).read_text(encoding='utf-8'))
        payload = envelope['payload']
        if stable_hash(payload) != envelope['artifact_hash']:
            raise IntegrityError('Warm checkpoint integrity failure')
        if payload['contract_hash'] != self.contract_hash or payload['panel_hash'] != self.panel_hash:
            raise IntegrityError('Warm contract/panel mismatch')
        warm_config = dict(payload['config'])
        compare_config = self.config.to_dict()
        # Fault injection changes only mock main scenarios, never common warm state.
        warm_config.pop('failures',None)
        compare_config.pop('failures',None)
        if warm_config != compare_config:
            raise IntegrityError('Warm/main configuration mismatch')
        states = {a:state_from_dict(s) for a,s in payload['states'].items()}
        for state in states.values():
            state.wealth = self.config.initial_wealth
            state.pending_plan=None
            state.last_planning_round=-999
            state.needs_refresh=False
        episode_id = episode_id or _new_id(policy)
        self.ledger.create_episode(episode_id,policy,self.config,states,payload['warm_id'])
        if self.backend and hasattr(self.backend,'restore_warm_memory'):
            self.backend.restore_warm_memory(payload.get('private_memory_manifest',{}))
        if self.backend and hasattr(self.backend,'begin_episode'):
            self.backend.begin_episode(episode_id,states)
        if self.backend and hasattr(self.backend,'register_parent'):
            for state in states.values():
                candidate=self.ledger.connection.execute('SELECT payload FROM candidates WHERE hash=?',(state.parent_hash,)).fetchone()
                if not candidate:
                    raise IntegrityError('Warm clone lacks immutable source artifact')
                self.backend.register_parent(state.parent_hash,json.loads(candidate[0])['candidate_path'])
        with self.ledger.transaction() as db:
            if not db.execute('SELECT 1 FROM warm_checkpoints WHERE id=?',(payload['warm_id'],)).fetchone():
                self.artifacts.put_json(payload)
                db.execute('INSERT INTO warm_checkpoints VALUES(?,?,?)',(payload['warm_id'],canonical_json(payload),envelope['artifact_hash']))
            db.execute('INSERT INTO arm_clones VALUES(?,?,?)',(episode_id,payload['warm_id'],canonical_json({'source_hash':envelope['artifact_hash'],'fresh_sessions':True,'wealth':str(self.config.initial_wealth)})))
        if self.backend and hasattr(self.backend,'set_directions'):
            self.backend.set_directions(payload['direction_briefs'])
        for index in range(self.config.rounds):
            self._round(episode_id,index,states,policy)
        with self.ledger.transaction() as db:
            db.execute("UPDATE episodes SET status='COMPLETED' WHERE id=?",(episode_id,))
        return {'episode_id':episode_id,'rounds':self.config.rounds,'champion':self._board(states,self.config.rounds-1),'status':'COMPLETED'}

    def _evaluation_settings(self, admission_capacity):
        """Freeze a resource ceiling before planning, independent of winners."""
        c = self.config
        batch_width = c.evaluation_batch_width(admission_capacity)
        return {'admission_capacity':admission_capacity,'cpu_workers':c.cpu_workers,
            'instance_workers':c.instance_workers,'evaluation_batch_width':batch_width,
            'effective_instance_workers':c.effective_instance_workers(batch_width)}

    def _ticket(self,e,r,a,candidate,plan,parent,champion,purpose,evaluation_settings=None):
        job_id = f'{e}:{r}:{a}:job'
        settings = evaluation_settings or self._evaluation_settings(1 if purpose in ('SETUP','HELDOUT') else self.config.slots)
        return {'ticket_id':job_id+':ticket','job_id':job_id,'episode_id':e,
            'round_index':r,'agent_id':a,'candidate_hash':candidate['candidate_hash'],
            'candidate_path':candidate.get('candidate_path'),
            'contract_hash':self.contract_hash,'panel_hash':self.panel_hash,
            'plan_version_id':plan.version_id if plan else 'setup',
            'parent_hash':plan.parent_hash if plan else candidate['candidate_hash'],
            'parent_score':parent,'champion_score':champion,'phase_epoch':0,
            'purpose':purpose,'seed':self.config.seed,
            'target':plan.target if plan else 'initialization',
            'mod_type':plan.mod_type if plan else 'other',
            'semantic_plan_key':stable_hash([plan.target,plan.mod_type,plan.change]) if plan else 'seed',
            'reserved_cpu_seconds':self.config.reserved_cpu_seconds,**settings}

    def _round(self,e,r,states,policy,purpose='MAIN',warm_invite=None):
        c = self.config
        admission_capacity = len(warm_invite) if warm_invite is not None else c.slots
        evaluation_settings = self._evaluation_settings(admission_capacity)
        planning_deadline = time.monotonic()+c.planning_window_seconds
        planning_expires = time.time()+c.planning_window_seconds
        frozen = self._state_payload(states)
        board = self._board(states,r-1)
        if self.backend and hasattr(self.backend,'set_board'):
            self.backend.set_board(board)
        champion = board['champion_score']
        snapshot = {'purpose':purpose,'states':frozen,'board':board,'seed':c.seed,
            'contract_hash':self.contract_hash,'panel_hash':self.panel_hash,
            'planning_deadline':planning_expires,
            'generation_window_seconds':c.generation_window_seconds,
            'warm_invite':list(warm_invite) if warm_invite is not None else None,
            **evaluation_settings}
        self.ledger.open_round(e,r,snapshot)
        submissions,plans = [],{}
        plan_seed = derive_seed(c.seed,purpose,'planning')
        live_outcomes={}
        if c.mode!='mock':
            from maestro.agents.planning_batch import parallel_plans
            refresh_states={a:s for a,s in states.items() if needs_refresh(s,r,c.refresh_interval)
                and (warm_invite is None or a in warm_invite)}
            live_contexts={a:{**build_context(s,board),'episode_id':e,'purpose':purpose} for a,s in refresh_states.items()}
            live_outcomes=parallel_plans(refresh_states,live_contexts,self.backend,r,
                deadline=planning_deadline,
                budget_unit='session_turn' if c.profile=='codex_session_pilot' else 'provider_request')
            for a,outcome in live_outcomes.items():
                receipt=outcome.receipt
                scope=f'{e}:{r}:{a}:PLAN_RO'
                measured={}
                store=getattr(self.backend,'budget_store',None) or getattr(self.backend,'turn_store',None)
                if store and hasattr(store,'usage'):
                    from maestro.execution.budgets import BudgetExceeded
                    try:
                        measured=store.usage(scope)
                    except BudgetExceeded:
                        pass
                actual=_receipt_value(receipt,'actual_requests')
                calls=(actual or 0) if receipt else int(measured.get('calls',0))
                tokens=((_receipt_value(receipt,'input_tokens') or 0)+(_receipt_value(receipt,'output_tokens') or 0)) if receipt else int(measured.get('known_input_tokens',0))+int(measured.get('known_output_tokens',0))
                self.ledger.usage(e,r,a,'planning',calls=calls,tokens=tokens,
                    payload={'simulated':False,'receipt':to_jsonable(receipt),'scope':scope,
                        'usage_complete':actual is not None and measured.get('usage_complete',receipt is not None),
                        'session_turns':_receipt_value(receipt,'session_turns',measured.get('session_turns',0)),
                        'request_count_unknown':outcome.inflight_unknown or bool(measured.get('request_count_unknown',False)) or (receipt is not None and actual is None),
                        'measurement':measured,'outcome_status':outcome.status})
                if outcome.error:
                    self.ledger.event('PLANNING_OUTCOME',{'agent_id':a,'status':outcome.status,
                        'error':outcome.error,'inflight_unknown':outcome.inflight_unknown},e,r)
            if any(o.inflight_unknown for o in live_outcomes.values()):
                self.ledger.set_round_phase(e,r,'PLANNING_UNKNOWN')
                with self.ledger.transaction() as db:
                    db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                raise MaestroError('Unresolved planning inference at common deadline; phase remains isolated pending reconciliation')
        for a,state in states.items():
            if warm_invite is not None and a not in warm_invite:
                continue
            refresh = needs_refresh(state,r,c.refresh_interval)
            context = build_context(state,board)
            context_id = self.artifacts.put_json(context)
            with self.ledger.transaction() as db:
                db.execute('INSERT OR IGNORE INTO context_manifests VALUES(?,?)',(context_id,canonical_json(context)))
            if refresh:
                plan=None
                if c.mode == 'mock':
                    plan = mock_plan(state,r,plan_seed)
                    plan = replace(plan,plan_id=e+':'+plan.plan_id,version_id=e+':'+plan.version_id)
                else:
                    try:
                        outcome=live_outcomes[a]
                        decision=outcome.decision
                        if outcome.error:
                            raise ValueError(outcome.error)
                        if outcome.context:
                            context_id=self.artifacts.put_json(outcome.context)
                        if isinstance(decision,PlanDecision):
                            apply_decision(state,decision,r)
                            plan=decision.plan
                        else:
                            plan=decision
                        if plan is not None:
                            validate_plan(plan,state.parent_hash,[h.experiment_id for h in state.history])
                    except (ValueError,TimeoutError) as error:
                        self.ledger.event('ABSTAIN',{'agent_id':a,'reason':type(error).__name__,'message':str(error)},e,r)
                state.last_planning_round = r
                state.needs_refresh = False
                state.pending_plan = plan
                receipt=live_outcomes[a].receipt if a in live_outcomes else None
                calls=1 if c.mode=='mock' else (_receipt_value(receipt,'actual_requests') or 0)
                tokens=(_receipt_value(receipt,'input_tokens') or 0)+(_receipt_value(receipt,'output_tokens') or 0) if receipt else 0
                if c.mode=='mock':
                    self.ledger.usage(e,r,a,'planning',calls=calls,tokens=tokens,
                        payload={'simulated':True,'context_hash':context_id,'usage_complete':True})
            else:
                plan = state.pending_plan
            with self.ledger.transaction() as db:
                db.execute('INSERT INTO phase_transitions VALUES(?,?)',
                    (f'{e}:{r}:{a}:planning',canonical_json({'phase':'PLANNED','state':state})))
            if plan is None:
                self.ledger.event('ABSTAIN',{'agent_id':a,'reason':'WITHDRAWN'},e,r)
                continue
            validate_plan(plan,state.parent_hash,[h.experiment_id for h in state.history])
            if plan.agent_id != a:
                raise AuthorizationError('Plan owner mismatch')
            if purpose == 'WARM':
                value,bid,samples = Decimal(0),Decimal(0),[]
            else:
                value,samples = estimate_value(state.history,plan.target,plan.mod_type,state.parent_score,champion,scale=c.reward_scale,catchup=c.catchup_fraction)
                bid = cooperative_bid(value,c.bid_cap)
            submission = Submission(a,plan.version_id,value,bid)
            submissions.append(submission)
            plans[a] = plan
            self.ledger.submit(e,r,plan,submission,{'reused':not refresh,'refresh_reason':'DUE' if refresh else 'ELIGIBLE_PENDING','sample_ids':samples,'context_hash':context_id})
        order = list(warm_invite) if warm_invite is not None else rank(policy,submissions,states,r,c.seed,slots=c.slots)
        order = [a for a in order if a in plans]
        with self.ledger.transaction() as db:
            db.execute('INSERT INTO priority_orders VALUES(?,?,?)',(e,r,canonical_json({'policy':policy,'order':order})))
        self.ledger.set_round_phase(e,r,'GENERATION')
        bids = {s.agent_id:s.bid for s in submissions}
        successful = []
        deadline = time.monotonic()+c.generation_window_seconds
        self.ledger.event('GENERATION_WINDOW_OPENED',{'deadline':time.time()+c.generation_window_seconds},e,r)
        injected_deadline = purpose=='MAIN' and r in c.failures.get('deadline_rounds',[])
        remaining = iter(order)
        while len(successful)<admission_capacity and time.monotonic()<deadline and not injected_deadline:
            # Invite only the available slots. All initial winners share the
            # same window; one slow agent cannot prevent another from starting.
            wave=[]
            for _ in range(admission_capacity-len(successful)):
                a=next(remaining,None)
                if a is None:
                    break
                amount=bids[a] if policy=='first_price' and purpose=='MAIN' else Decimal(0)
                self.ledger.invite(e,r,a,amount,{'plan_version_id':plans[a].version_id})
                wave.append((a,amount))
            if not wave:
                break
            pool=ThreadPoolExecutor(max_workers=len(wave),thread_name_prefix='maestro-generation')
            futures={a:pool.submit(self._generate,e,r,a,deepcopy(states[a]),plans[a],purpose,deadline)
                     for a,_ in wave}
            self._generation_futures = {f for f in self._generation_futures if not f.done()}
            self._generation_futures.update(futures.values())
            try:
                _,pending=wait(futures.values(),timeout=max(0,deadline-time.monotonic()))
                if pending:
                    # Threads cannot establish containment by cancellation.
                    # Never advance to eval or the next round while a callback
                    # could still change source/memory or consume its budget.
                    for future in pending:
                        future.cancel()
                    self.ledger.set_round_phase(e,r,'GENERATION_UNKNOWN')
                    with self.ledger.transaction() as db:
                        db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                    raise MaestroError('Generation callback unresolved at common deadline; containment reconciliation required')
                # Process in frozen policy order, not completion order. This
                # also deterministically keeps the higher-priority duplicate.
                for future in futures.values():
                    error=future.exception()
                    if getattr(error,'inflight_unknown',False):
                        raise error
                outcomes=[(a,amount,futures[a].result()) for a,amount in wave]
            except Exception as error:
                if getattr(error,'inflight_unknown',False):
                    self.ledger.set_round_phase(e,r,'GENERATION_UNKNOWN')
                    with self.ledger.transaction() as db:
                        db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                    self.ledger.event('GENERATION_INFERENCE_UNKNOWN',{'reason':str(error)},e,r)
                raise
            finally:
                pool.shutdown(wait=False,cancel_futures=True)
            for a,amount,candidate in outcomes:
                if candidate is None:
                    states[a].needs_refresh = True
                    self.ledger.release_hold(e,r,a,'GENERATION_PRECHECK_FAILED')
                    continue
                digest = candidate['candidate_hash']
                if any(item[1]['candidate_hash']==digest for item in successful):
                    states[a].needs_refresh = True
                    self.ledger.release_hold(e,r,a,'EXACT_DUPLICATE')
                    continue
                bootstrap=self.ledger.connection.execute('SELECT bootstrap_id FROM episodes WHERE id=?',(e,)).fetchone()[0]
                past=self.ledger.connection.execute('SELECT 1 FROM verified_results v JOIN eval_tickets t ON t.job_id=v.job_id WHERE v.candidate_hash=? AND (t.episode_id=? OR t.episode_id=?) LIMIT 1',(digest,e,bootstrap)).fetchone()
                if past:
                    states[a].pending_plan=None
                    states[a].needs_refresh=True
                    self.ledger.release_hold(e,r,a,'PREVIOUSLY_EVALUATED_CONTENT')
                    continue
                successful.append((a,candidate,amount))
        self.ledger.set_round_phase(e,r,'ADMISSION')
        tickets=[]
        for a,candidate,amount in successful:
            ticket=self._ticket(e,r,a,candidate,plans[a],frozen[a]['parent_score'],champion,purpose,evaluation_settings)
            fault=c.failures.get(f'{a}:{r}',{}) if purpose=='MAIN' else {}
            if isinstance(fault,dict) and fault.get('formal'):
                ticket['simulated_failure']=fault['formal']
            self.ledger.admit(ticket,amount,admission_capacity)
            tickets.append(ticket)
        self.ledger.set_round_phase(e,r,'EVALUATING')
        # All admitted evaluations finish before publishing state/memory/board.
        with ThreadPoolExecutor(max_workers=evaluation_settings['evaluation_batch_width']) as pool:
            futures=[pool.submit(self.dispatcher.dispatch,t['ticket_id'],t['job_id']) for t in tickets]
            results=[f.result() for f in futures]
        self._settle(e,r,states,frozen,champion,plans,results,purpose)

    def _generate(self,e,r,a,state,plan,purpose,deadline):
        c=self.config
        account=f'{e}:{r}:{a}:generation'
        expires=time.time()+max(0.01,deadline-time.monotonic())
        self.budgets.create(account,max_calls=c.generation_calls,max_request_bytes=1024*1024,
            max_output_tokens=32000,deadline=expires)
        last_reason='DEADLINE'
        max_attempts=min(c.generation_calls,c.max_candidate_revisions)
        for revision in range(max_attempts):
            if time.monotonic()>=deadline:
                break
            grant=self.grants.issue(episode_id=e,round_index=r,agent_id=a,
                plan_version_id=plan.version_id,parent_hash=plan.parent_hash,
                epoch=revision,expires_at=expires)
            with self.ledger.transaction() as db:
                db.execute('INSERT INTO edit_grants VALUES(?,?)',(grant.grant_id,canonical_json(grant)))
            self.grants.verify(grant,agent_id=a,plan_version_id=plan.version_id,
                parent_hash=plan.parent_hash,epoch=revision)
            start=time.monotonic()
            candidate=None
            passed=False
            calls,tokens,cpu,wall=0,0,0.0,0.0
            from maestro.execution.budgets import BudgetExceeded
            try:
                if c.mode=='mock':
                    reservation=self.budgets.reserve(account,f'revision-{revision}',request_bytes=512,output_tokens=4096)
                    body={'mode':'mock','agent_id':a,'round_index':r,'purpose':purpose,
                        'parent_hash':plan.parent_hash,'target':plan.target,'mod_type':plan.mod_type,
                        'change':plan.change,'revision':revision,'seed':c.seed}
                    if purpose=='MAIN' and r in c.failures.get('duplicate_candidate_rounds',[]):
                        body={'mode':'mock','forced_duplicate_round':r,'seed':c.seed}
                    digest=self.artifacts.put_json(body)
                    candidate={'candidate_hash':digest,'source':body,'kind':'mock'}
                    failure_cases=c.failures if purpose=='MAIN' else c.failures.get('warm',{})
                    passed,last_reason=precheck_mock(a,r,failure_cases,revision)
                    self.budgets.finish(reservation,actual_output_tokens=256)
                    calls,tokens,cpu,wall=1,256,0.02,0.025
                else:
                    if hasattr(self.backend,'last_generation_usage'):
                        self.backend.last_generation_usage={}
                    grant_args={'grant':grant,'grant_authority':self.grants} if getattr(self.backend,'require_edit_grant',False) else {}
                    candidate=self.backend.generate(state,plan,r,revision=revision,deadline=deadline,**grant_args)
                    passed,last_reason=self.backend.precheck(candidate)
            except (AuthorizationError,IntegrityError):
                raise
            except (TimeoutError,OSError,RuntimeError,BudgetExceeded) as error:
                if getattr(error,'inflight_unknown',False):
                    raise
                last_reason=f'{type(error).__name__}: {error}'
                self.ledger.event('GENERATION_ATTEMPT_FAILED',{'agent_id':a,'revision':revision,'reason':last_reason},e,r)
            finally:
                self.grants.revoke(grant)
                if c.mode!='mock':
                    measured=getattr(self.backend,'last_generation_usage',{})
                    calls=int(measured.get('calls') or 0)
                    tokens=int(measured.get('known_input_tokens',measured.get('input_tokens')) or 0)+int(measured.get('known_output_tokens',measured.get('output_tokens')) or 0)
                    cpu=float(measured.get('cpu_seconds') or 0)
                    measured_wall=measured.get('wall_seconds')
                    wall=float(measured_wall if measured_wall is not None else time.monotonic()-start)
                self.ledger.usage(e,r,a,'generation',cpu,wall,calls=calls,tokens=tokens,
                    payload={'revision':revision,'simulated':c.mode=='mock','grant_id':grant.grant_id,
                             'usage':getattr(self.backend,'last_generation_usage',None) if self.backend else None,
                             'session_turns':measured.get('session_turns',0) if c.mode!='mock' else 0,
                             'request_count_unknown':bool(measured.get('request_count_unknown',measured.get('calls') is None)) if c.mode!='mock' else False,
                             'usage_complete':c.mode=='mock' or bool(measured.get('usage_complete',False))})
            if candidate is None:
                continue
            if time.monotonic()>=deadline:
                passed,last_reason=False,'DEADLINE'
            with self.ledger.transaction() as db:
                db.execute('INSERT INTO prechecks VALUES(?,?,?,?,?,?,?)',
                    (f'{e}:{r}:{a}:{revision}',e,r,a,candidate['candidate_hash'],int(passed),canonical_json({'reason':last_reason,'revision':revision,'toy_only':True})))
            if passed:
                old=self.ledger.connection.execute('SELECT 1 FROM candidates WHERE hash=?',(candidate['candidate_hash'],)).fetchone()
                if not old:
                    self.ledger.record_candidate(candidate['candidate_hash'],candidate)
                elif c.mode!='mock':
                    canonical=self.ledger.connection.execute('SELECT payload FROM candidates WHERE hash=?',(candidate['candidate_hash'],)).fetchone()
                    candidate={**candidate,'candidate_path':json.loads(canonical[0])['candidate_path']}
                return candidate
        self.ledger.event('GENERATION_FAILED',{'agent_id':a,'reason':last_reason},e,r)
        return None

    def _settle(self,e,r,states,frozen,champion,plans,results,purpose):
        c=self.config
        public_results=[]
        # Apply to detached state. A failure in source registration, artifact
        # persistence or a database invariant must not mutate the caller's
        # history/UCB values before the transaction commits.
        settled_states=deepcopy(states)
        with self.ledger.transaction() as db:
            phase=db.execute('SELECT status,snapshot_json FROM rounds WHERE episode_id=? AND round_index=?',(e,r)).fetchone()
            if phase and phase[0]=='SETTLED':
                return
            if not phase or phase[0] not in ('EVALUATING','SETTLING'):
                raise AuthorizationError('Settlement requires the formal evaluation barrier')
            snapshot=json.loads(phase['snapshot_json'])
            if (canonical_json(frozen)!=canonical_json(snapshot['states'])
                    or champion!=snapshot['board']['champion_score']
                    or purpose!=snapshot['purpose']):
                raise IntegrityError('Settlement baselines differ from the frozen round')
            unresolved=db.execute("SELECT id FROM eval_tickets WHERE episode_id=? AND round_index=? AND status NOT IN('RESULT_RECORDED','NOT_STARTED')",(e,r)).fetchall()
            if unresolved:
                raise MaestroError('Cannot settle unresolved/unknown formal execution')
            ticket_rows=db.execute('SELECT * FROM eval_tickets WHERE episode_id=? AND round_index=?',(e,r)).fetchall()
            tickets={ticket['job_id']:ticket for ticket in ticket_rows}
            expected={job for job,ticket in tickets.items() if ticket['status']=='RESULT_RECORDED'}
            if len(results)!=len({result.job_id for result in results}) or {result.job_id for result in results}!=expected:
                raise IntegrityError('Settlement results must match every recorded round admission exactly once')
            by_agent={}
            for result in results:
                ticket=tickets[result.job_id]
                persisted=self.ledger.existing_result(result.job_id)
                if (result.candidate_hash!=ticket['candidate_hash'] or persisted is None
                        or canonical_json(result)!=canonical_json(persisted)):
                    raise IntegrityError('Settlement result differs from trusted durable evidence')
                a=ticket['agent_id']
                plan=plans.get(a)
                submitted=db.execute('SELECT plan_version_id FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',(e,r,a)).fetchone()
                if plan is None or not submitted or submitted[0]!=plan.version_id:
                    raise IntegrityError('Settlement plan differs from the frozen submission')
                persisted_plan=db.execute('SELECT payload FROM plan_versions WHERE id=?',(plan.version_id,)).fetchone()
                if not persisted_plan or canonical_json(plan)!=persisted_plan[0]:
                    raise IntegrityError('Settlement plan content differs from the frozen version')
                by_agent[a]=result
            for a,state in settled_states.items():
                before=Decimal(frozen[a]['wealth'])
                allocation=db.execute('SELECT * FROM final_allocations WHERE episode_id=? AND round_index=? AND agent_id=?',(e,r,a)).fetchone()
                payment=Decimal(allocation['payment']) if allocation else Decimal(0)
                result=by_agent.get(a)
                if allocation and bool(allocation['x'])!=(result is not None):
                    raise IntegrityError('Formal allocation and verified outcome disagree')
                if result is None and payment!=0:
                    raise IntegrityError('A non-admitted agent cannot retain a payment')
                earned=Decimal(0)
                parent_before=frozen[a]['parent_score']
                if result:
                    plan=plans[a]
                    earned=reward(result.score,parent_before,champion,scale=c.reward_scale,catchup=c.catchup_fraction,valid=result.valid)
                    record=HistoryRecord(result.job_id,a,parent_before,champion,result.score,
                        result.valid,plan.target,plan.mod_type,earned,result.status,result.candidate_hash)
                    state.history.append(record)
                    state.ucb_n+=1
                    state.ucb_total=money(state.ucb_total+earned)
                    if result.valid and result.score>parent_before:
                        state.parent_hash=result.candidate_hash
                        state.parent_score=result.score
                        if self.backend and hasattr(self.backend,'register_parent'):
                            candidate=db.execute('SELECT payload FROM candidates WHERE hash=?',(result.candidate_hash,)).fetchone()
                            self.backend.register_parent(result.candidate_hash,json.loads(candidate[0])['candidate_path'])
                    state.pending_plan=None
                    state.needs_refresh=True
                    self.ledger.usage(e,r,a,'formal',result.cpu_seconds,result.wall_seconds,payload={'purpose':purpose,'status':result.status,'simulated':c.mode=='mock'})
                    artifact_hash=self.artifacts.put_json(result)
                    public_results.append({'experiment_id':result.job_id,'candidate_hash':result.candidate_hash,
                        'agent_id':a,'score':result.score,'valid':result.valid,'status':result.status,'verified':True})
                    self.ledger.event('FORMAL_EVIDENCE',{'agent_id':a,'experiment_id':result.job_id,
                        'artifact_hash':artifact_hash,'gain':record.gain},e,r)
                after=money(before-payment+earned)
                state.wealth=after
                if not allocation:
                    db.execute('INSERT INTO final_allocations VALUES(?,?,?,?,?,?,?)',(e,r,a,None,0,'0','NOT_ADMITTED'))
                db.execute('INSERT INTO wallet_entries VALUES(?,?,?,?,?,?)',
                    (f'{e}:{r}:{a}:reward',e,a,'REWARD',str(earned),r))
                if money(self.ledger.balance(e,a))!=after:
                    raise IntegrityError('Wallet balance disagrees with frozen settlement')
                settlement={'parent_before':parent_before,'champion_before':champion,
                    'parent_after':state.parent_score,'parent_hash_after':state.parent_hash,
                    'result_job_id':result.job_id if result else None,'purpose':purpose}
                db.execute('INSERT INTO settlements VALUES(?,?,?,?,?,?,?,?)',
                    (e,r,a,str(payment),str(earned),str(before),str(after),canonical_json(settlement)))
                db.execute('UPDATE agents SET state_json=? WHERE episode_id=? AND agent_id=?',(canonical_json(state),e,a))
                db.execute('INSERT OR REPLACE INTO policy_statistics VALUES(?,?,?)',(e,a,canonical_json({'n':state.ucb_n,'sum_credit':state.ucb_total})))
            board=self._board(settled_states,r,public_results)
            checkpoint={'states':self._state_payload(settled_states),'board':board,'round_index':r}
            if self.backend and hasattr(self.backend,'export_checkpoint_memory'):
                checkpoint['private_memory_manifest']=self.backend.export_checkpoint_memory()
            checkpoint_hash=self.artifacts.put_json(checkpoint)
            db.execute('INSERT INTO checkpoints VALUES(?,?,?,?)',(e,r,checkpoint_hash,canonical_json(checkpoint)))
            db.execute('INSERT INTO public_board_versions VALUES(?,?,?)',(e,r,canonical_json(board)))
            db.execute("UPDATE rounds SET status='SETTLED' WHERE episode_id=? AND round_index=?",(e,r))
            db.execute("UPDATE eval_tickets SET status='SETTLED' WHERE episode_id=? AND round_index=? AND status='RESULT_RECORDED'",(e,r))
            db.execute('INSERT INTO publication_outbox VALUES(?,?,?)',(e,r,'PENDING'))
            self.ledger.event('ROUND_SETTLED',{'checkpoint_hash':checkpoint_hash,'board':board},e,r)
        states.clear()
        states.update(settled_states)
        self._publish(e,r,states,board)

    def _publish(self,e,r,states,board):
        from maestro.storage.projections import publish_views
        publish_views(self.root,e,r,states,board,self.artifacts)
        with self.ledger.transaction() as db:
            db.execute("UPDATE publication_outbox SET status='DONE' WHERE episode_id=? AND round_index=?",(e,r))

    def resume(self,episode_id):
        """Resume only from a fully settled boundary; reconcile in-flight jobs first."""
        db=self.ledger.connection
        row=db.execute('SELECT * FROM episodes WHERE id=?',(episode_id,)).fetchone()
        if not row:
            raise ValueError('Unknown episode')
        if row['config_hash']!=self.config.hash:
            raise IntegrityError('Resume config hash mismatch')
        if self.backend:
            if hasattr(self.backend,'begin_episode'):
                self.backend.begin_episode(episode_id)
            warm=db.execute('SELECT payload FROM warm_checkpoints WHERE id=?',(row['bootstrap_id'],)).fetchone()
            if warm:
                bootstrap=json.loads(warm[0])
                if hasattr(self.backend,'restore_warm_memory'):
                    self.backend.restore_warm_memory(bootstrap.get('private_memory_manifest',{}))
                if hasattr(self.backend,'set_directions'):
                    self.backend.set_directions(bootstrap['direction_briefs'])
            # Recovery must preserve the last settled main-arm memory before
            # settling an interrupted later round. Starting from warm memory
            # here would silently erase accumulated private lessons.
            prior=db.execute('SELECT * FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',(episode_id,)).fetchone()
            if prior:
                saved=json.loads(prior['payload'])
                if stable_hash(saved)!=prior['content_hash'] or self.artifacts.get_json(prior['content_hash'])!=saved:
                    raise IntegrityError('Resume checkpoint payload/CAS mismatch')
                if hasattr(self.backend,'restore_checkpoint_memory'):
                    self.backend.restore_checkpoint_memory(saved.get('private_memory_manifest',{}))
        unfinished=db.execute("SELECT * FROM rounds WHERE episode_id=? AND status!='SETTLED' ORDER BY round_index",(episode_id,)).fetchall()
        if unfinished:
            if len(unfinished)!=1 or unfinished[0]['status'] not in ('EVALUATING','SETTLING'):
                raise MaestroError('Interrupted preparation/generation requires explicit containment reconciliation; no work is silently replayed')
            interrupted=unfinished[0]
            r=interrupted['round_index']
            snapshot=json.loads(interrupted['snapshot_json'])
            states={a:state_from_dict(s) for a,s in snapshot['states'].items()}
            plans={}
            for a in states:
                decision=db.execute('SELECT payload FROM phase_transitions WHERE id=?',(f'{episode_id}:{r}:{a}:planning',)).fetchone()
                if decision:
                    states[a]=state_from_dict(json.loads(decision[0])['state'])
                    if states[a].pending_plan:
                        plans[a]=states[a].pending_plan
                failed=db.execute("SELECT 1 FROM invitations WHERE episode_id=? AND round_index=? AND agent_id=? AND status='FAILED'",(episode_id,r,a)).fetchone()
                if failed:
                    states[a].needs_refresh=True
            results=[]
            for ticket in db.execute('SELECT * FROM eval_tickets WHERE episode_id=? AND round_index=?',(episode_id,r)).fetchall():
                if ticket['status']=='ADMITTED':
                    results.append(self.dispatcher.dispatch(ticket['id'],ticket['job_id'],ticket['phase_epoch']))
                elif ticket['status']=='RESULT_RECORDED':
                    results.append(self.ledger.existing_result(ticket['job_id']))
                elif ticket['status']=='NOT_STARTED':
                    pass
                else:
                    raise MaestroError('Claimed/running/unknown job must be reconciled; refusing a duplicate physical launch')
            self._settle(episode_id,r,states,snapshot['states'],snapshot['board']['champion_score'],plans,results,snapshot['purpose'])
        checkpoint=db.execute('SELECT * FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',(episode_id,)).fetchone()
        if not checkpoint:
            raise MaestroError('No fully settled main checkpoint')
        payload=json.loads(checkpoint['payload'])
        if stable_hash(payload)!=checkpoint['content_hash'] or self.artifacts.get_json(checkpoint['content_hash'])!=payload:
            raise IntegrityError('Resume checkpoint payload/CAS mismatch')
        states={a:state_from_dict(s) for a,s in payload['states'].items()}
        if self.backend and hasattr(self.backend,'restore_checkpoint_memory'):
            self.backend.restore_checkpoint_memory(payload.get('private_memory_manifest',{}))
        if self.backend and hasattr(self.backend,'register_parent'):
            for state in states.values():
                candidate=db.execute('SELECT payload FROM candidates WHERE hash=?',(state.parent_hash,)).fetchone()
                if not candidate:
                    raise IntegrityError('Resume parent source missing')
                self.backend.register_parent(state.parent_hash,json.loads(candidate[0])['candidate_path'])
        for publication in db.execute("SELECT round_index FROM publication_outbox WHERE episode_id=? AND status='PENDING' ORDER BY round_index",(episode_id,)).fetchall():
            saved=db.execute('SELECT payload FROM checkpoints WHERE episode_id=? AND round_index=?',(episode_id,publication[0])).fetchone()
            data=json.loads(saved[0])
            self._publish(episode_id,publication[0],{a:state_from_dict(s) for a,s in data['states'].items()},data['board'])
        for index in range(checkpoint['round_index']+1,self.config.rounds):
            self._round(episode_id,index,states,row['policy'])
        with self.ledger.transaction() as conn:
            conn.execute("UPDATE episodes SET status='COMPLETED' WHERE id=?",(episode_id,))
        return {'episode_id':episode_id,'status':'COMPLETED','rounds':self.config.rounds}
