"""End-to-end warm start and synchronized V1 market episodes.

SQLite owns all admission and settlement decisions. Evaluators only receive
trusted, claimed tickets. Mock and real execution are explicitly different modes.
"""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from collections import Counter
from collections.abc import Mapping
import importlib
import json
import time
import traceback
import uuid

from maestro.config import RunConfig, CORAL_REVISION
from maestro.types import (AgentState, HistoryRecord, Submission, MaestroError,
    BootstrapIncomplete, IntegrityError, AuthorizationError, canonical_json,
    stable_hash, to_jsonable, state_from_dict)
from maestro.seeds import derive_seed
from maestro.storage import Ledger, ArtifactStore
from maestro.market.credit import reward, money
from maestro.market.policies import rank
from maestro.agents.valuation import estimate_value, estimate_legacy_value
from maestro.agents.bidding import cooperative_bid
from maestro.agents.planner import needs_refresh, mock_plan, validate_plan, PlanDecision, apply_decision
from maestro.agents.retrieval import build_context
from maestro.agents.visibility import agent_state_view
from maestro.bootstrap.director import make_direction_briefs
from maestro.bootstrap.warm_start import balanced_schedule, validate_warm_completion
from maestro.execution.dispatcher import Dispatcher
from maestro.execution.grants import GrantAuthority
from maestro.execution.budgets import BudgetStore
from maestro.execution.cpu_pool import GlobalCpuPool
from maestro.backends.mock import MockEvaluator
from maestro.tasks.base import benchmark_for
from maestro.execution.pause import PauseController
from maestro.execution.proposals import PrebidPipeline


def _new_id(prefix):
    return prefix + '-' + uuid.uuid4().hex[:16]


def _receipt_value(receipt,name,default=None):
    return receipt.get(name,default) if isinstance(receipt,dict) else getattr(receipt,name,default)


class ExperimentEngine(PrebidPipeline):
    def __init__(self, root, config: RunConfig, *, backend=None, evaluator=None, evaluation_only=False):
        from maestro.execution.pipeline import require_prebid,require_prebid_root
        require_prebid(config)
        require_prebid_root(root)
        if evaluation_only and (config.execution_mode != 'real' or backend is None):
            raise ValueError('Evaluation-only setup requires real execution and an injected runner backend')
        self.evaluation_only = evaluation_only
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
        self.pause = PauseController(self.root)
        self._pause_recorded = False
        if config.execution_mode == 'mock':
            if backend is not None:
                raise ValueError('Mock mode cannot silently use a live backend')
            self.evaluator = evaluator or MockEvaluator(seed=config.seed)
            self.panel_hash = stable_hash({'execution_mode':'mock','benchmark':config.benchmark,'instances':config.evaluation_instance_count,'seed':config.seed})
            self.contract_hash = stable_hash({'execution_mode':'mock','benchmark':config.benchmark,'reserved_cpu':config.reserved_cpu_seconds,'panel':self.panel_hash})
        else:
            try:
                self._prepare_live()
            except BaseException:
                self.budgets.close()
                self.ledger.close()
                raise
        self.dispatcher = Dispatcher(self.ledger, self.evaluator)
        if getattr(self.backend,'pause',None) is not None:
            self.pause = self.backend.pause
        (self.root / 'authority' / 'config.resolved.json').write_text(canonical_json(config.to_dict()),encoding='utf-8')
        if config.experiment_stage:
            (self.root / 'authority' / f'config.{config.experiment_stage}.resolved.json').write_text(canonical_json(config.to_dict()),encoding='utf-8')

    def _require_search(self):
        if self.evaluation_only:
            raise MaestroError('Evaluation-only setup cannot bootstrap, run or resume agents')

    def _prepare_live(self):
        """Every production prerequisite is explicit; there is no mock fallback."""
        benchmark = benchmark_for(self.config.benchmark)
        live = benchmark.live(self.config)
        self._live_panel, self._live_contract = live.panel, live.contract
        self.panel_hash = live.panel_hash
        self.contract_hash = live.contract_hash
        if self.backend is None:
            factory = self.config.coral.get('backend_factory')
            if not factory or ':' not in factory:
                raise MaestroError(f'execution_mode=real with benchmark={self.config.benchmark} requires a trusted coral.backend_factory module:callable or injected CoralBackend')
            module,name = factory.split(':',1)
            self.backend = getattr(importlib.import_module(module),name)(self.config,self.root)
        self.backend.preflight(self.config)
        if (not self.evaluation_only and
                (not getattr(self.backend,'supports_runtime_only_repair',False)
                 or not callable(getattr(self.backend,'activate_frozen_candidate',None)))):
            raise MaestroError('prebid_v2 requires separated implementation, frozen checking and runtime repair')
        if self.evaluator is None:
            runner = getattr(self.backend,'candidate_runner',None)
            self.evaluator = live.evaluator(runner=runner,artifact_store=self.artifacts.put,cpu_pool=self.cpu_pool)
        if isinstance(self.evaluator,MockEvaluator):
            raise MaestroError('MockEvaluator is forbidden with execution_mode=real')
        if hasattr(self.evaluator,'preflight'):
            self.evaluator.preflight()
        if not benchmark.seed_solver(self.config):
            raise MaestroError(f'{self.config.benchmark}.seed_solver is required')

    def close(self, *, timeout=30.0):
        if self._closed:
            return
        self._record_pause()
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

    def _record_pause(self):
        if self.pause.requested:
            self.pause.request()
            if not self._pause_recorded:
                self.ledger.event('PAUSE_REQUESTED',{'new_dispatch':False,
                    'drain_seconds':self.pause.grace_seconds,'automatic_restart':False})
                with self.ledger.transaction() as db:
                    db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE status='RUNNING'")
                self._pause_recorded = True

    def _check_pause(self):
        self._record_pause()
        self.pause.check()

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

    def _agent_payload(self,state,purpose):
        value=to_jsonable(state)
        if purpose=='WARM':value.pop('wealth',None)
        return value

    def _state_payload(self,states,purpose='MAIN'):
        return {a:self._agent_payload(s,purpose) for a,s in states.items()}

    def bootstrap(self):
        self._require_search()
        from maestro.execution.pipeline import require_prebid
        require_prebid(self.config)
        self._check_pause()
        c = self.config
        warm_config=c if c.experiment_stage=='warm_start' else replace(c,experiment_stage='warm_start')
        (self.root/'authority'/'config.warm_start.resolved.json').write_text(canonical_json(warm_config.to_dict()),encoding='utf-8')
        (self.root/'authority'/'config.resolved.json').write_text(canonical_json(warm_config.to_dict()),encoding='utf-8')
        if c.stage_configs and (c.experiment_stage!='warm_start' or c.hash!=c.for_stage('warm_start').hash):
            raise IntegrityError('Bootstrap requires the frozen warm_start stage')
        warm_id = _new_id('warm')
        agent_ids = [f'agent-{i+1}' for i in range(c.n_agents)]
        if c.execution_mode == 'mock':
            seed_hash = self.artifacts.put_json({'execution_mode':'mock','benchmark':c.benchmark,'solver':'common-seed-v1','seed':c.seed})
            seed_candidate = {'candidate_hash':seed_hash,'kind':'mock','seed':True}
        else:
            assert self.backend is not None  # _prepare_live installs one for every real run
            seed_candidate = self.backend.prepare_seed(benchmark_for(c.benchmark).seed_solver(c))
            seed_hash = seed_candidate['candidate_hash']
        states = {a:AgentState(a,seed_hash,0.0,Decimal(0)) for a in agent_ids}
        self.ledger.create_episode(warm_id,'warm',warm_config,states)
        if self.backend and hasattr(self.backend,'begin_episode'):
            self.backend.begin_episode(warm_id,states)
        if self.backend and hasattr(self.backend,'register_parent'):
            self.backend.register_parent(seed_hash,seed_candidate['candidate_path'])
        self.ledger.record_candidate(seed_hash,seed_candidate)
        initial_score = self._evaluate_seed(warm_id,states,seed_candidate)
        for state in states.values():
            state.parent_score = initial_score
        director_provider = getattr(self.backend,'director_provider',None) if self.backend else None
        briefs = make_direction_briefs(agent_ids,c.seed,directions=benchmark_for(c.benchmark).directions,provider=director_provider,
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
            fallback_enabled=bool(c.coral.get('deepseek_transport_retry',{}).get('warm_seed_fallback',False))
            validate_warm_completion(states,opportunities,c.warm_opportunities_per_agent,
                allow_seed_fallback=fallback_enabled,seed_hash=seed_hash,seed_score=initial_score)
        except BootstrapIncomplete:
            with self.ledger.transaction() as db:
                db.execute("UPDATE episodes SET status='BOOTSTRAP_INCOMPLETE' WHERE id=?",(warm_id,))
                self.ledger.event('BOOTSTRAP_INCOMPLETE',{'opportunities':dict(opportunities)},warm_id)
            raise
        fallback_agents={a:{'reason':'NO_FORMAL_WARM_HISTORY','parent_hash':seed_hash,
            'parent_score':initial_score,'formal_warm_evaluations':0} for a,s in states.items() if not s.history}
        if fallback_agents:self.ledger.event('WARM_SEED_FALLBACK',{'agents':fallback_agents},warm_id)
        payload = {'warm_id':warm_id,'schema_version':3,'config':warm_config.to_dict(),
            'config_hash':warm_config.hash,'shared_config_hash':warm_config.shared_config_hash,'contract_hash':self.contract_hash,'panel_hash':self.panel_hash,
            'coral_revision':CORAL_REVISION,'states':self._state_payload(states,'WARM'),
            'board':self._board(states,len(schedule)-1),'direction_briefs':briefs,
            'opportunities':dict(opportunities),'warm_waves':len(schedule),
            'seed_fallback_agents':fallback_agents}
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
            'warm_waves':len(schedule),'opportunities':dict(opportunities),
            'seed_fallback_agents':fallback_agents}

    def _evaluate_seed(self, episode_id: str, states: dict[str, AgentState], candidate: Mapping[str, object]) -> float:
        self._check_pause()
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

    def run(self, checkpoint, policy='first_price_naive', *, episode_id=None,
            legacy_warm_import=False):
        self._require_search()
        from maestro.execution.pipeline import require_prebid
        require_prebid(self.config)
        self._check_pause()
        from maestro.market.regimes import resolve_policy,contract
        policy=resolve_policy(policy)
        if policy in ('adjusted_valuation','agent_adjusted'):
            if not self.prebid_pipeline or self.config.execution_mode!='real' or self.config.valuation_discount_factor is None:
                raise ValueError('R4 requires real prebid_v2 and discounted actual-credit history')
            caps=getattr(self.backend,'valuation_capabilities',{})
            if (not callable(getattr(self.backend,'value_with_receipt',None))
                    or not all(caps.get(k) is True for k in ('read_only','actual_request_gate','fresh_session','no_retry'))):
                raise ValueError('R4 requires the qualified native Coral valuation interface')
        envelope = json.loads(Path(checkpoint).read_text(encoding='utf-8'))
        payload = envelope['payload']
        # Reject archives before rewriting any authority or creating a model scope.
        provenance = None
        if legacy_warm_import:
            from maestro.execution.legacy_warm_import import validate_legacy_warm
            provenance = validate_legacy_warm(checkpoint,envelope,self.config,self.contract_hash,self.panel_hash)
        else:
            require_prebid(payload['config'])
            if payload.get('schema_version') != 3:
                raise IntegrityError('Incompatible warm checkpoint; old experiments are not supported')
            warm_config = RunConfig.from_dict(payload['config'])
        if stable_hash(payload) != envelope['artifact_hash']:
            raise IntegrityError('Warm checkpoint integrity failure')
        if payload['contract_hash'] != self.contract_hash or payload['panel_hash'] != self.panel_hash:
            raise IntegrityError('Warm contract/panel mismatch')
        if not provenance:
            if payload['config_hash'] != warm_config.hash:
                raise IntegrityError('Warm configuration hash mismatch')
            if warm_config.stage_configs and payload.get('shared_config_hash') != warm_config.shared_config_hash:
                raise IntegrityError('Warm shared configuration hash mismatch')
            self.config.validate_warm_source(warm_config)
        (self.root/'authority'/'config.resolved.json').write_text(canonical_json(self.config.to_dict()),encoding='utf-8')
        states = {a:state_from_dict(s) for a,s in payload['states'].items()}
        for state in states.values():
            state.wealth = self.config.effective_initial_wealth
            state.pending_plan=None
            state.last_planning_round=-999
            state.needs_refresh=False
        episode_id = episode_id or _new_id(policy)
        regime_contract=contract(policy,self.config)
        with self.ledger.transaction():
            self.ledger.create_episode(episode_id,policy,self.config,states,payload['warm_id'])
            if provenance:
                self.ledger.event('LEGACY_WARM_SOURCE_IMPORTED',provenance,episode_id)
            contract_artifact=self.artifacts.put_json(regime_contract['contract'])
            self.ledger.event('REGIME_CONTRACT_FROZEN',{
                **regime_contract,'artifact_hash':contract_artifact},episode_id)
        contract_path=self.root/'arms'/episode_id/'regime-contract.json'
        contract_path.parent.mkdir(parents=True,exist_ok=True)
        contract_path.write_text(canonical_json(regime_contract)+'\n',encoding='utf-8')
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
            db.execute('INSERT INTO arm_clones VALUES(?,?,?)',(episode_id,payload['warm_id'],canonical_json({
                'source_hash':envelope['artifact_hash'],'fresh_sessions':True,
                **({'legacy_warm_import':provenance} if provenance else {}),
                'configured_credits':str(self.config.initial_wealth),
                'wealth':str(self.config.effective_initial_wealth)})))
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
            **({k:candidate[k] for k in ('proposal_hash','repair_chain')}
               if 'proposal_hash' in candidate else {}),
            'reserved_cpu_seconds':self.config.reserved_cpu_seconds,**settings}

    def _assert_provider_available(self, e, r):
        """Known provider rejections stop the episode, rather than spend waves.

        Read only trusted gateway evidence after the current parallel callbacks
        have drained. UNKNOWN handling takes precedence in the caller.
        """
        from contextlib import nullcontext
        store = getattr(self.backend, 'budget_store', None)
        db = getattr(store, 'connection', None)
        if db is None:
            return
        namespace=getattr(self.backend,'request_episode_id',e)
        prefixes=(e+':',namespace+':')
        with getattr(store, '_lock', nullcontext()):
            failures = []
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='codex_gateway_transport'").fetchone():
                failures = [dict(account=row[0],http_status=row[1],error_type=row[2],error_code=row[3])
                for row in db.execute("""SELECT r.account,t.http_status,t.error_type,t.error_code
                    FROM codex_gateway_transport t JOIN inference_reservations r USING(reservation_id)
                    WHERE t.known_rejected=1""") if row[0].startswith(prefixes)]
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='deepseek_request_events'").fetchone():
                failures += [dict(account=row[0],http_status=json.loads(row[1])['http_status'],
                                  error_type='known_provider_rejection',error_code='')
                    for row in db.execute('''SELECT r.account,t.data_json FROM deepseek_request_events t
                        JOIN inference_reservations r USING(reservation_id) WHERE t.kind='REJECTED' ''')
                    if row[0].startswith(prefixes)]
            if getattr(store,'retry_policy',None):
                failures=[f for f in failures if not store.replacement_complete(f['account'])
                    and not (f['http_status'] in (429,500,502,503,504) and store.abandonment_authorized(f['account']))]
        if failures:
            self.ledger.set_round_phase(e,r,'INFRASTRUCTURE_FAILURE')
            with self.ledger.transaction() as ledger_db:
                ledger_db.execute("UPDATE episodes SET status='STOPPED_INFRASTRUCTURE' WHERE id=?",(e,))
            self.ledger.event('PROVIDER_REJECTION_STOP',{'failures':failures,'retry':False},e,r)
            raise MaestroError('Provider rejected a request; stop subsequent waves/rounds without retry')

    def _prepare_agents(self, e, r, states, board):
        """All invited agents prepare in parallel before the planning window."""
        from maestro.agents.planning_batch import PlanOutcome
        deadline = time.monotonic()+self.config.preparation_window_seconds
        self.ledger.set_round_phase(e,r,'PREPARATION')
        self.ledger.event('PREPARATION_WINDOW_OPENED',
            {'deadline':time.time()+self.config.preparation_window_seconds,
             'max_requests_per_agent':self.config.preparation_calls or None,
             'configured_preparation_calls':self.config.preparation_calls,
             'unlimited_requests':self.config.preparation_calls==0},e,r)
        pool = ThreadPoolExecutor(max_workers=max(1,len(states)),thread_name_prefix='maestro-preparation')
        def execute(agent, state):
            began = time.monotonic()
            try:
                evidence, usage = self.backend.prepare_with_receipt(deepcopy(state),r,
                    {'board':deepcopy(board)},deadline=deadline)
                return evidence, usage, None, began, time.monotonic()
            except Exception as error:
                scope=f'{getattr(self.backend,"request_episode_id",e)}:{r}:{agent}:PREPARATION'
                usage=self.backend._usage(scope)
                return None, usage, error, began, time.monotonic()
        futures={pool.submit(execute,a,s):a for a,s in states.items()}
        self._generation_futures.update(futures)
        done,pending=wait(futures,timeout=max(0,deadline-time.monotonic()))
        if pending and getattr(self.backend,'failure_continuation_enabled',False):
            # Cleanup only: dispatch/admission deadlines remain unchanged.
            drained,pending=wait(pending,timeout=5.)
            done|=drained
            self.ledger.event('PHASE_CLEANUP_DRAIN',{'phase':'PREPARATION',
                'reclaimed_callbacks':len(drained),'pending_callbacks':len(pending),'deadline_extended':False},e,r)
        # A Future cancellation does not prove a running native CLI stopped.
        # Preserve the phase and stop progression if any callback is unresolved.
        prepared, failures, unknown = {}, {}, bool(pending)
        try:
            for future in done:
                agent=futures[future]
                evidence,usage,error,began,returned=future.result()
                uncertain=bool(getattr(error,'inflight_unknown',False) or usage.get('continuation_blocking_requests',usage.get('unresolved_requests',0)))
                unknown |= uncertain
                accepted=(evidence is not None and error is None
                          and evidence.get('accepted_monotonic',float('inf'))<deadline and returned<deadline)
                payload={'scope':f'{getattr(self.backend,"request_episode_id",e)}:{r}:{agent}:PREPARATION','usage':usage,
                    'status':'PREPARED' if accepted else 'UNKNOWN' if uncertain else 'ABSTAIN',
                    'request_count_unknown':uncertain,'usage_complete':usage.get('usage_complete',False),
                    'cpu_measurement_scope':'sandbox-only; no sandbox allowed in preparation',
                    'controller_native_cpu_seconds':None,
                    'error_type':type(error).__name__ if error else None,
                    'started_monotonic':began,'returned_monotonic':returned}
                if evidence is not None:
                    payload['evidence_hash']=self.artifacts.put_json(evidence)
                self.ledger.usage(e,r,agent,'preparation',wall=time.monotonic()-began,
                    calls=usage.get('calls',0),
                    tokens=usage.get('known_input_tokens',0)+usage.get('known_output_tokens',0),payload=payload)
                self.ledger.event('PREPARATION_OUTCOME',{'agent_id':agent,**payload},e,r)
                if accepted:
                    prepared[agent]=states[agent]
                else:
                    failures[agent]=PlanOutcome(agent,error='Preparation was not accepted',
                        status='INFLIGHT_UNKNOWN' if uncertain else 'ABSTAIN',inflight_unknown=uncertain)
            for future in pending:
                agent=futures[future]
                self.ledger.event('PREPARATION_OUTCOME',{'agent_id':agent,
                    'scope':f'{getattr(self.backend,"request_episode_id",e)}:{r}:{agent}:PREPARATION','status':'UNKNOWN',
                    'request_count_unknown':True,'reason':'CALLBACK_NOT_RECLAIMED_AT_DEADLINE'},e,r)
            if any(isinstance(future.result()[2],FileExistsError) for future in done):
                self.ledger.set_round_phase(e,r,'PREPARATION_FAILED')
                with self.ledger.transaction() as db:
                    db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                raise MaestroError('Preparation workspace collision; preserve evidence and reconcile before continuing')
            if unknown:
                self.ledger.set_round_phase(e,r,'PREPARATION_UNKNOWN')
                with self.ledger.transaction() as db:
                    db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                raise MaestroError('Preparation request/process unresolved; preserve evidence and reconcile')
            self._assert_provider_available(e,r)
            self.ledger.set_round_phase(e,r,'PLANNING')
            return prepared,failures
        finally:
            pool.shutdown(wait=False,cancel_futures=True)

    def _round(self,e,r,states,policy,purpose='MAIN',warm_invite=None):
        from maestro.execution.pipeline import require_prebid
        require_prebid(self.config)
        self._check_pause()
        c = self.config
        admission_capacity = len(warm_invite) if warm_invite is not None else c.slots
        evaluation_settings = self._evaluation_settings(admission_capacity)
        autonomous_preparation = bool(self.backend and callable(getattr(self.backend,'prepare_with_receipt',None)))
        planning_deadline = time.monotonic()+c.planning_window_seconds
        planning_expires = time.time()+c.planning_window_seconds
        frozen = self._state_payload(states,purpose)
        board = self._board(states,r-1)
        agent_board = board if purpose=='MAIN' else {}
        agent_states = {a:agent_state_view(state,purpose) for a,state in states.items()}
        if self.backend and hasattr(self.backend,'set_board'):
            self.backend.set_board(agent_board)
        champion = board['champion_score']
        snapshot = {'purpose':purpose,'states':frozen,'board':board,'seed':c.seed,
            'contract_hash':self.contract_hash,'panel_hash':self.panel_hash,
            'planning_deadline':None if autonomous_preparation else planning_expires,
            'preparation_window_seconds':c.preparation_window_seconds if autonomous_preparation else None,
            'generation_window_seconds':c.generation_window_seconds,
            'candidate_pipeline':c.coral.get('candidate_pipeline','postbid_v1'),
            'warm_invite':list(warm_invite) if warm_invite is not None else None,
            **evaluation_settings}
        recovery_namespace=getattr(self,'_recovery_namespace',None)
        if recovery_namespace:
            self.ledger.restart_reconciled_round(e,r,snapshot,recovery_namespace)
        else:
            self.ledger.open_round(e,r,snapshot)
        submissions,plans = [],{}
        plan_metadata={}
        plan_seed = derive_seed(c.seed,purpose,'planning')
        live_outcomes={}
        if c.execution_mode!='mock':
            from maestro.agents.planning_batch import parallel_plans
            refresh_states={a:s for a,s in states.items() if (getattr(self.backend,'requires_fresh_plans',False) or needs_refresh(s,r,c.refresh_interval))
                and (warm_invite is None or a in warm_invite)}
            preparation_failures={}
            if autonomous_preparation:
                refresh_states,preparation_failures=self._prepare_agents(
                    e,r,{a:agent_states[a] for a in refresh_states},agent_board)
                self._check_pause()
                planning_deadline=time.monotonic()+c.planning_window_seconds
                self.ledger.event('PLANNING_WINDOW_OPENED',
                    {'deadline':time.time()+c.planning_window_seconds},e,r)
            live_contexts={a:({'board':agent_board,'episode_id':e,'purpose':purpose} if autonomous_preparation
                else {**build_context(agent_states[a],agent_board),'episode_id':e,'purpose':purpose}) for a in refresh_states}
            live_outcomes=parallel_plans({a:agent_states[a] for a in refresh_states},live_contexts,self.backend,r,
                deadline=planning_deadline,
                budget_unit='session_turn' if c.profile=='codex_session_pilot' else 'provider_request')
            live_outcomes={**preparation_failures,**live_outcomes}
            for a,outcome in live_outcomes.items():
                receipt=outcome.receipt
                scope=f'{getattr(self.backend,"request_episode_id",e)}:{r}:{a}:PLAN_RO'
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
                tokens=int(measured.get('known_input_tokens',0))+int(measured.get('known_output_tokens',0)) if measured else ((_receipt_value(receipt,'input_tokens') or 0)+(_receipt_value(receipt,'output_tokens') or 0))
                self.ledger.usage(e,r,a,'planning',calls=calls,tokens=tokens,
                    payload={'simulated':False,'receipt':to_jsonable(receipt),'scope':scope,
                        'usage_complete':actual is not None and measured.get('usage_complete',receipt is not None),
                        'session_turns':_receipt_value(receipt,'session_turns',measured.get('session_turns',0)),
                        'request_count_unknown':outcome.inflight_unknown or bool(measured.get('request_count_unknown',False)) or (receipt is not None and actual is None),
                        'measurement':measured,'outcome_status':outcome.status})
                if outcome.error:
                    self.ledger.event('PLANNING_OUTCOME',{'agent_id':a,'status':outcome.status,
                        'error':outcome.error,'inflight_unknown':outcome.inflight_unknown,
                        'diagnostic':outcome.diagnostic},e,r)
            if any(o.inflight_unknown for o in live_outcomes.values()):
                self.ledger.set_round_phase(e,r,'PLANNING_UNKNOWN')
                with self.ledger.transaction() as db:
                    db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
                raise MaestroError('Unresolved planning inference at common deadline; phase remains isolated pending reconciliation')
            self._assert_provider_available(e,r)
        for a,state in states.items():
            if warm_invite is not None and a not in warm_invite:
                continue
            refresh = bool(getattr(self.backend,'requires_fresh_plans',False) or needs_refresh(state,r,c.refresh_interval))
            context = ({'agent_id':a,'parent_hash':state.parent_hash,'board':agent_board}
                       if autonomous_preparation else build_context(agent_states[a],agent_board))
            context_id = self.artifacts.put_json(context)
            with self.ledger.transaction() as db:
                db.execute('INSERT OR IGNORE INTO context_manifests VALUES(?,?)',(context_id,canonical_json(context)))
            withdraw_reason=None
            if refresh:
                plan=None
                if c.execution_mode == 'mock':
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
                            if decision.action=='withdraw':
                                withdraw_reason=decision.reason
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
                calls=1 if c.execution_mode=='mock' else (_receipt_value(receipt,'actual_requests') or 0)
                tokens=(_receipt_value(receipt,'input_tokens') or 0)+(_receipt_value(receipt,'output_tokens') or 0) if receipt else 0
                if c.execution_mode=='mock':
                    self.ledger.usage(e,r,a,'planning',calls=calls,tokens=tokens,
                        payload={'simulated':True,'context_hash':context_id,'usage_complete':True})
            else:
                plan = state.pending_plan
            with self.ledger.transaction() as db:
                db.execute('INSERT INTO phase_transitions VALUES(?,?)',
                    (f'{e}:{r}:{a}:planning'+(':recovery-1' if recovery_namespace else ''),
                     canonical_json({'phase':'PLANNED','state':self._agent_payload(state,purpose)})))
            if plan is None:
                self.ledger.event('ABSTAIN',{'agent_id':a,'reason':'WITHDRAWN','agent_reason':withdraw_reason},e,r)
                continue
            validate_plan(plan,state.parent_hash,[h.experiment_id for h in state.history])
            if plan.agent_id != a:
                raise AuthorizationError('Plan owner mismatch')
            plans[a]=plan
            plan_metadata[a]={'reused':not refresh,
                'refresh_reason':'DUE' if refresh else 'ELIGIBLE_PENDING','context_hash':context_id}
        self.ledger.set_round_phase(e,r,'IMPLEMENTATION')
        deadline=time.monotonic()+c.generation_window_seconds
        self.ledger.event('GENERATION_WINDOW_OPENED',{'deadline':time.time()+c.generation_window_seconds,
            'covers':['implementation','bidding','auction','precheck','runtime_repair','cleanup'],
            'paused_during':['valuation_ro'],
            'pipeline':'prebid_v2'},e,r)
        proposals=self._implement_all(e,r,agent_states,plans,purpose,deadline)
        self._assert_provider_available(e,r)
        self._check_pause()
        adjusted={}
        if policy in ('adjusted_valuation','agent_adjusted') and purpose=='MAIN' and proposals:
            from maestro.execution.valuation_phase import run_valuations,resume_generation_deadline,retire_failed_valuations
            valuation_started=time.monotonic()
            pause_brokers=getattr(self.backend,'pause_generation_brokers',None)
            resume_brokers=getattr(self.backend,'resume_generation_brokers',None)
            paused_accounts=(pause_brokers(r,plans,proposals,valuation_started,deadline)
                             if callable(pause_brokers) and callable(resume_brokers) else ())
            adjusted=run_valuations(self,e,r,states,plans,proposals,champion)
            retire_failed_valuations(self,e,r,states,proposals,adjusted)
            valuation_finished=time.monotonic()
            valuation_elapsed=valuation_finished-valuation_started
            # Valuation has its own common wall window. Resume the remaining
            # generation budget for bidding, precheck, repair, and cleanup.
            deadline=resume_generation_deadline(deadline,valuation_started,valuation_finished)
            if paused_accounts:
                resume_brokers(paused_accounts,valuation_finished,deadline)
            self.ledger.event('GENERATION_WINDOW_RESUMED',{
                'valuation_elapsed_seconds':valuation_elapsed,
                'deadline_monotonic':deadline,
                'remaining_seconds':max(0.,deadline-time.monotonic())},e,r)
            self._assert_provider_available(e,r)
        self.ledger.set_round_phase(e,r,'BIDDING')
        for a,plan in plans.items():
            if a not in proposals:continue
            state=states[a]
            if purpose=='WARM':value,bid,samples=Decimal(0),Decimal(0),[]
            elif not state.history and c.coral.get('deepseek_transport_retry',{}).get('warm_seed_fallback',False):
                value,bid,samples=Decimal(0),Decimal(0),[]
                self.ledger.event('COLD_START_ZERO_VALUATION',{'agent_id':a,'reason':'NO_FORMAL_HISTORY',
                    'parent_hash':state.parent_hash,'value':'0','sample_ids':[]},e,r)
            else:
                value,samples=(estimate_legacy_value(state.history,plan.target,plan.mod_type,state.parent_score,
                    champion,scale=c.reward_scale,catchup=c.catchup_fraction) if c.valuation_discount_factor is None
                    else estimate_value(state.history,gamma=c.valuation_discount_factor))
                bid=self._funded_bid(e,a,state,value,policy)
            if a in adjusted:
                value=Decimal(adjusted[a]['value'])
                bid=self._funded_bid(e,a,state,value,policy)
            submission=Submission(a,plan.version_id,value,bid)
            submissions.append(submission)
            self.ledger.submit(e,r,plan,submission,{**plan_metadata[a],'sample_ids':samples,
                'proposal_hash':proposals[a]['candidate_hash'],'candidate_pipeline':'prebid_v2'})
        order = list(warm_invite) if warm_invite is not None else rank(policy,submissions,states,r,c.seed,
            slots=c.slots,ucb_exploration_coefficient=c.ucb_exploration_coefficient,
            valuation_discount_factor=c.valuation_discount_factor)
        order = [a for a in order if a in plans and a in proposals]
        with self.ledger.transaction() as db:
            db.execute('INSERT INTO priority_orders VALUES(?,?,?)',(e,r,canonical_json({'policy':policy,'order':order})))
        self.ledger.set_round_phase(e,r,'CHECKING')
        bids = {s.agent_id:s.bid for s in submissions}
        successful = []
        injected_deadline = purpose=='MAIN' and r in c.failures.get('deadline_rounds',[])
        if not injected_deadline:
            successful = self._generate_invited(e,r,states,plans,order,bids,purpose,
                admission_capacity,deadline,proposals=proposals)
        self._assert_provider_available(e,r)
        self.ledger.set_round_phase(e,r,'ADMISSION')
        tickets=[]
        self._check_pause()
        for a,candidate,amount in successful:
            self._check_pause()
            ticket=self._ticket(e,r,a,candidate,plans[a],frozen[a]['parent_score'],champion,purpose,evaluation_settings)
            fault=c.failures.get(f'{a}:{r}',{}) if purpose=='MAIN' else {}
            if isinstance(fault,dict) and fault.get('formal'):
                ticket['simulated_failure']=fault['formal']
            self.ledger.admit(ticket,amount,admission_capacity)
            tickets.append(ticket)
        self.ledger.set_round_phase(e,r,'EVALUATING')
        # All admitted evaluations finish before publishing state/memory/board.
        def dispatch(ticket):
            self._check_pause()
            return self.dispatcher.dispatch(ticket['ticket_id'],ticket['job_id'])
        with ThreadPoolExecutor(max_workers=evaluation_settings['evaluation_batch_width']) as pool:
            futures=[pool.submit(dispatch,t) for t in tickets]
            results=[f.result() for f in futures]
        self._settle(e,r,states,frozen,champion,plans,results,purpose)

    def _funded_bid(self, episode_id, agent_id, state, value, policy):
        if policy not in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted'):
            return cooperative_bid(value)
        with self.ledger.transaction() as db:
            held=sum((Decimal(row[0]) for row in db.execute(
                "SELECT amount FROM holds WHERE episode_id=? AND agent_id=? AND status='HELD'",
                (episode_id,agent_id))),Decimal(0))
        return cooperative_bid(value,available_wealth=state.wealth-held)

    def _generate_invited(self,e,r,states,plans,order,bids,purpose,capacity,deadline,*,proposals):
        """Backfill reclaimed failures immediately, preserving frozen priority.

        A finished checking callback has closed its request/tool scopes and
        revoked its EditGrant. UNKNOWN callbacks never release a slot. Valid
        lower-priority candidates wait for earlier invitations before duplicate
        selection, so completion speed cannot change the priority winner.
        """
        successful, active, outcomes = [], {}, {}
        amounts = {}
        next_invitation = frontier = 0
        pool = ThreadPoolExecutor(max_workers=capacity,thread_name_prefix='maestro-generation')
        cleanup_deadline=None

        def mark_unknown(reason):
            self.ledger.set_round_phase(e,r,'GENERATION_UNKNOWN')
            with self.ledger.transaction() as db:
                db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
            self.ledger.event('GENERATION_INFERENCE_UNKNOWN',{'reason':reason},e,r)

        def invite_available():
            self._check_pause()
            nonlocal next_invitation
            # Completed candidates awaiting priority resolution still occupy
            # their invitations. Do not speculate beyond the allocated slots.
            occupied=len(successful)+len(active)+sum(c is not None for c in outcomes.values())
            while occupied<capacity and next_invitation<len(order) and time.monotonic()<deadline:
                a=order[next_invitation]
                next_invitation+=1
                amount=bids[a] if purpose=='MAIN' else Decimal(0)
                amounts[a]=amount
                self.ledger.invite(e,r,a,amount,{'plan_version_id':plans[a].version_id,
                    'generation_deadline_monotonic':deadline,
                    'generation_dispatch_monotonic':time.monotonic()})
                future=pool.submit(self._check_frozen_proposal,e,r,a,agent_state_view(states[a],purpose),plans[a],
                                   purpose,deadline,proposals[a])
                active[future]=a
                self._generation_futures.add(future)
                occupied+=1

        def resolve_priority():
            nonlocal frontier
            while frontier<next_invitation and order[frontier] in outcomes:
                a=order[frontier]
                candidate=outcomes.pop(a)
                frontier+=1
                if candidate is None:  # Failure hold was released immediately.
                    continue
                digest=candidate['candidate_hash']
                if any(item[1]['candidate_hash']==digest for item in successful):
                    states[a].needs_refresh=True
                    self.ledger.release_hold(e,r,a,'EXACT_DUPLICATE')
                    continue
                with self.ledger._lock:
                    bootstrap=self.ledger.connection.execute(
                        'SELECT bootstrap_id FROM episodes WHERE id=?',(e,)).fetchone()[0]
                    past=self.ledger.connection.execute(
                        'SELECT 1 FROM verified_results v JOIN eval_tickets t ON t.job_id=v.job_id '
                        'WHERE v.candidate_hash=? AND (t.episode_id=? OR t.episode_id=?) LIMIT 1',
                        (digest,e,bootstrap)).fetchone()
                if past:
                    states[a].pending_plan=None
                    states[a].needs_refresh=True
                    self.ledger.release_hold(e,r,a,'PREVIOUSLY_EVALUATED_CONTENT')
                    continue
                successful.append((a,candidate,amounts[a]))

        try:
            # Uniform/UCB invitations hold zero, just as before this change.
            if purpose=='MAIN':
                policy=self.ledger.connection.execute('SELECT policy FROM episodes WHERE id=?',(e,)).fetchone()[0]
                if policy not in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted'):
                    bids={a:Decimal(0) for a in bids}
            self._generation_futures={f for f in self._generation_futures if not f.done()}
            invite_available()
            while active:
                done,_=wait(active,timeout=max(0,deadline-time.monotonic()),return_when=FIRST_COMPLETED)
                if not done:
                    if getattr(self.backend,'failure_continuation_enabled',False):
                        if cleanup_deadline is None:cleanup_deadline=time.monotonic()+5.
                        done,_=wait(active,timeout=max(0,cleanup_deadline-time.monotonic()))
                        if done:
                            self.ledger.event('PHASE_CLEANUP_DRAIN',{'phase':'GENERATION',
                                'reclaimed_callbacks':len(done),'pending_callbacks':len(active)-len(done),
                                'deadline_extended':False},e,r)
                            # Late callbacks are failures, never admitted results.
                            for future in done:
                                if future.exception() is None:
                                    a=active.pop(future);outcomes[a]=None
                                    states[a].needs_refresh=True
                                    self.ledger.release_hold(e,r,a,'GENERATION_DEADLINE')
                            resolve_priority()
                            if not active:break
                            done={f for f in done if f in active}
                            if not done:continue
                    if done:pass
                    else:
                        for future in active:future.cancel()
                        mark_unknown('Generation callbacks unreturned at common deadline')
                        raise MaestroError('Generation callback unresolved at common deadline; containment reconciliation required')
                # Detect every uncertain callback in this completion batch
                # before releasing holds or starting any replacement.
                for future in done:
                    error=future.exception()
                    if (getattr(error,'inflight_unknown',False)
                            or getattr(error,'execution_unknown',False)
                            or getattr(error,'local_quiescent',None) is False):
                        mark_unknown(str(error))
                        raise error
                # Stable processing when several completions are observed at
                # once. Failure processing itself need not await a slow peer.
                for future in sorted(done,key=lambda f:order.index(active[f])):
                    a=active.pop(future)
                    candidate=future.result()
                    outcomes[a]=candidate
                    self.ledger.event('GENERATION_CALLBACK_COMPLETED',{'agent_id':a,
                        'status':'PRECHECK_PASSED' if candidate is not None else 'FAILED',
                        'monotonic':time.monotonic(),
                        'deadline_remaining_seconds':max(0,deadline-time.monotonic())},e,r)
                    if candidate is None:
                        states[a].needs_refresh=True
                        self.ledger.release_hold(e,r,a,'GENERATION_PRECHECK_FAILED')
                resolve_priority()
                invite_available()
            resolve_priority()
            return successful
        finally:
            # Retain futures for close()/reconciliation; cancelling a Future
            # cannot establish that its running CLI or sandbox stopped.
            pool.shutdown(wait=False,cancel_futures=True)

    def _generate_once_runtime_only(self,e,r,a,state,plan,purpose,deadline,*,initial_candidate):
        """Check an existing frozen proposal; only classified runtime errors repair.

        This is pre-admission checking. Formal outcomes never reenter this loop
        and no EvalTicket/evaluation budget is refunded or silently rerun.
        """
        from maestro.tasks.runtime_failure import RuntimePrecheckFailure
        from maestro.execution.budgets import BudgetExceeded
        candidate=initial_candidate
        repair_chain=[candidate['candidate_hash']]
        failure=None
        last_reason='DEADLINE'
        for attempt in range(1+self.config.runtime_repair_calls):
            self._check_pause()
            if time.monotonic()>=deadline:break
            if attempt and (not isinstance(failure,RuntimePrecheckFailure)
                            or not hasattr(self.backend,'repair_runtime')):break
            grant=self.grants.issue(episode_id=e,round_index=r,agent_id=a,
                plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=attempt,
                expires_at=time.time()+max(.001,deadline-time.monotonic()),
                purpose='postauction_check' if attempt==0 else 'runtime_repair',
                source_candidate_hash=candidate['candidate_hash'])
            with self.ledger.transaction() as db:
                db.execute('INSERT INTO edit_grants VALUES(?,?)',(grant.grant_id,canonical_json(grant)))
            started=time.monotonic()
            self.backend.last_generation_usage={}
            outcome=None
            try:
                if attempt==0:
                    self.backend.activate_frozen_candidate(state,plan,candidate,r,deadline,
                        grant=grant,grant_authority=self.grants)
                else:
                    self.ledger.event('RUNTIME_REPAIR_STARTED',{'agent_id':a,'repair_index':attempt-1,
                        'failed_candidate_hash':candidate['candidate_hash'],
                        'exception_type':failure.evidence.exception_type},e,r)
                    failed_hash=candidate['candidate_hash']
                    candidate=self.backend.repair_runtime(state,plan,candidate,failure.evidence,r,attempt-1,
                        deadline,grant=grant,grant_authority=self.grants)
                    candidate=self._register_source_candidate(candidate)
                    repair_chain.append(candidate['candidate_hash'])
                    self.ledger.event('RUNTIME_REPAIR_FROZEN',{'agent_id':a,
                        'plan_version_id':plan.version_id,'source_candidate_hash':failed_hash,
                        'candidate_hash':candidate['candidate_hash'],'repair_index':attempt-1,
                        'grant_id':grant.grant_id},e,r)
                self._check_pause()
                outcome=self.backend.precheck(candidate)
                passed,last_reason=outcome
            except (AuthorizationError,IntegrityError):
                raise
            except Exception as error:
                if (getattr(error,'inflight_unknown',False)
                        or getattr(error,'execution_unknown',False)
                        or getattr(error,'local_quiescent',None) is False):raise
                last_reason=type(error).__name__
                self.ledger.event('GENERATION_ATTEMPT_FAILED',{'agent_id':a,'revision':attempt,
                    'reason':last_reason,'message':str(error),'traceback':traceback.format_exc(),
                    'runtime_repair_eligible':False},e,r)
                break
            finally:
                self.grants.revoke(grant)
                usage=getattr(self.backend,'last_generation_usage',{})
                self.ledger.usage(e,r,a,'precheck' if attempt==0 else 'runtime_repair',
                    float(usage.get('cpu_seconds') or 0),
                    float(usage.get('wall_seconds') or time.monotonic()-started),
                    calls=int(usage.get('calls') or 0),
                    tokens=int(usage.get('known_input_tokens') or 0)+int(usage.get('known_output_tokens') or 0),
                    payload={'revision':attempt,'repair_index':attempt-1 if attempt else None,
                        'usage':usage,'grant_id':grant.grant_id,'simulated':False,
                        'usage_complete':bool(usage.get('usage_complete',False)),
                        'request_count_unknown':usage.get('calls') is None or bool(usage.get('continuation_blocking_requests',usage.get('unresolved_requests',0))),
                        'cost_usage_unknown':not bool(usage.get('usage_complete',False))})
            if time.monotonic()>=deadline:passed,last_reason=False,'DEADLINE';outcome=None
            with self.ledger.transaction() as db:
                db.execute('INSERT INTO prechecks VALUES(?,?,?,?,?,?,?)',
                    (f'{e}:{r}:{a}:{attempt}',e,r,a,candidate['candidate_hash'],int(passed),
                     canonical_json({'reason':last_reason,'revision':attempt,'toy_only':True,
                         'runtime_evidence':to_jsonable(outcome.evidence) if isinstance(outcome,RuntimePrecheckFailure) else None,
                         'runtime_repair_eligible':isinstance(outcome,RuntimePrecheckFailure)})))
            if passed:
                with self.ledger._lock:
                    old=self.ledger.connection.execute('SELECT payload FROM candidates WHERE hash=?',
                                                       (candidate['candidate_hash'],)).fetchone()
                    if old:
                        candidate={**candidate,'candidate_path':json.loads(old[0])['candidate_path']}
                    else:self.ledger.record_candidate(candidate['candidate_hash'],candidate)
                return {**candidate,'proposal_hash':initial_candidate['candidate_hash'],'repair_chain':repair_chain}
            failure=outcome
        self.ledger.event('GENERATION_FAILED',{'agent_id':a,'reason':last_reason,
            'runtime_only_repairs':True},e,r)
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
                before=Decimal(frozen[a]['wealth']) if purpose=='MAIN' else Decimal(0)
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
                    self.ledger.usage(e,r,a,'formal',result.cpu_seconds,result.wall_seconds,payload={'purpose':purpose,'status':result.status,'simulated':c.execution_mode=='mock'})
                    artifact_hash=self.artifacts.put_json(result)
                    public_results.append({'experiment_id':result.job_id,'candidate_hash':result.candidate_hash,
                        'agent_id':a,'score':result.score,'valid':result.valid,'status':result.status,'verified':True})
                    self.ledger.event('FORMAL_EVIDENCE',{'agent_id':a,'experiment_id':result.job_id,
                        'artifact_hash':artifact_hash,'gain':record.gain},e,r)
                after=money(before-payment+earned) if purpose=='MAIN' else Decimal(0)
                state.wealth=after
                if not allocation:
                    db.execute('INSERT INTO final_allocations VALUES(?,?,?,?,?,?,?)',(e,r,a,None,0,'0','NOT_ADMITTED'))
                if purpose=='MAIN':
                    db.execute('INSERT INTO wallet_entries VALUES(?,?,?,?,?,?)',
                        (f'{e}:{r}:{a}:reward',e,a,'REWARD',str(earned),r))
                if purpose=='MAIN' and money(self.ledger.balance(e,a))!=after:
                    raise IntegrityError('Wallet balance disagrees with frozen settlement')
                settlement={'parent_before':parent_before,'champion_before':champion,
                    'parent_after':state.parent_score,'parent_hash_after':state.parent_hash,
                    'result_job_id':result.job_id if result else None,'purpose':purpose}
                db.execute('INSERT INTO settlements VALUES(?,?,?,?,?,?,?,?)',
                    (e,r,a,str(payment),str(earned),
                     str(before) if purpose=='MAIN' else 'NOT_APPLICABLE',
                     str(after) if purpose=='MAIN' else 'NOT_APPLICABLE',canonical_json(settlement)))
                db.execute('UPDATE agents SET state_json=? WHERE episode_id=? AND agent_id=?',
                    (canonical_json(self._agent_payload(state,purpose)),e,a))
                db.execute('INSERT OR REPLACE INTO policy_statistics VALUES(?,?,?)',(e,a,canonical_json({'n':state.ucb_n,'sum_credit':state.ucb_total})))
            board=self._board(settled_states,r,public_results)
            checkpoint={'states':self._state_payload(settled_states,purpose),'board':board,'round_index':r}
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
        self._require_search()
        db=self.ledger.connection
        row=db.execute('SELECT * FROM episodes WHERE id=?',(episode_id,)).fetchone()
        if not row:
            raise ValueError('Unknown episode')
        from maestro.execution.pipeline import require_prebid
        require_prebid(self.config)
        require_prebid(json.loads(row['config_json']))
        from maestro.market.round_extension import latest_config, validate_extension, extension_payload
        previous_config, extension_events = latest_config(db,row)
        extending = previous_config.hash != self.config.hash
        if extending:
            funding_delta = validate_extension(previous_config,self.config)
            if row['policy']=='warm' or row['status']!='COMPLETED':
                raise IntegrityError('Market rounds can extend only at a completed episode boundary')
            boundary = db.execute('SELECT round_index FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',
                                  (episode_id,)).fetchone()
            if not boundary or boundary['round_index']!=previous_config.rounds-1:
                raise IntegrityError('Market round extension needs the completed final checkpoint')
            if db.execute("SELECT 1 FROM rounds WHERE episode_id=? AND status!='SETTLED' LIMIT 1",(episode_id,)).fetchone():
                raise IntegrityError('Market round extension has an unsettled round')
            if db.execute("SELECT 1 FROM publication_outbox WHERE episode_id=? AND status!='DONE' LIMIT 1",(episode_id,)).fetchone():
                raise IntegrityError('Market round extension has unpublished results')
        else:
            funding_delta=Decimal(0)
        # A clean round boundary must replay before any further model dispatch.
        # An interrupted evaluation follows the separate physical reconciliation
        # path below and cannot be replayed as a fully settled episode yet.
        if not db.execute("SELECT 1 FROM rounds WHERE episode_id=? AND status!='SETTLED' LIMIT 1",
                          (episode_id,)).fetchone():
            from maestro.replay import verify_ledger
            verify_ledger(self.root,episode_id)
        if row['policy']!='warm' and previous_config.initial_wealth==0:
            clone=db.execute('SELECT manifest FROM arm_clones WHERE episode_id=?',(episode_id,)).fetchone()
            funding_manifest=json.loads(clone[0]) if clone else {}
            if (funding_manifest.get('configured_credits')!='0'
                    or Decimal(str(funding_manifest.get('wealth','-1')))!=RunConfig.from_dict(json.loads(row['config_json'])).effective_initial_wealth):
                raise IntegrityError('Legacy zero-wallet episode cannot resume under sufficient-funding semantics')
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
        if len(unfinished)>1 or (unfinished and unfinished[0]['status'] not in ('EVALUATING','SETTLING','RESTART_AUTHORIZED')):
            raise MaestroError('Interrupted preparation/generation requires explicit containment reconciliation; no work is silently replayed')
        if unfinished and unfinished[0]['status']!='RESTART_AUTHORIZED':
            interrupted=unfinished[0]
            r=interrupted['round_index']
            snapshot=json.loads(interrupted['snapshot_json'])
            states={a:state_from_dict(s) for a,s in snapshot['states'].items()}
            plans={}
            restarted=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='ROUND_RESTARTED' ORDER BY id DESC LIMIT 1",(episode_id,r)).fetchone()
            phase_suffix=':recovery-1' if restarted else ''
            for a in states:
                decision=db.execute('SELECT payload FROM phase_transitions WHERE id=?',(f'{episode_id}:{r}:{a}:planning'+phase_suffix,)).fetchone()
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
        # A completed episode still has its original agent workspaces. Each
        # continuation needs a fresh local namespace while restoring code and
        # private memory from the verified checkpoint above. Keep the old files
        # as evidence and fail closed if this continuation was already used.
        if (not unfinished and checkpoint['round_index']+1 < self.config.rounds
                and self.backend and callable(getattr(self.backend,'isolate_recovered_workspaces',None))):
            namespace=f'{episode_id}-continuation-r{checkpoint["round_index"]+1}-to{self.config.rounds}'
            self.backend.isolate_recovered_workspaces(namespace)
        for publication in db.execute("SELECT round_index FROM publication_outbox WHERE episode_id=? AND status='PENDING' ORDER BY round_index",(episode_id,)).fetchall():
            saved=db.execute('SELECT payload FROM checkpoints WHERE episode_id=? AND round_index=?',(episode_id,publication[0])).fetchone()
            data=json.loads(saved[0])
            self._publish(episode_id,publication[0],{a:state_from_dict(s) for a,s in data['states'].items()},data['board'])
        if extending:
            event=extension_payload(previous_config,self.config,states)
            with self.ledger.transaction() as conn:
                for agent_id,state in states.items():
                    state.wealth=money(state.wealth+funding_delta)
                    if funding_delta:
                        conn.execute('INSERT INTO wallet_entries VALUES(?,?,?,?,?,?)',
                                     (f'{episode_id}:round-extension:{self.config.rounds}:{agent_id}',
                                      episode_id,agent_id,'ROUND_EXTENSION',str(funding_delta),None))
                    conn.execute('UPDATE agents SET state_json=? WHERE episode_id=? AND agent_id=?',
                                 (canonical_json(self._agent_payload(state,'MAIN')),episode_id,agent_id))
                self.ledger.event('MARKET_ROUNDS_EXTENDED',event,episode_id,previous_config.rounds-1)
                conn.execute("UPDATE episodes SET status='RUNNING' WHERE id=?",(episode_id,))
        else:
            # A process can restart after the extension event and before its
            # first new round. Apply its committed funding to checkpoint state.
            for extension in extension_events:
                record=json.loads(extension['payload'])
                if record['checkpoint_round_index']==checkpoint['round_index']:
                    extra=Decimal(record['funding_per_agent'])
                    for state in states.values():
                        state.wealth=money(state.wealth+extra)
        if row['status']=='PAUSED':
            with self.ledger.transaction() as conn:
                conn.execute("UPDATE episodes SET status='RUNNING' WHERE id=?",(episode_id,))
                self.ledger.event('ROUND_BOUNDARY_RESUMED',{
                    'checkpoint_round_index':checkpoint['round_index'],
                    'next_round_index':checkpoint['round_index']+1},episode_id)
        for index in range(checkpoint['round_index']+1,self.config.rounds):
            recovery=db.execute('SELECT status FROM rounds WHERE episode_id=? AND round_index=?',(episode_id,index)).fetchone()
            if recovery:
                if recovery['status']!='RESTART_AUTHORIZED':
                    raise MaestroError('Unfinished round has not passed explicit containment reconciliation')
                namespace=f'{episode_id}-recovery-r{index}-a1'
                authorization=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? "
                    "AND kind='PREBID_RECOVERY_AUTHORIZED' ORDER BY id DESC LIMIT 1",(episode_id,index)).fetchone()
                if (not authorization or json.loads(authorization[0]).get('request_namespace')!=namespace):
                    raise IntegrityError('Recovery authority event or request namespace missing')
                if self.backend is None:
                    raise MaestroError('Recovery requires a real backend with isolated request accounting')
                if not callable(getattr(self.backend,'isolate_recovered_workspaces',None)):
                    raise MaestroError('Recovery backend cannot isolate prior workspaces')
                self.backend.isolate_recovered_workspaces(namespace)
                self.backend.request_episode_id=namespace
                self._recovery_namespace=namespace
            try:
                self._round(episode_id,index,states,row['policy'])
            finally:
                if recovery:
                    self._recovery_namespace=None
                    self.backend.request_episode_id=episode_id
        with self.ledger.transaction() as conn:
            conn.execute("UPDATE episodes SET status='COMPLETED' WHERE id=?",(episode_id,))
        return {'episode_id':episode_id,'status':'COMPLETED','rounds':self.config.rounds}
