"""Market-controlled Coral agents using the pinned upstream CodexRuntime.

Warm and main share one backend. PREPARATION is read-only and separately gated;
PLAN_RO has exactly one tool-free request; GENERATION needs an EditGrant.
"""
from __future__ import annotations
from copy import deepcopy
import json
import threading
import time

from maestro.agents.planner import parse_plan_decision
from maestro.backends.codex_session.autonomous_backend import StrictCodexBackend, CodexPhaseUnknown
from maestro.backends.codex_session.tools import StageToolBroker
from maestro.llm import PlanningReceipt, PlanningResponseError, InferenceRequestUnknown
from maestro.types import stable_hash, canonical_json
from .native_client import CoralCodexClient, MCP_TOOL, MCP_NAMESPACE


class NativeCoralBackend(StrictCodexBackend):
    requires_fresh_plans = True
    preparation_capabilities = {'read_only': True, 'actual_request_gate': True,
                                'common_deadline': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._prepared = {}
        self._preparation_lock = threading.RLock()
        factory = self.broker_factory
        def guarded_broker(*args, **options):
            checker = options['grant_checker']
            def check():
                self._dispatch_guard()
                checker()
            return factory(*args, **{**options, 'grant_checker':check})
        self.broker_factory = guarded_broker

    def _dispatch_guard(self):
        with self._phase_lock:
            self._assert_clear()

    def begin_episode(self, episode_id, states=None):
        result = super().begin_episode(episode_id, states)
        with self._preparation_lock:
            self._prepared.clear()
        return result

    def _proxy(self, account, deadline, calls, *, tools=False):
        return self.proxy_factory(self.budget_store, account, self.client.model,
            deadline=deadline, max_calls=calls, max_request_bytes=16*1024*1024,
            max_output_tokens=96_000, allowed_tool_names=(MCP_TOOL,) if tools else (),
            tool_namespace=MCP_NAMESPACE if tools else None, request_guard=self._dispatch_guard,
            final_response_on_last_request=account.endswith(':PREPARATION'))

    def preflight(self, config=None):
        report = super().preflight(config)
        return {**report, 'generation_mode': 'coral_native_structured_implementation' if self.supports_runtime_only_repair else 'coral_native_codex_mcp',
                'preparation_mode': 'agent_selected_read_only',
                'agent_lifecycle': 'coral.agent.builtin.codex.CodexRuntime',
                'physical_runtime_qualified': False}

    def _identity_context(self, state):
        from maestro.tasks.taxonomy import TARGETS, MOD_TYPES
        return {'agent_id':state.agent_id,'parent_hash':state.parent_hash,
            'parent_score':state.parent_score,'taxonomy':{'targets':list(TARGETS),'mod_types':list(MOD_TYPES)},
            'direction':deepcopy(self._directions.get(state.agent_id,{})),
            'paths':{'code':'solver/','memory':'private/','board':'public/board.json',
                     'history':'history/index.json','lessons':'history/lessons.json'}}

    def _publish_own_history(self, workspace, state):
        from .workspace import remove_private_path
        if any(record.agent_id != state.agent_id for record in state.history):
            raise PermissionError('Own history contains a peer record')
        root=workspace.root/'history'
        if root.exists() or root.is_symlink():
            remove_private_path(root,workspace.root)
        root.mkdir(mode=0o700)
        index=[]
        for i,record in enumerate(state.history):
            name=f'record-{i:06d}.json'
            (root/name).write_text(canonical_json(record))
            index.append({'experiment_id':record.experiment_id,'path':'history/'+name,
                          'target':record.target,'mod_type':record.mod_type})
        (root/'index.json').write_text(canonical_json({'agent_id':state.agent_id,
            'parent_hash':state.parent_hash,'records':index}))
        (root/'lessons.json').write_text(canonical_json(state.lessons))

    def prepare_with_receipt(self, state, round_index, context, *, deadline):
        account = f'{self.episode_id}:{round_index}:{state.agent_id}:PREPARATION'
        key = (self.episode_id, round_index, state.agent_id)
        with self.budget_store._lock:
            previous=self.budget_store.connection.execute(
                'SELECT status FROM codex_phase_scopes WHERE account=?',(account,)).fetchone()
        if previous is not None:
            raise PermissionError('Preparation scope already exists; no retry or quota reset')
        with self._phase_lock:
            self._assert_clear()
        workspace = self._workspace(state)
        with self._preparation_lock:
            if key in self._prepared:
                raise PermissionError('Preparation sample already consumed; no automatic retry')
            self._prepared[key] = {'status': 'STARTED'}
        # _workspace may hold this agent's previous candidate. Bind the visible
        # solver to this round's frozen parent before any autonomous reads.
        self.workspaces.restore(workspace, self._parents.get(state.parent_hash, self.seed_solver))
        self._publish_own_history(workspace,state)
        assembled = {**self._identity_context(state),'episode_id':self.episode_id,'round_index':round_index}
        call_cap=self.config.preparation_calls
        budget_instruction=(
            'There is no model request-count cap for PREPARATION (preparation_calls=0). '
            'The controller still meters every actual request and enforces the common deadline '
            'and phase token, request-byte and tool budgets. Do not wait for the deadline: '
            'finish as soon as evidence is sufficient and leave time to return the final summary. '
            if call_cap==0 else
            f'You have at most {call_cap} actual model requests for this entire preparation phase, '
            'including this request and the final summary. Each model response requesting tools consumes '
            'one request; executing the broker tools does not itself consume another model request, '
            'but reasoning over their returned results requires another request. '
            f'Use at most {call_cap - 1} tool-calling model responses. '
            'Reserve the last request for your final evidence summary with NO tool calls. '
            'The controller disables tools on the last allowed request, regardless of the configured request count. '
            'When only one request remains, stop retrieving and return the summary immediately. ')
        prompt = (
            'PREPARATION: autonomously inspect your frozen parent code, own persistent memory, '
            'and the published board using the maestro MCP tool. Virtual roots are solver/, '
            'private/, public/, history/. Choose what to list/read/search; batch related reads. '
            'All files are read-only. No shell, code edits, evaluation or memory writes are allowed. '
            + budget_instruction +
            'Batch related list/read/search operations into the same response; avoid redundant confirmation reads. '
            'Tool results include preparation_budget with the actual consumed and remaining request counts. '
            'Finish earlier when evidence is sufficient. If evidence is incomplete, state the missing evidence '
            'and uncertainty in your final summary instead of requesting more tools. '
            'Return a concise evidence summary with relevant virtual paths and proposed rationale '
            'for a later, separate structured plan request. Do not implement code.\n' +
            json.dumps(assembled, ensure_ascii=False))
        policy=getattr(self.budget_store,'retry_policy',None)
        if policy:
            retry_limit=(self.budget_store.extra_request_limit()
                if hasattr(self.budget_store,'extra_request_limit') else policy.max_extra_requests)
            prompt=prompt.replace('actual model requests','logical model requests')
            prompt+=('\nNETWORK RECOVERY: the controller may repeat the identical failed HTTP request '
                f'at most {policy.max_retries_per_request} time(s), charged to a separate experiment-wide '
                f'{retry_limit}-request recovery budget. This is not an additional exploration '
                'turn. Logical turns govern the preparation cap; all physical attempts and unknown cost '
                'remain metered. Tool feedback reports both counters.')
        broker = StageToolBroker(workspace, phase='PREPARATION', deadline=deadline)
        read_trace = []
        def tool_call(name, arguments):
            if name != 'maestro_tool':
                raise PermissionError('Only the controller workspace tool is allowed')
            self._dispatch_guard()
            value = broker(arguments)
            measured=self._usage(account)
            used = measured.get('logical_calls',measured['calls'])
            remaining = max(0, call_cap - used) if call_cap else None
            value = {**value, 'preparation_budget': {
                'unit': 'logical_request_with_separately_metered_transport_retry' if 'logical_calls' in measured else 'actual_model_request',
                'actual_requests':measured['calls'],
                'retry_attempts':measured.get('retry_attempts',0),
                'configured_requests': call_cap,
                'maximum_requests': call_cap or None,
                'unlimited_requests': call_cap==0,
                'consumed_requests': used,
                'remaining_requests': remaining,
                'remaining_wall_seconds': max(0.,deadline-time.monotonic()),
                'next_response_must_be_final_summary': remaining == 1,
                'budget_exhausted': remaining == 0,
                'instruction': ('No request-count cap; finish the evidence summary before the common deadline. '
                                'Token, byte and tool budgets still apply.' if call_cap==0 else
                                'No model requests remain; do not issue another request.'
                                if remaining == 0 else
                                'Return your final evidence summary without tool calls.'
                                if remaining == 1 else
                                'Batch necessary reads and reserve one request for the final summary.'),
            }}
            read_trace.append({'arguments': deepcopy(arguments), 'result_hash': stable_hash(value),
                               'monotonic': time.monotonic()})
            return value
        started = time.monotonic()
        with self._phase(account):
            proxy = self._proxy(account, deadline, self.config.preparation_calls, tools=True)
            try:
                response = self.client.run_phase(prompt, base_url=proxy.start(), deadline=deadline,
                                                 tools=tool_call,
                    **({"agent_id":state.agent_id} if getattr(self.client,"supports_phase_identity",False) else {}))
                proxy.assert_resolved()
                usage = proxy.usage()
                if usage['protocol_violations'] or usage['terminal_failures']:
                    raise ValueError('Preparation response violated the request contract')
                logical=usage.get('logical_calls',usage['calls'])
                if logical<1 or (call_cap and logical>call_cap):
                    raise ValueError('Preparation requires metered requests within its quota')
                if time.monotonic() >= deadline:
                    raise TimeoutError('Preparation result arrived after common deadline')
                if len(response.text.encode()) > 128*1024:
                    raise ValueError('Preparation summary exceeds bounded plan context')
                evidence = {'status': 'PREPARED', 'agent_id': state.agent_id,
                    'parent_hash': state.parent_hash, 'summary': response.text,
                    'read_trace': read_trace, 'started_monotonic': started,
                    'accepted_monotonic': time.monotonic()}
                with self._preparation_lock:
                    self._prepared[key] = evidence
                return deepcopy(evidence), usage
            except Exception as error:
                if (self._blocking_requests(self._usage(account))
                        or getattr(error, 'inflight_unknown', False)):
                    raise InferenceRequestUnknown('Coral preparation needs reconciliation',
                        request_id=account, scope_id=account, context=assembled) from error
                raise
            finally:
                try:
                    proxy.close()
                finally:
                    broker.close()

    def plan_with_receipt(self, state, round_index, context, *, deadline):
        account = f'{self.episode_id}:{round_index}:{state.agent_id}:PLAN_RO'
        key = (self.episode_id, round_index, state.agent_id)
        with self._preparation_lock:
            prepared = deepcopy(self._prepared.get(key))
        if (not prepared or prepared.get('status') != 'PREPARED'
                or prepared['parent_hash'] != state.parent_hash):
            raise PermissionError('An accepted same-round preparation is required before planning')
        assembled = {**self._identity_context(state),'episode_id':self.episode_id,'round_index':round_index}
        assembled['preparation'] = prepared
        assembled['direction'] = deepcopy(self._directions.get(state.agent_id, {}))
        assembled['own_evidence_index'] = [
            {'experiment_id':record.experiment_id, 'target':record.target,
             'mod_type':record.mod_type, 'valid':record.valid, 'status':record.status}
            for record in state.history]
        from maestro.backends.codex_session.autonomous_backend import _PLAN_SCHEMA
        prompt = ('PLAN_RO: using your accepted preparation evidence, return one plan decision '
                  'matching the JSON schema. No tool calls or code changes. '
                  'For a revised plan, evidence_run_ids MUST include EVERY experiment_id in '
                  'own_evidence_index whose target AND mod_type match the proposed plan, '
                  'including invalid, failed and zero-improvement outcomes. Never invent IDs '
                  'or omit matching evidence. Other accessible own evidence may also be cited.\n'
                  +json.dumps(assembled, ensure_ascii=False))
        with self._phase(account):
            proxy = self._proxy(account, deadline, self.config.planning_calls)
            try:
                response = self.client.run_phase(prompt, base_url=proxy.start(), deadline=deadline,
                                                 output_schema=_PLAN_SCHEMA,
                    **({"agent_id":state.agent_id} if getattr(self.client,"supports_phase_identity",False) else {}))
                proxy.assert_resolved()
                usage = self._usage(account)
                diagnostics=proxy.usage()
                if diagnostics['protocol_violations'] or diagnostics['terminal_failures']:
                    raise ValueError('Planning violated the request contract')
                receipt = PlanningReceipt(account, account, stable_hash(assembled), response.response_hash,
                    self._planning_provider_id(account), usage.get('input_tokens'), usage.get('output_tokens'),
                    actual_requests=usage['calls'],logical_requests=usage.get('logical_calls'),
                    retry_attempts=usage.get('retry_attempts',0),
                    known_input_tokens=usage.get('known_input_tokens'),known_output_tokens=usage.get('known_output_tokens'))
                if usage.get('logical_calls',usage['calls']) != 1 or time.monotonic() >= deadline:
                    raise ValueError('Planning requires one timely metered request')
                decision = parse_plan_decision(response.text, state, round_index, namespace=self.episode_id)
                return decision, receipt, assembled
            except Exception as error:
                usage = self._usage(account)
                if self._blocking_requests(usage) or getattr(error, 'inflight_unknown', False):
                    raise InferenceRequestUnknown('Coral planning needs reconciliation',
                        request_id=account, scope_id=account, context=assembled) from error
                receipt = PlanningReceipt(account, account, stable_hash(assembled), stable_hash('rejected'), '',
                    usage.get('input_tokens'), usage.get('output_tokens'), actual_requests=usage.get('calls', 0))
                raise PlanningResponseError('Coral plan was not accepted', receipt=receipt, context=assembled) from error
            finally:
                proxy.close()


def native_factory(config, root=None):
    from maestro.backends.codex_session.autonomous_backend import strict_factory
    return strict_factory(config, root, client_factory=CoralCodexClient, backend_class=NativeCoralBackend)
