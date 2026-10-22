"""Single-use 3x4 real timing calibration. Never bootstraps or evaluates a panel."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
import argparse
import hashlib
import json
import math
import os
import signal
import sqlite3
import statistics
import subprocess
import threading
import traceback
import time
import uuid

from maestro.agents.planner import apply_decision
from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.backends.coral.runtime import load_factory
from maestro.llm import PlanningResponseError
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.backends.codex_session.tools import StageToolBroker, ToolScopeExpired
from maestro.bootstrap.director import make_direction_briefs
from maestro.config import load_config
from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.execution.grants import GrantAuthority
from maestro.execution.sandbox import DockerRunner, Mount
from maestro.tasks.pace.manifest import VERIFIER_SOURCE_FILES
from maestro.types import AgentState

IMAGE = 'python@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641'
LOCAL = threading.local()
DEBUG_COMMAND = 'python3 run < /input/calibration.gr | python3 /input/calibration-check.py /input/calibration.gr'


def utc():
    return datetime.now(timezone.utc).isoformat()


class CalibrationInfrastructureError(RuntimeError):
    pass


class CalibrationHalted(RuntimeError):
    pass


class StopState:
    """Monotone stop latch; UNKNOWN always takes precedence over known failure."""
    def __init__(self):
        self.event = threading.Event()
        self.lock = threading.RLock()
        self.reason = None
        self.kind = None

    def is_set(self):
        return self.event.is_set()

    def stop(self, kind, reason):
        with self.lock:
            if self.kind is None or kind == 'UNKNOWN':
                self.kind, self.reason = kind, reason
            self.event.set()


def exception_result(error, halt):
    # The strict adapter wraps pre-dispatch client failures as plan rejection.
    # Preserve their stack and classify the cause, rather than treating an
    # IndexError/transport implementation fault as a legitimate withdrawn plan.
    cause = error.__cause__
    if isinstance(error, PlanningResponseError) and cause is not None:
        detail = exception_result(cause, halt)
        detail['wrapper_error'] = type(error).__name__
        detail['traceback'] = ''.join(traceback.format_tb(error.__traceback__)) + detail['traceback']
        if isinstance(cause, ValueError) and not isinstance(cause, CalibrationInfrastructureError):
            detail['status'] = 'FAILED'
        return detail
    # Traceback frames without locals/exception chains cannot export native
    # argv capabilities or HTTP authentication headers in exception messages.
    safe = isinstance(error, (IndexError, KeyError, TypeError, ValueError,
                              TimeoutError, CalibrationInfrastructureError))
    message = str(error)[:512] if safe else type(error).__name__
    result = {'error': type(error).__name__, 'message': message,
              'traceback': ''.join(traceback.format_tb(error.__traceback__)) + type(error).__name__ + ': ' + message}
    if getattr(error, 'inflight_unknown', False) or getattr(error, 'execution_unknown', False):
        result['status'] = 'UNKNOWN'
        halt.stop('UNKNOWN', type(error).__name__)
    elif isinstance(error, CalibrationHalted):
        result['status'] = 'CANCELLED'
    elif isinstance(error, TimeoutError):
        result['status'] = 'TIMEOUT'
    elif isinstance(error, (PlanningResponseError, BudgetExceeded, ToolScopeExpired)):
        result['status'] = 'FAILED'
    elif isinstance(error, ValueError):
        result['status'] = 'FAILED'
    else:
        result['status'] = 'INFRASTRUCTURE_FAILURE'
        halt.stop('INFRASTRUCTURE', type(error).__name__)
    return result


def observe_sample(result, halt):
    if result['status'] == 'UNKNOWN':
        halt.stop('UNKNOWN', result.get('error', 'unconfirmed execution'))
    elif result['status'] == 'INFRASTRUCTURE_FAILURE':
        halt.stop('INFRASTRUCTURE', result.get('error', 'execution-chain failure'))


def execute_phases(wave, ids, workers, windows, audit, halt, samples):
    """One common deadline per phase; never enter the next phase after a fault."""
    for phase in ('planning', 'generation'):
        if halt.is_set():
            break
        opened = audit.emit('phase_open', wave=wave, phase=phase, window_seconds=windows[phase])
        deadline = opened + windows[phase]
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(workers[phase], agent, opened, deadline): agent for agent in ids}
            for future in as_completed(futures):
                try:
                    sample = future.result()
                except BaseException as error:
                    sample = {'wave': wave, 'agent': futures[future], 'phase': phase,
                              'opened': opened, 'dispatched': opened, 'deadline': deadline,
                              **exception_result(error, halt)}
                    sample['returned'] = audit.emit('controller_return', sample)
                    sample['elapsed_seconds'] = sample['returned'] - opened
                    audit.sample(sample)
                observe_sample(sample, halt)
                samples.append(sample)
                print(json.dumps({k: sample.get(k) for k in
                    ('wave', 'agent', 'phase', 'status', 'elapsed_seconds')}), flush=True)
        audit.emit('phase_closed', wave=wave, phase=phase)


class Audit:
    def __init__(self, root, *, append=False):
        self.root = root
        self.lock = threading.RLock()
        self.db = sqlite3.connect(root / 'calibration.sqlite', check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute(('CREATE TABLE IF NOT EXISTS ' if append else 'CREATE TABLE ') + 'events(id INTEGER PRIMARY KEY,monotonic REAL,utc TEXT,event TEXT,payload TEXT)')
        self.db.execute(('CREATE TABLE IF NOT EXISTS ' if append else 'CREATE TABLE ') + 'samples(wave INTEGER,agent TEXT,phase TEXT,status TEXT,result TEXT,PRIMARY KEY(wave,agent,phase))')
        self.db.commit()
        self.log = (root / 'events.jsonl').open('a' if append else 'x')

    def emit(self, event, data=None, **fields):
        row = {**getattr(LOCAL, 'scope', {}), **(data or {}), **fields}
        mono, timestamp = time.monotonic(), utc()
        with self.lock:
            self.db.execute('INSERT INTO events(monotonic,utc,event,payload) VALUES(?,?,?,?)',
                            (mono, timestamp, event, json.dumps(row, sort_keys=True)))
            self.db.commit()
            self.log.write(json.dumps({'monotonic': mono, 'utc': timestamp, 'event': event, **row}, sort_keys=True) + '\n')
            self.log.flush()
        return mono

    def close(self):
        with self.lock:
            self.log.close()
            self.db.close()

    def sample(self, result):
        with self.lock:
            self.db.execute('INSERT INTO samples VALUES(?,?,?,?,?)',
                (result['wave'], result['agent'], result['phase'], result['status'], json.dumps(result)))
            self.db.commit()


class CalibrationBudgets(BudgetStore):
    """Durable global 48-reservation cap in addition to per-sample 1/3 gates."""
    def __init__(self, path, episodes, *, max_reservations=48):
        if type(max_reservations) is not int or not 1 <= max_reservations <= 48:
            raise ValueError('Calibration request ceiling must be within 1..48')
        self.max_reservations = max_reservations
        super().__init__(path)
        self.episodes = set(episodes)
        self.bindings = {}
        self.connection.execute('CREATE TABLE IF NOT EXISTS codex_phase_scopes(account TEXT PRIMARY KEY,status TEXT NOT NULL)')

    def create(self, account, **limits):
        parts = account.split(':')
        if (len(parts) < 4 or parts[0] not in self.episodes or parts[1] != '0'
                or parts[2] not in {f'agent-{i}' for i in range(1, 5)}
                or parts[3] not in {'PLAN_RO', 'GENERATION'}):
            raise BudgetExceeded('Account outside fixed twelve samples')
        key = tuple(parts[:4])
        cap = 1 if parts[3] == 'PLAN_RO' else 3
        with self._lock:
            if limits['max_calls'] != cap or self.bindings.get(key, account) != account:
                raise BudgetExceeded('Cannot change sample plan/account or request cap')
            self.bindings[key] = account
            return super().create(account, **limits)

    def close(self):
        # Shared across independent sample-wave backends; driver owns final close.
        pass

    def reserve(self, *args, **kwargs):
        with self._lock:
            if self.connection.execute('SELECT COUNT(*) FROM inference_reservations').fetchone()[0] >= self.max_reservations:
                raise BudgetExceeded('Global calibration envelope consumed')
            return super().reserve(*args, **kwargs)


def union_seconds(intervals):
    end = total = 0.0
    for start, stop in sorted(intervals):
        if stop > end:
            total += stop - max(start, end)
            end = stop
    return total


def summarize(root, audit, budgets, samples, opens=None, *, halt=None, final=False, execution_kind='live_timing', expected_samples=12):
    events = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
    for sample in samples:
        relevant = [e for e in events if all(e.get(k) == sample[k] for k in ('wave', 'agent', 'phase'))]
        posts = [e['monotonic'] for e in relevant if e['event'] == 'provider_post']
        terms = [e['monotonic'] for e in relevant if e['event'] == 'provider_terminal']
        provider = list(zip(posts, terms))
        tools, cleanup = [], []
        starts = {}
        for e in relevant:
            if e['event'] in ('tool_start', 'native_stop_start'):
                starts[e['span']] = e['monotonic']
            elif e['event'] in ('tool_end', 'native_stop_end') and e['span'] in starts:
                (tools if e['event'] == 'tool_end' else cleanup).append((starts[e['span']], e['monotonic']))
        sample.update(dispatch_queue_seconds=sample['dispatched'] - sample['opened'],
            first_post_seconds=posts[0] - sample['opened'] if posts else None,
            provider_wait_union_seconds=union_seconds(provider),
            tool_union_seconds=union_seconds(tools), native_cleanup_union_seconds=union_seconds(cleanup),
            measured_union_seconds=union_seconds(provider + tools + cleanup))
    distributions = {}
    for phase in ('planning', 'generation'):
        rows = [s for s in samples if s['phase'] == phase]
        normal = [s['elapsed_seconds'] for s in rows if s['status'] == 'NORMAL']
        maxima = {str(w): max((s['returned'] - s['opened'] for s in rows if s['wave'] == w), default=None) for w in range(1, expected_samples // 4 + 1)}
        sufficient = (final and len(normal) == 12 and not (halt and halt.is_set())
                      and execution_kind == 'live_timing')
        recommended = math.ceil(max(normal) * 1.25 / 30) * 30 if sufficient else None
        distributions[phase] = {'normal_n': len(normal), 'median_seconds': statistics.median(normal) if normal else None,
            'max_seconds': max(normal) if normal else None, 'wave_return_max_seconds': maxima,
            'provisional_window_seconds': recommended, 'basis': 'ceil(max normal open-to-accept * 1.25 / 30) * 30',
            'evidence_sufficient_for_final_window': sufficient,
            'incomplete_or_failed': [dict(wave=s['wave'], agent=s['agent'], status=s['status']) for s in rows if s['status'] != 'NORMAL'],
            'observed_returns_exceeding_recommendation': [dict(wave=s['wave'], agent=s['agent'], status=s['status']) for s in rows if recommended is not None and s['returned'] - s['opened'] > recommended],
            'limitation': '12 samples do not establish population tail quantiles. Censored/failed samples are retained, not treated as normal times.'}
    with budgets._lock:
        requests = [dict(r) for r in budgets.connection.execute('SELECT * FROM inference_reservations')]
        receipts = [dict(r) for r in budgets.connection.execute('SELECT * FROM provider_receipts')]
        scopes = [dict(r) for r in budgets.connection.execute('SELECT * FROM codex_phase_scopes')]
    unknown = (any(s['status'] == 'UNKNOWN' for s in samples)
        or any(r['status'] != 'COMPLETED' for r in requests)
        or any(r['status'] in ('UNKNOWN', 'DISPATCHING') for r in scopes)
        or (halt is not None and halt.kind == 'UNKNOWN'))
    if unknown and halt is not None:
        halt.stop('UNKNOWN', 'Unresolved durable request or phase scope')
    stopped = halt is not None and halt.is_set()
    complete = len(samples) == expected_samples * 2 and all(s['status'] == 'NORMAL' for s in samples)
    status = ('STOPPED_UNKNOWN' if unknown else 'STOPPED_INFRASTRUCTURE' if stopped
              else 'COMPLETED' if final and complete else 'PARTIAL')
    summary = {'status': status, 'execution_kind': execution_kind,
        'sampling_complete': len(samples) == expected_samples * 2, 'expected_agent_samples': expected_samples, 'final': final,
        'stop_reason': halt.reason if halt else None,
        'finished_utc': utc(), 'samples': samples, 'windows': distributions,
        'actual_requests': len(requests) if execution_kind == 'live_timing' else 0,
        'offline_protocol_reservations': len(requests) if execution_kind != 'live_timing' else 0, 'max_requests': budgets.max_reservations, 'requests': requests, 'provider_receipts': receipts,
        'phase_scopes': scopes, 'input_tokens_known': sum(r['input_tokens'] or 0 for r in receipts),
        'output_tokens_known': sum(r['output_tokens'] or 0 for r in receipts),
        'token_usage_complete': len(receipts) == len(requests) and all(r['usage_complete'] for r in receipts),
        'tool_cpu_seconds': sum(s.get('tool_cpu_seconds', 0) for s in samples),
        'fixed_precheck_cpu_seconds': sum(s.get('precheck_cpu_seconds', 0) for s in samples),
        'cpu_scope': 'Measured cgroup CPU of tools/debug/precheck. Controller/native CPU not independently metered; unknown, not zero.',
        'formal_warm_started': False, 'formal_evaluations': 0, 'model_tool_overlap': 'Interval unions used; provider/tool/cleanup columns are not additive.'}
    temporary = root / 'report.json.tmp'
    temporary.write_text(json.dumps(summary, indent=2))
    temporary.replace(root / 'report.json')
    (root / 'suggested-windows.json').write_text(json.dumps({phase + '_window_seconds': info['provisional_window_seconds'] for phase, info in distributions.items()}, indent=2))
    return summary


def calibration_config(config, *, image=IMAGE):
    if config.execution_mode != 'real' or config.benchmark != 'pace':
        raise ValueError('Calibration requires execution_mode=real and benchmark=pace')
    factory = config.coral.get('backend_factory')
    if factory not in ('maestro.backends.codex_session:strict_v1_factory',
                       'maestro.backends.codex_session.autonomous_backend:strict_factory',
                       'maestro.backends.coral.native_backend:native_factory'):
        raise ValueError('The 48-request pilot is qualified only for strict native Codex without preparation')
    if (config.n_agents, config.planning_calls, config.generation_calls) != (4, 1, 3):
        raise ValueError('Calibration requires four agents with unchanged 1/3 request caps')
    # The evolving template now enables native preparation. This independent
    # pilot deliberately retains the authorized strict 1+3 request lifecycle.
    coral = {**config.coral, 'backend_factory': 'maestro.backends.codex_session:strict_v1_factory',
             'codex_cli_version': 'codex-cli 0.160.0'}
    contract = {**config.pace['contract'], 'image': image,
        'hardware_id': 'instance-20260923-033000:docker-cpu0-19-controller20-25',
        'toolchain_digest': hashlib.sha256(image.encode()).hexdigest(), 'solver_argv': ['/candidate/run']}
    return replace(config, coral=coral, planning_window_seconds=600, generation_window_seconds=1200,
                   pace={**config.pace, 'contract': contract})


def verify_never_dispatched(root):
    """Permit startup repair only before ANY stage, account, request or sample.

    Never resumes a failed model/tool sample or clears a quota/UNKNOWN record.
    """
    for file, queries in {
        'requests.sqlite': ['SELECT count(*) FROM inference_budgets',
                            'SELECT count(*) FROM inference_reservations',
                            'SELECT count(*) FROM codex_phase_scopes'],
        'calibration.sqlite': ['SELECT count(*) FROM samples',
                               "SELECT count(*) FROM events WHERE event='phase_open'"]}.items():
        db = sqlite3.connect(f'file:{(root/file).resolve()}?mode=ro', uri=True)
        try:
            if any(db.execute(query).fetchone()[0] for query in queries):
                raise ValueError('Cannot continue: a sample/account/phase has already started')
        finally:
            db.close()
    previous = json.loads((root/'report.json').read_text())
    if previous['status'] != 'STOPPED_INFRASTRUCTURE':
        raise ValueError('Only a proven pre-dispatch infrastructure stop is eligible')
    return previous


def run(root, *, backend_builder=None, proxy_upstream=None, client_instrumentation=True,
        execution_kind='live_timing', phase_windows=None, wave_count=3, request_cap=48, prior_ledger=None, continue_before_dispatch=False):
    from maestro.storage.paths import diagnostic_root
    root = diagnostic_root(root)
    base = Path(__file__).resolve().parents[1]
    if execution_kind != 'offline_no_model_acceptance' and (backend_builder is not None or proxy_upstream is not None):
        raise ValueError('Injected protocol fixtures are restricted to explicit offline acceptance')
    if type(wave_count) is not int or not 1 <= wave_count <= 3:
        raise ValueError('Calibration permits only one to three four-agent waves')
    if request_cap < wave_count * 4 * 4:
        raise ValueError('Reserve the full per-sample 1+3 envelope before launch')
    prior_cost = 0
    if prior_ledger is not None:
        old = sqlite3.connect(f'file:{Path(prior_ledger).resolve()}?mode=ro', uri=True)
        try:
            rows = old.execute('SELECT status FROM inference_reservations').fetchall()
            if any(row[0] != 'COMPLETED' for row in rows):
                raise ValueError('Prior pilot has unresolved requests; no new dispatch')
            if old.execute("SELECT count(*) FROM codex_phase_scopes WHERE status IN ('UNKNOWN','DISPATCHING')").fetchone()[0]:
                raise ValueError('Prior pilot has unresolved phase scopes')
            prior_cost = len(rows)
        finally:
            old.close()
    if prior_cost + request_cap > 48:
        raise ValueError('Prior charges plus new ceiling exceed original 48 requests')
    if continue_before_dispatch:
        previous = verify_never_dispatched(root)
        with (root/'startup-continuation.json').open('x') as marker:
            marker.write(json.dumps({'utc':utc(),'pid':os.getpid(),
                'reason':'Explicit environment repair before any dispatch; no sample retry',
                'previous_report':previous},indent=2))
    else:
        with (root / 'execution-started.json').open('x') as marker:
            marker.write(json.dumps({'utc': utc(), 'pid': os.getpid(), 'execution_kind': execution_kind}))
    audit = Audit(root, append=continue_before_dispatch)
    if continue_before_dispatch:
        audit.emit('startup_continued_before_dispatch', previous_status=previous['status'])
    halt = StopState()
    stamp = root.name.removeprefix('timing-calibration-')
    episodes = [f'calibration-{stamp}-w{w}' for w in range(1, wave_count + 1)]
    budgets = CalibrationBudgets(root / 'requests.sqlite', episodes, max_reservations=request_cap)
    samples, opens, backend = [], {}, None
    try:
        config = calibration_config(load_config(base / 'configs/experiments/pace_remote.yaml'))
        windows = phase_windows or {'planning': 600, 'generation': 1200}
        if execution_kind != 'offline_no_model_acceptance' and windows != {'planning': 600, 'generation': 1200}:
            raise ValueError('Live calibration deadlines must remain 600/1200')
        (root / 'resolved-config.json').write_text(json.dumps(config.to_dict(), indent=2))
        (root / 'execution-identity.json').write_text(json.dumps({
            'base_commit': subprocess.check_output(['git','rev-parse','HEAD'],cwd=base,text=True).strip(),
            'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'template_factory': load_config(base/'configs/experiments/pace_remote.yaml').coral.get('backend_factory'),
            'pilot_factory': config.coral['backend_factory'],
            'preparation_dispatched': False, 'request_envelope': f'{wave_count * 4} samples * (1 planning + 3 generation)',
            'prior_ledger': str(Path(prior_ledger).resolve()) if prior_ledger else None,
            'prior_charged_requests': prior_cost, 'new_request_ceiling': request_cap,
            'combined_request_ceiling': prior_cost + request_cap,
            'execution_kind': execution_kind},indent=2))
        source = root / 'debug-source'
        for name in VERIFIER_SOURCE_FILES:
            dest = source / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes((base / 'src' / name).read_bytes())
        checker = root / 'debug-check.py'
        checker.write_text("import sys,json\nsys.path.insert(0,'/input/calibration-source')\nfrom maestro.tasks.pace.verifier import verify_graph_solution\nn,v=verify_graph_solution(open(sys.argv[1]).read(),sys.stdin.read())\nprint(json.dumps({'valid':v.valid,'size':v.size,'reason':v.reason}))\nsys.exit(0 if v.valid else 1)\n")
        graph = base / 'data/training/heuristic_032.gr'
        graph_hash = hashlib.sha256(graph.read_bytes()).hexdigest()
        seed_hash = candidate_tree_hash(base / 'data/seed_solver')
        (root / 'frozen-seed.json').write_text(json.dumps({'candidate_hash': seed_hash,
            'graph_path': str(graph), 'graph_sha256': graph_hash, 'formal_score': None,
            'diagnostic_parent_score_scope': 'single real debug graph, prior verified receipt; not 20-instance formal score'}, indent=2))
        ids = [f'agent-{i}' for i in range(1, 5)]
        briefs = make_direction_briefs(ids, 0, context={'seed_artifact': seed_hash})
        instruction = (' Calibration task: implement one substantive improvement to the real PACE DS solver, '
            'read solver/run and list/read your own private memory using the bounded tools, and persist notes. '
            'The frozen runtime has Python3 standard library, no C++ compiler; preserve executable run entry point. '
            'One real public training graph is available ONLY in isolated shell as /input/calibration.gr. '
            f'For bounded real debugging run {DEBUG_COMMAND}. '
            'No reference, panel, ledger or formal evaluation is exposed. Shell changes do not persist; use write. '
            'Batch related file operations to fit three actual generation requests. '
            'The controller performs one mandatory real debug under the SAME 30 CPU-second tool budget and the ordinary fixed precheck. '
            'The supplied parent_score is only a measured single-debug-instance diagnostic, not a formal panel score. '
            'Return a concrete revise/retain plan using your direction; there is no prior experiment evidence to cite.')
        for brief in briefs:
            brief['objective'] += instruction
        (root / 'directions.json').write_text(json.dumps(briefs, indent=2))
        for wave, episode in enumerate(episodes, 1):
            if halt.is_set():
                break
            audit.emit('backend_initialization_start', wave=wave)
            builder = backend_builder or load_factory(config.coral['backend_factory'])
            backend = builder(config, root=root / f'wave-{wave}')
            if callable(getattr(backend, 'prepare_with_receipt', None)):
                raise CalibrationInfrastructureError('Preparation cannot be inserted into the authorized 48-request envelope')
            backend.budget_store.connection.close()
            backend.budget_store = budgets
            backend.begin_episode(episode)
            backend.set_directions(briefs)
            backend.register_parent(seed_hash, base / 'data/seed_solver')
            states = {agent: AgentState(agent, seed_hash, 0.803067819143041) for agent in ids}
            for state in states.values():
                ws = backend._workspace(state)
                assert candidate_tree_hash(ws.solver) == seed_hash and not any(ws.private.iterdir())
            pid_scopes = {}
            original_stop = getattr(backend.client, '_stop', None)
            def factory(*a, **kw):
                kw['start_new_session'] = True
                p = subprocess.Popen(*a, **kw)
                pid_scopes[p.pid] = dict(LOCAL.scope)
                audit.emit('native_started', pid=p.pid)
                return p
            if client_instrumentation:
                backend.client.factory = factory
            def stop(process):
                scope = pid_scopes.get(process.pid, {})
                span = uuid.uuid4().hex
                audit.emit('native_stop_start', scope, span=span, pid=process.pid)
                try:
                    original_stop(process)
                except BaseException as error:
                    error.inflight_unknown = True
                    halt.stop('UNKNOWN', 'Native process cleanup failed')
                    audit.emit('native_stop_failure', scope, span=span, **exception_result(error, halt))
                    raise
                members = []
                for path in Path('/proc').glob('[0-9]*/stat'):
                    try:
                        fields = path.read_text().rsplit(')', 1)[1].split()
                        if int(fields[2]) == process.pid and fields[0] != 'Z':
                            members.append(int(path.parent.name))
                    except (OSError, ValueError, IndexError):
                        pass
                if members:
                    error = RuntimeError('Native descendant reclamation unconfirmed')
                    error.inflight_unknown = True
                    halt.stop('UNKNOWN', 'Native descendant reclamation unconfirmed')
                    raise error
                audit.emit('native_stop_end', scope, span=span, pid=process.pid, returncode=process.poll())
            if client_instrumentation:
                backend.client._stop = stop
            class Proxy(CodexQuotaProxy):
                def __init__(self, *a, **kw):
                    self.scope = dict(LOCAL.scope)
                    super().__init__(*a, event_sink=lambda event, fields: audit.emit(event, {**self.scope, **fields}),
                        **({'upstream': proxy_upstream} if proxy_upstream is not None else {}), **kw)
                def _request(self, handler):
                    if halt.is_set():
                        raise CalibrationHalted('Calibration stopped after an execution-chain fault')
                    return super()._request(handler)
            backend.proxy_factory = Proxy
            class Broker(StageToolBroker):
                def call(self, action, args=None):
                    if halt.is_set():
                        raise CalibrationHalted('Calibration halted after an execution-chain fault')
                    span = uuid.uuid4().hex
                    audit.emit('tool_start', span=span, action=action, path=(args or {}).get('path'))
                    try:
                        return super().call(action, args)
                    finally:
                        audit.emit('tool_end', span=span, action=action)
            backend.broker_factory = Broker
            class Runner:
                def preflight(self, **kw):
                    return DockerRunner(IMAGE).preflight(**kw)
                def run(self, request, limits):
                    if halt.is_set():
                        raise CalibrationHalted('Calibration halted before container launch')
                    assert hashlib.sha256(graph.read_bytes()).hexdigest() == graph_hash
                    request = replace(request, mounts=request.mounts + (Mount(graph, '/input/calibration.gr'),
                        Mount(source, '/input/calibration-source'), Mount(checker, '/input/calibration-check.py')))
                    started = audit.emit('container_start', limits=asdict(limits))
                    receipt = DockerRunner(IMAGE).run(request, limits)
                    audit.emit('container_reclaimed', receipt=asdict(receipt), start_monotonic=started)
                    return receipt
            backend.candidate_runner = Runner()
            def planning(agent, opened, deadline):
                LOCAL.scope = {'wave': wave, 'agent': agent, 'phase': 'planning'}
                result = {'wave': wave, 'agent': agent, 'phase': 'planning', 'opened': opened,
                          'dispatched': audit.emit('dispatched'), 'deadline': deadline}
                try:
                    decision, receipt, context = backend.plan_with_receipt(states[agent], 0, {}, deadline=deadline)
                    apply_decision(states[agent], decision, 0)
                    result['plan_receipt'] = asdict(receipt)
                    result['status'] = 'NORMAL' if decision.plan is not None else 'WITHDRAWN'
                    result['accepted'] = audit.emit('plan_accepted', action=decision.action)
                except BaseException as error:
                    result.update(exception_result(error, halt))
                result['returned'] = audit.emit('controller_return', status=result['status'])
                result['elapsed_seconds'] = result.get('accepted', result['returned']) - opened
                if result['returned'] > deadline and result['status'] == 'NORMAL': result['status'] = 'TIMEOUT'
                audit.sample(result)
                return result
            def generation(agent, opened, deadline):
                LOCAL.scope = {'wave': wave, 'agent': agent, 'phase': 'generation'}
                state, authority = states[agent], GrantAuthority()
                result = {'wave': wave, 'agent': agent, 'phase': 'generation', 'opened': opened,
                          'dispatched': audit.emit('dispatched'), 'deadline': deadline}
                grant = None
                try:
                    if halt.is_set(): raise CalibrationHalted('Calibration halted')
                    if state.pending_plan is None:
                        result['status'] = 'NO_ADMITTED_PLAN'
                    else:
                        for revision in range(config.max_candidate_revisions):
                            if time.monotonic() >= deadline: raise TimeoutError('Common generation boundary elapsed')
                            plan = state.pending_plan
                            grant = authority.issue(episode_id=episode, round_index=0, agent_id=agent,
                                plan_version_id=plan.version_id, parent_hash=state.parent_hash,
                                epoch=revision, expires_at=time.time() + deadline-time.monotonic())
                            audit.emit('grant_issued', epoch=revision, plan_version_id=plan.version_id)
                            candidate = backend.generate(state, plan, 0, revision, deadline,
                                                         grant=grant, grant_authority=authority)
                            audit.emit('artifact_frozen', candidate=candidate)
                            account = backend.last_generation_usage['account']
                            broker = backend._brokers[account]
                            authority.verify(grant, agent_id=agent, plan_version_id=plan.version_id,
                                             parent_hash=plan.parent_hash, epoch=revision)
                            debug = broker.call('shell', {'command': DEBUG_COMMAND})
                            if debug['returncode'] != 0 and any(label in debug.get('stderr', '')
                                    for label in ('Permission denied', 'command not found',
                                                  'calibration-check.py: No such file')):
                                raise CalibrationInfrastructureError('Real debug execution chain failed')
                            backend._tool_spent[account] = broker.usage()['cpu_seconds']
                            audit.emit('real_debug_completed', result=debug)
                            audit.emit('precheck_start')
                            passed, reason = backend.precheck(candidate)
                            result['precheck_cpu_seconds'] = result.get('precheck_cpu_seconds', 0) + backend.last_generation_usage.get('precheck_cpu_seconds', 0)
                            audit.emit('precheck_end', passed=passed, reason=reason)
                            authority.revoke(grant)
                            result['tool_cpu_seconds'] = broker.usage()['cpu_seconds']
                            result['usage'] = backend._usage(account)
                            if time.monotonic() >= deadline: raise TimeoutError('Cleanup/precheck exceeded common boundary')
                            if passed and debug['returncode'] == 0 and candidate['candidate_hash'] != seed_hash:
                                result.update(status='NORMAL', candidate=candidate)
                                break
                            result['status'] = 'PRECHECK_OR_DEBUG_FAILED'
                            if result['usage']['calls'] >= 3: break
                except BaseException as error:
                    result.update(exception_result(error, halt))
                finally:
                    if grant is not None: authority.revoke(grant)
                    account = backend.last_generation_usage.get('account')
                    if account and account in backend._brokers:
                        broker = backend._brokers[account]
                        result['tool_cpu_seconds'] = broker.usage()['cpu_seconds']
                        result['usage'] = backend._usage(account)
                        broker.close()
                if result['status'] == 'NORMAL':
                    if time.monotonic() >= deadline:
                        result['status'] = 'TIMEOUT'
                    else:
                        result['accepted'] = audit.emit('generation_accepted')
                result['returned'] = audit.emit('controller_return', status=result['status'])
                result['elapsed_seconds'] = result.get('accepted', result['returned']) - opened
                audit.sample(result)
                return result
            try:
                execute_phases(wave, ids, {'planning': planning, 'generation': generation},
                               windows, audit, halt, samples)
            finally:
                try:
                    backend.close()
                except BaseException as error:
                    detail = exception_result(error, halt)
                    halt.stop('UNKNOWN', 'Backend cleanup failed; reclamation unconfirmed')
                    audit.emit('backend_cleanup_failure', wave=wave, **detail)
            backend = None
            assert candidate_tree_hash(base / 'data/seed_solver') == seed_hash
            summary = summarize(root, audit, budgets, samples, opens, halt=halt, execution_kind=execution_kind, expected_samples=wave_count * 4)
            if halt.is_set(): break
    except BaseException as error:
        detail = exception_result(error, halt)
        halt.stop('UNKNOWN' if detail['status'] == 'UNKNOWN' else 'INFRASTRUCTURE',
                  'Driver execution chain failed: ' + type(error).__name__)
        audit.emit('driver_failure', **detail)
    finally:
        if backend is not None:
            try:
                backend.close()
            except BaseException as error:
                detail = exception_result(error, halt)
                halt.stop('UNKNOWN', 'Backend cleanup after driver failure is unconfirmed')
                audit.emit('backend_cleanup_failure', **detail)
    summary = summarize(root, audit, budgets, samples, opens, halt=halt, final=True, execution_kind=execution_kind, expected_samples=wave_count * 4)
    print(json.dumps({'status': summary['status'], 'actual_requests': summary['actual_requests'], 'windows': summary['windows']}), flush=True)
    budgets.connection.close()
    audit.close()
    return 0 if summary['status'] == 'COMPLETED' else 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--waves', type=int, default=3)
    parser.add_argument('--max-requests', type=int, default=48)
    parser.add_argument('--prior-ledger', type=Path)
    parser.add_argument('--continue-before-dispatch',action='store_true')
    args = parser.parse_args()
    raise SystemExit(run(args.root.resolve(), wave_count=args.waves, request_cap=args.max_requests, prior_ledger=args.prior_ledger, continue_before_dispatch=args.continue_before_dispatch))
