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
from maestro.backends.codex_session.autonomous_backend import strict_factory
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.backends.codex_session.tools import StageToolBroker
from maestro.bootstrap.director import make_direction_briefs
from maestro.config import load_config, RunConfig
from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.execution.grants import GrantAuthority
from maestro.execution.sandbox import DockerRunner, Mount
from maestro.tasks.pace.manifest import VERIFIER_SOURCE_FILES
from maestro.types import AgentState

IMAGE = 'python@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641'
LOCAL = threading.local()


def utc():
    return datetime.now(timezone.utc).isoformat()


class Audit:
    def __init__(self, root):
        self.root = root
        self.lock = threading.RLock()
        self.db = sqlite3.connect(root / 'calibration.sqlite', check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE events(id INTEGER PRIMARY KEY,monotonic REAL,utc TEXT,event TEXT,payload TEXT)')
        self.db.execute('CREATE TABLE samples(wave INTEGER,agent TEXT,phase TEXT,status TEXT,result TEXT,PRIMARY KEY(wave,agent,phase))')
        self.db.commit()
        self.log = (root / 'events.jsonl').open('x')

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

    def sample(self, result):
        with self.lock:
            self.db.execute('INSERT INTO samples VALUES(?,?,?,?,?)',
                (result['wave'], result['agent'], result['phase'], result['status'], json.dumps(result)))
            self.db.commit()


class CalibrationBudgets(BudgetStore):
    """Durable global 48-reservation cap in addition to per-sample 1/3 gates."""
    def __init__(self, path, episodes):
        super().__init__(path)
        self.episodes = set(episodes)
        self.bindings = {}
        self.connection.execute('CREATE TABLE codex_phase_scopes(account TEXT PRIMARY KEY,status TEXT NOT NULL)')

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
            if self.connection.execute('SELECT COUNT(*) FROM inference_reservations').fetchone()[0] >= 48:
                raise BudgetExceeded('Global calibration envelope consumed')
            return super().reserve(*args, **kwargs)


def union_seconds(intervals):
    end = total = 0.0
    for start, stop in sorted(intervals):
        if stop > end:
            total += stop - max(start, end)
            end = stop
    return total


def summarize(root, audit, budgets, samples, opens):
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
        maxima = {str(w): max((s['elapsed_seconds'] for s in rows if s['wave'] == w), default=None) for w in range(1, 4)}
        recommended = math.ceil(max(normal) * 1.25 / 30) * 30 if normal else None
        distributions[phase] = {'normal_n': len(normal), 'median_seconds': statistics.median(normal) if normal else None,
            'max_seconds': max(normal) if normal else None, 'wave_return_max_seconds': maxima,
            'provisional_window_seconds': recommended, 'basis': 'ceil(max normal open-to-accept * 1.25 / 30) * 30',
            'evidence_sufficient_for_final_window': len(normal) == 12,
            'incomplete_or_failed': [dict(wave=s['wave'], agent=s['agent'], status=s['status']) for s in rows if s['status'] != 'NORMAL'],
            'observed_returns_exceeding_recommendation': [dict(wave=s['wave'], agent=s['agent'], status=s['status']) for s in rows if recommended is not None and s['elapsed_seconds'] > recommended],
            'limitation': '12 samples do not establish population tail quantiles. Censored/failed samples are retained, not treated as normal times.'}
    with budgets._lock:
        requests = [dict(r) for r in budgets.connection.execute('SELECT * FROM inference_reservations')]
        receipts = [dict(r) for r in budgets.connection.execute('SELECT * FROM provider_receipts')]
        scopes = [dict(r) for r in budgets.connection.execute('SELECT * FROM codex_phase_scopes')]
    summary = {'status': 'STOPPED_UNKNOWN' if any(s['status'] == 'UNKNOWN' for s in samples) else 'COMPLETED',
        'finished_utc': utc(), 'samples': samples, 'windows': distributions,
        'actual_requests': len(requests), 'max_requests': 48, 'requests': requests, 'provider_receipts': receipts,
        'phase_scopes': scopes, 'input_tokens_known': sum(r['input_tokens'] or 0 for r in receipts),
        'output_tokens_known': sum(r['output_tokens'] or 0 for r in receipts),
        'token_usage_complete': len(receipts) == len(requests) and all(r['usage_complete'] for r in receipts),
        'tool_cpu_seconds': sum(s.get('tool_cpu_seconds', 0) for s in samples),
        'fixed_precheck_cpu_seconds': sum(s.get('precheck_cpu_seconds', 0) for s in samples),
        'cpu_scope': 'Measured cgroup CPU of tools/debug/precheck. Controller/native CPU not independently metered; unknown, not zero.',
        'formal_warm_started': False, 'formal_evaluations': 0, 'model_tool_overlap': 'Interval unions used; provider/tool/cleanup columns are not additive.'}
    (root / 'report.json').write_text(json.dumps(summary, indent=2))
    (root / 'suggested-windows.json').write_text(json.dumps({phase + '_window_seconds': info['provisional_window_seconds'] for phase, info in distributions.items()}, indent=2))
    return summary


def run(root):
    base = Path(__file__).resolve().parents[1]
    (root / 'execution-started.json').write_text(json.dumps({'utc': utc(), 'pid': os.getpid()}), encoding='utf-8') if not (root / 'execution-started.json').exists() else (_ for _ in ()).throw(RuntimeError('Never restart a calibration root'))
    audit = Audit(root)
    halt = threading.Event()
    stamp = root.name.removeprefix('timing-calibration-')
    episodes = [f'calibration-{stamp}-w{w}' for w in range(1, 4)]
    budgets = CalibrationBudgets(root / 'requests.sqlite', episodes)
    config = load_config(base / 'configs/warm_start.yaml')
    coral = {**config.coral, 'codex_cli_version': 'codex-cli 0.160.0'}
    contract = {**config.pace['contract'], 'image': IMAGE,
        'hardware_id': 'instance-20260923-033000:docker-cpu0-19-controller20-25',
        'toolchain_digest': hashlib.sha256(IMAGE.encode()).hexdigest(), 'solver_argv': ['/candidate/run']}
    config = replace(config, coral=coral, planning_window_seconds=600, generation_window_seconds=1200,
                     pace={**config.pace, 'contract': contract})
    (root / 'resolved-config.json').write_text(json.dumps(config.to_dict(), indent=2))
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
        'For bounded real debugging run python3 run < /input/calibration.gr | python3 /input/calibration-check.py /input/calibration.gr. '
        'No reference, panel, ledger or formal evaluation is exposed. Shell changes do not persist; use write. '
        'Batch related file operations to fit three actual generation requests. '
        'The controller performs one mandatory real debug under the SAME 30 CPU-second tool budget and the ordinary fixed precheck. '
        'The supplied parent_score is only a measured single-debug-instance diagnostic, not a formal panel score. '
        'Return a concrete revise/retain plan using your direction; there is no prior experiment evidence to cite.')
    for brief in briefs:
        brief['objective'] += instruction
    (root / 'directions.json').write_text(json.dumps(briefs, indent=2))
    samples, opens = [], {}
    for wave, episode in enumerate(episodes, 1):
        if halt.is_set():
            break
        backend = strict_factory(config, root=root / f'wave-{wave}')
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
        original_stop = backend.client._stop
        def factory(*a, **kw):
            kw['start_new_session'] = True
            p = subprocess.Popen(*a, **kw)
            pid_scopes[p.pid] = dict(LOCAL.scope)
            audit.emit('native_started', pid=p.pid)
            return p
        backend.client.factory = factory
        def stop(process):
            scope = pid_scopes.get(process.pid, {})
            span = uuid.uuid4().hex
            audit.emit('native_stop_start', scope, span=span, pid=process.pid)
            original_stop(process)
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
                halt.set()
                raise error
            audit.emit('native_stop_end', scope, span=span, pid=process.pid, returncode=process.poll())
        backend.client._stop = stop
        class Proxy(CodexQuotaProxy):
            def __init__(self, *a, **kw):
                self.scope = dict(LOCAL.scope)
                super().__init__(*a, event_sink=lambda event, fields: audit.emit(event, {**self.scope, **fields}), **kw)
            def _request(self, handler):
                if halt.is_set():
                    raise PermissionError('Calibration stopped after uncertain execution')
                return super()._request(handler)
        backend.proxy_factory = Proxy
        class Broker(StageToolBroker):
            def call(self, action, args=None):
                if halt.is_set():
                    raise PermissionError('Calibration halted after uncertain execution')
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
                    raise PermissionError('Calibration halted before container launch')
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
                result.update(status='UNKNOWN' if getattr(error, 'inflight_unknown', False) else 'TIMEOUT' if isinstance(error, TimeoutError) or time.monotonic() >= deadline else 'FAILED', error=type(error).__name__, message=str(error)[:512], traceback=traceback.format_exc())
                if result['status'] == 'UNKNOWN': halt.set()
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
                if halt.is_set(): raise RuntimeError('Calibration halted')
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
                        debug = broker.call('shell', {'command': 'python3 run < /input/calibration.gr | python3 /input/calibration-check.py /input/calibration.gr'})
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
                            result.update(status='NORMAL', candidate=candidate, accepted=audit.emit('generation_accepted'))
                            break
                        result['status'] = 'PRECHECK_OR_DEBUG_FAILED'
                        if result['usage']['calls'] >= 3: break
            except BaseException as error:
                result.update(status='UNKNOWN' if getattr(error, 'inflight_unknown', False) or getattr(error, 'execution_unknown', False) else 'TIMEOUT' if isinstance(error, TimeoutError) or time.monotonic() >= deadline else 'FAILED', error=type(error).__name__, message=str(error)[:512], traceback=traceback.format_exc())
                if result['status'] == 'UNKNOWN': halt.set()
            finally:
                if grant is not None: authority.revoke(grant)
                account = backend.last_generation_usage.get('account')
                if account and account in backend._brokers:
                    broker = backend._brokers[account]
                    result['tool_cpu_seconds'] = broker.usage()['cpu_seconds']
                    result['usage'] = backend._usage(account)
                    broker.close()
            result['returned'] = audit.emit('controller_return', status=result['status'])
            result['elapsed_seconds'] = result.get('accepted', result['returned']) - opened
            audit.sample(result)
            return result
        try:
            for phase, window, worker in [('planning', 600, planning), ('generation', 1200, generation)]:
                if halt.is_set(): break
                opened = audit.emit('phase_open', wave=wave, phase=phase, window_seconds=window)
                deadline = opened + window
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(worker, agent, opened, deadline) for agent in ids]
                    for future in as_completed(futures):
                        sample = future.result()
                        samples.append(sample)
                        print(json.dumps({k: sample.get(k) for k in ('wave','agent','phase','status','elapsed_seconds')}), flush=True)
                audit.emit('phase_closed', wave=wave, phase=phase)
        finally:
            backend.close()
        assert candidate_tree_hash(base / 'data/seed_solver') == seed_hash
        summary = summarize(root, audit, budgets, samples, opens)
        if halt.is_set(): break
    summary = summarize(root, audit, budgets, samples, opens)
    print(json.dumps({'status': summary['status'], 'actual_requests': summary['actual_requests'], 'windows': summary['windows']}), flush=True)
    budgets.connection.close()
    return 2 if halt.is_set() else 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.root.resolve()))
