import importlib.util
from pathlib import Path
import time

import pytest

from maestro.execution.budgets import BudgetExceeded

spec = importlib.util.spec_from_file_location('timing_calibration',
    Path(__file__).resolve().parents[2] / 'scripts/calibrate_codex_timing.py')
calibration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibration)


def test_budget_is_per_sample_and_globally_bounded(tmp_path):
    budgets = calibration.CalibrationBudgets(tmp_path / 'requests.sqlite', ['cal-w1'])
    limits = dict(max_request_bytes=1000, max_output_tokens=100, deadline=time.time()+60)
    with pytest.raises(BudgetExceeded):
        budgets.create('unregistered:0:agent-1:PLAN_RO', max_calls=1, **limits)
    budgets.create('cal-w1:0:agent-1:PLAN_RO', max_calls=1, **limits)
    rid = budgets.reserve('cal-w1:0:agent-1:PLAN_RO', 'one', request_bytes=1, output_tokens=1)
    budgets.finish(rid, actual_output_tokens=1)
    with pytest.raises(BudgetExceeded):
        budgets.reserve('cal-w1:0:agent-1:PLAN_RO', 'two', request_bytes=1, output_tokens=1)
    budgets.create('cal-w1:0:agent-1:GENERATION:pv-one', max_calls=3, **limits)
    with pytest.raises(BudgetExceeded):
        budgets.create('cal-w1:0:agent-1:GENERATION:pv-two', max_calls=3, **limits)
    budgets.close()  # A wave backend cannot close the shared durable authority.
    assert budgets.connection.execute('SELECT COUNT(*) FROM inference_reservations').fetchone()[0] == 1
    budgets.connection.close()


def test_overlap_is_not_summed():
    assert calibration.union_seconds([(1, 3), (2, 5), (6, 7)]) == 5


def test_all_twelve_samples_cannot_exceed_48_actual_reservations(tmp_path):
    budgets = calibration.CalibrationBudgets(tmp_path / 'requests.sqlite', ['w1', 'w2', 'w3'])
    last = None
    for wave in range(1, 4):
        for agent in range(1, 5):
            for phase, cap in [('PLAN_RO', 1), ('GENERATION:pv-fixed', 3)]:
                account = f'w{wave}:0:agent-{agent}:{phase}'
                budgets.create(account, max_calls=cap, max_request_bytes=1000,
                    max_output_tokens=100, deadline=time.time()+60)
                for index in range(cap):
                    rid = budgets.reserve(account, str(index), request_bytes=1, output_tokens=1)
                    budgets.finish(rid, actual_output_tokens=1)
                last = account
    assert budgets.connection.execute('SELECT COUNT(*) FROM inference_reservations').fetchone()[0] == 48
    with pytest.raises(BudgetExceeded, match='Global calibration'):
        budgets.reserve(last, 'extra', request_bytes=1, output_tokens=1)
    budgets.connection.close()


def test_parallel_budget_snapshots_keep_their_own_row_identity(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from maestro.execution.budgets import BudgetStore
    store = BudgetStore(tmp_path / 'parallel.sqlite')
    for i in range(8):
        store.create(f'account-{i}', max_calls=1, max_request_bytes=100,
                     max_output_tokens=10, deadline=time.time()+60)
    def worker(i):
        for _ in range(300):
            with store._transaction():
                store.connection.execute('SELECT account FROM inference_budgets WHERE active>0').fetchall()
            assert store.snapshot(f'account-{i}')['account'] == f'account-{i}'
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))
    store.close()


def test_four_planners_create_read_and_claim_correct_accounts(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    store = calibration.CalibrationBudgets(tmp_path/'claims.sqlite', ['w1'])
    barrier = threading.Barrier(4)
    def worker(i):
        account = f'w1:0:agent-{i}:PLAN_RO'
        barrier.wait(timeout=5)
        store.create(account, max_calls=1, max_request_bytes=100,
                     max_output_tokens=10, deadline=time.time()+60)
        for _ in range(500):
            assert store.snapshot(account)['account'] == account
        rid = store.reserve(account, f'request-{i}', request_bytes=i, output_tokens=1)
        barrier.wait(timeout=5)  # All four claims coexist, outside SQLite lock.
        assert store.snapshot(account)['active'] == 1
        store.finish(rid, actual_output_tokens=1)
        assert store.usage(account)['calls'] == 1
        assert store.usage(account)['unresolved_requests'] == 0
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(1,5)))
    rows = store.connection.execute('SELECT account FROM inference_reservations').fetchall()
    assert {r[0] for r in rows} == {f'w1:0:agent-{i}:PLAN_RO' for i in range(1,5)}
    store.connection.close()


def test_shared_deadline_and_infrastructure_stop_with_traceback(tmp_path):
    import threading
    audit = calibration.Audit(tmp_path)
    halt = calibration.StopState()
    barrier = threading.Barrier(4)
    boundaries = []
    def planning(agent, opened, deadline):
        boundaries.append((opened,deadline))
        barrier.wait(timeout=5)
        raise IndexError('original planning failure')
    def generation(*args):
        pytest.fail('generation must not start after planning infrastructure failure')
    samples = []
    calibration.execute_phases(1, ['a','b','c','d'],
        dict(planning=planning,generation=generation),dict(planning=600,generation=1200),audit,halt,samples)
    assert len(set(boundaries)) == 1
    assert halt.kind == 'INFRASTRUCTURE'
    assert len(samples) == 4
    assert all('raise IndexError' in s['traceback'] for s in samples)
    budgets = calibration.CalibrationBudgets(tmp_path/'requests.sqlite', ['w1'])
    report = calibration.summarize(tmp_path,audit,budgets,samples,halt=halt,final=True)
    assert report['status'] == 'STOPPED_INFRASTRUCTURE'
    assert report['windows']['planning']['provisional_window_seconds'] is None
    budgets.connection.close()
    audit.close()


def test_initialization_failure_is_reported_without_wave_two(tmp_path, monkeypatch):
    import json
    from dataclasses import replace
    original_load=calibration.load_config
    monkeypatch.setattr(calibration,"load_config",lambda path:replace(original_load(path),generation_calls=3))
    calls=[]
    def broken(config,root):
        calls.append(root)
        raise IndexError('initialization row failure')
    assert calibration.run(tmp_path,backend_builder=broken,
        execution_kind='offline_no_model_acceptance',client_instrumentation=False) == 2
    report=json.loads((tmp_path/'report.json').read_text())
    assert report['status']=='STOPPED_INFRASTRUCTURE'
    assert len(calls)==1
    assert 'initialization row failure' in (tmp_path/'events.jsonl').read_text()
    with pytest.raises(FileExistsError):
        calibration.run(tmp_path,backend_builder=broken,
            execution_kind='offline_no_model_acceptance')


def test_wrapped_index_error_remains_infrastructure_failure():
    from maestro.llm import PlanningResponseError
    halt=calibration.StopState()
    try:
        try:
            raise IndexError('wrapped row lookup')
        except IndexError as cause:
            raise PlanningResponseError('No admitted plan',receipt=__import__('types').SimpleNamespace(request_id='r',scope_id='s'),context={}) from cause
    except PlanningResponseError as error:
        detail=calibration.exception_result(error,halt)
    assert detail['status']=='INFRASTRUCTURE_FAILURE'
    assert detail['wrapper_error']=='PlanningResponseError'
    assert 'wrapped row lookup' in detail['traceback']
    assert halt.is_set()


def test_interim_report_cannot_claim_completion(tmp_path):
    audit=calibration.Audit(tmp_path)
    budgets=calibration.CalibrationBudgets(tmp_path/'requests.sqlite',['w1'])
    result=calibration.summarize(tmp_path,audit,budgets,[],final=False)
    assert result['status']=='PARTIAL'
    budgets.connection.close()
    audit.close()


def test_fixed_precheck_wall_is_bounded_by_common_deadline():
    from types import SimpleNamespace
    from maestro.backends.codex_session.autonomous_backend import StrictCodexBackend
    from maestro.execution.sandbox import ResourceLimits
    from maestro.execution.cpu_pool import GlobalCpuPool
    seen=[]
    class Runner:
        def run(self,request,limits):
            seen.append(limits.wall_seconds)
            return 'receipt'
    backend=SimpleNamespace(tool_cpu_pool=GlobalCpuPool(4),candidate_runner=Runner())
    deadline=time.monotonic()+.15
    assert StrictCodexBackend._run_fixed_precheck(backend,object(),
        ResourceLimits(cpu_seconds=1,wall_seconds=30,memory_mb=128,pids=8),deadline=deadline)=='receipt'
    assert 0 < seen[0] <= .15
    with pytest.raises(TimeoutError):
        StrictCodexBackend._run_fixed_precheck(backend,object(),
            ResourceLimits(cpu_seconds=1,wall_seconds=30,memory_mb=128,pids=8),deadline=time.monotonic()-1)


def test_unknown_durable_scope_latches_stop_before_another_wave(tmp_path):
    audit=calibration.Audit(tmp_path)
    budgets=calibration.CalibrationBudgets(tmp_path/'requests.sqlite',['w1'])
    with budgets._transaction():
        budgets.connection.execute("INSERT INTO codex_phase_scopes VALUES('w1:0:agent-1:PLAN_RO','UNKNOWN')")
    halt=calibration.StopState()
    report=calibration.summarize(tmp_path,audit,budgets,[],halt=halt)
    assert report['status']=='STOPPED_UNKNOWN' and halt.kind=='UNKNOWN'
    budgets.connection.close()
    audit.close()


def test_reduced_global_ceiling_does_not_create_free_calls(tmp_path):
    store=calibration.CalibrationBudgets(tmp_path/'limited.sqlite',['w1'],max_reservations=4)
    for i in range(1,5):
        account=f'w1:0:agent-{i}:PLAN_RO'
        store.create(account,max_calls=1,max_request_bytes=100,max_output_tokens=10,deadline=time.time()+60)
        rid=store.reserve(account,f'r{i}',request_bytes=1,output_tokens=1)
        store.finish(rid,actual_output_tokens=1)
    with pytest.raises(BudgetExceeded,match='Global calibration'):
        store.reserve('w1:0:agent-1:PLAN_RO','extra',request_bytes=1,output_tokens=1)
    store.connection.close()


def test_two_wave_complete_report_is_not_twelve_sample_window_evidence(tmp_path):
    audit=calibration.Audit(tmp_path)
    budgets=calibration.CalibrationBudgets(tmp_path/'requests.sqlite',['w1','w2'],max_reservations=32)
    samples=[dict(wave=w,agent=f'agent-{i}',phase=p,status='NORMAL',opened=1.,dispatched=1.,
        accepted=2.,returned=2.1,elapsed_seconds=1.) for w in (1,2) for i in range(1,5) for p in ('planning','generation')]
    report=calibration.summarize(tmp_path,audit,budgets,samples,final=True,expected_samples=8)
    assert report['status']=='COMPLETED' and report['max_requests']==32
    assert report['windows']['generation']['normal_n']==8
    assert report['windows']['generation']['provisional_window_seconds'] is None
    budgets.connection.close();audit.close()


def test_never_dispatched_guard_preserves_history_and_refuses_any_started_phase(tmp_path):
    import json
    audit=calibration.Audit(tmp_path)
    store=calibration.CalibrationBudgets(tmp_path/'requests.sqlite',['w1'])
    audit.emit('driver_failure',error='BackendUnavailable')
    (tmp_path/'report.json').write_text(json.dumps({'status':'STOPPED_INFRASTRUCTURE'}))
    assert calibration.verify_never_dispatched(tmp_path)['status']=='STOPPED_INFRASTRUCTURE'
    audit.emit('phase_open',wave=1,phase='planning')
    with pytest.raises(ValueError,match='already started'):
        calibration.verify_never_dispatched(tmp_path)
    audit.close();store.connection.close()
    continued=calibration.Audit(tmp_path,append=True)
    assert continued.db.execute('select count(*) from events').fetchone()[0]==2
    continued.close()
