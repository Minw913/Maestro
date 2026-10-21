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
