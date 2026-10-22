"""Operator cap increases preserve historical costs and the physical request gate."""
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import threading
import time

import pytest

from maestro.backends.coral.deepseek_retry import (
    DeepSeekRetryStore, RetryingDeepSeekProxy, RetryPolicy, audit_retry_continuation)
from maestro.execution.budgets import BudgetExceeded


def gate(store, account, *, barrier=None, entered=None, release=None):
    calls=[]
    def upstream(payload, deadline):
        calls.append(payload)
        if len(calls)==1:
            if entered:entered.set();release.wait(3)
            if barrier:barrier.wait(3)
            raise ConnectionResetError('offline transport fault')
        return json.dumps({'type':'message','id':'accepted-'+account,'stop_reason':'end_turn',
            'content':[{'type':'text','text':'accepted'}],
            'usage':{'input_tokens':7,'output_tokens':3}}).encode(),'application/json'
    proxy=RetryingDeepSeekProxy(store,account,'offline',deadline=time.monotonic()+10,
        max_calls=1,max_request_bytes=100000,max_output_tokens=96000,per_request_tokens=96000,
        upstream=upstream,retry_policy=store.retry_policy)
    return proxy, calls


PAYLOAD={'model':'offline','messages':[{'role':'user','content':'frozen'}]}


def test_extension_keeps_original_policy_unknowns_and_recovery_proofs(tmp_path):
    path=tmp_path/'requests.sqlite';store=DeepSeekRetryStore(path)
    store.enable_retries(RetryPolicy(max_extra_requests=1,backoff_seconds=.01))
    one,_=gate(store,'e:0:a:PLAN_RO')
    try:
        one.complete(PAYLOAD);one.close()
        policy=store.connection.execute('SELECT payload FROM deepseek_retry_policy').fetchone()[0]
        before={name:[tuple(row) for row in store.connection.execute('SELECT * FROM '+name)]
            for name in ('inference_budgets','inference_reservations','provider_receipts','deepseek_recovered_attempts')}
        result=store.operator_extend_retry_limit(3,expected_used=1,authorization='operator: increase the global cap')
        assert result['remaining_extra_requests']==2 and store.extra_request_limit()==3
        assert store.connection.execute('SELECT payload FROM deepseek_retry_policy').fetchone()[0]==policy
        assert all([tuple(row) for row in store.connection.execute('SELECT * FROM '+name)]==rows for name,rows in before.items())
        two,calls=gate(store,'e:1:a:PLAN_RO')
        try:
            two.complete(PAYLOAD);two.assert_resolved()
            assert len(calls)==2 and calls[0]==calls[1]
        finally:two.close()
        audit=audit_retry_continuation(path)
        assert audit['status']=='CONTINUABLE_WITH_UNKNOWN_COST'
        assert audit['extra_requests']==2 and audit['effective_max_extra_requests']==3
        assert audit['remaining_extra_requests']==1 and not audit['cost_usage_complete']
        assert store.connection.execute("SELECT count(*) FROM inference_reservations WHERE status='UNKNOWN'").fetchone()[0]==2
        with pytest.raises(sqlite3.IntegrityError):
            store.connection.execute('UPDATE deepseek_retry_limit_extensions SET new_limit=100')
        with pytest.raises(sqlite3.IntegrityError):
            store.connection.execute('DELETE FROM deepseek_retry_limit_extensions')
        for limit,used in ((3,2),(2,2),(4,0)):
            with pytest.raises(BudgetExceeded):
                store.operator_extend_retry_limit(limit,expected_used=used,authorization='operator')
    finally:one.close();store.close()


def test_four_way_dispatch_shares_remaining_extension_cap(tmp_path):
    store=DeepSeekRetryStore(tmp_path/'requests.sqlite')
    store.enable_retries(RetryPolicy(max_extra_requests=1,backoff_seconds=.01))
    initial,_=gate(store,'e:0:a:PLAN_RO');initial.complete(PAYLOAD);initial.close()
    store.operator_extend_retry_limit(3,expected_used=1,authorization='operator')
    barrier=threading.Barrier(4)
    def run(index):
        proxy,calls=gate(store,f'e:1:a{index}:PLAN_RO',barrier=barrier)
        try:proxy.complete(PAYLOAD);return True,len(calls)
        except BudgetExceeded:return False,len(calls)
        finally:proxy.close()
    try:
        with ThreadPoolExecutor(4) as pool:outcomes=list(pool.map(run,range(4)))
        assert sum(ok for ok,_ in outcomes)==2
        assert sum(count for _,count in outcomes)==6
        assert store.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]==3
    finally:store.close()


def test_extension_refuses_active_requests_and_requires_authorization(tmp_path):
    store=DeepSeekRetryStore(tmp_path/'requests.sqlite')
    store.enable_retries(RetryPolicy(max_extra_requests=1,backoff_seconds=.01))
    entered,release=threading.Event(),threading.Event()
    proxy,_=gate(store,'e:0:a:PLAN_RO',entered=entered,release=release)
    try:
        with ThreadPoolExecutor(1) as pool:
            future=pool.submit(proxy.complete,PAYLOAD)
            try:
                assert entered.wait(3)
                with pytest.raises(BudgetExceeded):
                    store.operator_extend_retry_limit(3,expected_used=0,authorization='operator')
            finally:release.set()
            future.result()
        assert store.extra_request_limit()==1
        with pytest.raises(ValueError):
            store.operator_extend_retry_limit(3,expected_used=1,authorization='')
    finally:proxy.close();store.close()
