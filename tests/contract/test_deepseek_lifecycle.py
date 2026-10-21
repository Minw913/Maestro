"""Interruptions through the production transport/gate; no model or credentials."""
from concurrent.futures import ThreadPoolExecutor
import json
import signal
import sqlite3
import threading
import time

import pytest

from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy, DeepSeekUpstream
from maestro.backends.coral.deepseek_lifecycle import audit_ledger, check_restart_ledgers
from maestro.execution.budgets import BudgetStore
from maestro.execution.pause import PauseController, PauseRequested
from maestro.cli import main


def frame(value):
    return b'data: ' + json.dumps(value).encode() + b'\r\n\r\n'


def start():
    return frame({'type': 'message_start', 'message': {'type': 'message', 'id': 'msg_early',
        'content': [], 'usage': {'input_tokens': 10, 'output_tokens': 0}}})


def ending():
    return (frame({'type': 'message_delta', 'delta': {'stop_reason': 'end_turn'},
                   'usage': {'output_tokens': 3}}) + frame({'type': 'message_stop'}))


class Response:
    status = 200
    def __init__(self, chunks):
        self.chunks = iter(chunks)
    def getheader(self, name, default=None):
        return {'Content-Type': 'text/event-stream', 'request-id': 'request_early'}.get(name, default)
    def read1(self, size):
        value = next(self.chunks, b'')
        return value() if callable(value) else value


def install_transport(monkeypatch, response, closed=None):
    class Socket:
        def shutdown(self, how):
            if closed:closed.set()
    class Connection:
        def __init__(self, *args, **kwargs):
            self.sock = Socket()
        def connect(self):pass
        def request(self, *args, **kwargs):
            self.connect(); self.send(b'HTTP headers'); self.send(b'body')
        def send(self, data):pass
        def getresponse(self):return response
        def close(self):pass
    monkeypatch.setattr('maestro.backends.coral.deepseek_gate.http.client.HTTPSConnection', Connection)


def make_gate(tmp_path, pause=None, guard=None):
    store = BudgetStore(tmp_path/'requests.sqlite')
    upstream = DeepSeekUpstream('https://api.deepseek.com/anthropic', 'never-a-real-key', pause_controller=pause)
    gate = DeepSeekQuotaProxy(store, 'e:0:a:GENERATION', 'offline', deadline=time.monotonic()+5,
        max_calls=2, max_request_bytes=10000, max_output_tokens=100, per_request_tokens=20,
        upstream=upstream, request_guard=guard)
    return store, gate


def payload():
    return {'model': 'offline', 'messages': [{'role': 'user', 'content': 'PRIVATE_PROMPT_SENTINEL'}]}


def test_early_ids_survive_disconnect_without_fake_completion(tmp_path, monkeypatch):
    def disconnect():raise ConnectionResetError('private diagnostic must not be persisted')
    install_transport(monkeypatch, Response([start(), disconnect]))
    store, gate = make_gate(tmp_path)
    try:
        with pytest.raises(ConnectionResetError):gate.complete(payload())
        audit = audit_ledger(tmp_path/'requests.sqlite')
        row = audit['requests'][0]
        assert audit['status'] == 'BLOCKED' and row['state'] == 'UNKNOWN'
        assert row['provider_message_id'] == 'msg_early'
        assert row['response_request_id'] == 'request_early' and row['transport_closed']
        assert store.snapshot(gate.account)['active'] == 1
        events = '\n'.join(r[0] for r in store.connection.execute('select data_json from deepseek_request_events'))
        assert 'PRIVATE_PROMPT_SENTINEL' not in events
        assert 'never-a-real-key' not in events and 'private diagnostic' not in events
    finally:gate.close(); store.close()


def test_terminal_receipt_saved_before_eof_and_native_delivery(tmp_path, monkeypatch):
    def forbidden():pytest.fail('a valid terminal SSE frame must not wait for EOF')
    install_transport(monkeypatch, Response([start(), ending(), forbidden]))
    store, gate = make_gate(tmp_path)
    try:
        gate.complete(payload())
        assert store.usage(gate.account)['usage_complete']
        assert store.snapshot(gate.account)['active'] == 0
        assert audit_ledger(tmp_path/'requests.sqlite')['status'] == 'CLEAR'
        kinds = [r[0] for r in store.connection.execute('select kind from deepseek_request_events order by id')]
        assert kinds.index('MESSAGE_START') < kinds.index('TERMINAL') < kinds.index('TRANSPORT_CLOSED')
    finally:gate.close(); store.close()


def test_interruption_between_receipt_and_finish_is_read_only_recoverable(tmp_path, monkeypatch):
    install_transport(monkeypatch, Response([start(), ending()]))
    store, gate = make_gate(tmp_path)
    finish = store.finish
    def interrupted(reservation, **kwargs):
        if kwargs.get('actual_output_tokens') is not None:raise RuntimeError('injected commit gap')
        return finish(reservation, **kwargs)
    monkeypatch.setattr(store, 'finish', interrupted)
    try:
        with pytest.raises(RuntimeError, match='commit gap'):gate.complete(payload())
        before = dict(store.snapshot(gate.account))
        audit = audit_ledger(tmp_path/'requests.sqlite')
        assert audit['requests'][0]['state'] == 'TERMINAL_EVIDENCE_REQUIRES_RECONCILIATION'
        assert audit['status'] == 'BLOCKED'
        assert dict(store.snapshot(gate.account)) == before
        assert store.connection.execute('select count(*) from provider_receipts where usage_complete=1').fetchone()[0] == 1
    finally:gate.close(); store.close()


def test_pause_after_reservation_is_provably_unsent_without_refund(tmp_path):
    count = 0
    def guard():
        nonlocal count
        count += 1
        if count == 2:raise PauseRequested('offline pause')
    store, gate = make_gate(tmp_path, guard=guard)
    gate.upstream = lambda *_:pytest.fail('must never dispatch')
    try:
        with pytest.raises(PauseRequested):gate.complete(payload())
        audit = audit_ledger(tmp_path/'requests.sqlite')
        assert audit['status'] == 'CLEAR' and audit['requests'][0]['state'] == 'NEVER_DISPATCHED'
        snapshot = store.snapshot(gate.account)
        assert snapshot['calls'] == 1 and snapshot['output_tokens'] == 20 and snapshot['active'] == 0
    finally:gate.close(); store.close()


def test_pause_drains_terminal_and_prevents_next_dispatch(tmp_path, monkeypatch):
    opened = threading.Event(); release = threading.Event()
    def finish_later():
        opened.set(); assert release.wait(2); return ending()
    install_transport(monkeypatch, Response([start(), finish_later]))
    pause = PauseController(tmp_path)
    store, gate = make_gate(tmp_path, pause, pause.check)
    try:
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(gate.complete, payload())
            assert opened.wait(2)
            pause.request()
            with pytest.raises(PauseRequested):gate.complete(payload())
            release.set(); future.result(timeout=2)
        assert store.snapshot(gate.account)['calls'] == 1
        assert audit_ledger(tmp_path/'requests.sqlite')['status'] == 'CLEAR'
    finally:release.set(); gate.close(); store.close()


def test_pause_grace_expiry_keeps_unknown_and_full_charge(tmp_path, monkeypatch):
    closed = threading.Event(); opened = threading.Event()
    def stalled():
        opened.set(); assert closed.wait(2); raise ConnectionResetError('deadline socket close')
    install_transport(monkeypatch, Response([start(), stalled]), closed)
    pause = PauseController(tmp_path); pause.grace_seconds = .1
    store, gate = make_gate(tmp_path, pause, pause.check)
    try:
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(gate.complete, payload())
            assert opened.wait(2); pause.request()
            with pytest.raises(ConnectionResetError):future.result(timeout=2)
        audit = audit_ledger(tmp_path/'requests.sqlite')
        assert audit['requests'][0]['state'] == 'UNKNOWN'
        snapshot = store.snapshot(gate.account)
        assert snapshot['calls'] == 1 and snapshot['output_tokens'] == 20 and snapshot['active'] == 1
    finally:closed.set(); gate.close(); store.close()


def test_old_dispatching_record_blocks_new_sibling_root_without_mutation(tmp_path, capsys):
    old = tmp_path/'old'/'deepseek-native-authority'; old.mkdir(parents=True)
    store = BudgetStore(old/'requests.sqlite')
    store.create('old:0:a:GENERATION', max_calls=1, max_request_bytes=100,
                 max_output_tokens=20, deadline=time.time()+10)
    store.reserve('old:0:a:GENERATION', 'old-request', request_bytes=5, output_tokens=20)
    store.connection.execute('create table codex_phase_scopes(account text,status text)')
    store.connection.execute("insert into codex_phase_scopes values('old:0:a:GENERATION','DISPATCHING')")
    before = dict(store.snapshot('old:0:a:GENERATION'))
    with pytest.raises(RuntimeError, match='restart blocked'):check_restart_ledgers(tmp_path/'new')
    assert main(['deepseek-audit', '--ledger', str(old/'requests.sqlite')]) == 2
    assert json.loads(capsys.readouterr().out)['requests'][0]['state'] == 'UNKNOWN'
    assert store.connection.execute('select status from inference_reservations').fetchone()[0] == 'RESERVED'
    assert dict(store.snapshot('old:0:a:GENERATION')) == before
    store.close()


def test_pause_command_and_signals_only_request_cooperative_pause(tmp_path, capsys):
    pause = PauseController(tmp_path)
    assert main(['--root', str(tmp_path), 'pause']) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'PAUSE_REQUESTED'
    assert pause.requested
    other = PauseController(tmp_path/'signal')
    previous = signal.getsignal(signal.SIGINT)
    with other.signals():
        signal.raise_signal(signal.SIGINT)
        assert other.requested  # No KeyboardInterrupt and no child-group signal.
    assert signal.getsignal(signal.SIGINT) == previous


@pytest.mark.parametrize('status,expected', [(429, 'REJECTED'), (401, 'REJECTED'), (503, 'UNKNOWN')])
def test_explicit_rejection_is_distinct_from_ambiguous_server_error(tmp_path, monkeypatch, status, expected):
    response = Response([]); response.status = status
    install_transport(monkeypatch, response)
    store, gate = make_gate(tmp_path)
    try:
        with pytest.raises(RuntimeError):gate.complete(payload())
        row = audit_ledger(tmp_path/'requests.sqlite')['requests'][0]
        assert row['state'] == expected
        snapshot = store.snapshot(gate.account)
        assert snapshot['output_tokens'] == 20 and snapshot['calls'] == 1
        assert snapshot['active'] == (1 if expected == 'UNKNOWN' else 0)
        assert store.connection.execute('select count(*) from provider_receipts').fetchone()[0] == 0
    finally:gate.close(); store.close()


def test_missing_final_sse_frame_delimiter_remains_unknown(tmp_path, monkeypatch):
    install_transport(monkeypatch, Response([start(), ending().rstrip(b'\r\n')]))
    store, gate = make_gate(tmp_path)
    try:
        with pytest.raises(ValueError, match='Incomplete'):gate.complete(payload())
        assert audit_ledger(tmp_path/'requests.sqlite')['requests'][0]['state'] == 'UNKNOWN'
        assert store.connection.execute('select count(*) from provider_receipts').fetchone()[0] == 0
    finally:gate.close(); store.close()


def test_completed_label_without_provider_evidence_is_not_a_clear_audit(tmp_path):
    store, gate=make_gate(tmp_path)
    try:
        reservation=store.reserve(gate.account,'unsupported-completed',request_bytes=1,output_tokens=20)
        store.finish(reservation)
        audit=audit_ledger(tmp_path/'requests.sqlite')
        assert audit['status']=='BLOCKED' and audit['requests'][0]['state']=='UNKNOWN'
    finally:gate.close();store.close()


def test_legacy_complete_receipt_before_finish_is_recoverable_without_new_events(tmp_path):
    store, gate=make_gate(tmp_path)
    try:
        reservation=store.reserve(gate.account,'legacy-gap',request_bytes=1,output_tokens=20)
        store.save_receipt(reservation,{'input_tokens':10,'output_tokens':3},'a'*64,'legacy-message',protocol='/v1/messages')
        audit=audit_ledger(tmp_path/'requests.sqlite')
        assert audit['status']=='BLOCKED'
        assert audit['requests'][0]['state']=='TERMINAL_EVIDENCE_REQUIRES_RECONCILIATION'
        assert audit['requests'][0]['provider_message_id']=='legacy-message'
        assert store.snapshot(gate.account)['active']==1
    finally:gate.close();store.close()


def test_operator_exception_is_exact_read_only_and_cannot_exempt_current_run(tmp_path):
    from maestro.backends.coral.deepseek_lifecycle import ledger_fingerprint
    old=tmp_path/'old'/'deepseek-native-authority';old.mkdir(parents=True)
    store=BudgetStore(old/'requests.sqlite')
    store.create('old',max_calls=2,max_request_bytes=100,max_output_tokens=40,deadline=time.time()+20)
    reservation=store.reserve('old','pending',request_bytes=1,output_tokens=20)
    new=tmp_path/'new';(new/'authority').mkdir(parents=True)
    value={'new_run_root':str(new),'user_authorization':'Explicit user authorization for this historical request only',
        'remote_termination_confirmed':False,'retain_historical_budget':True,
        'entries':[{'ledger':str(old/'requests.sqlite'),'reservation_ids':[reservation],
                    'ledger_fingerprint':ledger_fingerprint(old/'requests.sqlite')}]}
    marker=new/'authority/deepseek-historical-exceptions.json';marker.write_text(json.dumps(value))
    before=dict(store.snapshot('old'));check_restart_ledgers(new)
    assert dict(store.snapshot('old'))==before
    assert audit_ledger(old/'requests.sqlite')['status']=='BLOCKED'
    # Copying the same operator grant to the current ledger is forbidden.
    (old.parent/'authority').mkdir()
    value['new_run_root']=str(old.parent)
    (old.parent/'authority/deepseek-historical-exceptions.json').write_text(json.dumps(value))
    with pytest.raises(RuntimeError,match='restart blocked'):check_restart_ledgers(old.parent)
    # Any changed quota snapshot invalidates the operator grant.
    store.connection.execute("update inference_budgets set max_calls=3 where account='old'")
    with pytest.raises(RuntimeError,match='restart blocked'):check_restart_ledgers(new)
    store.close()
