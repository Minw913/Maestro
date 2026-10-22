"""Interruptions through the production transport/gate; no model or credentials."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import shutil
import signal
import sqlite3
import socket
import ssl
import struct
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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
    def disconnect():raise ConnectionResetError(104, 'private diagnostic must not be persisted')
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
        error=json.loads(store.connection.execute(
            "select data_json from deepseek_request_events where kind='ERROR'").fetchone()[0])
        assert error['exception_errno']==104
        assert error['transport_operation']=='read_response_body'
        assert not error['local_abort_requested'] and error['local_abort_reason'] is None
        assert error['deadline_remaining_seconds']>0 and error['bytes_received']>0
        frames=error['exception_chain'][0]['frames']
        assert frames[0]['file']=='deepseek_gate.py'
        assert frames[-1]['function']=='disconnect' and frames[-1]['line']>0
        closed=json.loads(store.connection.execute(
            "select data_json from deepseek_request_events where kind='TRANSPORT_CLOSED'").fetchone()[0])
        assert closed['reason']=='exception' and closed['watchdog_reclaimed']
        assert store.connection.execute(
            "select count(*) from deepseek_request_events where kind='LOCAL_ABORT'").fetchone()[0]==0
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
        assert 'CONNECTED' in kinds and 'LOCAL_ABORT' not in kinds
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
        abort=json.loads(store.connection.execute(
            "select data_json from deepseek_request_events where kind='LOCAL_ABORT'").fetchone()[0])
        error=json.loads(store.connection.execute(
            "select data_json from deepseek_request_events where kind='ERROR'").fetchone()[0])
        assert abort['reason']=='pause_drain_deadline'
        assert error['local_abort_requested'] and error['local_abort_reason']=='pause_drain_deadline'
        assert error['local_abort_monotonic'] is not None
    finally:closed.set(); gate.close(); store.close()


def test_phase_deadline_abort_is_distinct_from_remote_reset(tmp_path, monkeypatch):
    closed=threading.Event()
    def stalled():
        assert closed.wait(2)
        raise ConnectionResetError(104,'socket interrupted by local watchdog')
    install_transport(monkeypatch,Response([start(),stalled]),closed)
    store,gate=make_gate(tmp_path);gate.deadline=time.monotonic()+.15
    try:
        with pytest.raises(ConnectionResetError):gate.complete(payload())
        error=json.loads(store.connection.execute(
            "select data_json from deepseek_request_events where kind='ERROR'").fetchone()[0])
        assert error['local_abort_reason']=='phase_deadline' and error['local_abort_requested']
        assert error['deadline_remaining_seconds']<=0
        assert audit_ledger(tmp_path/'requests.sqlite')['requests'][0]['state']=='UNKNOWN'
        assert store.snapshot(gate.account)['active']==1
    finally:closed.set();gate.close();store.close()


def test_exception_chain_records_locations_without_private_messages_or_locals():
    from maestro.backends.coral.deepseek_lifecycle import exception_details
    def fail():
        private_key='DO_NOT_RECORD_KEY'
        try:raise OSError(104,'DO_NOT_RECORD_RESPONSE')
        except OSError as error:raise RuntimeError(private_key) from error
    try:fail()
    except RuntimeError as error:details=exception_details(error)
    assert [e['exception_class'] for e in details['exception_chain']]==['RuntimeError','ConnectionResetError']
    assert details['exception_chain'][1]['exception_errno']==104
    serialized=json.dumps(details)
    assert 'DO_NOT_RECORD' not in serialized and 'private_key' not in serialized
    assert all(set(f)=={'file','function','line'} for e in details['exception_chain'] for f in e['frames'])


@pytest.mark.skipif(os.name!='posix' or not shutil.which('openssl'),reason='Local TLS/RST needs POSIX and openssl')
def test_actual_tls_peer_reset_is_read_failure_without_local_abort(tmp_path,monkeypatch):
    cert=tmp_path/'cert.pem';key=tmp_path/'key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
        '-subj','/CN=127.0.0.1','-addext','subjectAltName=IP:127.0.0.1',
        '-keyout',str(key),'-out',str(cert)],check=True,
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    monkeypatch.setenv('SSL_CERT_FILE',str(cert))
    opened=threading.Event();requests=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            requests.append(self.path)
            self.rfile.read(int(self.headers['Content-Length']))
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
            self.wfile.write(start());self.wfile.flush()
            if opened.wait(3):
                raw=socket.socket(fileno=self.connection.detach())
                raw.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('ii',1,0))
                raw.close()  # An actual peer TCP RST after the client records message_start.
            self.close_connection=True
    class ObservedUpstream(DeepSeekUpstream):
        def __call__(self,value,deadline,*,progress=None):
            def observe(kind,**data):
                progress(kind,**data)
                if kind=='MESSAGE_START':opened.set()
            return super().__call__(value,deadline,progress=observe)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key)
    server.socket=context.wrap_socket(server.socket,server_side=True)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    store,gate=make_gate(tmp_path)
    gate.upstream=ObservedUpstream(f'https://127.0.0.1:{server.server_port}/anthropic','localhost-only')
    try:
        with pytest.raises(ConnectionResetError):gate.complete(payload())
        rows=[(r[0],json.loads(r[1])) for r in store.connection.execute(
            'select kind,data_json from deepseek_request_events order by id')]
        error=next(data for kind,data in rows if kind=='ERROR')
        connected=next(data for kind,data in rows if kind=='CONNECTED')
        assert connected['peer_ip']=='127.0.0.1' and connected['tls_version'].startswith('TLS')
        assert error['transport_operation']=='read_response_body' and error['exception_errno']>0
        assert not error['local_abort_requested'] and all(kind!='LOCAL_ABORT' for kind,_ in rows)
        assert requests==['/anthropic/v1/messages']  # No automatic retry.
        assert audit_ledger(tmp_path/'requests.sqlite')['requests'][0]['state']=='UNKNOWN'
        assert store.snapshot(gate.account)['active']==1 and store.snapshot(gate.account)['output_tokens']==20
    finally:
        opened.set();gate.close();store.close();server.shutdown();server.server_close();thread.join(timeout=2)


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


def test_archived_pending_snapshot_requires_exact_original_terminal_proof(tmp_path):
    from maestro.backends.coral.deepseek_lifecycle import ledger_fingerprint
    original=tmp_path/'original/deepseek-native-authority';original.mkdir(parents=True)
    archive=tmp_path/'archive/deepseek-native-authority';archive.mkdir(parents=True)
    store=BudgetStore(original/'requests.sqlite')
    store.create('original',max_calls=1,max_request_bytes=100,max_output_tokens=20,deadline=time.time()+30)
    rid=store.reserve('original','request-identity',request_bytes=5,output_tokens=20)
    db=sqlite3.connect(archive/'requests.sqlite');store.connection.backup(db);db.close()
    store.save_receipt(rid,{'input_tokens':10,'output_tokens':3},'a'*64,'real-terminal-id',protocol='/v1/messages')
    store.finish(rid,actual_output_tokens=3)
    new=tmp_path/'new';(new/'authority').mkdir(parents=True)
    entry={'ledger':str(archive/'requests.sqlite'),'reservation_ids':[rid],
           'ledger_fingerprint':ledger_fingerprint(archive/'requests.sqlite'),
           'snapshot_of':str(original/'requests.sqlite'),
           'source_ledger_fingerprint':ledger_fingerprint(original/'requests.sqlite')}
    marker=new/'authority/deepseek-historical-exceptions.json'
    value={'new_run_root':str(new),'user_authorization':'Bounded qualification; archived copy has original terminal proof',
           'remote_termination_confirmed':False,'retain_historical_budget':True,'entries':[entry]}
    marker.write_text(json.dumps(value));check_restart_ledgers(new)
    assert audit_ledger(archive/'requests.sqlite')['status']=='BLOCKED'
    assert ledger_fingerprint(archive/'requests.sqlite')==entry['ledger_fingerprint']
    # A matching reservation ID with a different original request is no proof,
    # even if an operator updates the source fingerprint after that change.
    store.connection.execute("UPDATE inference_reservations SET request_id='different-request'")
    entry['source_ledger_fingerprint']=ledger_fingerprint(original/'requests.sqlite')
    marker.write_text(json.dumps(value))
    with pytest.raises(RuntimeError,match='restart blocked'):check_restart_ledgers(new)
    store.close()


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


def test_historical_cascade_quarantine_requires_exact_scopes_and_cleanup(tmp_path):
    import hashlib
    from maestro.backends.coral.deepseek_lifecycle import _historical_exception,ledger_fingerprint
    old=tmp_path/'old/deepseek-native-authority';old.mkdir(parents=True)
    store=BudgetStore(old/'requests.sqlite')
    store.connection.execute('CREATE TABLE codex_phase_scopes(account TEXT PRIMARY KEY,status TEXT)')
    accounts=['warm-old:1:a:PREPARATION','warm-old:1:b:PREPARATION']
    store.create(accounts[0],max_calls=1,max_request_bytes=100,max_output_tokens=20,deadline=time.time()+10)
    rid=store.reserve(accounts[0],'lost',request_bytes=1,output_tokens=20)
    store.connection.executemany('INSERT INTO codex_phase_scopes VALUES(?,?)',[(a,'UNKNOWN') for a in accounts])
    supervisor=tmp_path/'supervisor.json';supervisor.write_text(json.dumps({'kind':'systemd_cgroup_empty','MainPID':0,'ControlGroup':'','ActiveState':'failed','remaining_native_processes':0,'running_rootless_containers':0}))
    proofs=[]
    for a in accounts:
        phase=tmp_path/a.split(':')[2];phase.mkdir()
        scope=phase/'scope.json';scope.write_text(json.dumps({'prompt':'PREPARATION:\n'+json.dumps({'episode_id':'warm-old','round_index':1,'agent_id':a.split(':')[2]})}))
        cleanup=phase/'cleanup.json';cleanup.write_text(json.dumps({'local_process_group_reclaimed':True}))
        proof={'account':a}
        for role,p in [('scope',scope),('cleanup',cleanup),('supervisor',supervisor)]:
            proof[role+'_path']=str(p);proof[role+'_sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
        proofs.append(proof)
    entry={'ledger':str(old/'requests.sqlite'),'reservation_ids':[rid],'ledger_fingerprint':ledger_fingerprint(old/'requests.sqlite'),'reclaimed_scopes':proofs}
    new=tmp_path/'new';(new/'authority').mkdir(parents=True)
    manifest=new/'authority/deepseek-historical-exceptions.json'
    value={'new_run_root':str(new),'user_authorization':'Explicit isolated qualification','remote_termination_confirmed':False,'retain_historical_budget':True,'entries':[entry]}
    manifest.write_text(json.dumps(value));audit=audit_ledger(old/'requests.sqlite')
    assert _historical_exception(new,old/'requests.sqlite',audit)
    before=ledger_fingerprint(old/'requests.sqlite')
    # Omitting a completed-request scope from the cascade is not acceptable.
    entry['reclaimed_scopes']=proofs[:1];manifest.write_text(json.dumps(value))
    assert not _historical_exception(new,old/'requests.sqlite',audit)
    entry['reclaimed_scopes']=proofs;manifest.write_text(json.dumps(value))
    supervisor.write_text(supervisor.read_text().replace('"MainPID": 0','"MainPID": 123'))
    assert not _historical_exception(new,old/'requests.sqlite',audit)
    assert ledger_fingerprint(old/'requests.sqlite')==before
    store.close()
