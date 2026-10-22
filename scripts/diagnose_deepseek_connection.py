"""Read-only failed-request timeline plus bounded credential-free TLS probes.

No HTTP inference request, API key lookup, retry, cancellation or ledger mutation.
A successful current TLS handshake cannot diagnose a past connection reset.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import ssl
import time

from maestro.backends.coral.deepseek_lifecycle import audit_ledger, exception_details, ledger_fingerprint
from maestro.storage.paths import run_paths


def diagnose(run_root, output, *, tls_probes=3):
    if not 0<=tls_probes<=3:raise ValueError('At most three non-HTTP TLS probes')
    if output.exists():raise ValueError('Diagnostic output must be a new directory')
    output.mkdir(parents=True)
    ledger=run_root/'deepseek-native-authority/requests.sqlite'
    before=ledger_fingerprint(ledger)
    db=sqlite3.connect(ledger.resolve().as_uri()+'?mode=ro',uri=True)
    db.row_factory=sqlite3.Row
    try:
        errors=[]
        for row in db.execute("SELECT * FROM inference_reservations WHERE status='UNKNOWN'"):
            item=dict(row)
            item['events']=[{**dict(e),'data_json':json.loads(e['data_json'])} for e in db.execute(
                'SELECT kind,utc,monotonic,data_json FROM deepseek_request_events WHERE reservation_id=? ORDER BY id',
                (row['reservation_id'],))]
            budget=db.execute('SELECT deadline FROM inference_budgets WHERE account=?',(row['account'],)).fetchone()
            failure=next((e for e in item['events'] if e['kind']=='ERROR'),None)
            dispatch=next((e for e in item['events'] if e['kind']=='DISPATCH_STARTED'),None)
            if failure and dispatch:
                item['dispatch_to_error_seconds']=failure['monotonic']-dispatch['monotonic']
                if budget:item['budget_deadline_remaining_at_error_seconds']=budget['deadline']-datetime.fromisoformat(failure['utc']).timestamp()
            errors.append(item)
        statuses=[dict(x) for x in db.execute('SELECT status,count(*) AS count FROM inference_reservations GROUP BY status')]
        http=[dict(x) for x in db.execute("SELECT json_extract(data_json,'$.http_status') AS http_status,count(*) AS count FROM deepseek_request_events WHERE kind='HEADERS' GROUP BY 1")]
    finally:db.close()
    pause=run_paths(run_root).local/'authority/pause-request.json'
    pause_record=json.loads(pause.read_text()) if pause.exists() else None
    probes=[]
    for index in range(tls_probes):
        probe={'index':index,'started_utc':datetime.now(timezone.utc).isoformat(),'http_requests':0}
        started=time.monotonic()
        try:
            with socket.create_connection(('api.deepseek.com',443),timeout=5) as tcp:
                with ssl.create_default_context().wrap_socket(tcp,server_hostname='api.deepseek.com') as tls:
                    probe.update(status='TLS_CONNECTED',peer=tls.getpeername(),local=tls.getsockname(),
                                 tls_version=tls.version(),cipher=tls.cipher(),
                                 leaf_certificate_sha256=hashlib.sha256(tls.getpeercert(binary_form=True)).hexdigest())
        except (OSError,ssl.SSLError) as error:
            probe.update(status='FAILED',**exception_details(error))
        probe['wall_seconds']=time.monotonic()-started;probes.append(probe)
    after=ledger_fingerprint(ledger)
    assert before==after,'Read-only investigation changed ledger identity'
    result={'checked_utc':datetime.now(timezone.utc).isoformat(),'run_root':str(run_root),
            'actual_model_requests':0,'authenticated_api_requests':0,'http_requests':0,
            'ledger_fingerprint_before':before,'ledger_fingerprint_after':after,
            'ledger_unchanged':True,'request_statuses':statuses,'http_statuses':http,
            'unknown_requests':errors,'pause_record':pause_record,'current_tls_probes':probes,
            'limitations':['Old logs have no traceback, socket endpoints or explicit watchdog-abort event.',
                'Current successful handshakes do not exclude a past server or network reset.',
                'No provider-side private request log or terminal receipt was retrieved.'],
            'audit':audit_ledger(ledger),'automatic_retry':False,'reconciliation_performed':False}
    (output/'investigation.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--tls-probes',type=int,default=3)
    args=parser.parse_args()
    result=diagnose(args.run_root.resolve(),args.output.resolve(),tls_probes=args.tls_probes)
    print(json.dumps({'output':str(args.output.resolve()),'ledger_unchanged':result['ledger_unchanged'],
        'actual_model_requests':0,'tls_probe_statuses':[x['status'] for x in result['current_tls_probes']]}))
