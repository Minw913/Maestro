"""Four installed DSH CLIs recover from actual localhost TLS peer resets.

Synthetic local protocol acceptance only. Zero provider/API/model calls.
Uses the configured 1 retry / 10 extra requests / 10 second backoff policy.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import threading
import time

from maestro.backends.coral.deepseek_client import CoralDeepSeekClient
from maestro.backends.coral.deepseek_gate import DeepSeekUpstream
from maestro.backends.coral.deepseek_retry import DeepSeekRetryStore,RetryingDeepSeekProxy,RetryPolicy,audit_retry_continuation
from maestro.backends.coral.deepseek_lifecycle import audit_ledger


def main(root,executable):
    root.mkdir(parents=True,exist_ok=False)
    cert=root/'localhost-cert.pem';key=root/'localhost-key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
        '-subj','/CN=127.0.0.1','-addext','subjectAltName=IP:127.0.0.1',
        '-keyout',str(key),'-out',str(cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    old_cert=os.environ.get('SSL_CERT_FILE');os.environ['SSL_CERT_FILE']=str(cert)
    lock=threading.Lock();counter=0;barrier=threading.Barrier(4);opened=[threading.Event() for _ in range(4)]
    def frame(v):return b'data: '+json.dumps(v).encode()+b'\n\n'
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            nonlocal counter
            assert self.path=='/anthropic/v1/messages'
            self.rfile.read(int(self.headers['Content-Length']))
            with lock:index=counter;counter+=1
            if index<4:barrier.wait(timeout=15)
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
            self.wfile.write(frame({'type':'message_start','message':{'id':f'localhost-{index}',
                'type':'message','role':'assistant','model':'deepseek-flash','content':[],
                'usage':{'input_tokens':10,'output_tokens':0}}}));self.wfile.flush()
            if index<4:
                assert opened[index].wait(5),'Client must persist early message ID before peer reset'
                raw=socket.socket(fileno=self.connection.detach())
                raw.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('ii',1,0));raw.close()
                self.close_connection=True;return
            self.wfile.write(frame({'type':'content_block_start','index':0,'content_block':{'type':'text','text':''}})+
                frame({'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':'{"ok":true}'}})+
                frame({'type':'content_block_stop','index':0})+
                frame({'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':3}})+
                frame({'type':'message_stop'}));self.wfile.flush()
    class ObservedUpstream(DeepSeekUpstream):
        def __call__(self,payload,deadline,*,progress=None):
            def observe(kind,**data):
                progress(kind,**data)
                if kind=='MESSAGE_START':
                    index=int(data['provider_message_id'].split('-')[-1])
                    if index<4:opened[index].set()
            return super().__call__(payload,deadline,progress=observe)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key)
    server.socket=context.wrap_socket(server.socket,server_side=True)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    store=DeepSeekRetryStore(root/'requests.sqlite');policy=RetryPolicy();store.enable_retries(policy)
    executable=executable.resolve();client=CoralDeepSeekClient(model='deepseek-flash',work_root=root/'native',
        executable=str(executable),cli_version=subprocess.check_output([str(executable),'--version'],text=True).strip(),
        cli_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),max_output_tokens=96000)
    gates=[];deadline=time.monotonic()+40;began=time.monotonic()
    def run(index):
        gate=RetryingDeepSeekProxy(store,f'offline:0:agent-{index}:PLAN_RO','deepseek-flash',deadline=deadline,
            max_calls=1,max_request_bytes=1024*1024,max_output_tokens=96000,per_request_tokens=96000,
            upstream=ObservedUpstream(f'https://127.0.0.1:{server.server_port}/anthropic','synthetic-local-only'),retry_policy=policy)
        gates.append(gate)
        try:
            response=client.run_phase('Local protocol check only. Return exactly {"ok":true}; no tools.',
                base_url=gate.start(),deadline=deadline,agent_id=f'agent-{index}')
            assert json.loads(response.text)=={'ok':True};gate.assert_resolved()
            usage=gate.usage();assert usage['calls']==2 and usage['logical_calls']==1 and usage['retry_attempts']==1
            assert not usage['usage_complete'] and usage['continuation_blocking_requests']==0
            return {'agent':index,'status':'COMPLETED_WITH_UNKNOWN_COST','usage':usage}
        finally:gate.close()
    try:
        client.preflight()
        with ThreadPoolExecutor(4) as pool:outcomes=list(pool.map(run,range(4)))
        audit=audit_ledger(root/'requests.sqlite');continuation=audit_retry_continuation(root/'requests.sqlite')
        assert counter==8 and sum(r['state']=='UNKNOWN' for r in audit['requests'])==4
        assert sum(r['state']=='COMPLETED' for r in audit['requests'])==4
        assert audit['status']=='BLOCKED' and continuation['status']=='CONTINUABLE_WITH_UNKNOWN_COST'
        waits=store.connection.execute("SELECT logical_id,kind,monotonic FROM deepseek_retry_events WHERE kind IN ('RETRY_WAIT_STARTED','RETRY_WAIT_FINISHED')").fetchall()
        spans={}
        for row in waits:spans.setdefault(row[0],{})[row[1]]=row[2]
        assert len(spans)==4 and all(v['RETRY_WAIT_FINISHED']-v['RETRY_WAIT_STARTED']>=10 for v in spans.values())
        cleanup=[json.loads(f.read_text()) for f in (root/'native').glob('*/cleanup.json')]
        assert len(cleanup)==4 and all(x['local_process_group_reclaimed'] and x['tool_service_reclaimed'] for x in cleanup)
        result={'status':'PASSED_OFFLINE_ONLY','actual_model_requests':0,'synthetic_localhost_requests':counter,
            'policy':policy.__dict__,'wall_seconds':time.monotonic()-began,'outcomes':outcomes,'cost_audit':audit,
            'operational_audit':continuation,'four_local_process_groups_reclaimed':True,
            'unknown_remote_termination_claim':False,'automatic_candidate_or_evaluation_retry':False}
        (root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps({'status':result['status'],'localhost_requests':counter,'actual_model_requests':0,
            'actual_backoff_seconds_min':min(v['RETRY_WAIT_FINISHED']-v['RETRY_WAIT_STARTED'] for v in spans.values())}))
    finally:
        for e in opened:e.set()
        server.shutdown();server.server_close();thread.join(timeout=3)
        for gate in gates:gate.close()
        client.close();store.close()
        if old_cert is None:os.environ.pop('SSL_CERT_FILE',None)
        else:os.environ['SSL_CERT_FILE']=old_cert


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--executable',type=Path,required=True);args=parser.parse_args()
    main(args.root.resolve(),args.executable)
