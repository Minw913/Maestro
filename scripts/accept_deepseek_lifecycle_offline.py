"""Installed four-way DSH/TLS interruption acceptance, synthetic localhost only.

No model call, credential lookup, candidate evaluation or experiment. Exercises
actual Coral handles, MCP-free native CLI, HTTPS transport, request gate and
persistent ledger. One stream deliberately lacks a terminal receipt.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import ssl
import subprocess
import threading
import time

from maestro.backends.coral.deepseek_client import CoralDeepSeekClient
from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy, DeepSeekUpstream
from maestro.backends.coral.deepseek_lifecycle import audit_ledger
from maestro.execution.budgets import BudgetStore
from maestro.execution.pause import PauseController


def main(root, executable):
    root.mkdir(parents=True, exist_ok=False)
    cert=root/'localhost-cert.pem';key=root/'localhost-key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
        '-subj','/CN=127.0.0.1','-addext','subjectAltName=IP:127.0.0.1',
        '-keyout',str(key),'-out',str(cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    previous_cert=os.environ.get('SSL_CERT_FILE');os.environ['SSL_CERT_FILE']=str(cert)
    barrier=threading.Barrier(4); opened=threading.Event(); release=threading.Event();stop=threading.Event()
    lock=threading.Lock();counter=0;starts=0
    def frame(value):return b'data: '+json.dumps(value).encode()+b'\n\n'
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            nonlocal counter,starts
            assert self.path=='/anthropic/v1/messages'
            self.rfile.read(int(self.headers['Content-Length']))
            with lock:index=counter;counter+=1
            try:
                barrier.wait(timeout=15)
                self.send_response(200);self.send_header('Content-Type','text/event-stream')
                self.send_header('request-id',f'localhost-request-{index}');self.end_headers()
                self.wfile.write(frame({'type':'message_start','message':{'type':'message',
                    'id':f'localhost-message-{index}','model':'deepseek-flash','content':[],
                    'usage':{'input_tokens':10,'output_tokens':0}}}));self.wfile.flush()
                with lock:
                    starts+=1
                    if starts==4:opened.set()
                if index==3:
                    stop.wait(15)  # Deliberately never sends message_stop.
                    return
                if not release.wait(15):return
                self.wfile.write(frame({'type':'content_block_start','index':0,
                    'content_block':{'type':'text','text':''}})+frame({'type':'content_block_delta','index':0,
                    'delta':{'type':'text_delta','text':'{"ok":true}'}})+frame({'type':'content_block_stop','index':0})+
                    frame({'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':3}})+
                    frame({'type':'message_stop'}));self.wfile.flush()
            except (OSError,threading.BrokenBarrierError):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key)
    server.socket=context.wrap_socket(server.socket,server_side=True)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    store=BudgetStore(root/'requests.sqlite');pause=PauseController(root);pause.grace_seconds=.5
    executable=Path(executable).resolve()
    client=CoralDeepSeekClient(model='deepseek-flash',work_root=root/'native',executable=str(executable),
        cli_version=subprocess.check_output([str(executable),'--version'],text=True).strip(),
        cli_sha256=hashlib.sha256(executable.read_bytes()).hexdigest())
    client.pause=pause
    outcomes=[];gates=[];deadline=time.monotonic()+30
    def run(index):
        gate=DeepSeekQuotaProxy(store,f'offline:0:agent-{index}:PLAN_RO','deepseek-flash',
            deadline=deadline,max_calls=1,max_request_bytes=1024*1024,max_output_tokens=64,
            per_request_tokens=64,request_guard=pause.check,
            upstream=DeepSeekUpstream(f'https://127.0.0.1:{server.server_port}/anthropic',
                                      'synthetic-localhost-only',pause_controller=pause))
        gates.append(gate)
        try:
            response=client.run_phase('Offline protocol check. Return exactly {"ok":true}. No tools.',
                base_url=gate.start(),deadline=deadline,agent_id=f'agent-{index}')
            assert json.loads(response.text)=={'ok':True}
            return {'agent':index,'status':'COMPLETED'}
        except Exception as error:
            return {'agent':index,'status':type(error).__name__}
        finally:gate.close()
    try:
        client.preflight()
        with pause.signals(), ThreadPoolExecutor(4) as pool:
            futures=[pool.submit(run,i) for i in range(4)]
            assert opened.wait(20),'All four installed CLIs must reach the local TLS server'
            signal.raise_signal(signal.SIGINT)  # Signal controller only, never its children.
            release.set()
            outcomes=[f.result(timeout=10) for f in futures]
        audit=audit_ledger(root/'requests.sqlite')
        states=[r['state'] for r in audit['requests']]
        assert counter==4 and states.count('COMPLETED')==3 and states.count('UNKNOWN')==1
        assert all(r['provider_message_id'] and r['response_request_id'] and r['transport_closed'] for r in audit['requests'])
        cleanup=[json.loads(p.read_text()) for p in (root/'native').glob('*/cleanup.json')]
        assert len(cleanup)==4 and all(c['local_process_group_reclaimed'] and c['tool_service_reclaimed'] for c in cleanup)
        client.close()
        result={'status':'PASSED_OFFLINE_ONLY','actual_model_requests':0,'synthetic_localhost_requests':counter,
            'installed_cli_version':client.cli_version,'outcomes':outcomes,'audit':audit,
            'local_process_groups_reclaimed':4,'terminal_usage_receipts':3,
            'remote_termination_claim_for_truncated_stream':False,'budget_reset':False,'automatic_retry':False}
        (root/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
    finally:
        stop.set();release.set();server.shutdown();server.server_close();thread.join(timeout=3)
        for gate in gates:gate.close()
        store.close()
        if previous_cert is None:os.environ.pop('SSL_CERT_FILE',None)
        else:os.environ['SSL_CERT_FILE']=previous_cert


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--executable',required=True)
    args=parser.parse_args();main(args.root.resolve(),args.executable)
