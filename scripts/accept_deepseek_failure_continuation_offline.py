"""Installed four-way TLS reset exhaustion, local abstention, next-wave recovery.

Local synthetic protocol check only: zero real model requests/evaluations.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
import socket
import ssl
import struct
import subprocess
import threading
import time

from maestro.backends.coral.deepseek_backend import NativeDeepSeekBackend,LocalPhaseAbstention
from maestro.backends.coral.deepseek_client import CoralDeepSeekClient
from maestro.backends.coral.deepseek_gate import DeepSeekUpstream
from maestro.backends.coral.deepseek_retry import DeepSeekRetryStore,RetryPolicy,RetryingDeepSeekProxy,audit_retry_continuation
from maestro.backends.coral.failure_report import write_failure_report
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.types import AgentState


def main(root,executable):
    root.mkdir(parents=True,exist_ok=False);cert=root/'cert.pem';key=root/'key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=127.0.0.1',
        '-addext','subjectAltName=IP:127.0.0.1','-keyout',str(key),'-out',str(cert)],check=True,
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    previous=os.environ.get('SSL_CERT_FILE');os.environ['SSL_CERT_FILE']=str(cert)
    counter=0;lock=threading.Lock();barrier=threading.Barrier(4);opened=[threading.Event() for _ in range(8)]
    def frame(v):return b'data: '+json.dumps(v).encode()+b'\n\n'
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            nonlocal counter
            self.rfile.read(int(self.headers['Content-Length']))
            with lock:index=counter;counter+=1
            if index<4:barrier.wait(timeout=10)
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
            self.wfile.write(frame({'type':'message_start','message':{'type':'message','id':f'localhost-{index}',
                'role':'assistant','model':'deepseek-flash','content':[],'usage':{'input_tokens':10,'output_tokens':0}}}));self.wfile.flush()
            if index<8:
                assert opened[index].wait(5)
                raw=socket.socket(fileno=self.connection.detach());raw.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('ii',1,0));raw.close()
                self.close_connection=True;return
            self.wfile.write(frame({'type':'content_block_start','index':0,'content_block':{'type':'text','text':''}})+
                frame({'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':'Offline preparation summary; no further tools.'}})+
                frame({'type':'content_block_stop','index':0})+frame({'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':3}})+
                frame({'type':'message_stop'}));self.wfile.flush()
    class Upstream(DeepSeekUpstream):
        def __call__(self,value,deadline,*,progress=None):
            def observe(kind,**data):
                progress(kind,**data)
                if kind=='MESSAGE_START':
                    index=int(data['provider_message_id'].split('-')[-1])
                    if index<8:opened[index].set()
            return super().__call__(value,deadline,progress=observe)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key);server.socket=context.wrap_socket(server.socket,server_side=True)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    policy=RetryPolicy(continue_after_network_failure=True,warm_seed_fallback=True)
    store=DeepSeekRetryStore(root/'requests.sqlite');store.enable_retries(policy)
    cli=executable.resolve();client=CoralDeepSeekClient(model='deepseek-flash',work_root=root/'native',executable=str(cli),
        cli_version=subprocess.check_output([str(cli),'--version'],text=True).strip(),
        cli_sha256=hashlib.sha256(cli.read_bytes()).hexdigest(),max_output_tokens=96000)
    upstream=Upstream(f'https://127.0.0.1:{server.server_port}/anthropic','synthetic-local-only')
    def proxy(*args,**kwargs):return RetryingDeepSeekProxy(*args,retry_policy=policy,upstream=upstream,**kwargs)
    seed=root/'seed';seed.mkdir();(seed/'run').write_text('print("offline protocol fixture only")\n')
    backend=NativeDeepSeekBackend(client,store,WorkspaceStore(root/'work',root/'cas'),precheck=None,
        seed_solver=seed,candidate_runner=object(),image='',proxy_factory=proxy,simulation=True,
        config=RunConfig(preparation_calls=1,coral={'deepseek_transport_retry':policy.__dict__}))
    try:
        client.preflight();backend.begin_episode('offline-failure-continuation')
        parent=backend.prepare_seed();backend.register_parent(parent['candidate_hash'],parent['candidate_path'])
        states=[AgentState(f'agent-{i}',parent['candidate_hash'],.5) for i in range(4)]
        def wave(index):
            deadline=time.monotonic()+40
            def run(state):
                try:
                    _,usage=backend.prepare_with_receipt(state,index,{},deadline=deadline)
                    return {'agent':state.agent_id,'status':'PREPARED','usage':usage}
                except LocalPhaseAbstention:
                    return {'agent':state.agent_id,'status':'ABSTAIN','usage':backend._usage(f'{backend.episode_id}:{index}:{state.agent_id}:PREPARATION')}
            with ThreadPoolExecutor(4) as pool:return list(pool.map(run,states))
        first=wave(0);assert all(x['status']=='ABSTAIN' for x in first) and counter==8
        assert all(x['usage']['unresolved_requests']==2 and x['usage']['continuation_blocking_requests']==0 for x in first)
        backend._assert_clear();second=wave(1)
        assert all(x['status']=='PREPARED' for x in second) and counter==12
        audit=audit_retry_continuation(root/'requests.sqlite');assert audit['status']=='CONTINUABLE_WITH_UNKNOWN_COST'
        cleanup=[json.loads(p.read_text()) for p in (root/'native').glob('*/cleanup.json')]
        assert len(cleanup)==8 and all(p['local_process_group_reclaimed'] and p['tool_service_reclaimed'] for p in cleanup)
        reports=write_failure_report(root,ledger_path=root/'requests.sqlite')
        assert reports['counts']['unknown_requests']==8 and reports['counts']['affected_phase_scopes']==4
        result={'status':'PASSED_OFFLINE_ONLY','actual_model_requests':0,'synthetic_localhost_requests':counter,
            'first_wave':first,'second_wave':second,'unknown_requests':8,'phase_abstentions':4,
            'native_process_groups_reclaimed':8,'audit':audit,'failure_report':reports,
            'unknown_remote_termination_claim':False,'heldout_or_formal_evaluation':False}
        (root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps({'status':result['status'],'first_wave_abstentions':4,'second_wave_prepared':4,
            'unknown_requests':8,'affected_phase_scopes':4,'actual_model_requests':0}))
    finally:
        for e in opened:e.set()
        server.shutdown();server.server_close();thread.join(timeout=3);backend.close()
        if previous is None:os.environ.pop('SSL_CERT_FILE',None)
        else:os.environ['SSL_CERT_FILE']=previous


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',required=True,type=Path)
    parser.add_argument('--executable',required=True,type=Path);args=parser.parse_args();main(args.root.resolve(),args.executable)
