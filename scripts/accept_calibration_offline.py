"""No-model control acceptance: production broker, quota gate and real Docker.

Synthetic local protocol receipts are NEVER calibration/model usage evidence.
Real training graph debug and the production fixed precheck execute in Docker.
"""
from pathlib import Path
from types import SimpleNamespace
import argparse
import http.client
import importlib.util
import json
import threading
import subprocess
import sys
from urllib.parse import urlsplit

from maestro.backends.codex_session.autonomous_backend import StrictCodexBackend
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.execution.budgets import BudgetStore
from maestro.tasks.pace.runner import smoke_test
from maestro.types import stable_hash

spec = importlib.util.spec_from_file_location('calibration_driver', Path(__file__).with_name('calibrate_codex_timing.py'))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)
BASE = Path(__file__).resolve().parents[1]


class LocalResponse:
    status = 200
    headers = {'Content-Type': 'text/event-stream'}
    def iter_bytes(self, deadline):
        yield b'data: '+json.dumps({'type':'response.completed','response':{
            'id':'offline-control-only','status':'completed','output':[],
            'usage':{'input_tokens':0,'output_tokens':0}}}).encode()+b'\n\n'
    def close(self):
        pass


def local_upstream(path, headers, body, timeout):
    return LocalResponse()  # No external HTTP operation exists in this fixture.


class LocalProtocolClient:
    model = 'gpt-6-astra'
    def __init__(self, *, fault=False):
        self.barrier = threading.Barrier(4)
        self.fault = fault
        self.lock = threading.Lock()
        self.planners = 0
    def run_phase(self, prompt, *, base_url, deadline, tools, output_schema=None, session_id=None):
        if output_schema is not None:
            with self.lock:
                self.planners += 1
                index = self.planners
            self.barrier.wait(timeout=15)
            if self.fault and index == 2:
                raise IndexError('offline injected planning row failure')
        url = urlsplit(base_url)
        assert url.hostname in ('127.0.0.1', 'localhost')
        connection = http.client.HTTPConnection(url.hostname,url.port,timeout=10)
        connection.request('POST',url.path+'/responses',json.dumps({'model':self.model,'input':[],'tools':[]}),
            {'Authorization':'Bearer offline-control-fixture','Content-Type':'application/json'})
        response = connection.getresponse()
        body = response.read()
        connection.close()
        if response.status != 200:
            raise driver.CalibrationHalted('Stopped local protocol fixture')
        if output_schema is not None:
            raw = json.dumps({'action':'revise','plan':{'target':'local_search','mod_type':'parameter_change',
                'diagnosis':'Offline control acceptance only','change':'Preserve greedy solver and annotate control fixture',
                'expected_effect':'No claimed solver improvement','evidence_run_ids':[]},'lessons':[]})
        else:
            tools('maestro_tool', {'action':'read','path':'solver/run'})
            tools('maestro_tool', {'action':'write','path':'solver/run',
                'content':(BASE/'data/seed_solver/run').read_text()+'\n# Offline acceptance control; never a formal candidate.\n',
                'executable':True})
            tools('maestro_tool', {'action':'write','path':'private/notes.md','content':'Offline acceptance only'})
            raw = 'Offline protocol fixture complete; no model inference.'
        return SimpleNamespace(text=raw,thread_id='offline-control',response_hash=stable_hash(raw))
    def close(self):
        pass


def builder(*, fault=False):
    def build(config,root):
        root.mkdir(parents=True)
        backend = StrictCodexBackend(LocalProtocolClient(fault=fault),BudgetStore(root/'initial.sqlite'),
            WorkspaceStore(root/'workspaces',root/'cas'),precheck=None,
            seed_solver=BASE/'data/seed_solver',candidate_runner=None,image=driver.IMAGE,config=config)
        # Exactly the production fixed precheck and deadline/CPU-slot wrapper.
        def precheck(candidate):
            receipts=[]
            class Metered:
                def run(self,request,limits):
                    account=backend.last_generation_usage['account']
                    receipt=backend._run_fixed_precheck(request,limits,deadline=backend._brokers[account].deadline)
                    receipts.append(receipt)
                    return receipt
            try:
                return smoke_test(candidate['candidate_path'],BASE/'fixtures/toy_graphs/star5.gr',Metered(),
                    image=driver.IMAGE,solver_argv=('/candidate/run',))
            finally:
                usage=dict(backend.last_generation_usage)
                usage['precheck_cpu_seconds']=sum(r.cpu_seconds for r in receipts)
                backend.last_generation_usage=usage
        backend._precheck=precheck
        return backend
    return build


def main(root):
    root.mkdir(parents=True,exist_ok=False)
    outcomes={}
    for name,fault in [('complete-chain',False),('planning-fault-stop',True)]:
        case=root/name
        case.mkdir()
        code=driver.run(case,backend_builder=builder(fault=fault),proxy_upstream=local_upstream,
            client_instrumentation=False,execution_kind='offline_no_model_acceptance')
        report=json.loads((case/'report.json').read_text())
        events=[json.loads(x) for x in (case/'events.jsonl').read_text().splitlines()]
        if fault:
            assert code==2 and report['status']=='STOPPED_INFRASTRUCTURE',report['status']
            assert not any(e['event']=='phase_open' and e['phase']=='generation' for e in events)
            assert not any(e['event']=='backend_initialization_start' and e['wave']>1 for e in events)
        else:
            assert code==0 and len(report['samples'])==24,report['status']
            assert sum(e['event']=='real_debug_completed' for e in events)==12
            assert sum(e['event']=='precheck_end' and e['passed'] for e in events)==12
            assert sum(e['event']=='container_start' for e in events)==sum(e['event']=='container_reclaimed' for e in events)==24
            for wave in range(1,4):
                for phase in ('planning','generation'):
                    rows=[s for s in report['samples'] if s['wave']==wave and s['phase']==phase]
                    assert len({s['deadline'] for s in rows})==1
            # Real debug intervals must overlap: actual four-way Docker concurrency.
            intervals=[(e['start_monotonic'],e['monotonic']) for e in events if e['event']=='container_reclaimed']
            max_concurrency=max(sum(start<=t<end for start,end in intervals) for t,_ in intervals)
            assert max_concurrency==4,max_concurrency
            outcomes['max_container_concurrency']=max_concurrency
        assert report['actual_requests']==0
        assert all(s['status']!='UNKNOWN' for s in report['samples'])
        assert all(v['provisional_window_seconds'] is None for v in report['windows'].values())
        outcomes[name]={'status':report['status'],'report':str(case/'report.json'),
            'actual_model_requests':0,'local_protocol_reservations':report['offline_protocol_reservations']}
    subprocess.run([sys.executable,str(BASE/'scripts/qualify_remote_docker.py'),
        '--root',str(root/'physical-boundaries')],check=True)
    outcomes['physical_boundaries']=str(root/'physical-boundaries/physical-docker-results.json')
    (root/'acceptance.json').write_text(json.dumps({'passed':True,'scope':'offline control acceptance, not exploration timing',**outcomes},indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',required=True,type=Path)
    main(parser.parse_args().root.resolve())
