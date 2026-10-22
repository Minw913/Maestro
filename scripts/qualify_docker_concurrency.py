"""Bounded 20-worker real-graph DockerRunner acceptance; no model or experiment.

Each of the existing twenty frozen training graphs runs the unchanged greedy
seed once. Host cgroup process/thread counters are sampled during actual Docker
client creation, start, attached execution and reclamation.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime,timezone
import fcntl
import hashlib
import json
from pathlib import Path
import threading
import time

from maestro.config import load_config
from maestro.execution.sandbox import DockerRunner,RunRequest,ResourceLimits,Mount
from maestro.tasks.pace.verifier import verify_graph_solution


def main(args):
    root=args.root.resolve()
    if root.exists():raise FileExistsError('Single-use physical qualification root exists')
    root.mkdir(parents=True,mode=0o700)
    config=load_config(args.config,stage='warm_start');contract=config.pace['contract']
    path=Path(config.pace['panel_manifest']);manifest=json.loads(path.read_text());entries=manifest['instances']
    assert len(entries)==20
    seed=Path(config.pace['seed_solver']);image=contract['image']
    lock=Path(args.lock).open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    cgroup=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::',1)[1].lstrip('/')
    sample_lock=threading.Lock();stop=threading.Event();samples=[];created=[]
    def read(name):
        try:return (cgroup/name).read_text().strip()
        except OSError:return None
    def snap():return {'utc':datetime.now(timezone.utc).isoformat(),'monotonic':time.monotonic(),
        **{k:read(k) for k in ('pids.current','pids.peak','pids.max','pids.events','memory.current')}}
    initial=snap()
    def sampler():
        while not stop.is_set():
            value=snap();samples.append(value)
            with (root/'cgroup-samples.jsonl').open('a') as stream:stream.write(json.dumps(value)+'\n')
            stop.wait(.02)
    class RecordedRunner(DockerRunner):
        def _docker(self,argv,**kwargs):
            response=super()._docker(argv,**kwargs)
            if argv and argv[0]=='create' and response.returncode==0:
                with sample_lock:
                    created.append(response.stdout.strip())
                    (root/'created-container-ids.json').write_text(json.dumps(created,indent=2)+'\n')
            return response
    runner=RecordedRunner(image);gate=threading.Barrier(20)
    limits=ResourceLimits(cpu_seconds=config.cpu_seconds_per_instance,wall_seconds=contract['wall_seconds_per_instance'],
        memory_mb=contract.get('memory_mb',512),pids=contract.get('pids',32),output_bytes=contract['output_bytes'],cpus=1)
    results={};began=time.monotonic();thread=threading.Thread(target=sampler,daemon=True);thread.start()
    def execute(entry):
        key=entry['instance_id'];data=(path.parent/entry['path']).read_bytes()
        assert hashlib.sha256(data).hexdigest()==entry['sha256'];gate.wait(timeout=15);started=time.monotonic()
        try:
            receipt=runner.run(RunRequest(tuple(contract.get('solver_argv',('/candidate/run',))),seed,data.decode(),(Mount(seed,'/candidate'),)),limits)
            (root/(key+'.stdout')).write_text(receipt.stdout)
            return {'instance_id':key,'sha256':entry['sha256'],'started_monotonic':started,'finished_monotonic':time.monotonic(),'receipt':asdict(receipt)}
        except Exception as error:
            chain=[];e=error
            while e is not None:
                chain.append({'type':type(e).__name__,'errno':getattr(e,'errno',None)})
                e=e.__cause__
            return {'instance_id':key,'started_monotonic':started,'finished_monotonic':time.monotonic(),'exception_chain':chain,'error':str(error)}
    try:
        with ThreadPoolExecutor(20) as pool:
            futures=[pool.submit(execute,e) for e in entries]
            for future in futures:
                result=future.result();results[result['instance_id']]=result
                (root/'partial-results.json').write_text(json.dumps(results,indent=2)+'\n')
        # No model is exposed to these diagnostics. Verify actual dominating sets.
        for entry in entries:
            value=results[entry['instance_id']]
            if 'receipt' in value and value['receipt']['returncode']==0 and not value['receipt']['timed_out']:
                _,verified=verify_graph_solution((path.parent/entry['path']).read_text(),value['receipt']['stdout'])
                value['verified']={'valid':verified.valid,'size':verified.size}
    finally:
        stop.set();thread.join(timeout=2)
    final=snap();failure=[r for r in results.values() if 'exception_chain' in r or not r.get('verified',{}).get('valid')]
    def event_max(s):return int(dict(line.split() for line in (s.get('pids.events') or '').splitlines()).get('max',0))
    limit_events=event_max(final)-event_max(initial)
    status='FAILED' if failure else 'FAILED_TASK_LIMIT_PRESSURE' if limit_events else 'PASSED'
    summary={'status':status,'actual_model_requests':0,'formal_experiment_started':False,
        'wall_seconds':time.monotonic()-began,'instances':20,'successful_instances':20-len(failure),
        'initial_cgroup':initial,'final_cgroup':final,'peak_sampled_tasks':max(int(s['pids.current']) for s in samples if s['pids.current']),
        'pids_limit_events_delta':limit_events,'created_container_ids':created,'results':results,
        'seed_sha256':hashlib.sha256((seed/'run').read_bytes()).hexdigest(),'image':image,'limits':asdict(limits)}
    (root/'result.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({k:v for k,v in summary.items() if k not in ('results','created_container_ids')},indent=2),flush=True)
    return 0 if status=='PASSED' else 2


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--config',type=Path,required=True)
    p.add_argument('--lock',type=Path,required=True);raise SystemExit(main(p.parse_args()))
