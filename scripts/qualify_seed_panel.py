"""No-model physical acceptance of the frozen seed on the complete training panel.

Uses production DockerRunner and ContainerVerificationWorker with the unchanged
CPU/wall/memory limits. Outputs are diagnostics, never a Warm/Market ledger entry.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from dataclasses import asdict
from datetime import datetime,timezone
import hashlib,json,subprocess,threading,time
from pathlib import Path
from maestro.config import load_config
from maestro.backends.coral.workspace import WorkspaceStore,candidate_tree_hash
from maestro.execution.sandbox import DockerRunner,RunRequest,ResourceLimits,Mount
from maestro.tasks.pace.manifest import load_panel,EvaluationContract
from maestro.tasks.pace.verification import ContainerVerificationWorker


def main(root,config_path):
    from maestro.storage.paths import diagnostic_root
    root = diagnostic_root(root)
    root.mkdir(parents=True,exist_ok=False)
    config=load_config(config_path).for_stage('warm_start')
    contract=EvaluationContract.standard(**{**config.pace['contract'],'cpu_workers':config.cpu_workers,'instance_workers':config.instance_workers})
    (root/'config.resolved.json').write_text(json.dumps(config.to_dict(),indent=2))
    (root/'contract.json').write_text(json.dumps(asdict(contract),indent=2))
    panel=load_panel(config.pace['panel_manifest'],expected_split='training')
    assert len(panel.instances)==20
    candidate=WorkspaceStore(root/'work',root/'cas').freeze(Path(config.pace['seed_solver']))
    (root/'seed.json').write_text(json.dumps(candidate,indent=2))
    maximum=0;bounds=[]
    for entry in panel.instances:
        n=entry.n;size=len(str(n))+1
        for digits in range(1,len(str(n))+1):
            size+=max(0,min(n,10**digits-1)-10**(digits-1)+1)*(digits+1)
        bounds.append({'instance_id':entry.instance_id,'n':n,'canonical_all_vertices_bytes':size})
        maximum=max(maximum,size)
    assert maximum<=contract.output_bytes
    (root/'output-bound.json').write_text(json.dumps({'cap':contract.output_bytes,'maximum':maximum,'instances':bounds},indent=2))
    runner=DockerRunner(contract.image);runner.preflight(image=contract.image)
    limits=ResourceLimits(cpu_seconds=contract.cpu_seconds_per_instance,wall_seconds=contract.wall_seconds_per_instance,
        memory_mb=contract.memory_mb,pids=contract.pids,output_bytes=contract.output_bytes,cpus=1)
    lock=threading.Lock();events=[];outputs=[None]*20;started=time.monotonic();utc=datetime.now(timezone.utc).isoformat()
    def run(index):
        entry=panel.instances[index];raw=entry.path.read_bytes()
        assert hashlib.sha256(raw).hexdigest()==entry.sha256
        begin=time.monotonic()
        receipt=runner.run(RunRequest(tuple(contract.solver_argv),Path(candidate['candidate_path']),raw.decode(),
            (Mount(Path(candidate['candidate_path']),'/candidate'),),contract.image,container_workdir='/scratch'),limits)
        end=time.monotonic();data=receipt.stdout.encode()
        (root/(entry.instance_id+'.sol')).write_bytes(data)
        result={k:v for k,v in asdict(receipt).items() if k!='stdout'}
        result.update(instance_id=entry.instance_id,start_monotonic=begin,end_monotonic=end,
            stdout_bytes=len(data),stdout_sha256=hashlib.sha256(data).hexdigest())
        (root/(entry.instance_id+'.receipt.json')).write_text(json.dumps(result,indent=2))
        with lock:events.append(result)
        if (receipt.returncode!=0 or receipt.timed_out or receipt.diagnostics.get('reason')!='COMPLETED'
                or receipt.diagnostics.get('aggregate_cgroup_cpu') is not True):
            raise RuntimeError('Seed physical execution failed: '+entry.instance_id)
        return receipt
    status='FAILED';error=None;verified=None
    try:
        with ThreadPoolExecutor(max_workers=config.instance_workers) as pool:
            pending={};next_index=0
            for _ in range(config.instance_workers):pending[pool.submit(run,next_index)]=next_index;next_index+=1
            while pending:
                done,_=wait(pending,return_when=FIRST_COMPLETED)
                completed=[]
                for future in done:
                    index=pending.pop(future);outputs[index]=future.result();completed.append(index)
                for _ in completed:
                    if next_index<20:pending[pool.submit(run,next_index)]=next_index;next_index+=1
        verified=ContainerVerificationWorker(runner,temporary_root=root/'verification-temp').verify(panel,outputs,contract)
        assert verified.status=='COMPLETED' and len(verified.records)==20 and all(x['valid'] for x in verified.records)
        assert candidate_tree_hash(Path(config.pace['seed_solver']))==candidate['candidate_hash']
        status='PASSED'
    except BaseException as exc:
        status='UNKNOWN' if getattr(exc,'execution_unknown',False) or getattr(exc,'inflight_unknown',False) else 'FAILED'
        error=type(exc).__name__+': '+str(exc)[:256]
    intervals=[(e['start_monotonic'],e['end_monotonic']) for e in events]
    peak=max((sum(a<=t<b for a,b in intervals) for t,_ in intervals),default=0)
    remaining=subprocess.check_output(['docker','ps','-aq','--no-trunc'],text=True).split()
    if remaining:status='UNKNOWN';error='Container reclamation unconfirmed'
    result={'status':status,'error':error,'started_utc':utc,'finished_utc':datetime.now(timezone.utc).isoformat(),
        'wall_seconds':time.monotonic()-started,'instances_completed':len(events),'maximum_output_bytes':max((e['stdout_bytes'] for e in events),default=0),
        'seed_cpu_seconds':sum(e['cpu_seconds'] for e in events),'maximum_instance_cpu_seconds':max((e['cpu_seconds'] for e in events),default=0),
        'peak_instance_concurrency':peak,'verification':asdict(verified) if verified else None,
        'remaining_containers':remaining,'actual_model_requests':0,'formal_evaluation':False,'warm_started':False,'market_started':False}
    (root/'result.json').write_text(json.dumps(result,indent=2))
    print(json.dumps({k:v for k,v in result.items() if k!='verification'},indent=2),flush=True)
    if verified:print('verification',verified.status,verified.cpu_seconds,verified.wall_seconds,flush=True)
    return 0 if status=='PASSED' else 2


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--config',type=Path,required=True)
    a=p.parse_args();raise SystemExit(main(a.root.resolve(),a.config.resolve()))
