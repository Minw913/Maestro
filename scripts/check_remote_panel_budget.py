"""No-score data resource acceptance using production trusted verifier parsing."""
import json,pathlib,dataclasses
from maestro.execution.sandbox import DockerRunner,RunRequest,ResourceLimits,Mount
from maestro.tasks.pace.manifest import VERIFIER_SOURCE_FILES
base=pathlib.Path(__file__).resolve().parents[1];root=pathlib.Path(json.loads((base/'artifacts/remote-deployment-paths.json').read_text())['preflight_root']);records=json.loads((root/'selected-data-records.json').read_text());package=root/'trusted-package'
for name in VERIFIER_SOURCE_FILES:
 p=package/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((base/'src'/name).read_bytes())
code="""import sys,time,json
sys.path.insert(0,'/input/source')
from maestro.tasks.pace.parser import parse_graph
items=json.load(sys.stdin)
for item in items:
 t=time.process_time()
 with open('/input/graphs/'+item['path']) as f:g=parse_graph(f.read())
 assert g.n==int(item['reference_row']['Number of elements'])
 print(json.dumps({'instance_id':item['instance_id'],'n':g.n,'m':g.m,'parse_cpu_seconds':time.process_time()-t}),flush=True)
 del g
print('PANEL_PARSE_COMPLETE',flush=True)
"""
runner=DockerRunner('python@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641')
receipt=runner.run(RunRequest(('python3','-I','-c',code),base,stdin=json.dumps(records),mounts=(Mount(package,'/input/source'),Mount(base/'data/training','/input/graphs'))),ResourceLimits(cpu_seconds=20,wall_seconds=30,memory_mb=512,pids=8,output_bytes=1000000,cpus=1))
result={'purpose':'No-score lower workload acceptance: parse full real panel, without even solution verification or scoring','limits':{'cpu_seconds':20,'wall_seconds':30,'memory_mb':512,'pids':8},'receipt':dataclasses.asdict(receipt),'passed':receipt.returncode==0 and not receipt.timed_out and 'PANEL_PARSE_COMPLETE' in receipt.stdout}
(root/'real-panel-budget-acceptance.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2));raise SystemExit(0 if result['passed'] else 2)
