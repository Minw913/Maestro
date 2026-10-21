"""Operator physical acceptance, uses the production DockerRunner; no inference."""
from dataclasses import asdict
import argparse, json, os, pathlib, subprocess, time
from maestro.execution.sandbox import DockerRunner, RunRequest, ResourceLimits, Mount
BASE=pathlib.Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root',type=pathlib.Path)
args=parser.parse_args()
ROOT=args.root.resolve() if args.root else pathlib.Path(json.loads((BASE/'artifacts/remote-deployment-paths.json').read_text())['preflight_root'])
if args.root: ROOT.mkdir(parents=True,exist_ok=False)
IMAGE='python@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641'
runner=DockerRunner(IMAGE); results=[]
def case(name,code,limits,check,mounts=()):
 receipt=runner.run(RunRequest(('python3','-c',code),BASE,mounts=mounts),limits)
 record={'name':name,**asdict(receipt)}; record['passed']=bool(check(receipt)); results.append(record)
 (ROOT/'physical-docker-results.json').write_text(json.dumps(results,indent=2))
 print(name,record['passed'],receipt.returncode,receipt.cpu_seconds,receipt.diagnostics,flush=True)
 if not record['passed']:raise RuntimeError('Physical qualification failed: '+name)
normal=ResourceLimits(cpu_seconds=3,wall_seconds=8,memory_mb=128,pids=16)
case('limits_network_files',"""import pathlib,socket,json,os
assert float(pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().split()[0])/int(pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().split()[1])<=1
assert int(pathlib.Path('/sys/fs/cgroup/memory.max').read_text())==128*1024*1024
assert int(pathlib.Path('/sys/fs/cgroup/pids.max').read_text())==16
assert set(p.name for p in pathlib.Path('/sys/class/net').iterdir())=={'lo'}
assert not pathlib.Path('/var/run/docker.sock').exists()
assert not pathlib.Path('/home/zijianzhou/.codex/auth.json').exists()
for dest in ['/etc/escape','/candidate/escape']:
 try:pathlib.Path(dest).write_text('x')
 except OSError:pass
 else:raise AssertionError('write escape')
try:socket.create_connection(('1.1.1.1',443),timeout=.2)
except OSError:pass
else:raise AssertionError('network escape')
pathlib.Path('/scratch/ok').write_text('ok')
mounts=[line.split() for line in pathlib.Path('/proc/mounts').read_text().splitlines()]
assert any(row[1]=='/scratch' and 'noexec' in row[3].split(',') for row in mounts)
script=pathlib.Path('/scratch/probe');script.write_text('#!/bin/sh\\nexit 0\\n');script.chmod(0o700)
try:__import__('subprocess').run([str(script)],check=True)
except PermissionError:pass
else:raise AssertionError('scratch noexec bypass')
assert __import__('subprocess').run(['python3','-c','print(1)'],check=True,capture_output=True).stdout.strip()==b'1'
print('BOUNDARIES_NOEXEC_OK')
""",normal,lambda r:r.returncode==0 and 'BOUNDARIES_NOEXEC_OK' in r.stdout,(Mount(BASE/'fixtures/toy_graphs','/candidate'),))
case('cpu_timeout_fork_setsid',"""import os,time
pid=os.fork()
if pid==0:os.setsid()
while True:pass
""",ResourceLimits(cpu_seconds=.5,wall_seconds=5,memory_mb=128,pids=16),lambda r:r.timed_out and r.diagnostics['reason']=='CPU_LIMIT')
case('wall_timeout',"import time;time.sleep(60)",ResourceLimits(cpu_seconds=2,wall_seconds=.5,memory_mb=128,pids=16),lambda r:r.timed_out and r.diagnostics['reason']=='WALL_LIMIT')
case('memory_exhaustion',"a=[]\nwhile True:a.append(bytearray(8*1024*1024))",normal,lambda r:r.returncode!=0)
case('pid_exhaustion',"""import os,time
children=[]
try:
 while True:
  p=os.fork()
  if not p:time.sleep(60);os._exit(0)
  children.append(p)
except OSError:print('PIDS_ENFORCED',flush=True)
""",normal,lambda r:'PIDS_ENFORCED' in r.stdout)
# Runner's synchronous API has no cancellation token. Interrupt an isolated
# controller via SIGINT; finally must remove all newly-created containers.
before=subprocess.check_output(['docker','ps','-aq','--no-trunc'],text=True).split()
code="""from maestro.execution.sandbox import *
from pathlib import Path
DockerRunner(%r).run(RunRequest(('python3','-c','import os,time;os.fork();time.sleep(60)'),Path.cwd()),ResourceLimits(cpu_seconds=5,wall_seconds=60,memory_mb=128,pids=16))
"""%IMAGE
p=subprocess.Popen([str(BASE/'.venv-remote/bin/python'),'-c',code],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
new=[]
for _ in range(100):
 current=subprocess.check_output(['docker','ps','-q','--no-trunc'],text=True).split();new=[x for x in current if x not in before]
 if new:break
 time.sleep(.1)
if not new:p.kill();p.communicate();raise RuntimeError('Cancellation container never observed')
p.send_signal(2);out,err=p.communicate(timeout=30)
after=subprocess.check_output(['docker','ps','-aq','--no-trunc'],text=True).split();record={'name':'controller_sigint_cancellation','observed_ids':new,'controller_exit':p.returncode,'remaining_new_ids':[x for x in after if x not in before],'passed':all(x not in after for x in new) and not any(x not in before for x in after)};results.append(record)
(ROOT/'physical-docker-results.json').write_text(json.dumps(results,indent=2));print(record,flush=True)
assert record['passed']
