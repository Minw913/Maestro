"""Offline protocol fixture + production DockerRunner runtime-repair acceptance.
No native model process, credentials or external inference is used. This is not
live-model qualification and never produces a formal PACE evaluation/score.
"""
import argparse
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
import hashlib,http.client,json,os,subprocess,time
from urllib.parse import urlsplit
from maestro.backends.codex_session.autonomous_backend import StrictCodexBackend
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.execution.budgets import BudgetStore
from maestro.execution.grants import GrantAuthority
from maestro.execution.sandbox import DockerRunner,RunRequest,ResourceLimits,Mount
from maestro.tasks.pace.runner import smoke_test
from maestro.tasks.pace.sanity import default_sanity_graph_path
from maestro.tasks.pace.runtime_failure import RuntimePrecheckFailure
from maestro.tasks.pace.verifier import verify_graph_solution
from maestro.types import AgentState,Plan,stable_hash
base=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root',required=True,type=Path)
from maestro.storage.paths import diagnostic_root
root=diagnostic_root(parser.parse_args().root);root.mkdir(parents=True,exist_ok=False)
image='python@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641'
seed=base/'data/seed_solver';source=(seed/'run').read_text();line=next(i for i,s in enumerate(source.splitlines(),1) if 'stream=sys.stdin.buffer' in s)
broken=source.replace('stream=sys.stdin.buffer','stream=missing_stream')
class Response:
 status=200;headers={'Content-Type':'text/event-stream'}
 def iter_bytes(self,deadline):
  yield b'data: '+json.dumps({'type':'response.completed','response':{'id':'offline-only','status':'completed','output':[],'usage':{'input_tokens':0,'output_tokens':0}}}).encode()+b'\n\n'
 def close(self):pass
class Client:
 model='offline-model';supports_phase_identity=True
 def __init__(self):self.seen=[]
 def run_phase(self,prompt,*,base_url,deadline,tools,output_schema,session_id,agent_id):
  assert tools is None and session_id is None
  self.seen.append({'agent_id':agent_id,'initial':prompt.startswith('ONE INITIAL')})
  u=urlsplit(base_url);assert u.hostname=='127.0.0.1'
  c=http.client.HTTPConnection(u.hostname,u.port,timeout=5);c.request('POST',u.path+'/responses',json.dumps({'model':self.model,'input':[],'tools':[]}),{'Authorization':'Bearer offline-fixture','Content-Type':'application/json'});r=c.getresponse();r.read();c.close();assert r.status==200
  if prompt.startswith('ONE INITIAL'):obj={'files':[{'path':'run','content':broken,'executable':True}]}
  else:
   assert 'SECRET_MEMORY_MARKER' not in prompt and 'SECRET_PLAN_MARKER' not in prompt
   obj={'edits':[{'path':'run','start_line':line,'end_line':line,'replacement':'    stream=sys.stdin.buffer\n'}]}
  text=json.dumps(obj);return SimpleNamespace(text=text,response_hash=stable_hash(text),thread_id='not-reused')
 def close(self):pass
client=Client();store=BudgetStore(root/'physical-requests.sqlite');runner=DockerRunner(image);receipts=[]
class MeteredRunner:
 def run(self,request,limits):
  receipt=runner.run(request,limits);receipts.append({'returncode':receipt.returncode,'cpu_seconds':receipt.cpu_seconds,'wall_seconds':receipt.wall_seconds,'timed_out':receipt.timed_out,'diagnostics':receipt.diagnostics,'stdout_bytes':len(receipt.stdout.encode()),'stderr':receipt.stderr[:2000]});return receipt
metered=MeteredRunner()
def proxy(*a,**kw):return CodexQuotaProxy(*a,upstream=lambda *args:Response(),**kw)
b=StrictCodexBackend(client,store,WorkspaceStore(root/'workspaces',root/'cas'),precheck=None,seed_solver=seed,
 candidate_runner=metered,image=image,config=RunConfig(rounds=1),proxy_factory=proxy,simulation=True)
def precheck(candidate):
 class Bounded:
  def run(self,request,limits):return b._run_fixed_precheck(request,limits,deadline=deadline)
 return smoke_test(candidate['candidate_path'],default_sanity_graph_path(),Bounded(),image=image)
b._precheck=precheck;b.begin_episode('physical-runtime-repair');parent=b.prepare_seed();b.register_parent(parent['candidate_hash'],parent['candidate_path'])
state=AgentState('a',parent['candidate_hash'],0);plan=Plan('p','v','a',state.parent_hash,'local_search','parameter_change','SECRET_PLAN_MARKER','implement','test')
(b._workspace(state).private/'notes').write_text('SECRET_MEMORY_MARKER');a=GrantAuthority();deadline=time.monotonic()+60

def grant(epoch):return a.issue(episode_id=b.episode_id,round_index=0,agent_id='a',plan_version_id='v',parent_hash=state.parent_hash,epoch=epoch,expires_at=time.time()+deadline-time.monotonic())
try:
 g=grant(0);candidate=b.generate(state,plan,0,deadline=deadline,grant=g,grant_authority=a);outcome=b.precheck(candidate);a.revoke(g)
 assert isinstance(outcome,RuntimePrecheckFailure) and outcome.evidence.exception_type=='NameError'
 g=grant(1);fixed=b.repair_runtime(state,plan,candidate,outcome.evidence,0,0,deadline,grant=g,grant_authority=a);checked=b.precheck(fixed);a.revoke(g);assert checked==(True,'pass')
 graph=(base/'data/training/heuristic_032.gr').read_text();receipt=metered.run(RunRequest(('python3','/candidate/run'),Path(fixed['candidate_path']),stdin=graph,mounts=(Mount(Path(fixed['candidate_path']),'/candidate'),)),ResourceLimits(cpu_seconds=10,wall_seconds=20,memory_mb=1024,pids=32,output_bytes=1000000))
 assert receipt.returncode==0 and not receipt.timed_out
 n,verified=verify_graph_solution(graph,receipt.stdout);assert verified.valid
 assert store.connection.execute('select count(*) from inference_reservations').fetchone()[0]==2
 assert store.connection.execute("select count(*) from inference_reservations where status!='COMPLETED'").fetchone()[0]==0
 result={'passed':True,'actual_model_requests':0,'offline_protocol_requests':2,'runtime_exception':outcome.evidence.exception_type,'patch_line':line,'fixed_precheck':checked,'real_graph':{'instance':'heuristic_032','n':n,'valid':verified.valid,'size':verified.size,'output_bytes':len(receipt.stdout.encode())},'fresh_agents':client.seen,'receipts':receipts,'formal_evaluation':False}
finally:b.close()
result['remaining_rootless_containers']=subprocess.check_output(['docker','ps','-aq'],text=True).split();assert not result['remaining_rootless_containers']
(root/'physical.json').write_text(json.dumps(result,indent=2));print(json.dumps({k:v for k,v in result.items() if k!='receipts'},indent=2))
