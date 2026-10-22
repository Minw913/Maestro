"""One-use live four-agent protocol qualification: <=20 logical / <=30 physical requests.

Independent from Warm/Market. Four preparations (two turns each), four plans,
four one-shot implementations and four fresh runtime repairs. Real graph debug;
no formal panel evaluation. No injected network fault or automatic second run.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor,wait
from dataclasses import asdict,replace
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import threading
import time

from maestro.backends.coral.deepseek_backend import native_factory
from maestro.backends.coral.deepseek_retry import DeepSeekRetryStore,RetryPolicy,audit_retry_continuation
from maestro.backends.coral.deepseek_lifecycle import audit_ledger,exception_details
from maestro.backends.coral.failure_report import write_failure_report
from maestro.backends.coral.workspace import solver_files
from maestro.bootstrap.director import make_direction_briefs
from maestro.config import load_config
from maestro.execution.budgets import BudgetExceeded
from maestro.execution.grants import GrantAuthority
from maestro.execution.sandbox import DockerRunner,RunRequest,ResourceLimits,Mount
from maestro.tasks.pace.runtime_failure import RuntimePrecheckFailure
from maestro.tasks.pace.verifier import verify_graph_solution
from maestro.types import AgentState


def utc():return datetime.now(timezone.utc).isoformat()


class QualificationStore(DeepSeekRetryStore):
    def __init__(self,path,episode,policy):
        super().__init__(path);self.enable_retries(policy)
        self.connection.execute('CREATE TABLE IF NOT EXISTS codex_phase_scopes(account TEXT PRIMARY KEY,status TEXT NOT NULL)')
        self.allowed={f'{episode}:0:agent-{i}:{stage}':cap for i in range(1,5)
                      for stage,cap in [('PREPARATION',2),('PLAN_RO',1)]}
    def authorize_code(self,episode,agent,version):
        with self._lock:
            self.allowed[f'{episode}:0:{agent}:GENERATION:{version}']=1
            self.allowed[f'{episode}:0:{agent}:RUNTIME_REPAIR:{version}:0']=1
    def register_phase(self,account,calls,*limits):
        if self.allowed.get(account)!=calls:raise BudgetExceeded('Undeclared qualification logical account/cap')
        return super().register_phase(account,calls,*limits)
    def create(self,account,**limits):
        root=account.split(':TRANSPORT_ATTEMPT:')[0]
        if root not in self.allowed:raise BudgetExceeded('Undeclared qualification physical account')
        return super().create(account,**limits)
    def reserve(self,*args,**kwargs):
        with self._lock:
            if self.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]>=30:
                raise BudgetExceeded('Thirty-physical-request qualification envelope exhausted')
            return super().reserve(*args,**kwargs)


def check_initial(path,faulty):
    files={relative.as_posix():item for item,relative in solver_files(Path(path))}
    if set(files)!={'run'}:raise ValueError('Candidate must contain only relative file run')
    lines=files['run'].read_text().splitlines(keepends=True)
    markers=[i for i,line in enumerate(lines) if line.strip()=='# initial-qualified']
    if len(markers)!=1 or ''.join(line for i,line in enumerate(lines) if i!=markers[0])!=faulty:
        raise ValueError('Initial implementation must add exactly the planned comment and preserve the injected fault')


def run(args):
    root=args.root.resolve();ready=args.readiness_root.resolve()
    if not args.live:raise ValueError('--live is required')
    if root.exists():raise FileExistsError('Qualification root already exists: no restart')
    target=load_config(args.config,stage='warm_start')
    assert json.loads((ready/'config.warm_start.resolved.json').read_text())==target.to_dict()
    assert json.loads((ready/'prerequisites.json').read_text())['passed']
    root.mkdir(parents=True,exist_ok=False,mode=0o700)
    episode='qualification-'+root.name;event_lock=threading.Lock()
    result={'status':'FAILED','started_utc':utc(),'maximum_logical_requests':20,'maximum_actual_requests':30,
            'actual_requests':0,'formal_warm_started':False,'market_started':False,'network_fault_injection':False,
            'runtime_fault_injection':True,'samples':{},'stages':{}}
    def event(kind,**data):
        with event_lock:
            with (root/'events.jsonl').open('a') as f:f.write(json.dumps({'kind':kind,'utc':utc(),'monotonic':time.monotonic(),**data})+'\n')
    backend=None;store=None;authority=GrantAuthority();stage='setup'
    try:
        (root/'target-config.json').write_text(json.dumps(target.to_dict(),indent=2)+'\n')
        original=(Path(target.pace['seed_solver'])/'run').read_text()
        marker='    stream=sys.stdin.buffer\n';assert original.count(marker)==1
        faulty=original.replace(marker,marker+'    __maestro_qualification_missing__\n',1)
        seed=root/'isolated-runtime-fault-parent';seed.mkdir();(seed/'run').write_text(faulty);(seed/'run').chmod(0o755)
        config=replace(target,preparation_calls=2,pace={**target.pace,'seed_solver':str(seed)})
        (root/'qualification-config.json').write_text(json.dumps(config.to_dict(),indent=2)+'\n')
        runtime=root/'runtime';(runtime/'authority').mkdir(parents=True)
        template=json.loads((ready/'historical-exception-template.json').read_text())
        (runtime/'authority/deepseek-historical-exceptions.json').write_text(json.dumps({**template,'new_run_root':str(runtime)},indent=2)+'\n')
        backend=native_factory(config,root=runtime)
        backend.budget_store.close()
        store=QualificationStore(runtime/'deepseek-native-authority/requests.sqlite',episode,RetryPolicy.parse(config.coral['deepseek_transport_retry']))
        backend.budget_store=store;backend.begin_episode(episode)
        parent=backend.prepare_seed();backend.register_parent(parent['candidate_hash'],parent['candidate_path'])
        states={f'agent-{i}':AgentState(f'agent-{i}',parent['candidate_hash'],0) for i in range(1,5)}
        directions=make_direction_briefs(list(states),0,context={'seed_artifact':parent['candidate_hash']})
        nonces={}
        for direction in directions:
            agent=direction['agent_id'];nonce=hashlib.sha256((episode+agent).encode()).hexdigest();nonces[agent]=nonce
            workspace=backend._workspace(states[agent]);(workspace.private/'qualification-note.txt').write_text(nonce)
            direction['objective']+=(' LIVE PROTOCOL QUALIFICATION ONLY: use ONE batch to read solver/run and '
                'private/qualification-note.txt, then return the exact private note in your summary. '
                'You have only TWO logical preparation requests; the second must be final. '
                'The later plan MUST propose adding exactly one standalone comment # initial-qualified '
                'to existing relative file run, preserving every other line including the deliberate '
                '__maestro_qualification_missing__ NameError. Do not fix that fault: only fresh runtime repair may do so. '
                'No algorithm or performance change. Use a NEW plan action, no invented prior evidence.')
        backend.set_directions(directions);(root/'directions.json').write_text(json.dumps(directions,indent=2)+'\n')
        def parallel(name,deadline,callback):
            opened=time.monotonic();event('PHASE_OPEN',phase=name,deadline=deadline)
            gate=threading.Event();failures=[]
            def worker(agent):
                gate.wait();began=time.monotonic();event('TASK_DISPATCH',phase=name,agent=agent)
                try:
                    value=callback(agent)
                    if time.monotonic()>=deadline:raise TimeoutError('Late qualification result discarded')
                    result['samples'].setdefault(agent,{})[name]={'status':'PASSED','start_monotonic':began,'accepted_monotonic':time.monotonic(),'details':value}
                    event('RESULT_ACCEPTED',phase=name,agent=agent)
                except Exception as error:
                    result['samples'].setdefault(agent,{})[name]={'status':'FAILED','error_type':type(error).__name__,
                        'start_monotonic':began,'finished_monotonic':time.monotonic(),**exception_details(error)}
                    failures.append(agent);event('TASK_FAILED',phase=name,agent=agent,**exception_details(error))
            pool=ThreadPoolExecutor(4)
            futures=[pool.submit(worker,a) for a in states];gate.set()
            done,pending=wait(futures,timeout=max(0,deadline-time.monotonic())+5)
            pool.shutdown(wait=not pending,cancel_futures=True)
            result['stages'][name]={'opened_monotonic':opened,'returned_monotonic':time.monotonic(),'wall_seconds':time.monotonic()-opened,'common_deadline':deadline,'pending_callbacks':len(pending),'failed_agents':failures}
            event('PHASE_RETURN',phase=name,**result['stages'][name])
            if pending:raise RuntimeError('Qualification callback reclamation unconfirmed')
            if failures:raise RuntimeError('Qualification phase failed; no subsequent phase or automatic rerun')
        def prepare(agent):
            evidence,usage=backend.prepare_with_receipt(states[agent],0,{'board':{}},deadline=prep_deadline)
            assert evidence['read_trace'] and nonces[agent] in evidence['summary']
            return {'usage':usage,'read_operations':len(evidence['read_trace']),'memory_nonce_restored':True}
        stage='preparation';prep_deadline=time.monotonic()+target.preparation_window_seconds
        parallel(stage,prep_deadline,prepare)
        plans={}
        def planning(agent):
            decision,receipt,context=backend.plan_with_receipt(states[agent],0,{'board':{}},deadline=plan_deadline)
            assert decision.plan is not None and '# initial-qualified' in decision.plan.change
            plans[agent]=decision.plan;store.authorize_code(episode,agent,decision.plan.version_id)
            return {'receipt':asdict(receipt),'plan':asdict(decision.plan)}
        stage='planning';plan_deadline=time.monotonic()+target.planning_window_seconds
        parallel(stage,plan_deadline,planning)
        generation_deadline=time.monotonic()+target.generation_window_seconds
        candidates={};failures={}
        def grant(agent,epoch):return authority.issue(episode_id=episode,round_index=0,agent_id=agent,
            plan_version_id=plans[agent].version_id,parent_hash=states[agent].parent_hash,epoch=epoch,
            expires_at=time.time()+generation_deadline-time.monotonic())
        def initial(agent):
            token=grant(agent,0)
            try:
                candidate=backend.generate_once(states[agent],plans[agent],0,generation_deadline,grant=token,grant_authority=authority)
                check_initial(candidate['candidate_path'],faulty)
                outcome=backend.precheck(candidate)
                assert isinstance(outcome,RuntimePrecheckFailure) and outcome.evidence.exception_type=='NameError'
                candidates[agent]=candidate;failures[agent]=outcome.evidence
                return {'usage':dict(backend.last_generation_usage),'candidate_hash':candidate['candidate_hash'],'runtime_error':asdict(outcome.evidence),'planned_comment_checked':True}
            finally:authority.revoke(token)
        stage='generation-and-fixed-precheck';parallel(stage,generation_deadline,initial)
        repaired={}
        def repair(agent):
            token=grant(agent,1)
            try:
                candidate=backend.repair_runtime(states[agent],plans[agent],candidates[agent],failures[agent],0,0,generation_deadline,grant=token,grant_authority=authority)
                assert backend.precheck(candidate)==(True,'pass')
                repaired[agent]=candidate
                return {'usage':dict(backend.last_generation_usage),'candidate_hash':candidate['candidate_hash'],'fixed_precheck_passed':True}
            finally:authority.revoke(token)
        stage='fresh-runtime-repair';parallel(stage,generation_deadline,repair)
        prompts=[json.loads(p.read_text())['prompt'] for p in backend.client.root.glob('*/scope.json')]
        repair_prompts=[p for p in prompts if p.startswith('ISOLATED RUNTIME REPAIR.')]
        assert len(repair_prompts)==4
        for prompt in repair_prompts:
            assert not any(n in prompt for n in nonces.values())
            payload,_=json.JSONDecoder().raw_decode(prompt.split('\n',1)[1]);assert set(payload)=={'files','runtime_error','solver_argv'}
        result['repair_context_isolated']=True
        graph=Path(target.pace['panel_manifest']).parent/'heuristic_032.gr';raw=graph.read_text()
        def debug(agent):
            candidate=Path(repaired[agent]['candidate_path']);contract=target.pace['contract']
            receipt=DockerRunner(contract['image']).run(RunRequest(tuple(contract.get('solver_argv',('/candidate/run',))),candidate,raw,(Mount(candidate,'/candidate'),)),
                ResourceLimits(cpu_seconds=target.generation_cpu_seconds,wall_seconds=min(45,max(.001,generation_deadline-time.monotonic())),memory_mb=target.memory_bytes//1048576,pids=target.pids_max,output_bytes=contract['output_bytes']))
            assert receipt.returncode==0 and not receipt.timed_out
            n,verified=verify_graph_solution(raw,receipt.stdout);assert verified.valid
            return {'graph':str(graph),'graph_sha256':hashlib.sha256(raw.encode()).hexdigest(),'n':n,'dominating_set_size':verified.size,
                'cpu_seconds':receipt.cpu_seconds,'wall_seconds':receipt.wall_seconds,'stdout_bytes':len(receipt.stdout.encode()),'diagnostics':receipt.diagnostics}
        stage='real-graph-debug';parallel(stage,generation_deadline,debug)
        backend._assert_clear();result['status']='PASSED'
    except BaseException as error:
        result.update(status='FAILED',failed_stage=stage,**exception_details(error))
        event('QUALIFICATION_FAILED',stage=stage,**exception_details(error))
    finally:
        if store:
            result['actual_requests']=store.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]
            result['logical_requests']=store.connection.execute('SELECT count(*) FROM deepseek_logical_requests').fetchone()[0]
            result['extra_requests']=store.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]
            result['receipts']=[dict(r) for r in store.connection.execute('SELECT * FROM provider_receipts')]
            result['phase_scopes']=[dict(r) for r in store.connection.execute('SELECT * FROM codex_phase_scopes')]
            result['audit']=audit_ledger(store.path if hasattr(store,'path') else root/'runtime/deepseek-native-authority/requests.sqlite')
            result['continuation_audit']=audit_retry_continuation(root/'runtime/deepseek-native-authority/requests.sqlite')
            result['failure_report']=write_failure_report(root/'runtime',root/'reports')
            result['known_input_tokens']=sum(r['input_tokens'] or 0 for r in result['receipts'])
            result['known_output_tokens']=sum(r['output_tokens'] or 0 for r in result['receipts'])
            if result['status']=='PASSED' and (result['logical_requests']!=20
                    or len(result['receipts'])!=20
                    or not all(r['usage_complete'] for r in result['receipts'])):
                result.update(status='FAILED',failed_stage='terminal-receipt-audit')
            if result['audit']['status']!='CLEAR' and result['status']=='PASSED':result['status']='PASSED_WITH_UNKNOWN_COST'
        if backend:
            try:backend.close();result['local_backend_closed']=True
            except BaseException as error:result.update(status='BLOCKED_RECLAMATION',cleanup_error=exception_details(error))
        result['finished_utc']=utc()
        (root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps({k:result.get(k) for k in ('status','actual_requests','logical_requests','extra_requests','failed_stage','known_input_tokens','known_output_tokens','started_utc','finished_utc')},indent=2),flush=True)
    return 0 if result['status']=='PASSED' else 2


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True);parser.add_argument('--readiness-root',type=Path,required=True)
    parser.add_argument('--config',type=Path,required=True);parser.add_argument('--live',action='store_true')
    raise SystemExit(run(parser.parse_args()))
