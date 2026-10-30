from maestro.storage.paths import diagnostic_native_root
"""Single-use LIVE qualification of MCP preparation, one implementation and fresh runtime repair.

Four actual requests maximum: preparation 2, implementation 1, runtime repair 1.
Uses the real greedy solver with an explicitly injected NameError in an isolated
qualification parent; this is fault acceptance, not an experiment or new seed.
"""
import argparse
from dataclasses import asdict,replace
from datetime import datetime,timezone
import hashlib,json,sqlite3,time,uuid
from pathlib import Path
from maestro.config import load_config
from maestro.backends.coral.native_backend import native_factory
from maestro.backends.coral.workspace import solver_files
from maestro.execution.budgets import BudgetStore,BudgetExceeded
from maestro.execution.grants import GrantAuthority,GrantError
from maestro.tasks.pace.runtime_failure import RuntimePrecheckFailure
from maestro.execution.sandbox import DockerRunner,RunRequest,ResourceLimits,Mount
from maestro.tasks.pace.verifier import verify_graph_solution
from maestro.types import AgentState,Plan


class QualificationBudgets(BudgetStore):
    def __init__(self,path,episode,version):
        super().__init__(path)
        prefix=episode+':0:qualifier:'
        self.allowed={prefix+'PREPARATION':2,prefix+'GENERATION:'+version:1,prefix+'RUNTIME_REPAIR:'+version+':0':1}
        self.connection.execute('CREATE TABLE IF NOT EXISTS codex_phase_scopes(account TEXT PRIMARY KEY,status TEXT NOT NULL)')
    def create(self,account,**limits):
        if self.allowed.get(account)!=limits.get('max_calls'):raise BudgetExceeded('Undeclared qualification account/cap')
        return super().create(account,**limits)
    def reserve(self,*args,**kwargs):
        with self._lock:
            if self.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]>=4:
                raise BudgetExceeded('Four-request qualification envelope exhausted')
            return super().reserve(*args,**kwargs)


def check_repair_context(prompt,private_nonce):
    """Allow the declared runtime context, while excluding private session data."""
    if private_nonce in prompt:raise ValueError('Private memory leaked to repair')
    data=json.loads(prompt.split('\n',1)[1])
    expected={'files','runtime_error','solver_argv','parent_files','parent_hash',
              'source_index','parent_source_index'}
    if set(data)!=expected:raise ValueError('Undeclared or incomplete isolated repair context')
    test=data['runtime_error'].get('test_context',{})
    if (test.get('kind')!='fixed_sanity_only' or not test.get('stdin')
            or 'receipt' not in test or not data['files'] or not data['parent_files']):
        raise ValueError('Actual failed sanity input/receipt and source context are required')
    return data


def check_initial_candidate(path, faulty_source):
    """The qualification must modify the entry, not an unrelated new file."""
    files = {relative.as_posix(): item for item, relative in solver_files(Path(path))}
    if set(files) != {'run'}:
        raise ValueError('Initial qualification must return only the candidate-relative entry run')
    lines = files['run'].read_text().splitlines(keepends=True)
    markers = [i for i, line in enumerate(lines) if line.strip() == '# initial-qualified']
    if len(markers) != 1:
        raise ValueError('Initial qualification must add exactly one standalone # initial-qualified comment')
    remaining = ''.join(line for i, line in enumerate(lines) if i != markers[0])
    if remaining != faulty_source:
        raise ValueError('Initial qualification changed source beyond the requested comment')


def prior_cost(paths):
    total=0
    for path in paths:
        db=sqlite3.connect(f'file:{path.resolve()}?mode=ro',uri=True)
        try:
            rows=db.execute('SELECT status FROM inference_reservations').fetchall()
            assert all(row[0]=='COMPLETED' for row in rows),'Prior request unresolved'
            assert not db.execute("SELECT count(*) FROM codex_phase_scopes WHERE status IN ('UNKNOWN','DISPATCHING')").fetchone()[0]
            total+=len(rows)
        finally:db.close()
    return total


def run(root,readiness,config_path,prior_ledgers,live,standalone=False):
    from maestro.storage.paths import diagnostic_root
    root = diagnostic_root(root)
    if not live:raise ValueError('Explicit --live is required')
    if root.exists():raise FileExistsError('Qualification root must not exist; no retry')
    if standalone:
        assert not prior_ledgers,'Standalone qualification must not borrow historical pilot quota'
    else:
        assert prior_ledgers,'Historical pilot ledgers are required outside standalone readiness'
        assert prior_cost(prior_ledgers)+4<=48,'Original pilot envelope would be exceeded'
    resource=json.loads((readiness/'seed-panel/result.json').read_text())
    assert resource['status']=='PASSED' and resource['instances_completed']==20
    assert json.loads((readiness/'official-seed-crosscheck.json').read_text())['passed']
    physical=json.loads((readiness/'docker-boundaries/physical-docker-results.json').read_text())
    assert len(physical)==6 and all(r['passed'] for r in physical),'Physical Docker acceptance incomplete'
    target=load_config(config_path).for_stage('warm_start')
    frozen=json.loads((readiness/'config.warm_start.resolved.json').read_text())
    assert target.to_dict()==frozen,'Target config changed after readiness freeze'
    root.mkdir(parents=True,exist_ok=False)
    result={'status':'FAILED','actual_requests':0,'maximum_requests':4,'formal_warm_started':False,'market_started':False,'fault_injection':True,'checks':{},'started_utc':datetime.now(timezone.utc).isoformat()}
    backend=None;budgets=None;grant=None;authority=GrantAuthority();stage='setup'
    episode='native-policy-'+root.name;version='qualification-v1'
    try:
        source=Path(target.pace['seed_solver'])/'run';original=source.read_text()
        fault='    __maestro_qualification_missing__\n'
        assert '    stream=sys.stdin.buffer\n' in original
        faulty=original.replace('    stream=sys.stdin.buffer\n','    stream=sys.stdin.buffer\n'+fault,1)
        seed=root/'fault-parent';seed.mkdir();(seed/'run').write_text(faulty);(seed/'run').chmod(0o755)
        config=replace(target,preparation_calls=2,pace={**target.pace,'seed_solver':str(seed)})
        (root/'target-config.json').write_text(json.dumps(target.to_dict(),indent=2))
        (root/'qualification-config.json').write_text(json.dumps(config.to_dict(),indent=2))
        backend=native_factory(config,root=root/'runtime')
        assert backend.budget_store.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]==0
        backend.budget_store.close();budgets=QualificationBudgets(root/'requests.sqlite',episode,version);backend.budget_store=budgets
        backend.begin_episode(episode);candidate=backend.prepare_seed();backend.register_parent(candidate['candidate_hash'],candidate['candidate_path'])
        state=AgentState('qualifier',candidate['candidate_hash'],0)
        nonce=uuid.uuid4().hex
        workspace=backend._workspace(state);(workspace.private/'qualification-note.txt').write_text(nonce)
        backend.set_directions([{'agent_id':'qualifier','objective':
            'Connection qualification only. You have at most TWO actual preparation requests. '
            'In ONE batch read solver/run and private/qualification-note.txt. Then return a short summary '
            'including the exact private note content. No other tools are necessary. Do not fix code.'}])
        stage='preparation'
        evidence,usage=backend.prepare_with_receipt(state,0,{'board':{}},deadline=time.monotonic()+target.preparation_window_seconds)
        assert nonce in evidence['summary'] and evidence['read_trace']
        result['checks']['mcp_read_and_memory']=True;result['preparation_usage']=usage
        # Qualification has a controller-declared plan, not a model-generated
        # competition proposal; no market/valuation/EvalTicket is invoked.
        plan=Plan('qualification-plan',version,'qualifier',state.parent_hash,'data_structure','parameter_change',
            'Deliberately injected runtime fault; test separation of generator and repair agent.',
            'Edit the existing candidate-relative file run (files[].path must be "run", not "solver/run"). '
            'Add exactly one standalone harmless comment line # initial-qualified. Reproduce the rest of the file exactly, '
            'including __maestro_qualification_missing__. Do NOT fix or remove that line: only the subsequent '
            'independent runtime-repair stage may fix it. Do not change the algorithm.',
            'No performance claim; qualify initial generation and isolated runtime repair')
        deadline=time.monotonic()+target.generation_window_seconds
        def issue(epoch):return authority.issue(episode_id=episode,round_index=0,agent_id='qualifier',
            plan_version_id=version,parent_hash=state.parent_hash,epoch=epoch,expires_at=time.time()+deadline-time.monotonic(),
            purpose='prebid_implementation' if epoch==0 else 'runtime_repair',
            source_candidate_hash=state.parent_hash if epoch==0 else candidate['candidate_hash'])
        stage='implementation';grant=issue(0)
        candidate=backend.generate(state,plan,0,deadline=deadline,grant=grant,grant_authority=authority)
        result['initial_usage']=dict(backend.last_generation_usage)
        check_initial_candidate(candidate['candidate_path'], faulty)
        result['checks']['initial_entry_modified_as_planned']=True
        outcome=backend.precheck(candidate);authority.revoke(grant);grant=None
        assert isinstance(outcome,RuntimePrecheckFailure) and outcome.evidence.exception_type=='NameError'
        result['checks']['runtime_failure_observed']=True;result['failure']=asdict(outcome.evidence)
        stage='runtime-repair';grant=issue(1)
        repaired=backend.repair_runtime(state,plan,candidate,outcome.evidence,0,0,deadline,grant=grant,grant_authority=authority)
        result['repair_usage']=dict(backend.last_generation_usage)
        assert backend.precheck(repaired)==(True,'pass')
        authority.revoke(grant);grant=None
        result['checks']['fresh_runtime_patch_and_precheck']=True
        phase_scopes=list((diagnostic_native_root(root/'runtime')).glob('*/scope.json'))
        prompts=[json.loads(p.read_text())['prompt'] for p in phase_scopes]
        repair_prompts=[p for p in prompts if p.startswith('ISOLATED RUNTIME REPAIR.')]
        assert len(repair_prompts)==1 and nonce not in repair_prompts[0]
        check_repair_context(repair_prompts[0],nonce)
        result['checks']['repair_context_isolated']=True
        stage='real-graph-debug'
        graph_path=Path(config.pace['panel_manifest']).parent/'heuristic_032.gr';raw=graph_path.read_text()
        receipt=DockerRunner(target.pace['contract']['image']).run(RunRequest(tuple(target.pace['contract'].get('solver_argv',('/candidate/run',))),
            Path(repaired['candidate_path']),raw,(Mount(Path(repaired['candidate_path']),'/candidate'),)),
            ResourceLimits(cpu_seconds=target.generation_cpu_seconds,wall_seconds=min(45,max(.001,deadline-time.monotonic())),
                memory_mb=target.memory_bytes//1048576,pids=target.pids_max,output_bytes=target.pace['contract']['output_bytes']))
        assert receipt.returncode==0 and not receipt.timed_out
        n,verification=verify_graph_solution(raw,receipt.stdout);assert verification.valid
        result['real_graph_debug']={'n':n,'size':verification.size,'cpu_seconds':receipt.cpu_seconds,'wall_seconds':receipt.wall_seconds,
            'stdout_bytes':len(receipt.stdout.encode()),'stdout_sha256':hashlib.sha256(receipt.stdout.encode()).hexdigest(),'reclamation':receipt.diagnostics}
        result['checks']['real_graph_valid']=True;result['status']='PASSED'
    except BaseException as error:
        result['status']='UNKNOWN' if getattr(error,'inflight_unknown',False) or getattr(error,'execution_unknown',False) else 'FAILED'
        result['error_type']=type(error).__name__;result['failed_stage']=stage
    finally:
        if grant is not None:authority.revoke(grant)
        if budgets:
            result['actual_requests']=budgets.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]
            result['nonterminal_requests']=budgets.connection.execute("SELECT count(*) FROM inference_reservations WHERE status!='COMPLETED'").fetchone()[0]
            result['receipts']=[dict(r) for r in budgets.connection.execute('SELECT * FROM provider_receipts')]
            result['phase_scopes']=[dict(r) for r in budgets.connection.execute('SELECT * FROM codex_phase_scopes')]
            if result['nonterminal_requests'] or any(r['status'] in ('UNKNOWN','DISPATCHING') for r in result['phase_scopes']):result['status']='UNKNOWN'
        if backend:
            try:backend.close()
            except BaseException:result['status']='UNKNOWN';result['cleanup_confirmed']=False
        if result['status']=='PASSED':
            if not (result['actual_requests']==4 and len(result['receipts'])==4 and all(r['usage_complete'] for r in result['receipts'])):
                result['status']='FAILED';result['failed_stage']='terminal-receipt-audit'
        result['finished_utc']=datetime.now(timezone.utc).isoformat()
        (root/'result.json').write_text(json.dumps(result,indent=2))
        print(json.dumps({k:result[k] for k in ('status','actual_requests','checks','started_utc','finished_utc')},indent=2),flush=True)
    return 0 if result['status']=='PASSED' else 2


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--live',action='store_true');p.add_argument('--root',type=Path,required=True)
    p.add_argument('--readiness-root',type=Path,required=True);p.add_argument('--config',type=Path,required=True);p.add_argument('--prior-ledger',type=Path,action='append',default=[]);p.add_argument('--standalone',action='store_true',help='Independent four-request readiness envelope; no historical quota reset')
    args=p.parse_args();raise SystemExit(run(args.root.resolve(),args.readiness_root.resolve(),args.config.resolve(),args.prior_ledger,args.live,args.standalone))
