"""One live four-agent planning qualification; no generation, evaluation or retries.

Requires explicit historical-UNKNOWN exception. Historical reservations remain
unchanged and consumed; this qualification has a separate 20-request ceiling.
"""
import argparse,json,sqlite3,time,shutil,hashlib,subprocess,os
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,wait
from dataclasses import asdict,replace
from maestro.config import load_config
from maestro.types import state_from_dict,to_jsonable
from maestro.storage.artifacts import ArtifactStore
from maestro.backends.coral.native_backend import native_factory
from maestro.execution.budgets import BudgetStore,BudgetExceeded
from maestro.agents.planning_batch import parallel_plans

class QualificationBudget(BudgetStore):
    def __init__(self,path,episode,agents):
        super().__init__(path)
        self.allowed={f'{episode}:1:{agent}:{phase}':cap for agent in agents for phase,cap in [('PREPARATION',4),('PLAN_RO',1)]}
        self.connection.execute('CREATE TABLE IF NOT EXISTS codex_phase_scopes(account TEXT PRIMARY KEY,status TEXT NOT NULL)')
    def create(self,account,**limits):
        if self.allowed.get(account)!=limits.get('max_calls'):raise BudgetExceeded('Undeclared qualification scope/cap')
        return super().create(account,**limits)
    def reserve(self,*args,**kwargs):
        with self._lock:
            if self.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]>=20:raise BudgetExceeded('20-request ceiling exhausted')
            return super().reserve(*args,**kwargs)

def historical(path):
    c=sqlite3.connect(f'file:{path.resolve()}?mode=ro',uri=True)
    rows=c.execute('SELECT reservation_id,account,status,request_id FROM inference_reservations ORDER BY reservation_id').fetchall();c.close()
    return rows

def run(root,source,config_path,live,accept_unknown,planning_seconds=120,historical_ledgers=()):
    from maestro.storage.paths import diagnostic_root
    root = diagnostic_root(root)
    if not live or not accept_unknown:raise PermissionError('Explicit live and historical-UNKNOWN exception required')
    if root.exists():raise FileExistsError('No retries or reused qualification root')
    prior=source/'codex-autonomous-authority/requests.sqlite';before=historical(prior)
    unresolved=[r for r in before if r[2]!='COMPLETED']
    assert len(unresolved)==1 and unresolved[0][1]=='warm-58e175d21d5e41f3:1:agent-2:PLAN_RO' and unresolved[0][2]=='UNKNOWN'
    additional_before={str(p.resolve()):historical(p) for p in historical_ledgers}
    for rows in additional_before.values():
        assert all(r[2] in ('COMPLETED','UNKNOWN') for r in rows),'Historical execution still dispatching'
    assert not subprocess.check_output(['docker','ps','-aq'],text=True).strip(),'Local shared execution not reclaimed'
    config=load_config(config_path).for_stage('warm_start')
    assert config.preparation_calls==4 and config.planning_calls==1 and config.planning_window_seconds==120
    assert planning_seconds in (120,600),'Only declared diagnostic windows are admitted'
    config=replace(config,planning_window_seconds=planning_seconds)
    root.mkdir(parents=True,exist_ok=False)
    result={'status':'FAILED','maximum_requests':20,'historical_unknown_exception':True,'historical_unknown_settled':False,
            'generation_started':False,'formal_evaluation_started':False,'warm_started':False,'market_started':False,
            'planning_safety_deadline_seconds':planning_seconds,
            'additional_historical_unknowns':{p:sum(r[2]=='UNKNOWN' for r in rows) for p,rows in additional_before.items()},
            'started_utc':datetime.now(timezone.utc).isoformat(),'timings':{}}
    backend=budgets=None;stage='setup'
    def event(kind,**fields):
        with (root/'events.jsonl').open('a') as f:f.write(json.dumps({'kind':kind,'utc':datetime.now(timezone.utc).isoformat(),'monotonic':time.monotonic(),**fields})+'\n')
    try:
        (root/'historical-reservations.json').write_text(json.dumps({'source':before,'additional':additional_before},indent=2))
        db=sqlite3.connect(f'file:{source}/authority/ledger.sqlite?mode=ro',uri=True)
        snapshot=json.loads(db.execute('SELECT snapshot_json FROM rounds WHERE round_index=1').fetchone()[0])
        states={a:state_from_dict(s) for a,s in snapshot['states'].items()};assert len(states)==4
        (root/'input-snapshot.json').write_text(json.dumps(snapshot,indent=2))
        director=json.loads(db.execute("SELECT payload FROM events WHERE kind='DIRECTOR_COMPLETED'").fetchone()[0])
        directions=ArtifactStore(source/'authority/cas/sha256').get_json(director['artifact_hash'])
        (root/'directions.json').write_text(json.dumps(directions,indent=2))
        (root/'config.resolved.json').write_text(json.dumps(config.to_dict(),indent=2))
        backend=native_factory(config,root=root/'runtime')
        assert not backend.budget_store.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]
        backend.budget_store.close();episode='planning-qualification-'+root.name
        budgets=QualificationBudget(root/'requests.sqlite',episode,states);backend.budget_store=budgets
        backend.begin_episode(episode,states);backend.set_directions(directions)
        for agent,state in states.items():
            candidate=json.loads(db.execute('SELECT payload FROM candidates WHERE hash=?',(state.parent_hash,)).fetchone()[0])
            backend.register_parent(state.parent_hash,candidate['candidate_path'])
            workspace=backend._workspace(state)
            # Source episode is authoritative; never infer identity from run IDs.
            old_episode=db.execute('SELECT id FROM episodes').fetchone()[0]
            from maestro.storage.paths import run_paths
            workspace_base=run_paths(source).local/'coral-workspaces'
            if not workspace_base.exists():workspace_base=source/'coral-workspaces'
            private=workspace_base/old_episode/agent/'private'
            if private.exists():shutil.copytree(private,workspace.private,dirs_exist_ok=True,symlinks=False)
        backend.set_board(snapshot['board'])
        db.close()
        stage='preparation';opened=time.monotonic();deadline=opened+config.preparation_window_seconds
        event('PREPARATION_OPENED',deadline_monotonic=deadline)
        def prepare(agent,state):
            started=time.monotonic();event('PREPARATION_DISPATCHED',agent_id=agent)
            try:
                evidence,usage=backend.prepare_with_receipt(state,1,{'board':snapshot['board']},deadline=deadline)
                return {'accepted':True,'evidence':evidence,'usage':usage,'started':started,'returned':time.monotonic()}
            except Exception as e:
                return {'accepted':False,'error_type':type(e).__name__,'unknown':bool(getattr(e,'inflight_unknown',False)),
                        'started':started,'returned':time.monotonic()}
        pool=ThreadPoolExecutor(max_workers=4)
        futures={pool.submit(prepare,a,s):a for a,s in states.items()}
        done,pending=wait(futures,timeout=max(0,deadline-time.monotonic()))
        preparation={futures[f]:f.result() for f in done}
        pool.shutdown(wait=False,cancel_futures=True)
        (root/'preparation.json').write_text(json.dumps(preparation,indent=2))
        result['timings']['preparation_parallel_wall_seconds']=time.monotonic()-opened
        if pending:
            result['status']='UNKNOWN';raise RuntimeError('Preparation callback unresolved at common deadline')
        assert all(x['accepted'] and x['returned']<deadline for x in preparation.values()),'Preparation failure; no planning or retry'
        stage='planning';opened=time.monotonic();deadline=opened+config.planning_window_seconds
        event('PLANNING_OPENED',deadline_monotonic=deadline)
        outcomes=parallel_plans(states,{a:{'board':snapshot['board']} for a in states},backend,1,deadline)
        result['timings']['planning_parallel_wall_seconds']=time.monotonic()-opened
        (root/'planning.json').write_text(json.dumps(to_jsonable({a:asdict(x) for a,x in outcomes.items()}),indent=2))
        result['planning_outcomes']={a:{'status':x.status,'error':x.error,'action':x.decision.action if x.decision else None,
            'started_monotonic':x.started_at,'finished_monotonic':x.finished_at} for a,x in outcomes.items()}
        if any(x.inflight_unknown for x in outcomes.values()):result['status']='UNKNOWN'
        assert all(x.decision is not None and not x.error for x in outcomes.values()),'Planning not accepted; no retry'
        result['matching_evidence_plans']=sum(bool(x.decision.plan and x.decision.plan.evidence_run_ids) for x in outcomes.values())
        assert result['matching_evidence_plans']>=1,'Insufficient citation coverage; no extra calls authorized'
        result['status']='PASSED'
    except BaseException as e:
        result['failed_stage']=stage;result['error_type']=type(e).__name__
        if getattr(e,'inflight_unknown',False):result['status']='UNKNOWN'
    finally:
        if backend:
            try:backend.close();result['local_backend_closed']=True
            except BaseException as e:result['status']='UNKNOWN';result['local_backend_closed']=False;result['cleanup_error_type']=type(e).__name__
        if budgets:
            b=sqlite3.connect(f'file:{root}/requests.sqlite?mode=ro',uri=True);b.row_factory=sqlite3.Row
            result['actual_requests']=b.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]
            result['nonterminal_requests']=b.execute("SELECT count(*) FROM inference_reservations WHERE status!='COMPLETED'").fetchone()[0]
            receipts=[dict(x) for x in b.execute('SELECT * FROM provider_receipts')]
            result['terminal_usage_receipts']=len(receipts)
            result['known_input_tokens']=sum(x['input_tokens'] or 0 for x in receipts)
            result['known_output_tokens']=sum(x['output_tokens'] or 0 for x in receipts)
            scopes=[dict(x) for x in b.execute('SELECT * FROM codex_phase_scopes')];result['phase_scopes']=scopes
            if result['nonterminal_requests'] or any(x['status'] in ('UNKNOWN','DISPATCHING') for x in scopes):result['status']='UNKNOWN'
            if result['status']=='PASSED' and (len(receipts)!=result['actual_requests'] or not all(x['usage_complete'] for x in receipts)):result['status']='FAILED'
            b.close()
        result['historical_reservations_unchanged']=historical(prior)==before and all(historical(Path(p))==rows for p,rows in additional_before.items())
        result['finished_utc']=datetime.now(timezone.utc).isoformat()
        result['remaining_containers']=subprocess.check_output(['docker','ps','-aq'],text=True).split()
        if result['remaining_containers'] or not result['historical_reservations_unchanged']:result['status']='UNKNOWN'
        (root/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2),flush=True)
    return 0 if result['status']=='PASSED' else 2

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--live',action='store_true');p.add_argument('--accept-historical-unknown',action='store_true')
    p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--config',type=Path,required=True)
    p.add_argument('--planning-seconds',type=int,choices=[120,600],default=120)
    p.add_argument('--historical-ledger',type=Path,action='append',default=[])
    a=p.parse_args();raise SystemExit(run(a.root.resolve(),a.source.resolve(),a.config.resolve(),a.live,a.accept_historical_unknown,a.planning_seconds,a.historical_ledger))
