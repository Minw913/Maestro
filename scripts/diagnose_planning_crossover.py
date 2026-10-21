"""One four-cell concurrent prompt/label diagnostic; maximum four live requests.

Frozen preparation is input, not a new agent preparation or formal experiment.
No tools, generation, task evaluations, automatic retries or score publication.
"""
import argparse,copy,hashlib,json,sqlite3,time,subprocess,threading
from concurrent.futures import ThreadPoolExecutor,wait
from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
from maestro.backends.coral.native_client import CoralCodexClient
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.backends.codex_session.client import CodexSessionClient
from maestro.execution.budgets import BudgetStore,BudgetExceeded
from maestro.agents.planner import parse_plan_decision
from maestro.types import state_from_dict,to_jsonable

CELLS=[('content1-label1','agent-1','agent-1'),('content1-label3','agent-1','agent-3'),
       ('content3-label1','agent-3','agent-1'),('content3-label3','agent-3','agent-3')]

def prompt_cell(original,label):
    """Change only structured agent_id fields; preserve content and opaque IDs."""
    prefix,payload=original.split('\n',1)
    value=json.loads(payload)
    donor=value['agent_id']
    if donor==label:return original,[]
    changed=[]
    def visit(item,path=()):
        if isinstance(item,dict):
            for key,v in item.items():
                if key=='agent_id' and v==donor:item[key]=label;changed.append('.'.join((*path,key)))
                else:visit(v,(*path,key))
        elif isinstance(item,list):
            for i,v in enumerate(item):visit(v,(*path,str(i)))
    visit(value)
    return prefix+'\n'+json.dumps(value,ensure_ascii=False),changed

class FourRequestBudget(BudgetStore):
    def create(self,account,**limits):
        if account not in {cell[0] for cell in CELLS} or limits['max_calls']!=1:
            raise BudgetExceeded('Undeclared crossover account or cap')
        return super().create(account,**limits)
    def reserve(self,*args,**kwargs):
        with self._lock:
            if self.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]>=4:
                raise BudgetExceeded('Four-request global cap exhausted')
            return super().reserve(*args,**kwargs)

def reservations(path):
    db=sqlite3.connect(f'file:{path.resolve()}?mode=ro',uri=True)
    rows=db.execute('SELECT reservation_id,account,status,request_id FROM inference_reservations ORDER BY reservation_id').fetchall()
    db.close();return rows

def main(root,source,historical,live):
    if not live:raise PermissionError('Explicit --live required')
    if root.exists():raise FileExistsError('Never reuse or retry a diagnostic root')
    before={str(p.resolve()):reservations(p) for p in historical}
    assert not subprocess.check_output(['docker','ps','-aq'],text=True).strip()
    root.mkdir(parents=True,exist_ok=False)
    inputs={}
    for p in (source/'runtime/codex-autonomous-authority/native').glob('*/scope.json'):
        scope=json.loads(p.read_text())
        if scope['prompt'].startswith('PLAN_RO:'):
            context=json.loads(scope['prompt'].split('\n',1)[1])
            if context['round_index']==1:inputs[context['agent_id']]=(scope['prompt'],json.loads((p.parent/'schema.json').read_text()))
    assert set(inputs)>={'agent-1','agent-3'} and inputs['agent-1'][1]==inputs['agent-3'][1]
    snapshot=json.loads((source/'input-snapshot.json').read_text())
    model='gpt-6-astra';cli='/home/zijianzhou/.local/bin/codex'
    auth=CodexSessionClient(model=model,work_root=root/'auth-check',executable=cli)
    auth.require_chatgpt_login()
    budgets=FourRequestBudget(root/'requests.sqlite')
    result={'status':'FAILED','maximum_requests':4,'planning_safety_deadline_seconds':600,
        'started_utc':datetime.now(timezone.utc).isoformat(),'cells':{},'no_generation':True,'no_formal_evaluation':True}
    gate=threading.Barrier(4);event_lock=threading.Lock()
    def event(kind,**fields):
        with event_lock:
            with (root/'events.jsonl').open('a') as f:f.write(json.dumps({'kind':kind,'utc':datetime.now(timezone.utc).isoformat(),'monotonic':time.monotonic(),**fields})+'\n')
    opened=time.monotonic();deadline=opened+600
    event('COMMON_PLANNING_OPENED',deadline_monotonic=deadline)
    def execute(cell,donor,label):
        folder=root/cell;folder.mkdir();prompt,changed=prompt_cell(inputs[donor][0],label)
        (folder/'prompt.txt').write_text(prompt)
        state=state_from_dict(snapshot['states'][donor]);state.agent_id=label
        state.history=[replace(h,agent_id=label) for h in state.history]
        (folder/'input-manifest.json').write_text(json.dumps({'content_donor':donor,'runtime_and_prompt_label':label,
            'changed_agent_id_paths':changed,'prompt_sha256':hashlib.sha256(prompt.encode()).hexdigest(),
            'opaque_history_and_direction_ids_preserved':True},indent=2))
        client=CoralCodexClient(model=model,work_root=folder/'native',executable=cli,reasoning_effort='low',timeout=600)
        def clear():
            with budgets._lock:
                if budgets.connection.execute("SELECT count(*) FROM inference_reservations WHERE status='UNKNOWN'").fetchone()[0]:
                    raise BudgetExceeded('New UNKNOWN prevents further dispatch')
        proxy=CodexQuotaProxy(budgets,cell,model,deadline=deadline,max_calls=1,max_request_bytes=16*1024*1024,
            max_output_tokens=96000,request_guard=clear)
        row={'content_donor':donor,'label':label,'status':'FAILED'}
        try:
            gate.wait(timeout=15);row['dispatch_monotonic']=time.monotonic();event('CELL_DISPATCHED',cell=cell)
            response=client.run_phase(prompt,base_url=proxy.start(),deadline=deadline,
                output_schema=inputs[donor][1],agent_id=label)
            proxy.assert_resolved()
            decision=parse_plan_decision(response.text,state,1,namespace='crossover-'+root.name)
            row['status']='ACCEPTED';row['action']=decision.action
            row['plan']=to_jsonable(decision.plan);(folder/'result.txt').write_text(response.text)
        except BaseException as error:
            row['error_type']=type(error).__name__
            if getattr(error,'inflight_unknown',False):row['status']='UNKNOWN'
        finally:
            try:proxy.close()
            except BaseException:row['status']='UNKNOWN';row['proxy_cleanup_confirmed']=False
            try:client.close();row['local_client_closed']=True
            except BaseException:row['status']='UNKNOWN';row['local_client_closed']=False
            usage=proxy.usage();row['usage']=usage
            if usage['unresolved_requests']:row['status']='UNKNOWN'
            row['returned_monotonic']=time.monotonic();row['open_to_return_seconds']=row['returned_monotonic']-opened
            (folder/'outcome.json').write_text(json.dumps(row,indent=2));event('CELL_RETURNED',cell=cell,status=row['status'])
        return row
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures={pool.submit(execute,*cell):cell[0] for cell in CELLS}
            for future,cell in futures.items():result['cells'][cell]=future.result()
        result['status']='PASSED' if all(x['status']=='ACCEPTED' for x in result['cells'].values()) else 'FAILED'
        if any(x['status']=='UNKNOWN' for x in result['cells'].values()):result['status']='UNKNOWN'
    except BaseException as error:result['error_type']=type(error).__name__
    finally:
        with budgets._lock:
            result['actual_requests']=budgets.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]
            result['nonterminal_requests']=budgets.connection.execute("SELECT count(*) FROM inference_reservations WHERE status!='COMPLETED'").fetchone()[0]
            result['terminal_usage_receipts']=budgets.connection.execute('SELECT count(*) FROM provider_receipts WHERE usage_complete=1').fetchone()[0]
        if result['nonterminal_requests']:result['status']='UNKNOWN'
        result['historical_reservations_unchanged']=all(reservations(Path(p))==rows for p,rows in before.items())
        result['parallel_wall_seconds']=time.monotonic()-opened;result['finished_utc']=datetime.now(timezone.utc).isoformat()
        budgets.close();(root/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2),flush=True)
    return 0 if result['status']=='PASSED' else 2

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--historical-ledger',type=Path,action='append',required=True);p.add_argument('--live',action='store_true')
    a=p.parse_args();raise SystemExit(main(a.root.resolve(),a.source.resolve(),a.historical_ledger,a.live))
