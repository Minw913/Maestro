"""Four-call model x fixed-empty-field schema diagnostic; frozen real plan context."""
from pathlib import Path
import argparse,copy,json,time,threading,sqlite3,subprocess
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from maestro.execution.budgets import BudgetStore,BudgetExceeded
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.backends.codex_session.client import CodexSessionClient
from maestro.backends.coral.native_client import CoralCodexClient
from maestro.agents.planner import parse_plan_decision
from maestro.types import state_from_dict,to_jsonable
from diagnose_planning_crossover import reservations

CELLS=[('astra-current','gpt-6-astra','current'),('astra-no-empty-lessons','gpt-6-astra','no-empty-lessons'),
       ('sol-current','gpt-6.1-sol','current'),('sol-no-empty-lessons','gpt-6.1-sol','no-empty-lessons')]
class Budgets(BudgetStore):
    def create(self,account,**limits):
        if account not in {x[0] for x in CELLS} or limits['max_calls']!=1:raise BudgetExceeded('Undeclared scope/cap')
        return super().create(account,**limits)
    def reserve(self,*args,**kwargs):
        with self._lock:
            if self.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]>=4:raise BudgetExceeded('Four-call cap')
            return super().reserve(*args,**kwargs)

def schema_variant(original,variant):
    schema=copy.deepcopy(original)
    if variant=='no-empty-lessons':
        assert schema['properties']['lessons']['maxItems']==0
        del schema['properties']['lessons'];schema['required'].remove('lessons')
    return schema

class VisibleTrace:
    """Opt-in bounded visible output only; never records reasoning text/headers."""
    def __init__(self,folder):
        self.folder=folder;self.prefix=bytearray();self.last_flush=0;self.event_counts={};self.first_text=None;self.last_text=None;self.items=[]
    def __call__(self,event):
        kind=event['type'];self.event_counts[kind]=self.event_counts.get(kind,0)+1
        if 'text_delta' in event:
            self.first_text=self.first_text or event['monotonic'];self.last_text=event['monotonic']
            self.prefix.extend(event['text_delta'].encode()[:max(0,131072-len(self.prefix))])
        if event.get('item_type'):self.items.append({'type':event['item_type'],'monotonic':event['monotonic']})
        if time.monotonic()-self.last_flush>=1 or kind in ('response.completed','response.failed','response.incomplete'):self.flush()
    def flush(self):
        self.last_flush=time.monotonic();text=self.prefix.decode('utf-8',errors='ignore')
        tmp=self.folder/'visible-text-prefix.tmp';tmp.write_text(text);tmp.replace(self.folder/'visible-text-prefix.txt')
        value={'cap_bytes':131072,'recorded_prefix_bytes':len(self.prefix),'event_counts':self.event_counts,
            'first_text_monotonic':self.first_text,'last_text_monotonic':self.last_text,'output_item_types':self.items,
            'reasoning_content_recorded':False}
        try:
            parsed,end=json.JSONDecoder().raw_decode(text.lstrip());value['complete_json_prefix']=True
            value['json_end_character']=end;value['only_whitespace_after_json']=not text.lstrip()[end:].strip()
        except ValueError as e:
            value['complete_json_prefix']=False;value['json_error']=getattr(e,'msg',type(e).__name__);value['json_error_position']=getattr(e,'pos',None)
        (self.folder/'visible-trace.json').write_text(json.dumps(value,indent=2))

def main(root,source,historical,live):
    if not live:raise PermissionError('Explicit live required')
    if root.exists():raise FileExistsError('No reused roots/retries')
    before={str(p.resolve()):reservations(p) for p in historical}
    assert not subprocess.check_output(['docker','ps','-aq'],text=True).strip()
    root.mkdir(parents=True,exist_ok=False)
    inputs=[]
    for p in (source/'runtime/codex-autonomous-authority/native').glob('*/scope.json'):
        s=json.loads(p.read_text())
        if s['prompt'].startswith('PLAN_RO:') and json.loads(s['prompt'].split('\n',1)[1])['agent_id']=='agent-1':inputs.append((s['prompt'],json.loads((p.parent/'schema.json').read_text())))
    assert len(inputs)==1
    prompt,original=inputs[0];state_data=json.loads((source/'input-snapshot.json').read_text())['states']['agent-1']
    auth=CodexSessionClient(model='gpt-6-astra',work_root=root/'auth',executable='/home/zijianzhou/.local/bin/codex');auth.require_chatgpt_login()
    b=Budgets(root/'requests.sqlite');barrier=threading.Barrier(4)
    result={'status':'FAILED','maximum_actual_requests':4,'started_utc':datetime.now(timezone.utc).isoformat(),
        'cells':{},'generation_started':False,'formal_experiment_started':False}
    opened=time.monotonic();deadline=opened+600
    result['opened_monotonic']=opened;result['deadline_monotonic']=deadline
    def execute(cell,model,variant):
        folder=root/cell;folder.mkdir();schema=schema_variant(original,variant)
        (folder/'prompt.txt').write_text(prompt);(folder/'schema.json').write_text(json.dumps(schema,indent=2))
        trace=VisibleTrace(folder)
        client=CoralCodexClient(model=model,work_root=folder/'native',executable='/home/zijianzhou/.local/bin/codex',reasoning_effort='low',timeout=600)
        def guard():
            with b._lock:
                if b.connection.execute("SELECT count(*) FROM inference_reservations WHERE status='UNKNOWN'").fetchone()[0]:raise BudgetExceeded('New UNKNOWN blocks further dispatch')
        proxy=CodexQuotaProxy(b,cell,model,deadline=deadline,max_calls=1,max_request_bytes=16*1024*1024,max_output_tokens=96000,
            diagnostic_sink=trace,request_guard=guard)
        row={'model':model,'schema_variant':variant,'status':'FAILED','reasoning_effort':'low'}
        try:
            barrier.wait(timeout=15);row['dispatch_monotonic']=time.monotonic()
            response=client.run_phase(prompt,base_url=proxy.start(),deadline=deadline,output_schema=schema,agent_id='agent-1')
            proxy.assert_resolved()
            decision=parse_plan_decision(response.text,state_from_dict(state_data),1,namespace='model-schema-'+root.name)
            row['status']='ACCEPTED';row['decision']=to_jsonable(decision);(folder/'result.txt').write_text(response.text)
        except BaseException as e:row['error_type']=type(e).__name__
        finally:
            try:proxy.close();row['proxy_closed']=True
            except BaseException:row['status']='UNKNOWN';row['proxy_closed']=False
            try:client.close();row['client_closed']=True
            except BaseException:row['status']='UNKNOWN';row['client_closed']=False
            trace.flush();row['usage']=proxy.usage()
            if row['usage']['unresolved_requests']:row['status']='UNKNOWN'
            row['returned_monotonic']=time.monotonic();row['wall_from_common_open_seconds']=row['returned_monotonic']-opened
            (folder/'outcome.json').write_text(json.dumps(row,indent=2))
        return row
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures={pool.submit(execute,*cell):cell[0] for cell in CELLS}
            for f,cell in futures.items():result['cells'][cell]=f.result()
        result['status']='PASSED' if all(x['status']=='ACCEPTED' for x in result['cells'].values()) else 'FAILED'
    except BaseException as e:result['error_type']=type(e).__name__
    finally:
        with b._lock:
            result['actual_requests']=b.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]
            result['nonterminal_requests']=b.connection.execute("SELECT count(*) FROM inference_reservations WHERE status!='COMPLETED'").fetchone()[0]
            result['terminal_usage_receipts']=b.connection.execute('SELECT count(*) FROM provider_receipts WHERE usage_complete=1').fetchone()[0]
        if result['nonterminal_requests']:result['status']='UNKNOWN'
        result['historical_reservations_unchanged']=all(reservations(Path(p))==rows for p,rows in before.items())
        result['finished_utc']=datetime.now(timezone.utc).isoformat();result['parallel_wall_seconds']=time.monotonic()-opened
        b.close();(root/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2),flush=True)
    return 0 if result['status']=='PASSED' else 2

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--live',action='store_true');p.add_argument('--root',type=Path,required=True)
    p.add_argument('--source',type=Path,required=True);p.add_argument('--historical-ledger',type=Path,action='append',required=True)
    a=p.parse_args();raise SystemExit(main(a.root.resolve(),a.source.resolve(),a.historical_ledger,a.live))
