"""Independent read-only assessment barrier for frozen prebid candidates."""
from concurrent.futures import ThreadPoolExecutor,wait
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import difflib
import json
import time

from maestro.agents.valuation import estimate_value
from maestro.backends.coral.valuation import ValuationRepairFailed
from maestro.agents.valuation_reasoning import STRATEGY,screened,decode,expected_credit,validate_read_evidence,outcome_class_contract,canonicalize_rounded_no_reward
from maestro.backends.coral.workspace import candidate_tree_hash,solver_files
from maestro.storage.export import SECRETS
from maestro.types import IntegrityError,MaestroError,canonical_json,stable_hash


def context_bundle(engine,state,plan,proposal,champion,baseline):
    files={};omitted=[];size=0
    def put(path,text,required=False):
        nonlocal size
        raw=text.encode()
        if STRATEGY.search(path) or STRATEGY.search(text) or SECRETS.search(raw):
            if required:raise ValueError('Required technical context cannot be safely published')
            omitted.append('nontechnical content omitted');return
        if len(raw)>128*1024 or size+len(raw)>8*1024*1024 or len(files)>=512:
            raise ValueError('Complete context exceeds bounded read scope')
        files[path]=text;size+=len(raw)
    def code(root,prefix,expected):
        if candidate_tree_hash(root)!=expected:raise IntegrityError('Valuation source identity changed')
        for path,relative in solver_files(Path(root)):
            put(prefix+'/'+relative.as_posix(),path.read_text(encoding='utf-8'),required=True)
    code(proposal['candidate_path'],'solver',proposal['candidate_hash'])
    parent=engine.backend._parents.get(state.parent_hash)
    if parent is None:raise IntegrityError('Frozen parent unavailable')
    code(parent,'parent',state.parent_hash)
    view_plan={key:getattr(plan,key) for key in ('target','mod_type','diagnosis','change','expected_effect')}
    put('public/plan.json',canonical_json(screened(view_plan)),required=True)
    # The full numeric trajectory is controller-created, not raw history/memory JSON.
    index=[];trajectory=[];discount=Decimal(str(engine.config.valuation_discount_factor))
    n=len(state.history);weights=[discount**(n-1-j) for j in range(n)];normalizer=sum(weights,Decimal(0))
    for j,record in enumerate(state.history):
        if record.agent_id!=state.agent_id:raise IntegrityError('Peer history in valuation context')
        name=f'history/outcome-{j:04d}.json'
        row={'own_attempt':j+1,'parent_score':record.parent_score,'child_score':record.score,
             'champion_score_at_evaluation':record.champion_score,'actual_credit':str(record.credit),
             'improvement':record.gain,'valid':record.valid,'status':record.status,
             'weight':str(weights[j]),'normalized_weight':str(weights[j]/normalizer),
             'target':record.target,'mod_type':record.mod_type}
        put(name,canonical_json(row),required=True);index.append(name);trajectory.append(row)
        ticket=engine.ledger.connection.execute('SELECT payload FROM eval_tickets WHERE job_id=?',(record.experiment_id,)).fetchone()
        if ticket:
            old=engine.ledger.connection.execute('SELECT payload FROM plan_versions WHERE id=?',(json.loads(ticket[0])['plan_version_id'],)).fetchone()
            if old:
                body=json.loads(old[0]);put(f'history/plan-{j:04d}.json',canonical_json(screened({k:body.get(k) for k in view_plan})))
        old=engine.ledger.connection.execute('SELECT payload FROM candidates WHERE hash=?',(record.candidate_hash,)).fetchone()
        if old:
            source=json.loads(old[0]).get('candidate_path')
            if source and Path(source).is_dir():code(source,f'history/program-{j:04d}',record.candidate_hash)
    put('public/trajectory.json',canonical_json(trajectory),required=True)
    workspace=engine.backend._workspace(state)
    for j,path in enumerate(sorted(workspace.private.rglob('*'))):
        if path.is_symlink():continue
        if path.is_file() and path.suffix in {'.md','.txt'} and path.stat().st_size<=128*1024:
            put(f'private/note-{j:04d}.txt',path.read_text(encoding='utf-8'))
    for key in list(files):
        if key.startswith('solver/') and 'parent/'+key[7:] in files:
            diff='\n'.join(difflib.unified_diff(files['parent/'+key[7:]].splitlines(),files[key].splitlines(),fromfile='parent/'+key[7:],tofile=key))
            put('public/diff-'+str(len(files))+'.txt',diff)
    rules={'parent_score':state.parent_score,'champion_score':champion,'score_range':[0,1],
           'historical_baseline_credit':str(baseline),'discount_factor':str(discount),
           'historical_outcomes':index,'omissions':omitted,
           'baseline_formula':'sum(gamma^(n-j)*actual_credit_j)/sum(gamma^(n-j)); j=1..n, own settled attempts only',
           'reward_formula':'scale*(max(0,Y-C)+catchup*max(0,min(Y,C)-P)); invalid outcomes receive zero',
           'reward_scale':str(engine.config.reward_scale),'catchup_fraction':str(engine.config.catchup_fraction),
           'outcome_classes':{'no_positive_reward':'0 <= score <= P; includes failure risk',
                             'catchup':'P < score <= C; zero probability when P >= C',
                             'frontier':'max(P,C) < score <= 1; zero probability when max(P,C) = 1'},
           'scenario_response_rules':outcome_class_contract(state.parent_score,champion),
           'evaluation_limits':{'cpu_seconds_per_instance':engine.config.cpu_seconds_per_instance,
                'evaluation_instance_count':engine.config.evaluation_instance_count}}
    put('public/contract.json',canonical_json(rules),required=True)
    return files


def resume_generation_deadline(deadline,valuation_started,valuation_finished):
    """Exclude only the elapsed valuation stage from generation wall time."""
    if valuation_finished < valuation_started:
        raise ValueError('Valuation clock moved backwards')
    return deadline+(valuation_finished-valuation_started)


def retire_failed_valuations(engine,e,r,states,proposals,outcomes):
    """A clean failed regeneration forfeits this agent's current submission."""
    for agent_id,outcome in outcomes.items():
        if outcome['status']!='FAILED':continue
        proposals.pop(agent_id)
        states[agent_id].needs_refresh=True
        engine.ledger.event('VALUATION_AGENT_FAILED',{'agent_id':agent_id,
            'reason':outcome.get('reason','ValuationRepairFailed'),
            'requests':outcome.get('usage',{}).get('calls',0)},e,r)


def run_valuations(engine,e,r,states,plans,proposals,champion):
    # This is a separate read-only stage. Its common deadline must not inherit
    # the time already spent implementing the proposals it assesses.
    c=engine.config;opened=time.monotonic();deadline=opened+c.valuation_window_seconds
    engine.ledger.set_round_phase(e,r,'VALUATION_RO')
    conditional=int(bool(getattr(engine.backend,'valuation_output_retry',False)))
    engine.ledger.event('VALUATION_WINDOW_OPENED',{'opened_monotonic':opened,'opened_utc_epoch':time.time(),
        'deadline_monotonic':deadline,'ordinary_request_cap_per_agent':c.valuation_calls,
        'conditional_final_retry_requests':conditional,
        'actual_request_cap_per_agent':c.valuation_calls+conditional},e,r)
    def sample(a):
        state=deepcopy(states[a]);plan=plans[a];proposal=proposals[a];started=time.monotonic()
        baseline,ids=estimate_value(state.history,gamma=c.valuation_discount_factor) if state.history else (Decimal(0),[])
        record={'agent_id':a,'proposal_hash':proposal['candidate_hash'],'plan_version_id':plan.version_id,
                'baseline':str(baseline),'sample_ids':ids,'value':str(baseline),'status':'FALLBACK',
                'started_monotonic':started,'deadline_monotonic':deadline,'usage':{},'assessment':None,'read_trace':[]}
        account=f'{e}:{r}:{a}:VALUATION_RO';files={}
        try:
            with engine.ledger._lock:files=context_bundle(engine,state,plan,proposal,champion,baseline)
            context={'files':files,'manifest':{k:stable_hash(v) for k,v in files.items()}}
            context_hash=engine.artifacts.put_json(context);record['context_hash']=context_hash
            engine.ledger.event('VALUATION_INPUT_FROZEN',{'agent_id':a,'proposal_hash':proposal['candidate_hash'],
                'context_hash':context_hash,'dispatched_monotonic':time.monotonic()},e,r)
            if time.monotonic()>=deadline:raise TimeoutError('Assessment window expired before dispatch')
            response=engine.backend.value_with_receipt(a,r,files,deadline=deadline)
            record['repair_attempted']=bool(response.get('repair_attempted',False))
            record['usage']=response['usage'];record['read_trace']=response['read_trace'];record['cleanup']=response.get('cleanup',{})
            if time.monotonic()>=deadline:raise TimeoutError('Late assessment rejected')
            record['response_hash']=stable_hash(response['text'])
            if len(response['text'].encode())<=64*1024:record['response_text']=response['text']
            result,adjustments=canonicalize_rounded_no_reward(decode(response['text']),state.parent_score)
            record['value']=str(expected_credit(result,state.parent_score,champion,scale=c.reward_scale,
                catchup=c.catchup_fraction,paths=set(files)))
            validate_read_evidence(record['read_trace'],files,result)
            record['assessment']=result;record['boundary_adjustments']=adjustments;record['status']='ACCEPTED'
        except BaseException as error:
            if not isinstance(error,Exception) or isinstance(error,IntegrityError):raise
            uncertain=bool(getattr(error,'inflight_unknown',False) or getattr(error,'execution_unknown',False)
                or getattr(error,'local_quiescent',None) is False)
            if not uncertain and not isinstance(error,(ValueError,TimeoutError,PermissionError)):
                raise
            record['reason']=type(error).__name__
            if isinstance(error,ValuationRepairFailed) or record.get('repair_attempted'):
                record['status']='FAILED'
            if (getattr(error,'inflight_unknown',False) or getattr(error,'execution_unknown',False)
                    or getattr(error,'local_quiescent',None) is False):
                record['status']='UNKNOWN'
            if record['status']=='FALLBACK':
                # A numeric assessment can be computed before its read evidence
                # is rejected. Never submit that unaccepted value as fallback.
                record['value']=str(baseline)
                record['assessment']=None
        finally:
            store=getattr(engine.backend,'budget_store',None)
            if store is not None:
                try:record['usage']=store.usage(account)
                except Exception:pass
            if record['usage'].get('unresolved_requests',0) or (record['usage'].get('calls',0) and record['usage'].get('usage_complete') is not True):record['status']='UNKNOWN'
            record['returned_monotonic']=time.monotonic()
            if record['status']=='ACCEPTED' and record['returned_monotonic']>=deadline:
                record.update(status='FALLBACK',value=str(baseline),assessment=None,reason='LateResult')
            usage=record['usage']
            engine.ledger.usage(e,r,a,'valuation',wall=record['returned_monotonic']-started,
                calls=int(usage.get('calls',0)),tokens=int(usage.get('known_input_tokens') or 0)+int(usage.get('known_output_tokens') or 0),
                payload={'scope':account,'usage':usage,'usage_complete':usage.get('usage_complete',not usage),
                         'status':record['status'],'request_count_unknown':record['status']=='UNKNOWN','native_controller_cpu_seconds':None,
                         'cpu_scope':'no sandbox execution; native/controller CPU unmeasured'})
        record['delta']=str(Decimal(record['value'])-baseline)
        digest=engine.artifacts.put_json(record)
        engine.ledger.event('VALUATION_RESULT',{'agent_id':a,'artifact_hash':digest,'status':record['status'],
            'proposal_hash':proposal['candidate_hash'],'value':record['value'],
            'boundary_adjustment_count':len(record.get('boundary_adjustments',()))},e,r)
        return record
    pool=ThreadPoolExecutor(max_workers=c.n_agents,thread_name_prefix='maestro-valuation')
    futures={pool.submit(sample,a):a for a in proposals};engine._generation_futures.update(futures)
    try:
        done,pending=wait(futures,timeout=max(0,deadline-time.monotonic()))
        if pending:
            late,pending=wait(pending,timeout=5.);done.update(late)
        errors=[f.exception() for f in done if f.exception() is not None]
        if pending or errors:
            uncertain=bool(pending) or any(getattr(error,'inflight_unknown',False) for error in errors)
            engine.ledger.set_round_phase(e,r,'VALUATION_UNKNOWN' if uncertain else 'VALUATION_FAILED')
            with engine.ledger.transaction() as db:db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
            for error in errors:engine.ledger.event('VALUATION_CALLBACK_FAILED',{'error_type':type(error).__name__},e,r)
            raise MaestroError('Valuation callback unresolved or integrity failure; preserve evidence') from (errors[0] if errors else None)
        outcomes={a:f.result() for f,a in futures.items()}
        if any(v['status']=='UNKNOWN' for v in outcomes.values()):
            engine.ledger.set_round_phase(e,r,'VALUATION_UNKNOWN')
            with engine.ledger.transaction() as db:db.execute("UPDATE episodes SET status='INCOMPLETE' WHERE id=?",(e,))
            raise MaestroError('Valuation inference unresolved; no retry or automatic continuation')
        return outcomes
    finally:pool.shutdown(wait=False,cancel_futures=True)
