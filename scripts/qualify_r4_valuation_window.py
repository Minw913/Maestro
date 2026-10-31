"""One-use, four-agent live R4 valuation qualification; no formal evaluation.

The input is four immutable valuation contexts from a completed real PACE
Market round. Each fresh agent scope permits four ordinary physical requests
and one conditional final-answer regeneration after proven output truncation
or malformed JSON with complete terminal usage and cleanup.
The script does not retry, resume, score a candidate, or alter its source run.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time

from maestro.agents.valuation_reasoning import (
    canonicalize_rounded_no_reward, decode, expected_credit, validate_read_evidence)
from maestro.backends.coral.deepseek_backend import native_factory
from maestro.backends.coral.deepseek_lifecycle import audit_ledger
from maestro.config import load_config
from maestro.types import stable_hash


def utc():
    return datetime.now(timezone.utc).isoformat()


def contexts(source):
    ledger=source/'authority/ledger.sqlite'
    db=sqlite3.connect(ledger.resolve().as_uri()+'?mode=ro',uri=True)
    try:
        rows=db.execute("SELECT payload FROM events WHERE round_index=0 AND kind='VALUATION_INPUT_FROZEN'").fetchall()
    finally:
        db.close()
    identities={}
    for (payload,) in rows:
        item=json.loads(payload)
        agent=item['agent_id']
        if agent in identities:
            raise ValueError('Duplicate source valuation context')
        digest=item['context_hash']
        blob=(source/'authority/cas/sha256'/digest[:2]/digest).read_bytes()
        if hashlib.sha256(blob).hexdigest()!=digest:
            raise ValueError('Source valuation context hash mismatch')
        context=json.loads(blob)
        files=context['files']
        if {path:stable_hash(value) for path,value in files.items()}!=context['manifest']:
            raise ValueError('Source valuation file manifest mismatch')
        if not {'public/contract.json','public/trajectory.json','solver/run'}<=files.keys():
            raise ValueError('Incomplete real valuation context')
        identities[agent]={'hash':digest,'files':files}
    if set(identities)!={f'agent-{i}' for i in range(1,5)}:
        raise ValueError('Exactly four source agents required')
    return identities


def run(args):
    root=args.root.resolve()
    if not args.live or not root.is_dir() or (root/'result.json').exists():
        raise ValueError('One-use prepared root and --live required')
    native=root/'runtime'
    if (native/'deepseek-native-authority/requests.sqlite').exists():
        raise ValueError('Request ledger already exists; no retry')
    config=load_config(args.config,stage='market')
    if (config.execution_mode,config.benchmark,config.coral.get('runtime'))!=('real','pace','dsh'):
        raise ValueError('Real PACE DSH configuration required')
    if config.valuation_calls!=4 or config.valuation_window_seconds!=300:
        raise ValueError('Qualification freezes four requests and a 300-second valuation window')
    max_requests=4*(config.valuation_calls+1)
    inputs=contexts(args.source.resolve())
    (root/'inputs.json').write_text(json.dumps({'source_run':str(args.source.resolve()),
        'contexts':{agent:item['hash'] for agent,item in inputs.items()},
        'maximum_physical_requests':max_requests,'ordinary_requests_per_agent':config.valuation_calls,
        'conditional_final_retries_per_agent':1,'window_seconds':300},indent=2)+'\n')
    lock=threading.Lock()
    def event(kind,**fields):
        with lock,(root/'events.jsonl').open('a') as stream:
            stream.write(json.dumps({'kind':kind,'utc':utc(),'monotonic':time.monotonic(),**fields})+'\n')
    result={'status':'FAILED','started_utc':utc(),'maximum_physical_requests':max_requests,
            'formal_evaluations':0,'samples':{}}
    backend=None
    try:
        backend=native_factory(config,root=native)
        episode='qualification-'+root.name
        backend.begin_episode(episode)
        opened=time.monotonic();deadline=opened+config.valuation_window_seconds
        result['window']={'opened_monotonic':opened,'deadline_monotonic':deadline,
                          'opened_utc':utc(),'duration_seconds':config.valuation_window_seconds}
        event('VALUATION_WINDOW_OPENED',deadline_monotonic=deadline)
        def sample(agent):
            began=time.monotonic();event('TASK_DISPATCH',agent=agent)
            files=inputs[agent]['files']
            try:
                response=backend.value_with_receipt(agent,0,files,deadline=deadline)
                if time.monotonic()>=deadline:
                    raise TimeoutError('Late valuation result')
                contract=json.loads(files['public/contract.json'])
                decoded,adjustments=canonicalize_rounded_no_reward(
                    decode(response['text']),contract['parent_score'])
                credit=expected_credit(decoded,contract['parent_score'],contract['champion_score'],
                    scale=config.reward_scale,catchup=config.catchup_fraction,paths=set(files))
                validate_read_evidence(response['read_trace'],files,decoded)
                usage=response['usage']
                cleanup=response.get('cleanup',{})
                repair_attempted=bool(response.get('repair_attempted',False))
                if (usage.get('usage_complete') is not True or usage.get('unresolved_requests')
                        or usage.get('unknown_requests') or not 1<=usage.get('calls',0)<=4+int(repair_attempted)
                        or repair_attempted and usage.get('calls')!=5
                        or not cleanup.get('local_process_group_reclaimed')
                        or not cleanup.get('tool_service_reclaimed')):
                    raise ValueError('Incomplete usage or local cleanup')
                sample={'status':'ACCEPTED','context_hash':inputs[agent]['hash'],
                        'started_monotonic':began,'accepted_monotonic':time.monotonic(),
                        'response_sha256':hashlib.sha256(response['text'].encode()).hexdigest(),
                        'expected_credit':str(credit),'boundary_adjustments':adjustments,
                        'usage':usage,'cleanup':cleanup,'repair_attempted':repair_attempted,
                        'read_operations':len(response['read_trace'])}
            except BaseException as error:
                account=f'{episode}:0:{agent}:VALUATION_RO'
                try:usage=backend.budget_store.usage(account)
                except Exception:usage={}
                sample={'status':'UNKNOWN' if getattr(error,'inflight_unknown',False) else 'FAILED',
                        'context_hash':inputs[agent]['hash'],'started_monotonic':began,
                        'finished_monotonic':time.monotonic(),'error_type':type(error).__name__,
                        'usage':usage}
                if usage.get('unresolved_requests') or usage.get('unknown_requests') or (
                        usage.get('calls',0) and usage.get('usage_complete') is not True):
                    sample['status']='UNKNOWN'
                event('TASK_FAILED',agent=agent,error_type=type(error).__name__,status=sample['status'])
            else:
                event('RESULT_ACCEPTED',agent=agent,requests=usage['calls'])
            with lock:
                result['samples'][agent]=sample
            return sample
        pool=ThreadPoolExecutor(max_workers=4,thread_name_prefix='r4-qualification')
        futures=[pool.submit(sample,agent) for agent in sorted(inputs)]
        done,pending=wait(futures,timeout=max(0,deadline-time.monotonic())+5)
        pool.shutdown(wait=not pending,cancel_futures=True)
        result['window']['returned_monotonic']=time.monotonic()
        result['window']['pending_callbacks']=len(pending)
        if pending:
            raise RuntimeError('Valuation callback cleanup unconfirmed')
        for future in done:
            future.result()
        backend.close();backend=None
        authority=native/'deepseek-native-authority/requests.sqlite'
        audit=audit_ledger(authority)
        result['audit']={'status':audit['status'],'unresolved_scopes':audit['unresolved_scopes'],
                         'active_budgets':audit['active_budgets']}
        result['actual_requests']=len(audit['requests'])
        result['status']='PASSED' if (audit['status']=='CLEAR' and len(audit['requests'])<=max_requests
            and len(result['samples'])==4
            and all(sample['status']=='ACCEPTED' for sample in result['samples'].values())) else 'FAILED'
    except BaseException as error:
        result['error_type']=type(error).__name__
        result['status']='UNKNOWN' if getattr(error,'inflight_unknown',False) else 'FAILED'
    finally:
        if backend is not None:
            backend.close()
        result['finished_utc']=utc()
        (root/'result.json').write_text(json.dumps(result,indent=2,default=str)+'\n')
    if result['status']!='PASSED':
        raise RuntimeError('R4 valuation qualification did not pass; preserve the one-use root')
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--live',action='store_true')
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--config',type=Path,required=True)
    run(parser.parse_args())
