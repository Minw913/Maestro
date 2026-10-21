"""Local command line; every real mode preflight is fail closed."""
from __future__ import annotations

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import urllib.request

from maestro.config import RunConfig,load_config
from maestro.types import canonical_json,MaestroError


def _config(args):
    if getattr(args,'config',None):
        config=load_config(args.config)
    elif getattr(args,'bootstrap',None):
        config=RunConfig.from_dict(json.loads(Path(args.bootstrap).read_text(encoding='utf-8'))['payload']['config'])
    else:
        config=RunConfig()
    if getattr(args,'seed',None) is not None:
        config=replace(config,seed=args.seed)
    overrides = {name: getattr(args, name) for name in ('execution_mode', 'benchmark')
                 if getattr(args, name, None) is not None}
    if overrides:
        config = replace(config, **overrides)
    return config


def _control_request(action,payload):
    if action=='candidate-submit' and os.environ.get('MAESTRO_EVAL_OUTBOX'):
        from maestro.backends.coral.submission import write_bound_eval_request
        path=write_bound_eval_request()
        return {'request_path':str(path),'status':'REQUESTED','evaluation_authorized':False}
    endpoint=os.environ.get('MAESTRO_CONTROL_URL')
    token=os.environ.get('MAESTRO_AGENT_TOKEN')
    if not endpoint or not token:
        raise MaestroError('Agent commands require an authenticated MAESTRO control endpoint; they never open the authority database')
    request=urllib.request.Request(endpoint.rstrip('/')+'/'+action,data=canonical_json(payload).encode(),
        headers={'Content-Type':'application/json','Authorization':'Bearer '+token},method='POST')
    with urllib.request.urlopen(request,timeout=15) as response:
        return json.load(response)


def build_parser():
    parser=argparse.ArgumentParser(prog='maestro',description='Market allocation for Coral research agents')
    parser.add_argument('--root',default='artifacts/local',help='Local experiment artifact root')
    sub=parser.add_subparsers(dest='command',required=True)
    codex_check=sub.add_parser('codex-check',help='Check local ChatGPT Codex sign-in without reading credentials')
    codex_check.add_argument('--probe',action='store_true',help='Run one real, tool-free structured Codex turn')
    codex_check.add_argument('--strict',action='store_true',help='Check native autonomous app-server protocol and login without inference')
    qualify=sub.add_parser('codex-qualify',help='Bounded real Codex connection qualification; never starts an experiment')
    qualify.add_argument('--live',action='store_true',help='Explicitly enable at most five real model requests')
    qualify.add_argument('--model',required=True,help='Frozen model to qualify using the existing ChatGPT login')
    qualify.add_argument('--reasoning-effort',choices=['low','medium','high','xhigh','max','ultra'],default='low',help='Frozen effort; does not inherit desktop preferences')
    qualify.add_argument('--executable',help='Optional native Codex executable')
    qualify.add_argument('--phase-seconds',type=float,default=120)
    qualify.add_argument('--historical-unknown',help='Read-only legacy UNKNOWN database to hash-attest')
    qualify.add_argument('--historical-sha256',help='Expected SHA256 of the historical evidence file')
    for command in ('bootstrap','run','resume','smoke','final-eval'):
        item=sub.add_parser(command)
        item.add_argument('--config')
        item.add_argument('--seed',type=int)
        item.add_argument('--execution-mode', choices=['mock','real'], help='Simulated evaluation or real execution')
        item.add_argument('--benchmark', choices=['pace'], help='Benchmark task (PACE only)')
        if command=='run':
            item.add_argument('--bootstrap',required=True)
            item.add_argument('--policy',choices=['first_price','uniform','ucb'],default='first_price')
            item.add_argument('--episode')
        if command=='resume':
            item.add_argument('--episode',required=True)
        if command=='final-eval':
            item.add_argument('--episode',required=True)
            item.add_argument('--manifest',required=True)
    for command in ('report','replay','status'):
        item=sub.add_parser(command)
        item.add_argument('--episode')
        if command=='replay':
            item.add_argument('--verify-ledger',action='store_true')
    agent=sub.add_parser('agent')
    a=agent.add_subparsers(dest='agent_command',required=True)
    plan=a.add_parser('plan-submit')
    plan.add_argument('--file',required=True)
    candidate=a.add_parser('candidate-submit')
    candidate.add_argument('--grant',required=True)
    candidate.add_argument('--worktree',required=True)
    a.add_parser('status')
    evaluate=sub.add_parser('eval',help='Controlled candidate submission; never launches a panel directly')
    evaluate.add_argument('--grant')
    evaluate.add_argument('--worktree')
    return parser


def main(argv=None):
    args=build_parser().parse_args(argv)
    try:
        if args.command=='codex-check':
            from maestro.backends.codex_session import CodexSessionClient
            import time
            client=CodexSessionClient(model=None,work_root=Path(args.root)/'codex-check',timeout=60)
            result=client.login_status()
            if args.strict:
                if args.probe:
                    raise ValueError('--strict is a no-inference protocol check; do not combine it with --probe')
                from maestro.backends.codex_session.autonomous_client import AutonomousCodexClient
                native=AutonomousCodexClient(model='gpt-6-astra',work_root=Path(args.root)/'codex-strict-check')
                result=native.preflight()
            if args.probe:
                client.require_chatgpt_login()
                response=client.structured_proposal(
                    {'instruction':'Return exactly {"ok":true}. Do not use tools, read files, or execute commands.'},
                    {'type':'object','properties':{'ok':{'type':'boolean'}},'required':['ok'],'additionalProperties':False},
                    purpose='planning',deadline=time.monotonic()+60)
                result['live_probe']={'ok':response.value.get('ok') is True,
                    'completed_session_turns':response.completed_turns,
                    'actual_inference_requests':response.actual_inference_requests,
                    'input_tokens':response.input_tokens,'output_tokens':response.output_tokens}
        elif args.command=='codex-qualify':
            if not args.live:
                raise ValueError('codex-qualify requires --live to authorize its bounded real requests')
            from maestro.backends.codex_session.qualification import run_qualification
            result=run_qualification(args.root,model=args.model,executable=args.executable,
                historical_path=args.historical_unknown,historical_sha256=args.historical_sha256,
                phase_seconds=args.phase_seconds,reasoning_effort=args.reasoning_effort)
            print(canonical_json(result))
            return 0 if result.get('status')=='PASSED' else 2
        elif args.command in ('bootstrap','run','resume','smoke','final-eval'):
            from maestro.loop import ExperimentEngine
            config=_config(args)
            if args.command=='resume':
                from maestro.storage import Ledger
                with Ledger(Path(args.root)/'authority'/'ledger.sqlite') as ledger:
                    row = ledger.connection.execute('SELECT config_json,config_hash FROM episodes WHERE id=?', (args.episode,)).fetchone()
                    if row is None:
                        raise ValueError('Unknown episode')
                    RunConfig.from_dict(json.loads(row['config_json']))
                    if row['config_hash'] != config.hash:
                        raise ValueError('Resume config hash mismatch')
            if args.command=='smoke' and config.execution_mode!='mock':
                raise MaestroError('smoke is explicitly simulated; use bootstrap/run for real evaluation')
            with ExperimentEngine(args.root,config) as engine:
                if args.command=='bootstrap':
                    result=engine.bootstrap()
                elif args.command=='run':
                    result=engine.run(args.bootstrap,args.policy,episode_id=args.episode)
                elif args.command=='resume':
                    result=engine.resume(args.episode)
                elif args.command=='final-eval':
                    from maestro.tasks.pace.heldout import evaluate_final
                    result=evaluate_final(engine,args.episode,args.manifest)
                else:
                    warm=engine.bootstrap()
                    runs=[engine.run(warm['checkpoint'],policy) for policy in ('uniform','ucb','first_price')]
                    result={'warm':warm,'runs':runs}
            if args.command=='smoke':
                from maestro.replay import verify_ledger
                from maestro.reports import report
                result['replay']=verify_ledger(args.root)
                result['report']=report(args.root)['report']
        elif args.command=='replay':
            from maestro.replay import verify_ledger
            result=verify_ledger(args.root,args.episode)
        elif args.command=='report':
            from maestro.reports import report
            result=report(args.root,args.episode)
        elif args.command=='status':
            from maestro.storage import Ledger
            with Ledger(Path(args.root)/'authority'/'ledger.sqlite') as ledger:
                query='SELECT id,policy,execution_mode,benchmark,status FROM episodes'+(' WHERE id=?' if args.episode else '')
                result={'episodes':[dict(r) for r in ledger.connection.execute(query,(args.episode,) if args.episode else ())]}
        elif args.command=='eval' or (args.command=='agent' and args.agent_command=='candidate-submit'):
            result=_control_request('candidate-submit',{'grant_id':args.grant,'worktree':args.worktree})
        elif args.command=='agent' and args.agent_command=='plan-submit':
            result=_control_request('plan-submit',{'plan':json.loads(Path(args.file).read_text(encoding='utf-8'))})
        else:
            result=_control_request('status',{})
        print(canonical_json(result))
        return 0
    except (MaestroError,ValueError,OSError,PermissionError,RuntimeError) as error:
        print(canonical_json({'error':type(error).__name__,'message':str(error)}),file=sys.stderr)
        return 2


if __name__=='__main__':
    raise SystemExit(main())
