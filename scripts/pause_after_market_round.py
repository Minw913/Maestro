"""Request a cooperative pause after a target Market round can settle safely.

Run independently of the controller. Never cancels model requests or evaluator
jobs; it arms the existing pause marker only after all target-round evaluation
jobs have launched and no model request remains in flight. The controller then
settles that round and checks the marker before opening the next one.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time

from maestro.execution.pause import PauseController
from maestro.storage.paths import run_paths


def utc():
    return datetime.now(timezone.utc).isoformat()


def monitor(root: Path, episode: str, target: int, interval: float = .02):
    root=root.resolve(strict=True)
    if target < 0 or not .01 <= interval <= 1:
        raise ValueError('Invalid target or polling interval')
    local=run_paths(root).local
    output=local/'authority'/f'pause-after-round-{target+1}.json'
    if output.exists():
        raise FileExistsError('Pause monitor already used for this round')
    ledger=sqlite3.connect((root/'authority/ledger.sqlite').resolve().as_uri()+'?mode=ro',uri=True)
    requests=sqlite3.connect((root/'deepseek-native-authority/requests.sqlite').resolve().as_uri()+'?mode=ro',uri=True)
    result={'episode_id':episode,'target_round_index':target,'started_utc':utc(),
            'pause_requested':False,'status':'MONITORING'}
    try:
        row=ledger.execute('select status from episodes where id=?',(episode,)).fetchone()
        if row is None or row[0]!='RUNNING':
            raise ValueError('Target episode is not running')
        while True:
            status=ledger.execute('select status from episodes where id=?',(episode,)).fetchone()[0]
            unknown=requests.execute("select count(*) from inference_reservations where status='UNKNOWN'").fetchone()[0]
            if unknown and 'unknown_requests_observed' not in result:
                result['unknown_requests_observed']=unknown
                current=ledger.execute('select max(round_index) from rounds where episode_id=?',(episode,)).fetchone()[0]
                if current is not None and current<target:
                    target=current
                    result['effective_target_round_index']=target
            target_row=ledger.execute('select status from rounds where episode_id=? and round_index=?',
                                      (episode,target)).fetchone()
            next_row=ledger.execute('select status from rounds where episode_id=? and round_index>? limit 1',
                                    (episode,target)).fetchone()
            if next_row is not None:
                result.update(status='MISSED_SAFE_BOUNDARY',next_round_status=next_row[0])
                break
            if status!='RUNNING':
                result.update(status='EPISODE_STOPPED_EARLY',episode_status=status)
                break
            if target_row is not None and target_row[0] in ('EVALUATING','SETTLED'):
                tickets=[r[0] for r in ledger.execute(
                    'select status from eval_tickets where episode_id=? and round_index=?',(episode,target))]
                inflight=requests.execute("select count(*) from inference_reservations where status='RESERVED'").fetchone()[0]
                # Once all evaluation callbacks reached RUNNING, the controller
                # has passed its last pause check for this round. The marker
                # cannot strand a not-yet-dispatched evaluation ticket.
                safe=(target_row[0]=='SETTLED' or target_row[0]=='EVALUATING' and
                    (not tickets or all(item in ('RUNNING','RESULT_RECORDED') for item in tickets)))
                if safe and not inflight:
                    PauseController(root).request()
                    result.update(status='PAUSE_REQUESTED',pause_requested=True,
                        requested_utc=utc(),target_status_at_request=target_row[0],
                        ticket_statuses=tickets)
                    break
            time.sleep(interval)
    finally:
        ledger.close();requests.close()
        result['finished_utc']=utc()
        output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(result,indent=2)+'\n')
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--episode',required=True)
    parser.add_argument('--round-number',type=int,required=True,
                        help='Human 1-based completed round count; 10 pauses after index 9')
    args=parser.parse_args()
    outcome=monitor(args.root,args.episode,args.round_number-1)
    print(json.dumps(outcome),flush=True)
    raise SystemExit(0 if outcome['status']=='PAUSE_REQUESTED' else 2)
