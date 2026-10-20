"""Read/recompute execution facts without recalling a model or benchmark."""
from decimal import Decimal
from pathlib import Path
import json
from maestro.storage import Ledger,ArtifactStore
from maestro.storage.ledger import validate_round_resources
from maestro.types import AuthorizationError,IntegrityError,stable_hash,Submission,state_from_dict,canonical_json
from maestro.market.credit import reward,money
from maestro.market.policies import rank


def verify_ledger(root,episode_id=None):
    root=Path(root)
    with Ledger(root/'authority'/'ledger.sqlite') as ledger:
        db=ledger.connection
        events=ledger.verify_event_chain()
        artifacts=ArtifactStore(root/'authority'/'cas'/'sha256')
        artifact_count=0
        for row in db.execute('SELECT * FROM artifact_refs'):
            data=artifacts.get(row['hash'])
            if len(data)!=row['size_bytes']:
                raise IntegrityError('Artifact size mismatch')
            artifact_count+=1
        for row in db.execute('SELECT * FROM plan_versions'):
            if stable_hash(json.loads(row['payload']))!=row['content_hash']:
                raise IntegrityError('Immutable plan payload changed')
        for row in db.execute('SELECT * FROM checkpoints'):
            payload=json.loads(row['payload'])
            if stable_hash(payload)!=row['content_hash'] or artifacts.get_json(row['content_hash'])!=payload:
                raise IntegrityError('Checkpoint payload/CAS mismatch')
            for reference in payload.get('private_memory_manifest',{}).values():
                from maestro.backends.coral.workspace import candidate_tree_hash
                if candidate_tree_hash(reference['candidate_path'])!=reference['candidate_hash']:
                    raise IntegrityError('Private memory checkpoint source changed')
        for row in db.execute('SELECT * FROM candidates'):
            candidate=json.loads(row['payload'])
            if candidate.get('candidate_path'):
                from maestro.backends.coral.workspace import candidate_tree_hash
                if candidate_tree_hash(candidate['candidate_path'])!=row['hash']:
                    raise IntegrityError('Frozen candidate source changed')
        for row in db.execute('SELECT * FROM warm_checkpoints'):
            payload=json.loads(row['payload'])
            if stable_hash(payload)!=row['content_hash'] or artifacts.get_json(row['content_hash'])!=payload:
                raise IntegrityError('Warm checkpoint payload/CAS mismatch')
        parameters=(episode_id,) if episode_id else ()
        query='SELECT * FROM episodes'+(' WHERE id=?' if episode_id else '')
        episodes=db.execute(query,parameters).fetchall()
        if episode_id and not episodes:
            raise ValueError('Unknown episode')
        settlements=0
        for episode in episodes:
            config=json.loads(episode['config_json'])
            if stable_hash(config)!=episode['config_hash']:
                raise IntegrityError('Episode configuration tampered')
            e=episode['id']
            for row in db.execute('SELECT * FROM rounds WHERE episode_id=?',(e,)):
                snapshot=json.loads(row['snapshot_json'])
                if stable_hash(snapshot)!=row['snapshot_hash']:
                    raise IntegrityError('Frozen round snapshot changed')
                anchors=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='ROUND_OPENED'",(e,row['round_index'])).fetchall()
                if len(anchors)!=1 or json.loads(anchors[0][0])['snapshot_hash']!=row['snapshot_hash']:
                    raise IntegrityError('Round snapshot differs from its committed event anchor')
                try:
                    capacity=validate_round_resources(config,snapshot)['admission_capacity']
                except AuthorizationError as error:
                    raise IntegrityError(f'Frozen round resource replay mismatch: {error}') from error
                allocations=db.execute('SELECT * FROM final_allocations WHERE episode_id=? AND round_index=?',(e,row['round_index'])).fetchall()
                if sum(a['x'] for a in allocations)>capacity:
                    raise IntegrityError('Slot budget exceeded')
                if snapshot.get('purpose')=='MAIN':
                    priority=db.execute('SELECT payload FROM priority_orders WHERE episode_id=? AND round_index=?',(e,row['round_index'])).fetchone()
                    # A crash during planning can precede the frozen order.
                    if priority:
                        priority=json.loads(priority[0])
                        submissions=[]
                        for submission in db.execute('SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=?',(e,row['round_index'])):
                            data=json.loads(submission[0])
                            submissions.append(Submission(data['agent_id'],data['plan_version_id'],
                                Decimal(data['value']),Decimal(data['bid']),data.get('eligible',True)))
                        frozen={a:state_from_dict(s) for a,s in snapshot['states'].items()}
                        expected=rank(episode['policy'],submissions,frozen,row['round_index'],config['seed'],slots=config['slots'])
                        if priority.get('policy')!=episode['policy'] or priority.get('order')!=expected:
                            raise IntegrityError('Auction priority differs from frozen policy inputs')
                        invites=[json.loads(event[0])['agent_id'] for event in db.execute(
                            "SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='INVITED' ORDER BY id",(e,row['round_index']))]
                        if invites!=expected[:len(invites)]:
                            raise IntegrityError('Generation invitations violated frozen backfill priority')
            for s in db.execute('SELECT * FROM settlements WHERE episode_id=? ORDER BY round_index,agent_id',(e,)):
                data=json.loads(s['payload'])
                roundrow=db.execute('SELECT snapshot_json FROM rounds WHERE episode_id=? AND round_index=?',(e,s['round_index'])).fetchone()
                snapshot=json.loads(roundrow[0])
                frozen=snapshot.get('states',{}).get(s['agent_id'])
                if frozen and (data['parent_before']!=frozen['parent_score'] or data['champion_before']!=snapshot['board']['champion_score']):
                    raise IntegrityError('Settlement baseline differs from frozen round snapshot')
                earned=Decimal(0)
                if data['result_job_id']:
                    result=ledger.existing_result(data['result_job_id'])
                    if result is None:
                        raise IntegrityError('Settlement missing verified result')
                    earned=reward(result.score,data['parent_before'],data['champion_before'],
                        Decimal(config['reward_scale']),Decimal(config['catchup_fraction']),valid=result.valid)
                    stored=db.execute('SELECT payload_hash FROM verified_results WHERE job_id=?',(result.job_id,)).fetchone()[0]
                    if stored!=stable_hash(result):
                        raise IntegrityError('Verified result payload tampered')
                    result_anchors=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='RESULT_RECORDED'",(e,s['round_index'])).fetchall()
                    hashes={json.loads(a[0])['job_id']:json.loads(a[0])['result_hash'] for a in result_anchors}
                    if hashes.get(result.job_id)!=stored:
                        raise IntegrityError('Result differs from its committed event anchor')
                if money(earned)!=money(s['reward']):
                    raise IntegrityError('Reward replay mismatch')
                expected=money(Decimal(s['wealth_before'])-Decimal(s['payment'])+earned)
                if expected!=money(s['wealth_after']):
                    raise IntegrityError('Wealth replay mismatch')
                allocation=db.execute('SELECT * FROM final_allocations WHERE episode_id=? AND round_index=? AND agent_id=?',(e,s['round_index'],s['agent_id'])).fetchone()
                if not allocation or Decimal(allocation['payment'])!=Decimal(s['payment']):
                    raise IntegrityError('Allocation payment replay mismatch')
                if episode['policy']!='first_price' and Decimal(s['payment'])!=0:
                    raise IntegrityError('A no-payment policy charged money')
                if episode['policy']=='first_price' and allocation['x']:
                    submission=db.execute('SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',(e,s['round_index'],s['agent_id'])).fetchone()
                    if not submission or Decimal(json.loads(submission[0])['bid'])!=Decimal(s['payment']):
                        raise IntegrityError('First price did not charge own bid')
                settlements+=1
            for checkpoint in db.execute('SELECT * FROM checkpoints WHERE episode_id=?',(e,)):
                anchors=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='ROUND_SETTLED'",(e,checkpoint['round_index'])).fetchall()
                if len(anchors)!=1 or json.loads(anchors[0][0])['checkpoint_hash']!=checkpoint['content_hash']:
                    raise IntegrityError('Checkpoint differs from its settlement event anchor')
            for t in db.execute('SELECT * FROM eval_tickets WHERE episode_id=?',(e,)):
                anchors=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='ADMITTED'",(e,t['round_index'])).fetchall()
                admissions={json.loads(a[0])['ticket_id']:json.loads(a[0]) for a in anchors}
                admitted=admissions.get(t['id'])
                persisted=json.loads(t['payload'])
                if not admitted or any(admitted.get(k)!=v for k,v in persisted.items()):
                    raise IntegrityError('Ticket differs from its admission event anchor')
                roundrow=db.execute('SELECT snapshot_json FROM rounds WHERE episode_id=? AND round_index=?',(e,t['round_index'])).fetchone()
                if roundrow is None:
                    raise IntegrityError('Ticket has no frozen execution round')
                snapshot=json.loads(roundrow[0])
                try:
                    validate_round_resources(config,snapshot,persisted)
                except AuthorizationError as error:
                    raise IntegrityError(f'Ticket resource replay mismatch: {error}') from error
                debit=db.execute('SELECT amount FROM wallet_entries WHERE operation_key=?',(t['job_id']+':payment',)).fetchone()
                if not debit or Decimal(debit[0])!=-Decimal(admitted['payment']):
                    raise IntegrityError('Admission debit differs from committed own-bid payment')
            for agent in db.execute('SELECT * FROM agents WHERE episode_id=?',(e,)):
                state=json.loads(agent['state_json'])
                if money(state['wealth'])!=money(ledger.balance(e,agent['agent_id'])):
                    raise IntegrityError('Final balance disagrees with agent state')
                if state['ucb_n']!=len(state['history']):
                    raise IntegrityError('UCB history count mismatch')
                total=sum((Decimal(record['credit']) for record in state['history']),Decimal(0))
                if money(state['ucb_total'])!=money(total):
                    raise IntegrityError('UCB credit total mismatch')
                latest=db.execute('SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',(e,)).fetchone()
                if latest and canonical_json(state)!=canonical_json(json.loads(latest[0])['states'][agent['agent_id']]):
                    raise IntegrityError('Materialized agent state differs from settled checkpoint')
        foreign=db.execute('PRAGMA foreign_key_check').fetchall()
        if foreign:
            raise IntegrityError('Foreign key check failed')
        return {'verified':True,'episodes':len(episodes),'settlements':settlements,'events':events,'artifacts':artifact_count,'physical_execution_replayed':False}
