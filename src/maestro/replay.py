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
    with Ledger(root/'authority'/'ledger.sqlite',read_only=True) as ledger:
        db=ledger.connection
        events=ledger.verify_event_chain()
        artifacts=ArtifactStore(root/'authority'/'cas'/'sha256',read_only=True)
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
            from maestro.market.round_extension import latest_config
            effective_config,extensions=latest_config(db,episode)
            prior_rounds=config['rounds']
            expected_extension_keys=set()
            for extension in extensions:
                change=json.loads(extension['payload'])
                if change['previous_rounds']!=prior_rounds or change['checkpoint_round_index']!=prior_rounds-1:
                    raise IntegrityError('Market extension boundary mismatch')
                settled=db.execute("SELECT COUNT(*) FROM rounds WHERE episode_id=? AND round_index<? AND status='SETTLED'",
                                   (e,prior_rounds)).fetchone()[0]
                checkpoint=db.execute('SELECT content_hash FROM checkpoints WHERE episode_id=? AND round_index=?',
                                      (e,prior_rounds-1)).fetchone()
                publication=db.execute('SELECT status FROM publication_outbox WHERE episode_id=? AND round_index=?',
                                       (e,prior_rounds-1)).fetchone()
                opened_later=db.execute("SELECT 1 FROM events WHERE episode_id=? AND kind='ROUND_OPENED' "
                                        "AND round_index>=? AND id<? LIMIT 1",(e,prior_rounds,extension['id'])).fetchone()
                settled_event=db.execute("SELECT id FROM events WHERE episode_id=? AND round_index=? "
                                         "AND kind='ROUND_SETTLED'",(e,prior_rounds-1)).fetchone()
                if (settled!=prior_rounds or not checkpoint or not publication or publication['status']!='DONE'
                        or opened_later or not settled_event or settled_event['id']>=extension['id']):
                    raise IntegrityError('Market extension did not follow a completed published prefix')
                amount=money(change['funding_per_agent'])
                agents=[row[0] for row in db.execute('SELECT agent_id FROM agents WHERE episode_id=? ORDER BY agent_id',(e,))]
                if change['agent_ids']!=agents:
                    raise IntegrityError('Market extension agent set mismatch')
                for agent_id in agents:
                    key=f'{e}:round-extension:{change["rounds"]}:{agent_id}'
                    if amount:
                        expected_extension_keys.add(key)
                    row=db.execute('SELECT kind,amount FROM wallet_entries WHERE operation_key=?',(key,)).fetchone()
                    if amount and (not row or row['kind']!='ROUND_EXTENSION' or money(row['amount'])!=amount):
                        raise IntegrityError('Market extension funding mismatch')
                    if not amount and row:
                        raise IntegrityError('Unexpected Market extension funding')
                prior_rounds=change['rounds']
            actual_extension_keys={item[0] for item in db.execute(
                "SELECT operation_key FROM wallet_entries WHERE episode_id=? AND kind='ROUND_EXTENSION'",(e,))}
            if actual_extension_keys!=expected_extension_keys:
                raise IntegrityError('Unanchored Market extension funding')
            latest=db.execute('SELECT MAX(round_index) FROM rounds WHERE episode_id=?',(e,)).fetchone()[0]
            if episode['policy']!='warm' and latest is not None and latest>=effective_config.rounds:
                raise IntegrityError('Market ran beyond its declared round horizon')
            if episode['policy']!='warm' and episode['status']=='COMPLETED':
                completed=db.execute("SELECT COUNT(*) FROM rounds WHERE episode_id=? AND status='SETTLED'",(e,)).fetchone()[0]
                if completed!=effective_config.rounds:
                    raise IntegrityError('Completed Market round count differs from declared horizon')
            regime_events=db.execute("SELECT payload FROM events WHERE episode_id=? AND kind='REGIME_CONTRACT_FROZEN'",(e,)).fetchall()
            if regime_events:
                if len(regime_events)!=1:raise IntegrityError('Duplicate regime contract')
                frozen=json.loads(regime_events[0][0]);body=frozen['contract']
                from maestro.market.regimes import describe
                try:
                    identity=describe(episode['policy'],registry_revision=body.get('registry_revision'))
                except ValueError as error:
                    raise IntegrityError('Unsupported frozen regime registry revision') from error
                if (stable_hash(body)!=frozen['content_hash'] or frozen['artifact_hash']!=frozen['content_hash']
                        or body['config_hash']!=episode['config_hash']
                        or body['regime']['id']!=identity['id'] or body['regime']['number']!=identity['number']
                        or body['regime']['policy']!=episode['policy']
                        or artifacts.get_json(frozen['artifact_hash'])!=body):
                    raise IntegrityError('Frozen regime contract mismatch')
            for row in db.execute('SELECT * FROM rounds WHERE episode_id=?',(e,)):
                snapshot=json.loads(row['snapshot_json'])
                if stable_hash(snapshot)!=row['snapshot_hash']:
                    raise IntegrityError('Frozen round snapshot changed')
                anchors=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='ROUND_OPENED'",(e,row['round_index'])).fetchall()
                if len(anchors)!=1 or json.loads(anchors[0][0])['snapshot_hash']!=row['snapshot_hash']:
                    raise IntegrityError('Round snapshot differs from its committed event anchor')
                try:
                    capacity=validate_round_resources(config,snapshot)['admission_capacity']
                    if snapshot.get('candidate_pipeline')=='prebid_v2':
                        if config.get('coral',{}).get('candidate_pipeline')!='prebid_v2':
                            raise AuthorizationError('Pipeline differs from frozen configuration')
                        from maestro.execution.proposals import validate_prebid_round
                        validate_prebid_round(db,e,row['round_index'])
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
                        if config.get('valuation_discount_factor') is not None:
                            from maestro.agents.valuation import estimate_value
                            for submission in submissions:
                                state=frozen[submission.agent_id]
                                if state.history:
                                    value,samples=estimate_value(state.history,gamma=config['valuation_discount_factor'])
                                elif config.get('coral',{}).get('deepseek_transport_retry',{}).get('warm_seed_fallback',False):
                                    value,samples=Decimal(0),[]
                                else:
                                    raise IntegrityError('Discounted valuation missing formal history')
                                payload=json.loads(db.execute('SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',
                                    (e,row['round_index'],submission.agent_id)).fetchone()[0])
                                if episode['policy'] in ('adjusted_valuation','agent_adjusted'):
                                    from maestro.agents.valuation_reasoning import expected_credit,validate_read_evidence
                                    valuation_events=db.execute("SELECT payload FROM events WHERE episode_id=? AND round_index=? AND kind='VALUATION_RESULT'",
                                        (e,row['round_index'])).fetchall()
                                    matches=[json.loads(v[0]) for v in valuation_events if json.loads(v[0])['agent_id']==submission.agent_id]
                                    if len(matches)!=1:raise IntegrityError('Missing or duplicate R4 assessment')
                                    event=matches[0];record=artifacts.get_json(event['artifact_hash'])
                                    if event['status']!=record['status'] or record['agent_id']!=submission.agent_id:
                                        raise IntegrityError('R4 assessment event differs from artifact')
                                    usage=record.get('usage',{})
                                    cap=config.get('valuation_calls',4)+int(record.get('repair_attempted',False))
                                    if (record.get('repair_attempted') and usage.get('calls',0)!=cap):
                                        raise IntegrityError('R4 conditional regeneration count differs from frozen cap')
                                    if (usage.get('unresolved_requests',0) or usage.get('calls',0)>cap
                                            or usage.get('calls',0) and usage.get('usage_complete') is not True):
                                        raise IntegrityError('R4 assessment has unresolved or excessive requests')
                                    if (record['proposal_hash']!=payload.get('proposal_hash') or event['proposal_hash']!=record['proposal_hash']
                                            or record['plan_version_id']!=submission.plan_version_id or Decimal(record['baseline'])!=value
                                            or record['sample_ids']!=samples):raise IntegrityError('Assessment binding differs from frozen inputs')
                                    if record['status']=='ACCEPTED':
                                        context=artifacts.get_json(record['context_hash'])
                                        if context['manifest']!={k:stable_hash(v) for k,v in context['files'].items()}:raise IntegrityError('Valuation context manifest mismatch')
                                        validate_read_evidence(record['read_trace'],context['files'],record['assessment'])
                                        if not 1<=record['usage'].get('calls',0)<=cap or record['usage'].get('usage_complete') is not True:
                                            raise IntegrityError('R4 request accounting incomplete or excessive')
                                        value=expected_credit(record['assessment'],state.parent_score,snapshot['board']['champion_score'],
                                            scale=Decimal(str(config['reward_scale'])),catchup=Decimal(str(config['catchup_fraction'])),paths=set(context['files']))
                                        if record['returned_monotonic']>=record['deadline_monotonic']:raise IntegrityError('Late assessment admitted')
                                    elif record['status']!='FALLBACK':raise IntegrityError('Unresolved assessment used in allocation')
                                    if Decimal(record['value'])!=value or Decimal(event['value'])!=value:raise IntegrityError('R4 expected credit mismatch')
                                if (submission.value!=value or submission.bid!=money(min(value,state.wealth) if episode['policy'] in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') else value)
                                        or payload.get('sample_ids')!=samples):
                                    raise IntegrityError('Submission differs from frozen discounted actual-credit valuation')
                        expected=rank(episode['policy'],submissions,frozen,row['round_index'],config['seed'],
                            slots=config['slots'],ucb_exploration_coefficient=config.get('ucb_exploration_coefficient',1.0),
                            valuation_discount_factor=config.get('valuation_discount_factor'))
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
                if episode['policy']=='warm' and 'wealth' not in snapshot.get('states',{}).get(s['agent_id'],{}):
                    if (Decimal(s['payment'])!=0 or s['wealth_before']!='NOT_APPLICABLE'
                            or s['wealth_after']!='NOT_APPLICABLE'):
                        raise IntegrityError('Warm Start cannot settle wallet funds')
                else:
                    expected=money(Decimal(s['wealth_before'])-Decimal(s['payment'])+earned)
                    if expected!=money(s['wealth_after']):
                        raise IntegrityError('Wealth replay mismatch')
                allocation=db.execute('SELECT * FROM final_allocations WHERE episode_id=? AND round_index=? AND agent_id=?',(e,s['round_index'],s['agent_id'])).fetchone()
                if not allocation or Decimal(allocation['payment'])!=Decimal(s['payment']):
                    raise IntegrityError('Allocation payment replay mismatch')
                if episode['policy'] not in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') and Decimal(s['payment'])!=0:
                    raise IntegrityError('A no-payment policy charged money')
                if episode['policy'] in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') and allocation['x']:
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
                    if snapshot.get('candidate_pipeline')=='prebid_v2' and persisted['purpose'] in ('WARM','MAIN'):
                        from maestro.execution.proposals import validate_proposal_binding
                        submission=db.execute('SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?',
                            (e,t['round_index'],t['agent_id'])).fetchone()
                        if not submission:raise AuthorizationError('Missing proposal-bound bid')
                        validate_proposal_binding(db,persisted,json.loads(submission[0]))
                except AuthorizationError as error:
                    raise IntegrityError(f'Ticket resource replay mismatch: {error}') from error
                debit=db.execute('SELECT amount FROM wallet_entries WHERE operation_key=?',(t['job_id']+':payment',)).fetchone()
                if persisted['purpose']=='MAIN':
                    if not debit or Decimal(debit[0])!=-Decimal(admitted['payment']):
                        raise IntegrityError('Admission debit differs from committed own-bid payment')
                elif debit and not db.execute("SELECT 1 FROM wallet_entries WHERE episode_id=? AND agent_id=? AND kind='INITIAL'",
                        (e,t['agent_id'])).fetchone():
                    raise IntegrityError('Warm admission must not debit a wallet')
            for agent in db.execute('SELECT * FROM agents WHERE episode_id=?',(e,)):
                state=json.loads(agent['state_json'])
                if episode['policy']=='warm' and 'wealth' not in state:
                    if db.execute('SELECT 1 FROM wallet_entries WHERE episode_id=? AND agent_id=?',
                            (e,agent['agent_id'])).fetchone():
                        raise IntegrityError('Warm Start must not have a wallet')
                elif money(state['wealth'])!=money(ledger.balance(e,agent['agent_id'])):
                    raise IntegrityError('Final balance disagrees with agent state')
                if state['ucb_n']!=len(state['history']):
                    raise IntegrityError('UCB history count mismatch')
                total=sum((Decimal(record['credit']) for record in state['history']),Decimal(0))
                if money(state['ucb_total'])!=money(total):
                    raise IntegrityError('UCB credit total mismatch')
                latest=db.execute('SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',(e,)).fetchone()
                checkpoint_state=json.loads(latest[0])['states'][agent['agent_id']] if latest else None
                if checkpoint_state is not None:
                    checkpoint_index=db.execute('SELECT MAX(round_index) FROM checkpoints WHERE episode_id=?',(e,)).fetchone()[0]
                    for extension in extensions:
                        change=json.loads(extension['payload'])
                        if change['checkpoint_round_index']==checkpoint_index:
                            checkpoint_state['wealth']=str(money(Decimal(checkpoint_state['wealth'])+
                                                                 Decimal(change['funding_per_agent'])))
                if latest and canonical_json(state)!=canonical_json(checkpoint_state):
                    raise IntegrityError('Materialized agent state differs from settled checkpoint')
        foreign=db.execute('PRAGMA foreign_key_check').fetchall()
        if foreign:
            raise IntegrityError('Foreign key check failed')
        return {'verified':True,'episodes':len(episodes),'settlements':settlements,'events':events,'artifacts':artifact_count,'physical_execution_replayed':False}
