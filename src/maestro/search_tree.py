"""Read-only search-tree extraction and artifact generation from settled ledgers."""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

REGIMES = ('first_price_naive', 'first_price', 'ucb', 'uniform', 'adjusted_valuation', 'agent_adjusted')


def _component(value):
    if not value or value in ('.', '..') or any(c in value for c in '/\\\x00'):
        raise ValueError('Run selectors must each be one nonempty path component')
    return value


def resolve_root(*, root=None, runs_dir='artifacts/runs', benchmark=None,
                 model=None, regime=None, run=None):
    """Resolve an exact nested run or its historical flat name, never a fuzzy match."""
    if regime not in REGIMES:
        raise ValueError('Select first_price_naive, ucb, uniform, or adjusted_valuation')
    if root is not None:
        if run is not None:
            raise ValueError('Use either --root or --run/--timestamp/--name')
        chosen = Path(root).resolve()
    else:
        if not all((benchmark, model, run)):
            raise ValueError('Provide --benchmark, --model and --run, or an explicit --root')
        benchmark, model, run = map(_component, (benchmark, model, run))
        base = Path(runs_dir).resolve()
        options = [base/benchmark/model/regime/run, base/f'{benchmark}_{model}_{run}']
        found = [p for p in options if (p/'authority/ledger.sqlite').is_file()]
        if len(found) > 1:
            raise ValueError('Both nested and legacy roots match; choose one with --root')
        if not found:
            raise ValueError('No experiment ledger found at '+str(options[0])+' or '+str(options[1]))
        chosen = found[0]
    if not (chosen/'authority/ledger.sqlite').is_file():
        raise ValueError('Missing experiment ledger: '+str(chosen/'authority/ledger.sqlite'))
    return chosen


def _require(condition, message):
    if not condition:
        raise ValueError('Search-tree evidence mismatch: '+message)


def _one(db, query, params, label):
    row = db.execute(query, params).fetchone()
    if row is None:
        raise ValueError('Missing search-tree evidence: '+label)
    return row


def load_search_tree(root, regime, *, episode=None, benchmark=None, model=None):
    """Use a read transaction; never create a Ledger or an execution backend."""
    root = Path(root).resolve()
    path = root/'authority/ledger.sqlite'
    if not path.is_file():
        raise ValueError('Missing experiment ledger: '+str(path))
    if regime not in REGIMES:
        raise ValueError('Unknown regime')
    db = sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        matches = db.execute('SELECT * FROM episodes WHERE policy=?'+(' AND id=?' if episode else ''),
                             (regime, episode) if episode else (regime,)).fetchall()
        if len(matches) != 1:
            raise ValueError(f'Expected one {regime} episode; found {len(matches)}. Use --episode to disambiguate.')
        market = matches[0]
        config = json.loads(market['config_json'])
        from maestro.market.round_extension import recorded_horizon
        if 'config_hash' in market.keys():
            effective_rounds, extension_events = recorded_horizon(db,market)
        else:
            # Minimal read-only visualization fixtures predate the authority
            # configuration hash and cannot contain round-extension events.
            extension_events=[]
            effective_rounds=config.get('rounds')
        extension_funding={}
        for extension in extension_events:
            change=json.loads(extension['payload'])
            extension_funding[change['previous_rounds']]=Decimal(change['funding_per_agent'])
        actual_model = config.get('coral', {}).get('model')
        actual_benchmark = market['benchmark']
        if benchmark:
            expected = 'pace' if benchmark == 'pace-ds' else benchmark
            _require(actual_benchmark == expected, 'benchmark selector differs from frozen episode')
        if model:
            _require(actual_model == model, 'model selector differs from frozen episode')
        stages = []
        if market['bootstrap_id']:
            warm = _one(db, 'SELECT * FROM episodes WHERE id=?', (market['bootstrap_id'],), 'linked Warm episode')
            _require(warm['policy'] == 'warm', 'bootstrap_id is not a Warm episode')
            _require(warm['status'] == 'COMPLETED', 'linked Warm episode has not completed')
            stages.append(('warm', warm))
        stages.append(('market', market))
        rows, flat, initial, missing, episodes = [], [], {}, {}, {}
        agents = None
        for stage, ep in stages:
            e = ep['id']; episodes[stage] = e
            rounds = db.execute('SELECT * FROM rounds WHERE episode_id=? AND round_index>=0 ORDER BY round_index', (e,)).fetchall()
            settled = [r for r in rounds if r['status'] == 'SETTLED']
            missing[stage] = [r['round_index']+1 for r in rounds if r['status'] != 'SETTLED']
            if not settled:
                raise ValueError('No settled rounds for '+stage+'; no scores will be invented')
            _require([r['round_index'] for r in settled] == list(range(len(settled))), 'settled rounds are not a contiguous prefix')
            previous = None
            for r in settled:
                idx = r['round_index']; before = json.loads(r['snapshot_json'])
                cp = _one(db, 'SELECT payload FROM checkpoints WHERE episode_id=? AND round_index=?', (e, idx), 'settled checkpoint')
                after = json.loads(cp[0])
                if agents is None:
                    agents = sorted(before['states'], key=lambda a: (int(a.split('-')[-1]) if a.split('-')[-1].isdigit() else 0, a))
                _require(set(agents) == set(before['states']) == set(after['states']), 'agent population changed')
                if idx == 0:
                    initial[stage] = before
                if previous:
                    for a in agents:
                        _require(before['states'][a]['parent_hash'] == previous['states'][a]['parent_hash'], 'parent continuity')
                        if stage=='market' or 'wealth' in before['states'][a]:
                            topup=extension_funding.get(idx,Decimal(0)) if stage=='market' else Decimal(0)
                            _require(Decimal(before['states'][a]['wealth']) ==
                                     Decimal(previous['states'][a]['wealth'])+topup, 'wealth continuity')
                order = json.loads(_one(db, 'SELECT payload FROM priority_orders WHERE episode_id=? AND round_index=?', (e,idx), 'priority order')[0])['order']
                detail = {'stage': stage, 'episode': e, 'round': idx+1, 'ledger_round_index': idx,
                          'champion_hash': after['board']['champion_hash'], 'champion_score': after['board']['champion_score'], 'agents': []}
                for a in agents:
                    params = (e,idx,a)
                    submission = db.execute('SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=? AND agent_id=?', params).fetchone()
                    sub = json.loads(submission[0]) if submission else {}
                    st = _one(db, 'SELECT * FROM settlements WHERE episode_id=? AND round_index=? AND agent_id=?', params, 'settlement')
                    sp = json.loads(st['payload'])
                    allocation = _one(db, 'SELECT * FROM final_allocations WHERE episode_id=? AND round_index=? AND agent_id=?', params, 'allocation')
                    funded = bool(allocation['x']); result = None
                    if funded:
                        job = _one(db, 'SELECT job_id,candidate_hash FROM eval_tickets WHERE id=?', (allocation['ticket_id'],), 'admitted ticket')
                        _require(job['job_id'] == sp['result_job_id'], 'ticket and settlement job differ')
                        result = json.loads(_one(db, 'SELECT payload FROM verified_results WHERE job_id=?', (job['job_id'],), 'verified result')[0])
                        _require(result['candidate_hash'] == job['candidate_hash'], 'ticket and result candidate differ')
                    else:
                        _require(sp.get('result_job_id') is None, 'nonadmitted formal result')
                    p0,p1 = before['states'][a],after['states'][a]
                    payment, reward = Decimal(allocation['payment']), Decimal(st['reward'])
                    _require(Decimal(st['payment']) == payment, 'settlement payment differs from allocation')
                    if stage=='market' or 'wealth' in p0:
                        _require(Decimal(st['wealth_before']) == Decimal(p0['wealth']) and Decimal(st['wealth_after']) == Decimal(p1['wealth']), 'snapshot and settlement wealth differ')
                        _require(Decimal(p1['wealth']) == Decimal(p0['wealth'])-payment+reward, 'wallet equation')
                    else:
                        _require(payment==0 and 'wealth' not in p0 and 'wealth' not in p1,
                                 'Warm Start has no wallet')
                    if funded:
                        expected_payment = Decimal(sub['bid']) if stage=='market' and regime in ('first_price_naive','first_price','adjusted_valuation','agent_adjusted') else Decimal(0)
                        _require(payment == expected_payment, 'payment does not match regime')
                    else:
                        _require(payment == reward == 0, 'nonadmitted agent has payment or reward')
                    accepted = p0['parent_hash'] != p1['parent_hash']
                    improved = bool(result and result['valid'] and result['score']>p0['parent_score'])
                    _require(accepted == improved, 'parent adoption')
                    _require(p1['parent_score'] == (result['score'] if improved else p0['parent_score']), 'best-parent score')
                    if accepted:
                        _require(p1['parent_hash'] == result['candidate_hash'], 'adopted candidate hash')
                    inv = db.execute('SELECT status FROM invitations WHERE episode_id=? AND round_index=? AND agent_id=?', params).fetchone()
                    node = {'agent': a, 's_before':p0['parent_score'], 's_after':p1['parent_score'],
                            'parent_before':p0['parent_hash'], 'parent_after':p1['parent_hash'],
                            'valuation':sub.get('value'), 'bid':sub.get('bid'),
                            'wealth_before':p0.get('wealth'), 'wealth_after':p1.get('wealth'),
                            'reward':st['reward'] if funded else None, 'payment':allocation['payment'],
                            'funded':funded, 'accepted':accepted, 'champion':p1['parent_hash']==detail['champion_hash'],
                            'candidate_hash':result['candidate_hash'] if result else None,
                            **({'proposal_hash':sub['proposal_hash'],
                                'proposal_status':'FORMALLY_EVALUATED' if funded else 'NO_FORMAL_EVALUATION'}
                               if 'proposal_hash' in sub else {}),
                            'candidate_score':result['score'] if result else None,
                            'candidate_valid':result['valid'] if result else None,
                            'candidate_status':result['status'] if result else None,
                            'priority_rank':order.index(a)+1 if a in order else None,
                            'invitation_status':inv[0] if inv else 'NOT_INVITED'}
                    detail['agents'].append(node)
                    flat.append({'stage':stage, 'round':idx+1, 'ledger_round_index':idx, **node})
                holders = [n for n in detail['agents'] if n['champion']]
                _require(bool(holders), 'champion is not a retained parent')
                _require(all(n['s_after']==detail['champion_score'] for n in holders), 'champion score')
                rows.append(detail); previous = after
        boundary = None
        if 'warm' in initial:
            last_warm = next(r for r in reversed(rows) if r['stage']=='warm')
            _require(not missing['warm'], 'linked Warm episode is unsettled')
            for n in last_warm['agents']:
                _require(initial['market']['states'][n['agent']]['parent_hash']==n['parent_after'], 'Warm-to-Market parent continuity')
            ws = [initial['market']['states'][a]['wealth'] for a in agents]
            legacy_warm_wallet=all(n['wealth_after'] is not None for n in last_warm['agents'])
            boundary = {'market_initial_wealth':dict(zip(agents,ws)),
                        'warm_has_wallet':legacy_warm_wallet}
            if legacy_warm_wallet:
                boundary['warm_final_wealth']={n['agent']:n['wealth_after'] for n in last_warm['agents']}
        directions = {}
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'warm_checkpoints' in tables and market['bootstrap_id']:
            wc = db.execute('SELECT payload FROM warm_checkpoints WHERE id=?',(market['bootstrap_id'],)).fetchone()
            if wc:
                directions = {b['agent_id']:b.get('direction_id','').removeprefix('direction-') for b in json.loads(wc[0]).get('direction_briefs',[])}
        complete = market['status']=='COMPLETED' and not missing['market']
        if complete and effective_rounds is not None:
            _require(sum(r['stage']=='market' for r in rows)==effective_rounds,
                     'completed Market is missing configured rounds')
        meta = {'root':str(root), 'source_ledger':str(path), 'episodes':episodes, 'regime':regime,
                'ucb_exploration_coefficient':config.get('ucb_exploration_coefficient',1.0),
                'benchmark':benchmark or actual_benchmark, 'model':actual_model, 'seed':config.get('seed'),
                'instance_count':config.get('evaluation_instance_count'), 'agents':agents, 'directions':directions,
                'status':'COMPLETED' if complete else 'PARTIAL_SETTLED_PREFIX',
                'unsettled_rounds_omitted':missing, 'boundary':boundary,
                'numbering':'Displayed round N = ledger round_index N-1; 0 is the initial state',
                'unknown_cost_note':'This score/wealth illustration does not reconcile provider UNKNOWN costs.'}
        return {'metadata':meta, 'initial':initial, 'rounds':rows, 'nodes':flat}
    except sqlite3.Error as error:
        raise ValueError('Cannot read search-tree ledger: '+str(error)) from error
    finally:
        db.rollback(); db.close()


def plot_search_tree(root, regime, *, episode=None, benchmark=None, model=None,
                     output=None, dpi=145, detail_rounds=None):
    if not 50<=dpi<=300:
        raise ValueError('--dpi must be between 50 and 300')
    data = load_search_tree(root,regime,episode=episode,benchmark=benchmark,model=model)
    # Import lazily: ordinary execution and data extraction need no plotting stack.
    try:
        from maestro.search_tree_plot import SearchTreePlot
    except ImportError as error:
        raise ValueError('Plotting dependencies are missing; install with: python -m pip install -e ".[plots]"') from error
    renderer = SearchTreePlot(data,dpi=dpi,detail_rounds=detail_rounds)
    out = Path(output).resolve() if output else Path(root).resolve()/'reports/search-tree'/regime
    if out == Path(root).resolve() or out in Path(root).resolve().parents:
        raise ValueError('Choose a dedicated figure output directory')
    for protected in ('authority','coral-cas','deepseek-native-authority'):
        p = Path(root).resolve()/protected
        if out==p or p in out.parents:
            raise ValueError('Figure outputs cannot overwrite experiment authority or immutable evidence')
    out.mkdir(parents=True,exist_ok=True)
    (out/'nodes.json').write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')
    with (out/'nodes.csv').open('w',newline='',encoding='utf-8') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(data['nodes'][0]));writer.writeheader();writer.writerows(data['nodes'])
    figures = renderer.render(out)
    digest = hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    audit = {'created_utc':datetime.now(timezone.utc).isoformat(),'source_ledger':data['metadata']['source_ledger'],
             'status':data['metadata']['status'],'source_evidence_sha256':digest,
             'rounds':len(data['rounds']),'nodes':len(data['nodes']),
             'formal_evaluations':{stage:sum(n['funded'] for n in data['nodes'] if n['stage']==stage) for stage in ('warm','market')},
             'new_model_requests':0,'new_candidate_evaluations':0,
             'checks':['snapshot/settlement wealth','wallet equation','regime payment','nonadmission has no reward or payment',
                       'adopted candidate identity and strict improvement','champion hash/score','program/wealth continuity'],
             'files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [*figures,out/'nodes.json',out/'nodes.csv']}}
    (out/'audit.json').write_text(json.dumps(audit,indent=2)+'\n',encoding='utf-8')
    return {'root':str(Path(root).resolve()),'regime':regime,'episode':data['metadata']['episodes']['market'],
            'status':audit['status'],'output':str(out),'png':str(out/'search-tree.png'),
            'detail_png':str(out/'search-tree-detail.png'),'audit':str(out/'audit.json'),
            'rounds':audit['rounds'],'nodes':audit['nodes'],'new_model_requests':0,'new_candidate_evaluations':0}
