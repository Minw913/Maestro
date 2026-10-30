"""Versioned Market regime IDs; settings come from frozen experiment configs."""
from maestro.types import stable_hash, to_jsonable

REGISTRY_REVISION = 5

# Revision 5 renames R3 and R4 without changing allocation or payment.
# Historical contracts retain their original policy strings and revision.
REGIMES = (
    {'number':1,'id':'R1','policy':'uniform','name':'Uniform',
     'allocation':'seeded balanced round-robin','payment':'zero'},
    {'number':2,'id':'R2','policy':'ucb','name':'Bandit-based allocation',
     'allocation':'reward estimate plus configured exploration bonus','payment':'zero'},
    {'number':3,'id':'R3','policy':'first_price_naive','name':'First-price naive',
     'allocation':'descending affordable bid','payment':'own bid on formal admission'},
    {'number':4,'id':'R4','policy':'adjusted_valuation','name':'Agent-adjusted valuation',
     'allocation':'descending affordable adjusted-value bids','payment':'own bid on formal admission'},
)


def _registry(revision):
    if type(revision) is not int:
        raise ValueError('Invalid regime registry revision')
    if revision == REGISTRY_REVISION:
        return REGIMES
    if revision in (1,2,3,4):
        historical_numbers={'first_price':1,'ucb':2,'uniform':3,'agent_adjusted':4}
        rows=tuple({**row,'policy':'first_price','name':'First-price'}
                   if row['policy']=='first_price_naive' else
                   {**row,'policy':'agent_adjusted'}
                   if row['policy']=='adjusted_valuation' else row for row in REGIMES)
        return tuple({**row,'number':historical_numbers[row['policy']] if revision<4 else row['number'],
                      'id':'R'+str(historical_numbers[row['policy']] if revision<4 else row['number'])}
                     for row in rows if revision>=2 or row['policy']!='agent_adjusted')
    raise ValueError('Unsupported regime registry revision')


def resolve_policy(value, *, registry_revision=REGISTRY_REVISION):
    if isinstance(value,bool):raise ValueError('Unknown Market regime')
    for row in _registry(registry_revision):
        if str(value) in (str(row['number']),row['id'],row['id'].lower(),row['policy']):
            return row['policy']
    raise ValueError('Unknown Market regime; use 1/R1/uniform, 2/R2/ucb, 3/R3/first_price_naive, or 4/R4/adjusted_valuation')


def describe(value, *, registry_revision=REGISTRY_REVISION):
    policy=resolve_policy(value,registry_revision=registry_revision)
    return dict(next(row for row in _registry(registry_revision) if row['policy']==policy))


def contract(value, config):
    """A prospective run contract, not evidence of execution or qualification."""
    row=describe(value)
    fields=('n_agents','slots','rounds','seed','initial_wealth','valuation_discount_factor',
            'ucb_exploration_coefficient','reward_scale','catchup_fraction','refresh_interval',
            'preparation_calls','preparation_window_seconds','planning_calls','planning_window_seconds',
            'valuation_calls','valuation_window_seconds','generation_calls','runtime_repair_calls','generation_window_seconds','generation_cpu_seconds',
            'cpu_workers','instance_workers','evaluation_instance_count','cpu_seconds_per_instance',
            'verification_cpu_seconds','memory_bytes','pids_max','output_bytes')
    body={'schema_version':1,'registry_revision':REGISTRY_REVISION,'regime':row,
          'config_hash':config.hash,'settings':{key:to_jsonable(getattr(config,key)) for key in fields},
          'execution_mode':config.execution_mode,'benchmark':config.benchmark,
          'candidate_pipeline':config.coral.get('candidate_pipeline','postbid_v1'),
          'valuation':('adjusted_valuation_expected_credit_with_discounted_baseline' if row['policy']=='adjusted_valuation' else 'discounted_actual_credit_per_own_settled_attempt'
                       if config.valuation_discount_factor is not None else 'legacy'),
          'wealth_limited_bidding':row['policy'] in ('first_price_naive','adjusted_valuation'),
          'wallet_funding':{'configured_credits':str(config.initial_wealth),
                            'effective_initial_wealth':str(config.effective_initial_wealth),
                            'mode':'sufficient' if config.initial_wealth==0 else 'finite'},
          'requires_prebid':row['policy']=='adjusted_valuation',
          'ucb_exploration_active':row['policy']=='ucb' and config.ucb_exploration_coefficient>0}
    return {'contract':body,'content_hash':stable_hash(body)}
