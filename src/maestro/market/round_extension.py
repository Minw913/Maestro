"""Append-only extension of a completed Market episode's round horizon."""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import json

from maestro.config import RunConfig
from maestro.market.credit import money
from maestro.types import IntegrityError, stable_hash


EVENT_KIND = 'MARKET_ROUNDS_EXTENDED'


def recorded_horizon(db, episode):
    """Read archives without interpreting retired settings as a new RunConfig."""
    from maestro.execution.pipeline import pipeline_version, RETIRED
    raw = json.loads(episode['config_json'])
    if stable_hash(raw) != episode['config_hash']:
        raise IntegrityError('Episode configuration tampered')
    if pipeline_version(raw) == RETIRED:
        if db.execute('SELECT 1 FROM events WHERE episode_id=? AND kind=?',
                      (episode['id'], EVENT_KIND)).fetchone():
            raise IntegrityError('Retired episodes cannot have Market round extensions')
        rounds = raw['rounds']
        if type(rounds) is not int or rounds < 1:
            raise IntegrityError('Invalid archived round horizon')
        return rounds, []
    config, events = latest_config(db, episode)
    return config.rounds, events


def _fixed_settings(config: RunConfig):
    value = deepcopy(config.to_dict())
    value.pop('rounds', None)
    value.get('stage_configs', {}).get('market', {}).pop('rounds', None)
    # Editing the Market stage file changes its source digest. The resolved
    # settings above must still differ only in the round horizon.
    value.get('config_sources', {}).get('market', {}).pop('sha256', None)
    return value


def validate_extension(previous: RunConfig, proposed: RunConfig):
    """Permit only a strictly larger Market horizon; retain all other rules."""
    if _fixed_settings(previous) != _fixed_settings(proposed):
        raise IntegrityError('Resume config hash mismatch: settings differ beyond Market rounds')
    if proposed.rounds <= previous.rounds:
        raise IntegrityError('Resume config hash mismatch: Market round extension must increase rounds')
    if proposed.experiment_stage != previous.experiment_stage:
        raise IntegrityError('Market stage changed during round extension')
    if previous.initial_wealth == 0:
        return money(proposed.effective_initial_wealth - previous.effective_initial_wealth)
    return money(0)


def latest_config(db, episode):
    """Read the latest declared horizon without changing the original contract."""
    original = RunConfig.from_dict(json.loads(episode['config_json']))
    if original.hash != episode['config_hash']:
        raise IntegrityError('Episode configuration tampered')
    rows = db.execute('SELECT id,round_index,payload FROM events WHERE episode_id=? AND kind=? ORDER BY id',
                      (episode['id'], EVENT_KIND)).fetchall()
    current = original
    for row in rows:
        event = json.loads(row['payload'])
        proposed = RunConfig.from_dict(event['config'])
        funding = validate_extension(current, proposed)
        if (event['previous_config_hash'] != current.hash or event['config_hash'] != proposed.hash
                or event['previous_rounds'] != current.rounds or event['rounds'] != proposed.rounds
                or money(event['funding_per_agent']) != funding
                or row['round_index'] != current.rounds - 1):
            raise IntegrityError('Market round extension event mismatch')
        current = proposed
    return current, rows


def extension_payload(previous, proposed, agent_ids):
    funding = validate_extension(previous, proposed)
    return {'previous_config_hash': previous.hash, 'config_hash': proposed.hash,
            'previous_rounds': previous.rounds, 'rounds': proposed.rounds,
            'config': proposed.to_dict(), 'funding_per_agent': str(funding),
            'agent_ids': sorted(agent_ids), 'checkpoint_round_index': previous.rounds - 1}
