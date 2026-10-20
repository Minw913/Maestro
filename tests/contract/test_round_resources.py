"""Frozen admission and CPU-worker contracts, without executing experiments."""

from collections import Counter
from copy import deepcopy

import pytest

from maestro.bootstrap.warm_start import balanced_schedule
from maestro.config import RunConfig
from maestro.storage.ledger import validate_round_resources
from maestro.types import AuthorizationError


def _binding(capacity, cpu_workers, instance_workers, batch_width, effective):
    return {"admission_capacity": capacity, "cpu_workers": cpu_workers,
            "instance_workers": instance_workers, "evaluation_batch_width": batch_width,
            "effective_instance_workers": effective}


def _snapshot(config, purpose, binding, *, invited=None):
    return {"purpose": purpose, "states": {f"a{i}": {} for i in range(config.n_agents)},
            "warm_invite": invited, **binding}


def _ticket(purpose, binding, *, agent="a0"):
    return {"purpose": purpose, "agent_id": agent, **binding}


def test_defaults_match_five_warm_opportunities_and_four_parallel_agents():
    config = RunConfig()
    assert config.warm_opportunities_per_agent == 5
    assert config.warm_concurrency == 4
    assert config.warm_batch_width == 4
    assert config.slots == 2
    assert config.cpu_workers == 4 and config.instance_workers == 1


def test_default_warm_schedule_is_five_unique_waves_with_equal_opportunities():
    config = RunConfig()
    agents = [f"a{i}" for i in range(config.n_agents)]
    waves = balanced_schedule(agents, config.warm_batch_width,
                              config.warm_opportunities_per_agent, config.seed)
    assert len(waves) == 5
    assert all(len(wave) == 4 and set(wave) == set(agents) for wave in waves)
    assert Counter(agent for wave in waves for agent in wave) == Counter({agent: 5 for agent in agents})
    assert waves == balanced_schedule(agents, 4, 5, config.seed)


def test_short_last_warm_wave_keeps_every_agent_exactly_once():
    config = RunConfig(n_agents=5)
    agents = [f"a{i}" for i in range(config.n_agents)]
    waves = balanced_schedule(agents, config.warm_batch_width, 1, config.seed)
    assert [len(wave) for wave in waves] == [4, 1]
    assert all(len(set(wave)) == len(wave) for wave in waves)
    assert Counter(agent for wave in waves for agent in wave) == Counter(agents)
    for wave in waves:
        binding = _binding(len(wave), 4, 1, len(wave), 1)
        snapshot = _snapshot(config, "WARM", binding, invited=wave)
        assert validate_round_resources(config.to_dict(), snapshot) == binding


@pytest.mark.parametrize(("cpu_workers", "instance_workers", "batch_width", "effective"), [
    (1, 4, 1, 1), (2, 4, 2, 1), (4, 4, 2, 2), (8, 4, 2, 4),
])
def test_main_admission_stays_two_slots_even_when_cpu_parallelism_is_smaller(
        cpu_workers, instance_workers, batch_width, effective):
    config = RunConfig(cpu_workers=cpu_workers, instance_workers=instance_workers)
    binding = _binding(2, cpu_workers, instance_workers, batch_width, effective)
    snapshot = _snapshot(config, "MAIN", binding)
    assert config.evaluation_batch_width(2) == batch_width
    assert validate_round_resources(config.to_dict(), snapshot,
                                    _ticket("MAIN", binding)) == binding


def test_one_cpu_queues_main_winners_without_changing_auction_capacity():
    config = RunConfig(cpu_workers=1, instance_workers=3)
    binding = _binding(2, 1, 3, 1, 1)
    snapshot = _snapshot(config, "MAIN", binding)
    assert validate_round_resources(config.to_dict(), snapshot)["admission_capacity"] == 2
    assert config.warm_batch_width == 1
    waves = balanced_schedule(["a0", "a1", "a2", "a3"], config.warm_batch_width, 5, 0)
    assert len(waves) == 20 and all(len(wave) == 1 for wave in waves)


@pytest.mark.parametrize(("invited", "batch_width", "effective"), [
    (["a0", "a1", "a2", "a3"], 4, 2),
    (["a2", "a3"], 2, 3),
    (["a3"], 1, 3),
])
def test_warm_capacity_uses_its_frozen_invited_wave_including_short_tail(
        invited, batch_width, effective):
    config = RunConfig(cpu_workers=8, instance_workers=3)
    binding = _binding(len(invited), 8, 3, batch_width, effective)
    snapshot = _snapshot(config, "WARM", binding, invited=invited)
    assert validate_round_resources(config.to_dict(), snapshot,
                                    _ticket("WARM", binding, agent=invited[0])) == binding


@pytest.mark.parametrize("purpose", ["SETUP", "HELDOUT"])
def test_setup_and_heldout_have_one_admission_and_can_use_instance_workers(purpose):
    config = RunConfig(cpu_workers=8, instance_workers=4)
    binding = _binding(1, 8, 4, 1, 4)
    assert validate_round_resources(config.to_dict(), _snapshot(config, purpose, binding),
                                    _ticket(purpose, binding)) == binding


def test_panel_length_caps_effective_instance_workers():
    config = RunConfig(cpu_workers=8, instance_workers=10, panel_size=2)
    binding = _binding(2, 8, 10, 2, 2)
    assert validate_round_resources(config.to_dict(), _snapshot(config, "MAIN", binding),
                                    _ticket("MAIN", binding)) == binding


def test_leftover_cpu_worker_is_usable_without_increasing_admission():
    config = RunConfig(cpu_workers=5, instance_workers=3)
    binding = _binding(4, 5, 3, 4, 2)
    invited = ["a0", "a1", "a2", "a3"]
    assert config.effective_instance_workers(4) == 2
    assert validate_round_resources(config.to_dict(),
        _snapshot(config, "WARM", binding, invited=invited),
        _ticket("WARM", binding)) == binding


@pytest.mark.parametrize("record", ["snapshot", "ticket"])
@pytest.mark.parametrize(("field", "replacement"), [
    ("admission_capacity", 3), ("cpu_workers", 8), ("instance_workers", 3),
    ("evaluation_batch_width", 1), ("effective_instance_workers", 1),
    ("admission_capacity", True), ("cpu_workers", True),
])
def test_frozen_resource_fields_cannot_be_enlarged_or_reinterpreted(record, field, replacement):
    config = RunConfig(instance_workers=2)
    binding = _binding(2, 4, 2, 2, 2)
    snapshot = _snapshot(config, "MAIN", binding)
    ticket = _ticket("MAIN", binding)
    (snapshot if record == "snapshot" else ticket)[field] = replacement
    with pytest.raises(AuthorizationError):
        validate_round_resources(config.to_dict(), snapshot, ticket)


@pytest.mark.parametrize("missing", ["cpu_workers", "instance_workers",
                                    "evaluation_batch_width", "effective_instance_workers"])
def test_new_snapshot_requires_every_resource_field(missing):
    config = RunConfig(instance_workers=2)
    binding = _binding(2, 4, 2, 2, 2)
    snapshot = _snapshot(config, "MAIN", binding)
    del snapshot[missing]
    with pytest.raises(AuthorizationError):
        validate_round_resources(config.to_dict(), snapshot)


@pytest.mark.parametrize("missing", ["admission_capacity", "cpu_workers", "instance_workers",
                                    "evaluation_batch_width", "effective_instance_workers"])
def test_new_ticket_requires_complete_frozen_resource_binding(missing):
    config = RunConfig(instance_workers=2)
    binding = _binding(2, 4, 2, 2, 2)
    ticket = _ticket("MAIN", binding)
    del ticket[missing]
    with pytest.raises(AuthorizationError):
        validate_round_resources(config.to_dict(), _snapshot(config, "MAIN", binding), ticket)


def test_ticket_purpose_must_match_frozen_round():
    config = RunConfig()
    binding = _binding(2, 4, 1, 2, 1)
    with pytest.raises(AuthorizationError, match="purpose"):
        validate_round_resources(config.to_dict(), _snapshot(config, "MAIN", binding),
                                 _ticket("WARM", binding))


def test_noninvited_agent_cannot_use_a_warm_ticket():
    config = RunConfig()
    binding = _binding(2, 4, 1, 2, 1)
    with pytest.raises(AuthorizationError, match="outside"):
        validate_round_resources(config.to_dict(),
            _snapshot(config, "WARM", binding, invited=["a0", "a1"]),
            _ticket("WARM", binding, agent="a3"))


@pytest.mark.parametrize("invited", [[], ["a0", "a0"], ["unknown"], "a0", [None]])
def test_warm_invited_population_must_be_unique_known_and_nonempty(invited):
    config = RunConfig()
    binding = _binding(1, 4, 1, 1, 1)
    with pytest.raises(AuthorizationError):
        validate_round_resources(config.to_dict(),
                                 _snapshot(config, "WARM", binding, invited=invited))


@pytest.mark.parametrize("config_changes", [
    {"cpu_workers": 3}, {"warm_concurrency": 3}, {"n_agents": 3},
])
def test_warm_wave_cannot_exceed_frozen_cpu_population_or_concurrency(config_changes):
    config = RunConfig().to_dict()
    config.update(config_changes)
    binding = _binding(4, config["cpu_workers"], 1, min(4, config["cpu_workers"]), 1)
    snapshot = {"purpose": "WARM", "states": {f"a{i}": {} for i in range(4)},
                "warm_invite": ["a0", "a1", "a2", "a3"], **binding}
    with pytest.raises(AuthorizationError, match="concurrency"):
        validate_round_resources(config, snapshot)


@pytest.mark.parametrize("purpose", ["MAIN", "WARM", "SETUP", "HELDOUT"])
def test_legacy_snapshots_keep_original_auction_slot_limit(purpose):
    config = RunConfig(cpu_workers=1).to_dict()
    # Removing only the new capacity binding identifies a historical snapshot.
    # Its limit remains the original episode slots even for a warm purpose.
    snapshot = {"purpose": purpose, "warm_invite": ["a0", "a1", "a2", "a3"]}
    ticket = {"purpose": purpose, "admission_capacity": 999}
    assert validate_round_resources(config, snapshot, ticket) == {"admission_capacity": 2}


@pytest.mark.parametrize("field", ["cpu_workers", "instance_workers", "panel_size", "slots"])
def test_new_snapshot_rejects_invalid_frozen_config_instead_of_coercing_booleans(field):
    config = RunConfig(instance_workers=2).to_dict()
    config[field] = True
    binding = _binding(2, 4, 2, 2, 2)
    snapshot = {"purpose": "MAIN", **binding}
    with pytest.raises(AuthorizationError):
        validate_round_resources(config, snapshot)


def test_validating_resource_binding_does_not_mutate_frozen_records():
    config = RunConfig(instance_workers=2)
    binding = _binding(2, 4, 2, 2, 2)
    snapshot = _snapshot(config, "MAIN", binding)
    ticket = _ticket("MAIN", binding)
    before = deepcopy((snapshot, ticket))
    assert validate_round_resources(config.to_dict(), snapshot, ticket) == binding
    assert (snapshot, ticket) == before
