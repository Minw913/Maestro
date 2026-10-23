from dataclasses import replace

import pytest

from maestro.config import RunConfig
from maestro.tasks.taxonomy import TARGETS, MOD_TYPES, TAXONOMY_VERSION, taxonomy_identity


def test_single_vocabulary_source():
    from maestro.agents import planner, retrieval
    assert planner.TARGETS is TARGETS
    assert planner.MOD_TYPES is MOD_TYPES
    assert retrieval.TARGETS is TARGETS
    assert retrieval.TAXONOMY_VERSION == TAXONOMY_VERSION


def test_taxonomy_frozen_in_config_and_roundtrip():
    config=RunConfig()
    assert config.to_dict()['plan_taxonomy']==taxonomy_identity()
    assert RunConfig.from_dict(config.to_dict()).hash==config.hash
    changed=taxonomy_identity()
    changed['targets'].append('new_category')
    with pytest.raises(ValueError,match='taxonomy'):
        replace(config,plan_taxonomy=changed)
    identity=taxonomy_identity()
    identity['targets'].clear()
    assert RunConfig().plan_taxonomy==config.plan_taxonomy
