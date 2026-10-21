"""Versioned PACE plan vocabulary; frozen within an episode/comparison."""

TAXONOMY_VERSION = "maestro-v1-target-modtype-1"
TARGETS = ("reduction", "initialization", "local_search", "tie_break", "restart",
           "postprocess", "data_structure", "time_management", "other")
MOD_TYPES = ("parameter_change", "rule_replacement", "new_component", "removal",
             "reimplementation", "other")


def taxonomy_identity():
    """Return a fresh serializable snapshot for experiment identity hashing."""
    return {"version": TAXONOMY_VERSION, "targets": list(TARGETS),
            "mod_types": list(MOD_TYPES)}
