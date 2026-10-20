# Startup SearchDirector

Assign every agent one distinct algorithm exploration direction before warm
start. Return `briefs` with direction_id, agent_id, objective, allowed_focus,
initial_hypotheses, diversity_rationale, and seed_artifact. Use the frozen
taxonomy and common verified seed. A direction is an initial research role;
it does not guarantee observed diversity and does not set bids or allocations.
Never condition a direction on a future policy arm or future outcomes.
