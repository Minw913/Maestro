# Pilot experiment: three Market regimes

This directory publishes three completed 20-round Market episodes initialized
from the same frozen five-wave Warm checkpoint, `warm-5e4162c13d3149ff`.
The historical directory names `warm-start-comparison-six-runs-20261029` and
`old-warm/` are retained for stable links; the published experiment is called
**Pilot experiment**. The `new-warm/` group is no longer tracked or published on
the current GitHub branch. Its files remain on the server locally.

| Directory | Regime | Final training score |
|---|---|---:|
| `old-warm/R1/` | Uniform | 0.9415265044 |
| `old-warm/R2/` | History bandit | 0.9415322468 |
| `old-warm/R4/` | Agent valuation | 0.9465068033 |

## Design

All three arms restore the same four agents' frozen programs, histories,
directions and private-memory snapshot. The starting champion score is
0.9414633934. This historical schema-2 Warm checkpoint is explicitly imported
into the current prebid Market; its original identity is preserved.

Market settings are identical across the arms: four Coral agents using
DeepSeek `deepseek-flash`, low effort, seed 0, 20 rounds, two formal evaluation
slots per round, 20 CPU workers, up to 10 instance workers per admitted candidate,
discount factor 0.8, and sufficient Market funding (`credits=0`). The effective
configuration hash is
`5db761352165069e345b93e9fb0a210e2c8d165b1da0d453258914b08af36e31`.
All share the same 20 public PACE DS training instances and frozen evaluation
contract. Scores use frozen best-known references and are internal measurements,
not official PACE competition results. No held-out evaluation was run.

Uniform uses balanced rotation. History bandit uses discounted own settled
rewards with exploration coefficient zero. Agent valuation uses a read-only
Coral assessment of frozen proposals and the controller's reward formula for
expected-credit bidding. Candidates are frozen before allocation; only admitted
candidates receive formal evaluation. Agent valuation has extra model cost.

## Contents and limitations

Each regime contains `results.json` (recorded report, costs and provenance),
`config.json`, `trajectory.csv`, `allocations.csv`, `settlements.csv` and the
final solver under `champion/`. The root `results.json` indexes these three
results. `SHA256SUMS` covers only the published files. Round indices are
zero-based; the trajectory also records the Warm baseline.

These are single trajectories, not replicated causal estimates. Completion
means 20 rounds settled, not 40 successful evaluations: History bandit admitted
38 candidates, while Uniform and Agent valuation each admitted 40. Request
UNKNOWN costs remain retained (4/7/2 attempts respectively); incomplete token
usage fields are lower bounds. Consult the original report fields for failures
and measurement completeness. This compact bundle is not a resumable ledger.
The incomplete insufficient-balance attempt is excluded. Original ledgers,
other experimental groups and local logs remain on the server.
