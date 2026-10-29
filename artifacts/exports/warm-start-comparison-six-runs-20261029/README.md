# Warm Start comparison: six Market results

This bundle contains exactly six completed, 20-round MAESTRO Market episodes:
R1, R2 and R4 initialized from each of two frozen five-wave Warm checkpoints.
The question is whether the inherited programs and reward histories change the
subsequent allocation and improvement trajectories under the same Market settings.
Scores are internal evaluations on 20 public PACE 2025 Dominating Set heuristic
training instances, using frozen best-known references. They are not official
PACE competition scores. No held-out evaluation was run for these six episodes.

## Two groups

| Group | Warm checkpoint | Warm pipeline | Starting champion | Market dates (UTC) |
|---|---|---|---:|---|
| `new-warm/` | `warm-f430c1a053314b2c` | `prebid_v2`, schema 3 | 0.9327561083 | October 27, 2026 |
| `old-warm/` | `warm-5e4162c13d3149ff` | historical `postbid_v1`, schema 2 | 0.9414633934 | October 29, 2026 |

Both Warm runs gave each of four agents five opportunities. Each group clones
its one checkpoint into three independent Market episodes, restoring its frozen
agent programs, own history, directions and private-memory snapshot. Market wallets
are initialized independently. Old Warm state is explicitly imported into the
current Market pipeline; its original checkpoint/configuration is not relabeled.
The incomplete October 29 attempt stopped by insufficient API balance is excluded;
`old-warm/` contains only the subsequently completed fresh episodes.

## Market settings held constant

All six episodes have effective Market configuration hash
`5db761352165069e345b93e9fb0a210e2c8d165b1da0d453258914b08af36e31`.
They use four Coral agents, DeepSeek `deepseek-flash` with low effort, seed 0,
20 Market rounds, two evaluation slots per round, 20 CPU workers and up to 10
instance workers per admitted candidate. The same training panel and evaluation
contract hashes are recorded in every `results.json`. Credits are set to 0
(sufficient funding), and the own-settled-reward discount factor is 0.8.

| Regime | Allocation and valuation |
|---|---|
| R1 — Uniform | Seeded balanced rotation among eligible agents; no payment |
| R2 — History Bandit | Discounted own actual settled credits; exploration coefficient 0; no payment |
| R4 — Agent-adjusted valuation | Read-only Coral assessment of the frozen candidate and own history; controller computes expected credit with the reward formula; affordable first-price allocation |

All Market episodes use `prebid_v2`: candidates are implemented and frozen before
valuation/allocation; only admitted candidates receive formal evaluation. R4 has
an additional model-based valuation stage, so model costs are not equal across
regimes. Consult each `config.json` for exact phase windows and request/tool limits.

## Results

| Regime | New Warm final score | Gain over New Warm | Old Warm final score | Gain over Old Warm |
|---|---:|---:|---:|---:|
| R1 | 0.9455038585 | 0.0127477502 | 0.9415265044 | 0.0000631110 |
| R2 | 0.9544096946 | 0.0216535863 | 0.9415322468 | 0.0000688535 |
| R4 | 0.9327561083 | 0.0000000000 | 0.9465068033 | 0.0050434100 |

The ranking changes: R2 leads in the New Warm group, while R4 leads in the Old
Warm group. This motivates studying the interaction between initial programs,
inherited reward histories and resource allocation. It does not establish that
one Warm procedure or regime is generally superior.

## Interpretation and accounting limits

- These are single trajectories per group/regime, not replicated estimates of a
  causal effect. The two Warm checkpoints differ in programs, histories, memory
  and historical execution settings; this is a comparison of inherited states.
- Market model outputs and service behavior are stochastic. Execution/recovery
  fixes also differ between historical source snapshots, despite identical
  effective Market settings. Original full records remain on the server.
- Completion means all 20 rounds settled and final checkpoints exist; it does
  not mean 40 successful evaluations. In the Old Warm group R2 admitted 38
  candidates, versus 40 for R1 and R4. Invalid results and phase failures remain
  represented in the reports.
- Preserve `source_summary.comparison_eligible` as reported. In particular the
  New Warm R4 report marks it false; this completed trajectory should be used
  descriptively, not silently promoted into an unqualified comparison.
- Transport UNKNOWN records remain charged and are not declared resolved by
  publication. In the Old Warm group R1/R2/R4 retain 4/7/2 UNKNOWN attempts.
  Read the exact failure counts and phase `usage_complete` fields for every run.
  Token totals with incomplete usage are known lower bounds, not exact costs.
- Scores shown here are training scores. There are no held-out claims.

## Contents

```
results.json                       # Index, source identities, scores and hashes
new-warm/{R1,R2,R4}/
old-warm/{R1,R2,R4}/
  results.json                     # Recorded report, costs and source provenance
  config.json                      # Effective frozen 20-round Market configuration
  trajectory.csv                   # Warm baseline and 20 post-settlement champions
  allocations.csv                  # Recorded formal-admission decisions
  settlements.csv                  # Per-agent payments, rewards and wallets
  champion/                        # Final immutable solver source
SHA256SUMS                         # Checksums of all published files
```

`round_index` is zero-based; `round_number` is one-based for Market rounds, with
0 denoting the shared Warm baseline. Full precision is retained in JSON/CSV.
This compact, reviewed bundle is not a resumable ledger export. Credentials,
private memory, raw model/controller logs, real graph files and other experiments
are excluded. Source report hashes bind the copied report fields to their local
originals. Older published result bundles have been removed from Git tracking;
local historical records are retained.
