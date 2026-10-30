# Configuration layers

`experiments/pace_remote.yaml` is the real PACE experiment entry. Both commands
use that entry; the command selects its stage:

```sh
run_root=artifacts/runs/pace-ds/MODEL/first_price_naive/seed0-r1
python -m maestro --root "$run_root" bootstrap --config configs/experiments/pace_remote.yaml
python -m maestro --root "$run_root" run --bootstrap CHECKPOINT_PATH --config configs/experiments/pace_remote.yaml --policy first_price_naive
```

Replace `MODEL` with the configured model identifier. New runs use
`artifacts/runs/<benchmark>/<model>/<regime>/<timestamp-or-name>/`, with one
Market regime per leaf directory and the same root for Warm Start and Market.
Use `first_price_naive`, `ucb`, `uniform`, or `adjusted_valuation` for the regime and pass the matching
`--policy`; the CLI does not derive it from the directory name. The last
component may be a UTC timestamp such as `20261025T120000Z` or a short unique
name such as `seed0-r1`. Local runtime paths mirror the complete hierarchy.
See [the output directory contract](../artifacts/README.md). Existing flat and
multi-regime experiments keep their original paths and accounting.

## UCB exploration coefficient

`ucb_exploration_coefficient` is an experiment-level policy parameter (or a
top-level field in a flat config). Its formula is:

```text
U_i = historical_mean_reward_i
      + ucb_exploration_coefficient * sqrt(2 * log(1 + total_samples) / own_samples)
```

The default for new configurations and direct policy calls is `0`, selecting
mean-reward-only greedy allocation. The DeepSeek and DeepSeek pre-bid entries
also set `0` explicitly. It is still recorded under regime `ucb`;
reports and search-tree figures expose the coefficient. First-price and Uniform
do not use this parameter.

At zero, zero-history agents receive ranking value zero, not infinite forced
exploration priority. This is a cold-start convention, not an observed reward;
normal completed Warm histories already have positive sample counts. With a
positive coefficient, the original infinite cold-start priority is retained.

`bootstrap`, `run`, and `resume` accept `--ucb-exploration-coefficient 0` as an
explicit override. Choose it before bootstrap; both stages freeze the setting.
Running from that checkpoint without `--config` restores it automatically.
Changing it on a frozen checkpoint or resume is rejected. Existing experiment
artifacts are not rewritten: a missing coefficient in a historical frozen wire
record means the original value 1. New YAML/JSON documents instead receive the
new default 0 explicitly. Value 1 is omitted from canonical serialization to
preserve historical hashes; the new default 0 is explicitly serialized and hashed.

These are usage examples, not launch authorization. The checked-in deployment
still contains missing image/hardware/toolchain identities and cannot qualify
for real execution as supplied. Formal Warm Start remains paused.

| Layer | Owns |
| --- | --- |
| `runtimes/coral_codex.yaml` | Coral factory/revision, model, CLI version, reasoning effort, director/profile |
| `deployments/remote.yaml` | Allocated CPU/instance workers, checkout/data/seed paths, image and platform/toolchain identities |
| `benchmarks/pace/benchmark.yaml` | Benchmark identity, instance count, evaluation/verification resource contract |
| `benchmarks/pace/warm_start.yaml` | Warm opportunities/concurrency, preparation/planning/generation budgets |
| `benchmarks/pace/market.yaml` | Market rounds, slots, refresh, bidding/rewards/wealth, phase budgets |
| `experiments/pace_remote.yaml` | Explicit file composition, execution mode, population and random seed |

A new benchmark should provide its own benchmark and stage files and reference
an existing runtime/deployment where appropriate. Only PACE is implemented now;
adding configuration alone does not implement another benchmark.

Every layered document declares `kind` and `schema_version: 1`. Fragment references
resolve relative to the experiment entry. Host paths resolve relative to the
fragment declaring them, independently of the current working directory. Container
paths such as `/candidate/run` remain container paths. No environment/home
interpolation, recursive includes, implicit overrides or list concatenation occurs.
Unknown/misplaced fields, duplicate keys, inconsistent contract values and repeated
sources are errors. Both stage configurations are validated before execution.

Explicit CLI `--seed`, `--execution-mode` and `--benchmark` override their entry
values. Changing these cannot bypass checkpoint/resume consistency checks.
Bootstrap freezes the Warm settings and the Market rules. At Market launch,
`rounds` may be selected independently of the Warm checkpoint's predeclared
Market horizon; changing the Market fragment's `rounds` value and checksum is
allowed. Market credits are also selected at launch. Every other resolved
setting must still match the checkpoint. Resolved JSON files are saved under
`authority/config.warm_start.resolved.json` and `authority/config.market.resolved.json`.
Episode records retain their full configuration, and reports expose source hashes.
Run without `--config` derives Market from the frozen checkpoint; resume without
`--config` restores the last declared episode horizon. Resume with a config must
match every frozen setting except a strictly larger Market `rounds` value (and
the corresponding Market fragment checksum). A completed 10-round episode can
therefore continue through round 20 by editing only
`benchmarks/pace/market.yaml` from `rounds: 10` to `rounds: 20`, then running
`resume --episode EPISODE --config EXPERIMENT`. The original configuration and
regime contract stay immutable; an append-only extension event records the new
horizon. The same episode may be extended again at later completed boundaries,
such as 10 to 30 to 40 to 50 rounds. For `initial_wealth: 0`, the controller
adds the difference in guaranteed funding to each wallet at the boundary, and
replay verifies it. This cannot
extend a partially settled episode or alter phase budgets, regime, model, data,
or credits. Do not use `smoke` with a layered experiment.

`mock_smoke.yaml` remains a self-contained offline fixture. The obsolete Claude
Code configuration example has been removed; the backend implementation remains.
The default native runtime entry selects Codex;
changing to Claude requires equivalent permission/metering integration and
qualification, not merely swapping a model string. The versioned PACE vocabulary lives in
`src/maestro/tasks/pace/taxonomy.py`; its version and complete contents are frozen
in the resolved configuration as `plan_taxonomy` and included in its hash. These
labels are suggestions: agents may supply their own printable `target` and
`mod_type` labels of at most 64 characters. The host still validates the plan's
owner, parent, evidence IDs and other bounds before granting edits.

## Generation window and replacement

All maintained entries and new configuration documents select
`coral.candidate_pipeline: prebid_v2`: all valid plans are implemented and frozen
before bidding; only invited proposals receive sanity checks/runtime repair and
formal evaluation. The common generation budget covers implementation through
post-auction cleanup. R4's read-only valuation has its own common window; the
generation clock pauses during valuation and resumes with its prior remainder.
`postbid_v1` execution is retired:
bootstrap/run/resume reject both explicit legacy settings and historical records
without the pipeline field. Archived checkpoints cannot be reused for v2; their
original hashes and records remain available for replay, reports, plots and
evaluation of a completed Market champion. See the
[pipeline, repair context and compatibility contract](../docs/prebid_pipeline.md).
The `_prebid` filenames remain configuration aliases. The changed execution
contract requires fresh qualification; changing a preset launches no experiment.

`generation_window_seconds` controls the common wall-clock generation deadline,
including fixed precheck and process cleanup. Both PACE stage presets now use
`360` seconds, a provisional increase from `300`, not a measured calibration
result. Warm and Market can configure this field independently.
`valuation_window_seconds` independently bounds R4 valuation. Its physical
request cap remains `valuation_calls`; DeepSeek valuation read turns permit
at most 8,192 output tokens each, and the final structured-answer request
permits 16,384. The ordinary four-request phase cap is 40,960 output tokens.
Only a proven truncated or malformed fourth final answer with terminal usage
and reclaimed processes can release one additional tool-free final-answer request within the
same deadline. The conditional fifth request adds at most 16,384 output tokens;
if it fails cleanly, that agent skips the current round. UNKNOWN still stops.
Missing terminal usage still stops the episode.
R4 records and restores a no-reward score only when it is an upward rounding
of the exact parent score to at least three decimal places. The restored score
equals the parent and earns zero credit; other class-boundary violations remain
invalid and use the documented fallback.
The final valuation prompt asks for up to four concise evidence observations
(about 200 characters each), a 400-character rationale, and up to three brief
uncertainties (about 160 characters each). These are writing targets only;
the original schema and local validation limits remain in force. Hidden
thinking still counts toward the provider's output-token limit.

After a definite generation/precheck failure with confirmed cleanup, the
controller immediately invites the next agent in frozen priority order while
other agents continue. Replacements receive only the remaining time before the
same deadline. Completion speed does not change duplicate-selection priority.
UNKNOWN or unconfirmed cleanup stops progression. Request and CPU budgets remain
unchanged; formal evaluation and settlement still follow the generation barrier.
Changing a frozen window requires a new experiment configuration; resume cannot
silently extend it.


## Optional DeepSeek runtime

`runtimes/coral_deepseek.yaml` is an additive native DeepSeek Harness adapter.
Select `experiments/pace_remote_deepseek.yaml` instead of the default Codex entry
for a new experiment. Both reuse the same deployment, benchmark and stage files;
Codex remains the default and its adapter is retained. The DeepSeek entry has
explicit unqualified CLI identity placeholders and cannot be launched as supplied.
Provide credentials only in the controller environment. See
[DeepSeek provisioning, protocol boundaries and switching](../docs/deepseek_runtime.md).
Runtime changes invalidate checkpoint/resume compatibility; no automatic fallback
or conversion occurs.

The DeepSeek preset allows at most 20 extra transport retry requests across a
new experiment, with one retry per logical request after a 10-second wait. Phase
deadlines and request limits still apply. Existing frozen configurations and
ledger retry policies keep their recorded limits; editing this preset does not
extend an existing experiment's quota or settle UNKNOWN costs.

An operator can explicitly extend a quiescent experiment's shared retry cap with
`DeepSeekRetryStore.operator_extend_retry_limit(...)` after obtaining authorization
and verifying controller/container reclamation under the formal-controller lock.
The method appends a durable extension, retains the frozen base policy and all
previous charges, and rejects active or unreconciled requests. Reports distinguish
the root's effective cap from each episode's original configuration. This does
not change model-call caps, deadlines, retry eligibility or per-request retries.

### Discounted actual-credit valuation

New experiments set `valuation_discount_factor: 0.8` in the experiment entry
(shared by Warm Start and Market). The numeric factor must satisfy `0 < gamma <= 1`;
`gamma=1` gives the ordinary mean. It is frozen in resolved configs, hashes and
reports; changing it requires a new experiment rather than silently resuming one.

Both first-price and UCB use all of an agent's own settled formal evaluation
credits in chronological order, without target/modification-category filtering
or rebasing historical improvements onto the current parent/champion:
`S <- gamma*S + actual_credit`, `W <- gamma*W + 1`, `value = S/W`.
Only the agent's next settled formal evaluation advances this sequence. Warm
history remains the Market prefix. Zero-credit terminal failures participate;
losing bids, pre-admission failures and unresolved UNKNOWN do not become samples.

First-price bids `min(value, available_wealth)`; UCB uses `value` plus its existing
count-based exploration bonus. The existing `ucb_exploration_coefficient` setting
is unchanged (currently zero by default). Uniform allocation and actual credit
settlement are unchanged. Legacy wire records without the discount field retain
the old valuation/ranking behavior for historical replay; `null` is not allowed
in new configuration files. This is a UCB-style heuristic, not a claim of the
classical stationary-bandit regret guarantee.

First-price and R4 have no fixed `bid_cap`. Each bids
`min(value, wealth - outstanding_holds)`. PACE Market presets start each agent
at 5 credits (a provisional moderate budget); the generic RunConfig default
remains 2000. Set `initial_wealth: '0'` in the Market preset, or pass
`--credits 0` when launching Market, to
request sufficient funding. The controller deposits `rounds` times the larger
of the Warm and Market values of `money(reward_scale * (1 + catchup_fraction))`
per agent. This finite
bound covers the maximum possible R3/R4 bid in every frozen round even if
rewards are zero, so affordability never clips a valid bid. The real deposit,
payments and earned credits remain visible in the wallet ledger; `0` is the
configuration sentinel, not the ledger balance. Any positive credits amount
is a finite, exhaustible wallet. Negative amounts are rejected.

`--credits N` overrides the Market-stage `initial_wealth` field at `run`.
Warm Start has no credits argument, agent wallet or wallet transactions. Its
checkpoint excludes Market credits. Each Market arm creates its own wallet from
the chosen value; different Market arms may use the same Warm checkpoint with
different credits. Resume rejects a change to that arm's frozen credits. When
using a layered checkpoint without `--config`, supply `--credits` at `run`.
There is no automatic
refill, borrowing, or strategic bid shading. UCB and uniform still reserve/pay zero; their ranking
is not constrained by wealth. Existing invitation/admission/settlement guards
continue to prevent overspending or duplicate payments. Removed `bid_cap` keys
are rejected rather than silently ignored.
Historical episodes that used `initial_wealth: '0'` as a literal zero wallet
cannot resume under the new sentinel semantics; their records remain readable.

### Stable Market regime IDs

See [Regime Registry](REGIMES.md): R1=uniform, R2=Bandit-based allocation,
R3=first-price naive, R4=agent-adjusted valuation. Registry revision 5 records
the current policy names; historical frozen contracts retain their recorded mapping.
Use `maestro regimes --config <experiment.yaml>` to inspect effective contracts,
and `maestro run --bootstrap <checkpoint> --regime 1` to select by number.
Settings remain in the authoritative layered configuration files. New Market
runs freeze the chosen contract in CAS, the ledger and an episode JSON file.
R4 requires the real `prebid_v2` entry and an actual-gated native Coral
backend. `valuation_calls` (maximum 4) and `valuation_window_seconds`
(currently 300) are frozen phase budgets in the Market preset. Assessment
uses a separate read-only session and request account after proposal freeze,
within the original common generation deadline. It cannot run in a mock
episode. See the registry for its visible context, prompt/output contract,
fallback rules, and UNKNOWN handling.
