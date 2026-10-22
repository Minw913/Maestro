# Configuration layers

`experiments/pace_remote.yaml` is the real PACE experiment entry. Both commands
use that entry; the command selects its stage:

```sh
python -m maestro --root artifacts/runs/EXPERIMENT bootstrap --config configs/experiments/pace_remote.yaml
python -m maestro --root artifacts/runs/EXPERIMENT run --bootstrap CHECKPOINT_PATH --config configs/experiments/pace_remote.yaml --policy first_price
```

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
Bootstrap freezes both resolved stage configurations and all six source paths and
SHA256 checksums. Market may differ from Warm only by the stage values already
frozen at bootstrap. Editing even the unused Market fragment after bootstrap
invalidates reuse of that checkpoint. Resolved JSON files are saved under
`authority/config.warm_start.resolved.json` and `authority/config.market.resolved.json`.
Episode records retain their full configuration, and reports expose source hashes.
Run without `--config` derives Market from the frozen checkpoint; resume without
`--config` restores the exact episode configuration from the ledger. Resume with a
config must match its recorded hash. Do not use `smoke` with a layered experiment.

`mock_smoke.yaml` remains a self-contained offline fixture. The obsolete Claude
Code configuration example has been removed; the backend implementation remains.
The default native runtime entry selects Codex;
changing to Claude requires equivalent permission/metering integration and
qualification, not merely swapping a model string. The versioned PACE vocabulary lives in
`src/maestro/tasks/pace/taxonomy.py`; its version and complete contents are frozen
in the resolved configuration as `plan_taxonomy` and included in its hash.

## Generation window and replacement

`generation_window_seconds` controls the common wall-clock generation deadline,
including fixed precheck and process cleanup. Both PACE stage presets now use
`360` seconds, a provisional increase from `300`, not a measured calibration
result. Warm and Market can configure this field independently.

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
