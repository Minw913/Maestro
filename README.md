# MAESTRO

MAESTRO is a local, transactional compute scheduler for bounded autonomous research agents. Coral supplies the coding-agent runtime, sessions and grader interfaces; MAESTRO owns plan-first permissions, auctions, formal-evaluation tickets and settlement.

Coral is cloned as `vendor/coral` and pinned to `0123dfb939b35228cf2c1fde224cd0561e727408`. The detailed design is [maestro_plan_V1.md](maestro_plan_V1.md). Existing `old_docs/` research materials and `artifacts/` validation evidence are preserved locally and are not published to GitHub. Runtime data and credentials also remain local.

## Clone with the Coral backend

```sh
git clone --recurse-submodules https://github.com/Minw913/Maestro.git
cd Maestro
```

For an existing checkout, run `git submodule update --init --recursive` to
materialize the pinned Coral source.

## Install and run the self-contained simulation

Python 3.12+:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e '.[test]'
.venv\Scripts\python -m maestro --root artifacts/runs/demo smoke --config configs/mock_smoke.yaml
.venv\Scripts\python -m maestro --root artifacts/runs/demo replay --verify-ledger
.venv\Scripts\python -m maestro --root artifacts/runs/demo report
.venv\Scripts\python -m pytest -q --basetemp .test-temp
```

An installation-free alternative is to set `PYTHONPATH=src` and use `python -m maestro`. Reports and immutable artifacts are generated under the selected root. `smoke` is explicitly simulated; simulated scores are never reported as real PACE results.

The default smoke configuration gives each of four agents five warm opportunities (five waves with `warm_concurrency: 4` and at least four CPU workers), freezes their common histories/parents/UCB statistics, and runs 20 main rounds for each of `uniform`, `ucb`, and `first_price`, with at most two formal slots per round.

`cpu_workers` declares the CPU capacity reserved for formal evaluation. Set
`instance_workers` to request parallel instances within each candidate; for
example, `cpu_workers: 8` and `instance_workers: 2` allow four warm candidates
to each run two instances at once. Solver and verifier share one engine-wide
pool. Each instance remains single-core and the 620 CPU-second candidate
reservation stays unchanged. See [concurrency settings](docs/operations.md#cpu-and-instance-concurrency).

## Individual commands

```text
python -m maestro --root artifacts/runs/EXPERIMENT bootstrap --config configs/mock_smoke.yaml --seed 0
python -m maestro --root artifacts/runs/EXPERIMENT run --bootstrap <checkpoint.json> --policy first_price
python -m maestro --root artifacts/runs/EXPERIMENT resume --episode <episode-id>
python -m maestro --root artifacts/runs/EXPERIMENT status
python -m maestro --root artifacts/runs/EXPERIMENT report
```

The bootstrap command prints its checkpoint path. Reuse that exact checkpoint for all policy arms. Eligible pending plans are resubmitted without another planning call; a refresh is due every three rounds or after execution/failure/parent change.

Warm Start and its subsequent Market belong to one experiment output root.
Keep the same `--root` for both commands: they share the authority ledger, CAS,
provider request accounting and checkpoint directory. They retain distinct
episode IDs, and Market records its source Warm ID. Run Market only after a
successful, complete bootstrap; the final unfiltered `report` includes both
episodes. A separate `runtime/runs/EXPERIMENT/` holds local logs and mutable
workspaces, not another experiment or another Warm/Market output root.

## Execution and accounting

1. A frozen parent/history/public-board snapshot feeds one structured planning request.
2. A provisional invitation holds the bid and grants bounded editing, not a formal evaluation.
3. Compile/toy checks can trigger bounded repair. Final pre-evaluation failure releases the hold and permits backfill under the same frozen order/deadline.
4. A final EvalTicket binds the candidate, plan, parent, panel, contract and job. The trusted dispatcher claims it before invoking any evaluator.
5. Actual formal failures consume the slot and payment. Results are withheld until simultaneous settlement. SQLite prevents duplicate charges/rewards; physical launch ambiguity remains explicit `UNKNOWN` until reconciled.

Source versions, prompts, output and verifier artifacts are immutable; wallet and admission facts are in SQLite. Private agent repositories do not share Git object stores. Public views contain only champion code and verified summaries. OS containment, not directory names or a prompt, enforces live isolation.

## Real execution with Coral and the PACE benchmark

For an existing ChatGPT Codex login, check the connection without an external API token:

```powershell
.venv\Scripts\python -m maestro --root runtime/session-check codex-check
.venv\Scripts\python -m maestro --root runtime/session-check codex-check --strict
.venv\Scripts\python -m maestro --root runtime/session-check codex-check --probe
```

`--strict` checks the native autonomous app-server protocol and existing login
without inference. `configs/experiments/pace_remote.yaml` selects the strict tokenless native
Coral backend: upstream CodexRuntime owns the native coding-agent loop. Each
round has read-only preparation (four actual requests), one structured planning
request, and up to three generation requests across continuations and repairs. Every upstream request
is gated before dispatch. Read the current qualification and UNKNOWN status in
[IMPLEMENTATION_REPORT.md](IMPLEMENTATION_REPORT.md) before launching.

`--probe` is the older tool-free JSON proposal probe. Its historical success with
Codex CLI 0.156.1 does not qualify the strict native connection. The older proposal backend counts turns rather than actual requests; its
runnable preset has been removed and it remains excluded from strict V1 comparisons. See [docs/codex_session.md](docs/codex_session.md).

The obsolete API-backed PACE preset has been removed; `configs/experiments/pace_remote.yaml`
composes reusable runtime, deployment and PACE benchmark files with separate Warm Start
and Market stage configurations. See [the configuration layout](configs/README.md).
The strict native Codex configuration requires no external API token. Real PACE
execution still needs actual training/reference manifests, a seed solver, a pinned
candidate image and qualified Linux rootless Docker/cgroup limits. Windows can
check native login/protocol and run simulated contracts; that is not physical
candidate qualification. Missing prerequisites fail without a mock fallback.

The factory is `maestro.backends.coral.native_backend:native_factory`.
Coral `CodexRuntime.start`, `AgentSandboxSpec`, and `AgentHandle` run the native
CLI lifecycle; MAESTRO supplies request gates, MCP capabilities, deadlines, grants,
and formal dispatch. Stock Coral manager/daemon and automatic evaluation queues
are not started. The legacy app-server and API-backed implementations remain
for isolated diagnostics, not as alternate shipped experiment presets.
The Coral/MCP path requires its own live and physical qualification; historical
app-server connection results do not qualify it. Runtime/library contract tests use injected fakes
and are distinct from an authenticated live-model run.

Read [docs/operations.md](docs/operations.md), [docs/PACE_ADAPTER.md](docs/PACE_ADAPTER.md), [docs/coral_contract.md](docs/coral_contract.md) and [IMPLEMENTATION_REPORT.md](IMPLEMENTATION_REPORT.md) for setup, validation evidence, issues and next actions. Richer strategic/disclosure algorithms are separate extension APIs and do not silently modify V1.

## Validated local delivery

Strict native Codex connection qualification passed using cached ChatGPT login,
Codex CLI 0.156.1, `gpt-6-astra`, and explicit low reasoning effort. The final run
took **26.344 seconds**: one planning, three generation, and one restored-memory
read request, with **five terminal usage receipts and zero unresolved requests**.
Actual model-origin batches read memory and wrote code/private notes. Missing
grants, public-memory writes, and a fourth generation dispatch were denied.
See the [sanitized evidence](docs/validation/codex-live-20261020.json) and
[implementation report](IMPLEMENTATION_REPORT.md).

The full no-model regression passed **581 tests, zero failures or skips, in
171.18 seconds**, including the installed native binary against fake credentials
and a fake upstream, ticket launch, synchronous auctions, backfill and settlement.
Local evidence is under `artifacts/codex-live-qualification-20261020/`.
Three isolated historical diagnostic requests remain UNKNOWN and charged; new
success does not resolve their old execution or usage. All diagnostic attempts
and the original probe total 17 charged calls. No automatic retries or refunds.

**Warm Start has not been launched.** The live qualification executed file tools
only, not candidate code, shell, toy prechecks or formal evaluations. Real PACE
data and Linux/rootless Docker containment still require independent qualification.
Older validation records remain in `artifacts/review-validation/`,
`artifacts/review-smoke-final/` and `artifacts/strict-codex-validation/`.

Configuration uses `execution_mode: mock | real` and `benchmark: pace`.
See [execution configuration](docs/operations.md#execution-configuration) for
instance counts, CPU limits, CLI overrides, and incompatible historical records.


An optional [Coral DeepSeek runtime](docs/deepseek_runtime.md) is available through
`configs/experiments/pace_remote_deepseek.yaml`. The original Codex adapter and
default entry remain in place. The DeepSeek path requires its own installed CLI,
API credential and live/physical qualification before real use.


Standard outputs now live under `artifacts/runs/<run>/`; local model/controller
logs, mutable workspaces and operational state belong under Git-ignored
`runtime/`. Use `export-artifacts --output artifacts/exports/<snapshot>` to
prepare standard outputs with standalone SQLite backups for GitHub publication.
See [the output directory contract](artifacts/README.md).
