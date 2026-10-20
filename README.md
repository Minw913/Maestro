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
.venv\Scripts\python -m maestro --root artifacts/demo smoke --config configs/mock_smoke.yaml
.venv\Scripts\python -m maestro --root artifacts/demo replay --verify-ledger
.venv\Scripts\python -m maestro --root artifacts/demo report
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
python -m maestro --root artifacts/run bootstrap --config configs/mock_smoke.yaml --seed 0
python -m maestro --root artifacts/run run --bootstrap <checkpoint.json> --policy first_price
python -m maestro --root artifacts/run resume --episode <episode-id>
python -m maestro --root artifacts/run status
```

The bootstrap command prints its checkpoint path. Reuse that exact checkpoint for all policy arms. Eligible pending plans are resubmitted without another planning call; a refresh is due every three rounds or after execution/failure/parent change.

## Execution and accounting

1. A frozen parent/history/public-board snapshot feeds one structured planning request.
2. A provisional invitation holds the bid and grants bounded editing, not a formal evaluation.
3. Compile/toy checks can trigger bounded repair. Final pre-evaluation failure releases the hold and permits backfill under the same frozen order/deadline.
4. A final EvalTicket binds the candidate, plan, parent, panel, contract and job. The trusted dispatcher claims it before invoking any evaluator.
5. Actual formal failures consume the slot and payment. Results are withheld until simultaneous settlement. SQLite prevents duplicate charges/rewards; physical launch ambiguity remains explicit `UNKNOWN` until reconciled.

Source versions, prompts, output and verifier artifacts are immutable; wallet and admission facts are in SQLite. Private agent repositories do not share Git object stores. Public views contain only champion code and verified summaries. OS containment, not directory names or a prompt, enforces live isolation.

## Real Coral and PACE mode

For an existing ChatGPT Codex login, check the connection without an external API token:

```powershell
.venv\Scripts\python -m maestro --root artifacts/session-check codex-check
.venv\Scripts\python -m maestro --root artifacts/session-check codex-check --strict
.venv\Scripts\python -m maestro --root artifacts/session-check codex-check --probe
```

`--strict` checks the native autonomous app-server protocol and existing login
without inference. `configs/warm_start.yaml` selects the strict tokenless native
backend: real tool calls and Coral memory, one actual planning request and three
generation requests across all continuations and repairs. Every upstream request
is gated before dispatch. Read the current qualification and UNKNOWN status in
[IMPLEMENTATION_REPORT.md](IMPLEMENTATION_REPORT.md) before launching.

`--probe` is the older tool-free JSON proposal probe. Its historical success with
Codex CLI 0.156.1 does not qualify the strict native connection. The optional
`configs/codex_session_pilot.yaml` still counts turns rather than actual requests
and remains excluded from strict V1 comparisons. See [docs/codex_session.md](docs/codex_session.md).

The older API-backed Coral/Claude configuration remains in `configs/pace_pilot.yaml`.
The strict native Codex configuration requires no external API token. Real PACE
execution still needs actual training/reference manifests, a seed solver, a pinned
candidate image and qualified Linux rootless Docker/cgroup limits. Windows can
check native login/protocol and run simulated contracts; that is not physical
candidate qualification. Missing prerequisites fail without a mock fallback.

The strict Codex factory is `maestro.backends.codex_session:strict_v1_factory`.
The retained API-backed factory is `maestro.backends.coral.backend:default_factory`.
Native Coral manager/daemon are not started; the composition adapter owns process
lifecycle and formal dispatch. Runtime/library contract tests use injected fakes
and are distinct from an authenticated live-model run.

Read [docs/operations.md](docs/operations.md), [docs/PACE_ADAPTER.md](docs/PACE_ADAPTER.md), [docs/coral_contract.md](docs/coral_contract.md) and [IMPLEMENTATION_REPORT.md](IMPLEMENTATION_REPORT.md) for setup, validation evidence, issues and next actions. Richer strategic/disclosure algorithms are separate extension APIs and do not silently modify V1.

## Validated local delivery

Before the instance-worker revision, 276 tests passed with no skips in the target
repository, including the real pinned Coral subprocess grader contract. That
revision's complete simulation used ten warm opportunities per agent and passed
ledger replay (320 settlements). Historical evidence remains in
`artifacts/review-validation/` and `artifacts/review-smoke-final/reports/`; its
authenticated Codex JSON probe also passed. The new instance-worker and five-wave
warm revision's regressions have now been rerun under the user's later testing
authorization: **515 passed, zero failures or skips, in 145.39 seconds**, including
the installed Codex binary against a fake upstream. Current source/test evidence and the still-unresolved real
connection probe are recorded in `artifacts/strict-codex-validation/` and
`IMPLEMENTATION_REPORT.md`. Warm Start itself has not been launched. Full
physical Linux/PACE qualification remains a deployment next action.
