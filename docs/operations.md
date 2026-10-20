# Local operation and recovery

## Simulation

Run `python -m maestro --root artifacts/demo smoke --config configs/mock_smoke.yaml` with the installed package or `PYTHONPATH=src`. It performs one common warm start and all three V1 policy arms. `replay --verify-ledger` recomputes accounting and checks immutable artifacts and committed event anchors. `report` writes `summary.json`, `trajectory.csv`, `wallets.csv`, `trajectory.svg` and an events JSONL export.

The default source configuration is intentionally explicit simulation. Fault injection is only used by mock:

```yaml
failures:
  agent-1: precheck
  agent-2: [precheck, null]   # repair succeeds on revision two
  agent-3:0:
    formal: invalid         # admitted failure consumes payment/slot
  deadline_rounds: [3]
  duplicate_candidate_rounds: [5]
```

Never put fake formal results into a warm checkpoint to resolve empty history. Bootstrap without a formal record for every agent is `BOOTSTRAP_INCOMPLETE`.

## CPU and instance concurrency

Configure these values before creating the warm checkpoint:

```yaml
n_agents: 4
slots: 2
warm_opportunities_per_agent: 5
warm_concurrency: 4
cpu_workers: 8
instance_workers: 2
```

Here warm start has five waves of four independent agents. Each candidate can
run two panel instances concurrently. Main auction still admits at most two
candidates per round; it does not auction instance workers as additional slots.
With fewer CPU cores, excess formal jobs queue and the shared solver/verifier
pool limits physical concurrency. For example, four CPU workers permit only
one instance per warm candidate, even if `instance_workers` requests two.

`cpu_workers` is the explicit allocation for formal evaluation, not automatic
detection of all host cores. Leave capacity for Coral agent tools, model control
processes, the operating system and simultaneous container memory. Its default
is four and the default `instance_workers` is one; set both explicitly when
provisioning a larger platform. The per-instance compute limits, payment,
620 CPU-second reservation, score aggregation and result publication barrier
do not increase with the worker count. Worker settings are frozen with the
episode and task contract; a warm checkpoint cannot be reused under different
settings.

The CPU pool is shared within one MAESTRO controller process, including its
held-out evaluator. For multiple independent controllers, partition CPU capacity
between them or use an external executor with one shared resource authority.
An unconfirmed container shutdown poisons the pool and prevents new launches
until operator reconciliation; a failed Docker command never proves termination.

This configuration change does not execute evaluation or tests. Qualification
must separately verify resource limits on the intended Linux platform.

## Real mode

1. Provision supported Linux, rootless Docker/cgroup-v2 and the digest-pinned images described in `docker/README.md`.
2. Install the exact cloned backend: `python -m pip install -e vendor/coral`. The live preflight verifies both Git HEAD and Python's actual import path.
3. Supply frozen training/reference manifests, a solver directory with the contract executable, and a disjoint heldout panel. `docs/PACE_ADAPTER.md` defines the formats.
4. `configs/warm_start.yaml` selects native Codex with the existing ChatGPT login,
   brokered autonomous tools and hard 1/3 actual-request quotas. Model reasoning
   effort is explicitly frozen in `coral.codex_reasoning_effort` (currently low). Run
   `python -m maestro codex-check --strict` for a no-inference login/protocol check;
   it does not qualify live inference or physical containment. Read the current
   connection/UNKNOWN evidence in `IMPLEMENTATION_REPORT.md` before launch.
   An explicitly authorized `codex-qualify --live` tests only the connection,
   file tools and memory with a single-use five-request envelope; it never
   substitutes for Linux executor/data qualification. The
   older `configs/codex_session_pilot.yaml` only proposes JSON files and counts
   turns; `configs/pace_pilot.yaml` selects the older external-provider Coral
   runtime. Neither is a substitute for strict native Codex. See `docs/codex_session.md`.
5. Run bootstrap and the three policies from its printed checkpoint. Keep images, prompts, taxonomy, panel and contract identical across arms.
6. Use operator-only `final-eval --episode <id> --manifest <heldout.json> --config <pace.yaml>`. Heldout results remain under authority and reports; they never feed agent memory or training settlement.

The current warm configuration reserves 20 formal CPU workers and five instance
workers per candidate. Four agents each receive five warm opportunities in five
waves. `bootstrap` runs warm only; `smoke` also starts main policies in simulation.
Do not run either while the user has asked to hold Warm Start.

`pace` refuses missing prerequisites. A simulated containment test is not a real qualification test.

## Recovery semantics

- `resume --episode <id>` checks the original config and last fully settled checkpoint.
- Durable ADMITTED jobs can be dispatched once; RESULT_RECORDED jobs settle without reevaluation. A crash after settlement replays only missing projections.
- CLAIMED/LAUNCHING/RUNNING/UNKNOWN jobs require executor reconciliation. LAUNCHING records an attempt before the runner call and blocks assertion-only refunds. A database claim cannot prove that an OS process did or did not start. No blind relaunch or refund is performed.
- A trusted operator/executor can call `Ledger.void_never_started(ticket_id, proof)` only with confirmed non-start evidence. It appends one compensating entry, leaves no formal outcome, and does not reopen backfill after the formal batch.
- Interrupted preparation/generation fails closed until containment has been reconciled. Preserve the old episode and its costs; do not delete rows or reset a quota account to make a retry work.
- Inspect authority events, job/fence, executor receipts, private runtime logs and quota reservations together. Keep private material out of public bug reports.

For unresolved execution states, retain the incomplete episode and follow a predeclared whole-episode rerun policy. Never label an incomplete run as a completed comparison.

## Information and data

The authority directory and global CAS are never agent mounts. Only explicit private/public projections are visible. Native Coral pending files are requests, not tickets. Optional disclosure is a separate full-spec API; mandatory evidence and experiment accounting remain intact.

Existing `old_docs/` is research reference material and is not imported into runtime prompts, containers or report projections.
