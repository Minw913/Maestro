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
4. `configs/experiments/pace_remote.yaml` selects native Codex with the existing ChatGPT login,
   brokered autonomous tools and separate preparation/planning/initial-generation request gates (currently 4/1/1),
   plus at most three conditional runtime-only repairs. Model reasoning
   effort is explicitly frozen in `coral.codex_reasoning_effort` (currently low). Run
   `python -m maestro codex-check --strict` for a no-inference login/protocol check;
   it does not qualify live inference or physical containment. Read the current
   connection/UNKNOWN evidence in `IMPLEMENTATION_REPORT.md` before launch.
   An explicitly authorized `codex-qualify --live` tests only the connection,
   file tools and memory with a single-use five-request envelope; it never
   substitutes for Linux executor/data qualification. The older JSON proposal
   backend counts turns and has no shipped preset. The older API-backed PACE
   preset is also removed. `configs/experiments/pace_remote.yaml` selects the same Coral native
   backend for Warm and Market. Historical app-server diagnostics do not qualify
   the new Coral/MCP runtime; separately bounded live qualification remains required. See `docs/codex_session.md`.
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

## Execution configuration

`execution_mode: mock | real` selects simulated evaluation or real execution.
`benchmark: pace` selects the task; PACE is the only implemented benchmark.
Both execution modes support this benchmark. Explicit `--execution-mode` and
`--benchmark` arguments override the corresponding configuration values. Resume
requires the complete frozen configuration hash to match before backend startup.

`evaluation_instance_count` counts distinct benchmark inputs evaluated for each
candidate (for example graphs, circuit netlists, or simulation inputs). Repeated
measurements, training runs, and test-set samples do not automatically count as
independent instances. `cpu_seconds_per_instance` limits execution CPU time for
one instance. The reservation remains
`evaluation_instance_count * cpu_seconds_per_instance + verification_cpu_seconds`.

Configuration loading rejects unknown and duplicate keys. There are no aliases
or migrations for superseded configuration fields. Ledger schema 2 and warm
checkpoint schema 2 reject previous experimental records; preserve historical
artifacts separately. Initialization always uses the common verified seed and
predeclared directions; it has no configurable mode.

## Read-only preparation settings

Preparation means autonomous reading of permitted code, memory, and public
information before submitting a plan. It is distinct from the experiment's
search directions and subsequent candidate implementation.

`preparation_calls` (default `4`) configures the maximum actual model requests
per agent per round for the read-only preparation stage.
`preparation_window_seconds` (default `300`) configures its common wall-clock
window; late dispatch must not extend the deadline. Both are tunable: the
request count must be a positive integer and the window a positive finite number.
They are separate from `planning_calls` / `planning_window_seconds` and
`generation_calls` / `generation_window_seconds`; unused requests are not
authorization to increase another stage's budget.

The Coral native backend enforces these settings through an independent request
gate and common monotonic deadline. Preparation allows list/read/search/batch
operations, including own history/, and forbids shell and all writes. Planning
begins after the preparation barrier with its full independent window; generation
begins after allocation and requires an EditGrant. Unknown inference or unconfirmed
process cleanup stops progression and preserves the authority records. Each
participating native agent prepares and plans anew each round.

The obsolete unimplemented preparation CPU knob is removed. File reads remain
bounded by broker file/output/call limits; preparation has no separately enforced
aggregate CPU-seconds budget. Generation's sandbox tool CPU limit and separately
metered fixed precheck limits retain their existing semantics.

These fields are included in the frozen configuration hash. They are not an
authorization to increase the previously bounded 48-request two-stage calibration.
That retained calibration path is a separate legacy protocol; a new three-stage
pilot needs its own explicit aggregate request budget and acceptance evidence.

## Layered experiment configuration

The supported PACE entry is `configs/experiments/pace_remote.yaml`; the former
root-level Warm preset has been split into reusable runtime, deployment, benchmark
and benchmark-specific Warm/Market files. See [configuration ownership and examples](../configs/README.md).
`bootstrap` selects `warm_start`; `run` selects `market`. The entry is the same,
while the two stages can have different predeclared phase budgets and scheduling.
Both stages and all configuration source hashes are frozen at bootstrap. Changes
to either stage or any common input invalidate checkpoint reuse; resume uses the
recorded configuration and rejects explicit overrides that change its hash.
Checked-in deployment identity placeholders still need qualified real values.
This restructuring does not authorize a real run or change the paused Warm status.

## One implementation and runtime-only repair (current policy)

Maintained Warm/Market presets use `generation_calls: 1` and
`runtime_repair_calls: 3`. Generation calls remain a positive, tunable actual
inference-request ceiling; increasing it does not request alternative candidates.
There is one initial implementation response/artifact and no initial tool loop.
The complete bounded source and admitted plan are supplied to Coral; the trusted
controller applies the structured files under its EditGrant. Sources exceeding
128 KiB or 32 files fail before inference rather than silently truncating code.

Only the controller's pre-admission fixed precheck may authorize runtime repair.
A normal exit with an invalid/malformed answer, syntax/dependency errors, time,
output/memory limits, signals, executor failure and UNKNOWN cannot authorize it.
The current conservative classifier recognizes selected Python traceback errors
with ordinary exit code 1; it does not interpret arbitrary nonzero exits as
runtime exceptions. Candidate stderr is untrusted diagnostic evidence and can
be fabricated; this classifier is not proof of program intent.

Each eligible repair starts an independent native session with no resumed thread,
plan, score, history, direction, private memory or tools. It receives the failed
source, bounded exception diagnostic and solver argv. Each attempt has a separate
one-request durable account, up to three attempts total. No new files are allowed;
at most three patches/twelve replacement lines are accepted within three lines
of traceback locations. Exact failed-candidate identity and controller-recorded
evidence are required. These bounds constrain edits but cannot prove semantic
minimality or algorithm equivalence. Every patch uses a fresh EditGrant, followed
by a new fixed precheck; only another eligible runtime exception permits another
repair. Generation and all repairs share the original monotonic deadline. Broker
CPU accounting is not reset, and each fixed precheck retains its independent cap.

Formal ticket evaluation results never reenter this repair loop: there are no
free full-panel reruns, ticket refunds or changes to settlement. Usage records
separate `generation` from `runtime_repair`. Old 48-call pilot results and legacy
five-call connection qualification do not qualify this changed policy; the latter
now explicitly reports its legacy transport scope. This change launches no model
calls or experiment, does not increase earlier authorization, and cannot use old
checkpoints/configuration hashes as if they represented this policy.


## Selecting Coral's model runtime

The default `configs/experiments/pace_remote.yaml` selects the preserved Codex
adapter. For DeepSeek, a new experiment can select
`configs/experiments/pace_remote_deepseek.yaml`, subject to its separate
[provisioning and qualification requirements](deepseek_runtime.md).
The two runtimes share the benchmark and stage files. A runtime/model change
cannot resume or reuse a checkpoint from the other runtime. Existing Codex
qualification commands do not qualify the new DeepSeek transport.

For DeepSeek runs, pause via `python -m maestro --root EXISTING_RUN pause`, then
verify controller/service exit and inspect the request ledger with
`python -m maestro deepseek-audit --ledger EXISTING_RUN/deepseek-native-authority/requests.sqlite`.
A pause-request acknowledgement is not a termination receipt. Do not use a
whole-cgroup signal as the normal pause mechanism. See the
[DeepSeek pause and evidence contract](deepseek_runtime.md#cooperative-pause-and-request-evidence)
for bounded draining, UNKNOWN preservation and restart checks.
