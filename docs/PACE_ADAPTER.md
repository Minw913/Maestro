# PACE evaluator integration

`maestro.tasks.pace` implements strict PACE `.gr` and solution parsing, a
trusted Dominating Set verifier, checksummed ordered panel manifests, and the
MAESTRO normalized internal score. It does not use solver-reported scores.

The public API is:

```python
panel = load_panel("data/training/panel.json", expected_split="training")
heldout = load_panel("data/heldout/panel.json", expected_split="heldout")
panel.assert_disjoint(heldout)
contract = EvaluationContract.standard(
    image="local/solver@sha256:<real-digest>",
    hardware_id="<frozen-hardware-id>",
    toolchain_digest="<frozen-toolchain-id>",
    solver_argv=("python3", "/candidate/solver.py"),
)
evaluator = PaceEvaluator(
    panel, contract, DockerRunner(contract.image),
    candidate_resolver=lambda trusted_ticket: Path(trusted_ticket["candidate_path"]),
    artifact_store=immutable_private_store.put_bytes,
)
evaluator.preflight()
result = evaluator.evaluate(ledger_ticket)
```

The dispatcher, rather than the agent, supplies `ledger_ticket`. Required
fields are `job_id`, `candidate_hash`, `candidate_path`, `panel_hash`,
`contract_hash`, and `purpose`. The candidate resolver must return an immutable,
authority-captured candidate directory. `candidate_hash` is the ledger's source
tree identity, not a Git commit ID. The dispatcher validates the identity before
the evaluator is invoked. The evaluator independently checks panel/contract
bindings, graph byte checksums, and purpose. Never expose this trusted function
directly to an agent as an unguarded tool.

Panel JSON has this shape; paths are relative to the manifest directory or an
explicit `data_root`. An ordered list of 20 distinct instances is required by
the standard contract. Checksums and reference sizes must be real values.

```json
{
  "schema_version": 1,
  "panel_id": "training-v1",
  "split": "training",
  "seed": 7,
  "instances": [
    {"instance_id": "g001", "path": "g001.gr", "sha256": "<sha256-of-graph-bytes>", "reference_size": 12}
  ]
}
```

`split` is `training`, `heldout`, or `smoke`. Smoke manifests cannot create
formal scores. The fixed `fixtures/toy_graphs/star5.gr` can be used with
`smoke_test(...)`, which returns only pass/fail. Do not put that graph into a
training/held-out panel. All fixtures are synthetic parser/contract test data.

Candidates see one graph on stdin and a read-only `/candidate` directory. They
do not see panel manifests, reference sizes, held-out paths, the host ledger,
agent memory, network or API credentials. `/scratch` is the bounded writable
container directory. Compilation should finish before the candidate is frozen;
the formal evaluator does not repair or edit code. Runtime packages/image must
already exist; the runner never pulls images automatically.

The default `FailClosedRunner` refuses all execution. Real execution requires
the digest-pinned image, declared hardware/toolchain, a durable private output
artifact store and verified rootless Linux/cgroup-v2 containment. See
`execution/sandbox.py`. This Windows development run has not demonstrated
physical Docker isolation or a real PACE research dataset. Missing prerequisites
are errors; there is no native subprocess or mock fallback.

The standard slot reserves 620 CPU seconds: 20 instances x 30 seconds plus a
20-second trusted verification reserve. Wall limits, memory, pids and combined
stdout/stderr limits are separate. CPU metering belongs to the containment
runner. After all candidate processes have terminated, one separate trusted
verification container parses and checks the complete panel under the single
20-second cgroup CPU reserve and a distinct 30-second wall deadline. It mounts
only the trusted verifier source and read-only individual graphs, with no
candidate executable. Source checksums are bound as `verification_code_hash`
in the contract. The runner's independent host watchdog kills the container
even if its worker hangs. Timeout, crash, protocol failure or incomplete
metering yields aggregate `INFRA_FAILURE`, score zero and no valid parent;
the already admitted formal slot remains consumed. This shortened
contract is always labeled `pace_style_internal`, never official competition
performance. The current runner's internal time-limit policy kills the process
group rather than implementing the official competition's five-minute SIGTERM
protocol; `timed_out` and runner diagnostics preserve that distinction.

## Instance concurrency

`RunConfig.cpu_workers` declares the number of CPU cores reserved for formal
solver and trusted verification processes across the engine. It is explicit;
MAESTRO does not assume every core returned by `os.cpu_count()` is available.
`instance_workers` is the requested maximum concurrent instances within one
candidate (default 1). Each instance still receives one core and its original
CPU, wall, memory, pids and output limits.

For a round, candidate evaluation concurrency is
`batch_width = min(admission_capacity, cpu_workers)`, and the frozen per-candidate
width is
`min(instance_workers, panel_size, ceil(cpu_workers / batch_width))`.
The global pool bounds actual concurrent solver and verification processes;
the ceiling allows spare cores to serve part of a batch when the number of
available cores is not divisible by its candidate count.
Main admission capacity remains `slots` even when CPU capacity is smaller;
admitted candidates queue for execution. Warm admission capacity is the current
balanced warm wave, independently bounded by `warm_concurrency`, population and
`cpu_workers`. Setup and final held-out evaluation use a batch width of one.
The resource settings are recorded with the round and its tickets before launch;
empty slots do not change another candidate's frozen instance width.

For example, with four warm agents, `cpu_workers: 8` and `instance_workers: 2`,
four candidates can each evaluate two instances at once. With `cpu_workers: 4`,
the same request permits one instance per warm candidate and two per main
candidate when `slots: 2`. With six CPU workers and four candidates, the
per-candidate cap can be two while the shared pool admits only six physical
processes in total. These settings participate in the frozen task contract.

Instance completion order does not change panel order or scoring. All instance
workers finish and their processes terminate before aggregate verification.
The 620 CPU-second reservation is unchanged: parallelism shortens elapsed time
without multiplying the per-candidate compute budget. Provision memory for all
simultaneous containers; `cpu_workers` is not a host-wide memory reservation or
a claim about agent planning/generation CPU use.

An illegal or malformed completed result invalidates the entire candidate
with aggregate zero. A timeout without a usable result contributes zero for
only that instance. A valid set present at timeout can be verified and scored;
the timeout flag remains true. A non-timeout nonzero exit is a candidate
failure. Valid results below the reference size are clipped to one and flagged
for reference review. Every result contains per-instance validity, size,
timeout, score, CPU/wall usage and output artifact references.

`simulate_containment=True` allows injected deterministic receipts for tests;
only this explicit test mode can use the inline simulated verifier. A physical
verifier transport can be injected independently to test quota enforcement,
worker timeouts and crashes without Docker.

`tasks.pace.heldout.evaluate_final(engine, source_episode_id, manifest_path)` is
the trusted operator helper for final generalization. It only accepts a fully
completed PACE main episode, reads the immutable final champion, checks held-out
disjointness, and creates a deterministic independent `policy=heldout` episode.
The new purpose-specific ticket goes through the ordinary dispatcher with zero
payment and reward. Repeating the same request returns the recorded result;
unknown in-flight work requires reconciliation and is never automatically
rerun. This helper writes private authority records and no agent/public-board
views. Its cost is recorded as `HELDOUT`, outside the main auction budget.
the result explicitly records `execution_kind=simulated_containment`. It must
never be used to report real PACE measurements. Unit/contract tests exercise
isolated vertices, malformed headers, comments, duplicate/out-of-range IDs,
wrong counts, missing references/checksums, non-dominating sets, partial
timeouts, SIGTERM-like receipts, no output, held-out authority and ticket
binding mismatches.
