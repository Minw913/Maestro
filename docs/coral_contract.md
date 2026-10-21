# Coral composition contract

Pinned Git SHA: `0123dfb939b35228cf2c1fde224cd0561e727408`.

MAESTRO uses Coral `AgentRuntime.start`, `AgentHandle`, `AgentSandboxSpec`, real Task/ScoreBundle serialization and the TaskGrader entrypoint. It supplies its own controlled supervisor and independent private repositories. It does not run stock `AgentManager.start_all`, monitor loop, native auto-resume or grader daemon.

The runtime settings and private log directories are provisioned before start. The current Codex path uses ephemeral phase sessions; only accepted preparation summaries, plans and persistent workspace files cross phase boundaries. Every new process rechecks phase and resource authorization. No `max_turns` or output-stall timeout is treated as an actual inference/CPU/wall budget.

The current native factory uses Coral CodexRuntime for autonomous read-only preparation, a separate single-request structured plan, and grant-bound generation. Model-selected retrieval replaces fixed source/memory excerpts. Each stage has a separate request account and common monotonic deadline. Per-invitation gateway and compute budgets survive repair and process restart. All runtime model requests share the same frozen model identity and quota scope.

The retained legacy API-backed factory ships with a networkless Docker provider and a Unix-socket inference relay. The current tokenless native factory instead uses the existing ChatGPT login and gated MCP tools; candidate shell execution remains in DockerRunner. The injected monitor is trusted, runs as a different UID, and alone writes authority receipts. Candidate files, peer workspaces and credentials cannot modify that monitor or its accounting. Actual deployment still requires image/runtime qualification.

`Task.metadata['maestro']` carries ticket, job, plan, parent, candidate, phase, panel and contract identities. IDs are validated against SQLite; the Task is not itself a capability. `TicketedGradeBridge` checks immutable candidate content before dispatch. Grader venv initialization and candidate containment are separate responsibilities.

Default formal execution can call the task evaluator directly behind the same dispatcher. A Coral grader entrypoint is provided for integrations requiring the grader process boundary; native pending queues remain disabled.

Compatibility validation has three levels: pure type/metadata roundtrip; real cloned Coral Task/TaskGrader subprocess with a simulated executor; authenticated live CLI and physical isolation. Tests label the level they establish. Only the last level qualifies a production run.
