# Coral DeepSeek runtime

MAESTRO supports separate opt-in runtime entries. The existing Codex adapter,
ChatGPT authentication and gate remain unchanged. The default
`configs/experiments/pace_remote.yaml` still references `coral_codex.yaml`.
`configs/experiments/pace_remote_deepseek.yaml` references `coral_deepseek.yaml`;
it reuses the same deployment, benchmark and Warm/Market stages. Switching the
entry changes the model runtime, not the auction, valuation, evaluator or ticket
contract. Both Warm and Market of a comparison must use the same frozen runtime.
Checkpoint and resume hashes reject an intervening runtime/model change.

## Provisioning and launch status

The DeepSeek integration is implemented and offline-tested, **not live-qualified**.
No DeepSeek CLI or API call was installed/executed as part of this adapter change.
The provided `deepseek_cli_version: UNQUALIFIED` and empty executable SHA256 are
intentional launch blockers. Existing physical/data/seed qualification and formal
Warm pause still apply. Offline synthetic-provider tests do not qualify a model
connection or physical containment.

1. Install the official DeepSeek Harness CLI `dsh` separately in the intended
   deployment. Set optional deployment `coral.deepseek_executable` to its path
   (relative filesystem paths are relative to that deployment fragment).
2. Audit and freeze the full `dsh --version` stdout and executable SHA256 as
   `coral.deepseek_cli_version` and `coral.deepseek_cli_sha256` in the DeepSeek
   runtime fragment. Audit the resolved package/dependency installation as part
   of runtime qualification; hashing a launcher alone does not attest its dependencies.
3. Select the intended `coral.model` and Messages-compatible
   `coral.deepseek_api_base`. The supplied example uses the official Messages
   root `https://api.deepseek.com/anthropic` and `deepseek-flash`. Provider aliases
   may change the served weights; do not claim that the alias pins model weights.
4. Supply the API key only in the controller environment under the variable
   named by `coral.deepseek_api_key_env` (default `DEEPSEEK_API_KEY`). Never put
   its value in YAML, source, artifacts or candidate images. ChatGPT sign-in
   cannot authenticate a DeepSeek API request.
5. Separately qualify the installed CLI, real connection and tool/containment
   contract under an explicitly bounded authorization before a real experiment.
   `codex-check` and `codex-qualify` remain Codex commands; their receipts do not
   qualify DeepSeek. This change adds no live qualification command or authorization.

After qualification and experiment authorization, choose the corresponding entry
via the ordinary `--config` argument; there is no automatic fallback:

```sh
# Command shapes only; do not start while qualification or pause conditions remain.
python -m maestro --root artifacts/NEW_RUN bootstrap --config configs/experiments/pace_remote_deepseek.yaml
python -m maestro --root artifacts/NEW_RUN run --bootstrap CHECKPOINT_PATH --config configs/experiments/pace_remote_deepseek.yaml --policy first_price
```

Changing back means choosing the original Codex entry for a **new** experiment.
Neither entry silently converts an existing checkpoint or resets a ledger.

## Execution and accounting

The adapter invokes pinned Coral's actual `DeepSeekHarnessRuntime.start()` and
`AgentHandle`, with an enforcing command prefix; it does not implement a new
model/tool loop. It uses fresh native phases and isolated HOME/DSH_HOME. Files and
private memory remain in MAESTRO's existing workspace; the only exposed tool is
the phase-authorized `maestro_tool` over MCP. Preparation remains agent-selected
read-only retrieval. Planning and the current structured implementation/runtime
repair path inherit the same policy and budgets as Codex. Native filesystem,
shell, subagent, web, plugin-management, automatic retry/title generation and
telemetry capabilities are disabled. No native Coral eval daemon is started.

The API key stays inside the controller's fixed-HTTPS upstream transport. The
CLI receives only a dummy key and a phase-scoped loopback gateway URL. The gate
reserves each actual request in a separate persistent
`deepseek-native-authority/requests.sqlite`, enforces the configured model,
request count, absolute deadline, byte and output-token limits, and removes all
non-broker tool definitions and historical registry updates. Provider tool
responses are checked before any delivery to the CLI. Both JSON and streaming
Messages responses are buffered until a complete terminal message and usage
receipt. This buffers SSE and therefore changes first-token latency; measure
actual stage completion latency in the separately authorized timing pilot.

`deepseek_reasoning_effort` is independently configurable (`off`, `low`, `high`,
`max`); it does not inherit `codex_reasoning_effort`. The per-request output ceiling
is `deepseek_max_output_tokens` (default 32000), within the existing phase token
account. Phase request counts and generation/runtime repair rules are taken from
the common stage configuration, never expanded by selecting DeepSeek. Missing
usage, a truncated response or ambiguous transport failure leaves its reservation
UNKNOWN and blocks further inference. No retry, budget reset or automatic refund
is performed. Local process cleanup is recorded separately and never claimed as
proof of remote provider termination.

Factory: `maestro.backends.coral.deepseek_backend:native_factory`.
Source modules: `deepseek_backend.py`, `deepseek_client.py`, `deepseek_exec.py`,
`deepseek_gate.py` under `src/maestro/backends/coral/`.

## Protocol sources

The adapter policy was inspected against official DeepSeek Harness commit
[`687f282e8625c6b8d11ff58211a90524be2e4317`](https://github.com/deepseek-ai/deepseek-harness/tree/687f282e8625c6b8d11ff58211a90524be2e4317),
particularly its [headless profile](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/packages/bundle/headless/cordis.patch.yml),
[Messages serializer](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/packages/llm/llm-deepseek/src/serialize.ts)
and MCP client. This is a source-review reference, not an assertion that the
server has that harness installation. See also the official
[API authentication and model names](https://api-docs.deepseek.com/).
A different installed harness version must pass its own protocol qualification.

## Cooperative pause and request evidence

Use the controller's pause command instead of killing a whole service cgroup:

```sh
python -m maestro --root artifacts/EXISTING_RUN pause
python -m maestro deepseek-audit --ledger artifacts/EXISTING_RUN/deepseek-native-authority/requests.sqlite
```

`pause` writes an operator marker under `authority/pause-request.json`; its return
means **pause requested**, not confirmed remote cancellation. The DeepSeek CLI
controller also handles SIGINT/SIGTERM cooperatively when signalled directly.
Do not signal all processes in the service: that also interrupts the transport
and native children before they can preserve receipts. A service wrapper's main
PID need not be the MAESTRO controller PID. Prefer the pause command.

The controller rejects new inference dispatches, repairs, backfill invitations,
subsequent waves/rounds, and new evaluation jobs. Already-running requests have
up to 30 seconds to drain, bounded by their original phase deadlines; no wall
window, model request, tool CPU, permission or quota is expanded. Requests that
finish during draining retain their terminal usage receipt. Grace expiry closes
the local transport and reclaims the CLI; without terminal evidence the request
remains UNKNOWN. Local socket/process cleanup never proves provider termination.
SIGKILL, machine loss or killing the complete cgroup cannot be made graceful.
Already-running formal evaluations retain their original resource/cleanup contract.

Production HTTPS transport records an append-only `deepseek_request_events`
stream keyed by the budget reservation: dispatch intent, response status/request
ID, early `message_start` ID and usage, bounded periodic stream progress, usage
updates, terminal message and transport close. UTC and monotonic timestamps are
stored, with no API key, authorization headers, prompt, reasoning or answer text.
Text-delta progress is throttled to avoid one SQLite commit per token. Responses
are still buffered for native delivery. A complete `message_stop` frame and
complete valid usage are saved before returning output to the CLI; the transport
need not wait for socket EOF after a complete terminal event. Truncated frames
and streams cannot manufacture terminal receipts.

`deepseek-audit` opens the ledger read-only and exits 2 when blocked. It separates
completed, explicitly rejected, provably never-dispatched, UNKNOWN, and durable
terminal-evidence-but-unsettled records. Even the latter remain blocked until
trusted reconciliation; the audit does not clear scopes, refund quotas, restart
requests or convert local cleanup into cancellation. Legacy RESERVED/DISPATCHING
rows without sufficient evidence remain UNKNOWN in the audit while their original
rows are preserved. The runtime factory checks the target run, sibling runs and
the workspace artifacts tree before native CLI startup. Choosing a new run root
cannot bypass an old unresolved request in that tree.

These changes do not provide a DeepSeek remote cancel/retrieve API, and an early
message ID is not itself proof that such an API exists or that usage is settled.
They cannot reconstruct a missing historical receipt retroactively. Original
pending requests require original trustworthy evidence; no timer-based reset is
permitted. See the [official Messages compatibility documentation](https://api-docs.deepseek.com/guides/anthropic_api/).

Offline acceptance is available via `scripts/accept_deepseek_lifecycle_offline.py`.
It runs four actual installed `dsh` CLI processes against a **synthetic localhost
TLS provider**, checks graceful SIGINT, three terminal receipts, one deliberately
truncated UNKNOWN and all four local process-group cleanups. It makes zero model
requests and does not qualify model performance, real-provider cancellation or a
formal Warm/Market experiment.

An explicit user-authorized historical quarantine exception can be recorded by
an operator in the new run's `authority/deepseek-historical-exceptions.json`.
It binds the exact new root, old ledger, reservation IDs and a read-only logical
fingerprint covering the old rows and quota charges. It must explicitly retain
the historical budget and acknowledge unconfirmed remote termination. It cannot
exempt the current run, a changed historical snapshot, an unrelated scope or any
new UNKNOWN. The old audit remains BLOCKED and the old request is never retried
or reported as reconciled. This exception is not an agent-facing capability and
must never be generated automatically from a restart attempt.
