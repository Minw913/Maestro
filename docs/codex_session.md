# Local Codex login, strict autonomous runtime and proposal pilot

Codex can reuse an existing ChatGPT login. The local session adapter never reads,
copies, prints, or mounts `auth.json`, and requires no external API token. Codex
itself owns authentication and token refresh. Exported OpenAI API key/token
environment variables are removed from this adapter's child environment so they
cannot silently replace the selected ChatGPT login.

## Strict native runtime

`maestro.backends.codex_session:strict_v1_factory` now constructs the native
`StrictCodexBackend`; it is no longer an unconditional rejection stub.
`configs/warm_start.yaml` selects this path with four agents, 20 formal CPU
workers, five instance workers per candidate and five warm opportunities each.
This revision does not start Warm Start.
The runnable configuration pins `coral.codex_cli_version` to the qualified
installed binary. A changed CLI version requires repeating the native wire
qualification before updating that pin. `coral.codex_reasoning_effort: low` is also
frozen: both effective configuration and every native turn override desktop
preferences, and mismatches are rejected before inference.

The native `AutonomousCodexClient` uses Codex app-server with the existing
ChatGPT sign-in. Each phase has an ephemeral loopback Responses provider with
`requires_openai_auth=true`; the client obtains/refreshes authentication itself.
MAESTRO never reads or copies user `auth.json`. The quota proxy receives native
authentication headers only in memory and forwards them to the fixed ChatGPT
TLS origin, without writing headers or credentials to artifacts.

Before each actual upstream POST, `CodexQuotaProxy` reserves one durable request.
Planning allows one request; generation allows three total across tools,
continuations, retries, compaction and all repair revisions of the invitation.
Native retries and WebSockets are disabled, but every HTTP retry would still
consume another reservation. A fourth request never reaches the provider.
Quota exhaustion after completed tools can freeze the resulting code for the
ordinary toy precheck; it grants no formal evaluation. Completed responses with
missing token usage remain completed execution with unknown metering. Native
ChatGPT does not support an output-token cap parameter: **request count is a
hard limit; token ceilings are accounting estimates**, not claimed hard caps.

Planning receives frozen source/history/own memory in one structured request;
there is no interactive tool round trip under that one-request cap. Generation
uses a real native agent tool loop. Its only exposed tool is `maestro_tool`:
read/list/search/write/mkdir/remove, bounded file batches, and isolated shell/debug. `StageToolBroker`
exposes virtual solver, own private notes/skills, and read-only shared verified
memory. Shell runs a detached snapshot in a networkless container with bounded
CPU, memory, pids and wall time. Scratch edits do not persist: use the write tool
for lasting code changes. `action=batch` accepts at most eight file operations;
nested batches and shell within a batch are rejected. Every primitive operation
counts against the tool allowance and rechecks authorization. Batches stop at the
first error; prior writes remain. Use batches for related reads/writes because
Codex 0.156.1 can set `parallel_tool_calls=false`. Grants are checked before every operation and again
after waiting for CPU. The debug CPU budget is cumulative across repairs; an
interrupted invitation cannot reopen with a new tool budget after restart.
Fixed toy prechecks are separately bounded by the existing one-CPU-second
precheck contract and share the debug CPU permit pool; they are not extra model
requests. Closing the controller drains late generation callbacks before closing
the authority databases, preserving usage and grant cleanup after a deadline.

Native host shell/file tools, user plugins/MCPs, hooks and cross-project native
memory are disabled. Coral's own private/shared memory projections and private
native thread history remain available. The gateway replaces both classic and
native `input.additional_tools` manifests with its trusted tool definition and
validates buffered tool-call outputs before Codex can dispatch them. Neither a
model tool nor stock `coral eval` can open the market ledger or benchmark panel.
Coral WorkspaceStore/CAS, plan-first, EditGrant, EvalTicket, synchronous auction,
backfill and settlement remain the controller's responsibility.

`python -m maestro --root artifacts/connection-check codex-check --strict` checks
cached login, installed CLI version and app-server schemas without inference.
It does not prove live model entitlement or physical sandbox qualification.
Native wire tests use a fabricated test-only sign-in in an isolated CODEX_HOME,
the actual installed binary and a fake upstream; no user credentials are copied.
For the current live connection probe and unresolved status, see
`IMPLEMENTATION_REPORT.md` and `artifacts/strict-codex-validation/`.

An unknown provider request or container is not retried/refunded automatically.
The durable scope blocks further execution until trusted reconciliation.
`codex_phase_scopes`, the request reservations and numeric gateway diagnostics
retain this evidence. A new output directory is not a recovery procedure.

Authentication is supported by the [official authentication documentation](https://developers.openai.com/codex/auth).
[Non-interactive mode](https://developers.openai.com/codex/noninteractive) describes
cached-login reuse, JSONL events and schema-constrained final output. The installed
CLI also documents `--ignore-user-config`: configuration is ignored while
authentication still uses `CODEX_HOME`. The adapter invokes the native executable
directly, including the npm-installed Windows `codex.exe`; it does not interpolate
prompts through a shell.

## Explicit real connection qualification

The following command requires explicit authorization for up to five real
requests. It creates a single-use output root and never starts the experiment
engine, warm bootstrap, toy precheck, candidate shell, or formal evaluation:

```sh
python -m maestro --root artifacts/connection-qualification codex-qualify --live --model gpt-6-astra --reasoning-effort low --phase-seconds 300
```

The fixed envelope is one structured planning request, three generation requests
under a signed EditGrant, then one restored-memory read from a new native process.
The deadlines are 120/300/120 seconds. A batch reads private/shared memory, a second
batch writes code/private notes, and the third generation request finalizes.
Negative grant/shared-write checks and an extra local exhausted-quota request do
not forward model requests. A one-request continuity read may end at the expected
local continuation denial; every forwarded request still needs a terminal usage
receipt. Existing output roots cannot be reused, and there is no automatic retry
or directory rotation after failure.

Response framing is detected from a bounded SSE/JSON prefix, including streams
without a recognizable Content-Type. Tools and terminal frames are still fully
validated before delivery. Safe evidence includes HTTP status, response request
identity when available, event types, static error labels, request hashes and
usage. Only a proven connect/TLS failure before entering POST can be classified
never-dispatched; calls remain consumed. A truncated dispatched stream remains
UNKNOWN.

Historical diagnostic UNKNOWNs remain immutable, charged and separate. A fresh
qualification after a fix requires explicit authorization; success cannot prove
an older request finished. Use `--historical-unknown PATH --historical-sha256 HASH`
to attest preserved evidence. An UNKNOWN in the experiment being resumed still
blocks that experiment until trusted reconciliation. See the implementation report
for actual current results and the independent executor/data prerequisites.

## Profiles and what the budget means

`v1_cooperative` uses the strict native runtime above. A Codex CLI turn can
perform multiple inference requests; its turn count is never request accounting.
The older proposal transport below remains an explicitly different pilot and
cannot be substituted for the strict factory.

`codex_session_pilot` is an explicit alternative in
`configs/codex_session_pilot.yaml`. It reserves one planning proposal turn and at
most three generation proposal turns in a durable SQLite turn store. Receipts
carry `budget_unit=session_turn`, `session_turns=1`, `actual_requests=null` and
`request_count_unknown=true`. A failed/ambiguous request remains consumed across
restart. An unresolved dispatch blocks later scopes/episodes until operator
reconciliation; creating a fresh run directory is not an automatic recovery
procedure. Reserved turn counts include dispatch attempts whose completion is
unknown. Turn tokens are reported when the CLI emits a completed-turn receipt;
they do not establish an inference-request count. This pilot cannot be used as
an exact-budget reproduction of strict V1.

The proposal broker starts ephemeral read-only Codex sessions with shell,
subagents, hooks, memories, apps and web search disabled using documented settings.
It gives the model bounded host-filtered source/history JSON, accepts only JSON
proposals, and rejects unexpected tool events. It does not run an autonomous
coding loop or internal candidate tests. New Codex versions and their tool-disable
behavior require qualification. A local deadline kills the CLI process tree, but
cannot prove an already dispatched remote inference stopped; the accounting does
not refund such a turn. Response size is checked before acceptance; this is not
an independently qualified CPU/memory containment boundary for the Codex broker.

## Authorization is preserved

1. Planning returns a `PlanDecision` before any candidate editing. Its host parser
   validates owner, parent, taxonomy and mandatory own evidence.
2. Generation requires the scheduler's signed `EditGrant`. The host rechecks
   episode, round, agent, plan, parent, epoch, expiry and revocation before
   dispatch and after the model returns.
3. Strict generation writes through the grant-bound tool broker. The older
   proposal pilot applies a validated text-file manifest. Both enforce path,
   Git metadata, link and byte limits inside the private workspace, preserve
   existing executable permissions and freeze the candidate through Coral's
   `WorkspaceStore` content-addressed artifact interface.
4. The fixed toy precheck still uses the physically bounded Docker runner. Formal
   experiments still require a separately admitted and claimed `EvalTicket`.
   Codex login and generation grants do not authorize evaluation.
5. Auction, backfill, round barriers and settlement use the same market ledger.
   Only the older pilot uses the alternate session-turn budget unit.

Real PACE execution still requires its data/reference manifests, a seed solver,
qualified digest-pinned candidate image and Linux rootless Docker/cgroup limits.
No missing prerequisite falls back to mock. Windows supports authentication,
proposal probes, mock runs and interface tests; it does not acquire a fake
physical candidate sandbox merely because Codex is logged in.

## Interfaces

`CodexSessionClient(model=..., work_root=..., executable=...)` exposes
`login_status()` without inference, and `structured_proposal(context, schema,
purpose='planning', deadline=...)` for an explicit single proposal turn. Its
`plan(...)` method validates a `PlanDecision`; `propose_files(...)` requires a
verified edit grant. No method in the client executes candidate code.
An auth/proposal probe may use `model=None` to check the CLI's configured default;
the runnable pilot factory requires an explicit frozen model. Nonfatal CLI
diagnostic error items are counted without exporting their text; failed turns
and unexpected tool events are rejected.

The live factory is
`maestro.backends.codex_session.backend:session_factory`. It builds a
`CodexSessionBackend`, reusing Coral workspace/CAS, private memory and champion
projection behavior. The factory verifies the pinned Coral source, existing
ChatGPT login and real candidate sandbox. It uses predeclared startup direction
briefs; optional live SearchDirector inference is not enabled in this pilot.

`tests/contract/test_codex_session.py` uses injected CLI output and real local
workspace/SQLite operations. It proves interface and authorization behavior,
not live model availability or physical Linux containment. The CLI authentication
probe and a separately authorized real proposal are the live-login evidence.
