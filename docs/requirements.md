# V1 requirement-to-code acceptance map

Specification revision: `9ec29c82b48addf077137cd9b844e5ed4d9f51ff`.
Coral backend revision: `0123dfb939b35228cf2c1fde224cd0561e727408`.
The local plan is `maestro_plan_V1.md` at the repository root.

- Plans before edits, typed taxonomy, pending reuse and three-round refresh: `agents/planner.py`, `llm.py`, `loop.py`; `test_market_agents.py`, `test_v1_episodes.py`.
- Own history-only gain samples, current parent/champion credit conversion: `agents/valuation.py`, `market/credit.py`; exact 2.125-credit regression fixture.
- Cooperative first-price, seeded balanced uniform and warm-prefix UCB: `market/policies.py`, `loop.py`; unit ranking tests and 4-agent/20-round E2E.
- Provisional hold, bounded repair/backfill, final admission/debit: `execution/generation.py`, `storage/ledger.py`, `loop.py`; precheck, final failure, deadline and duplication scenarios.
- Ticket binding and real execution gate: `execution/dispatcher.py`, `backends/coral/grading.py`; missing ticket, stale epoch, wrong parent/panel/contract, duplicate claim and actual Coral Task serialization tests.
- Common directed warm start: `bootstrap/director.py`, `warm_start.py`, `snapshot.py`, `loop.py`; five opportunities per agent, five waves with four warm agents and sufficient CPU capacity, nonempty formal histories, cloned UCB statistics, equal main wealth. This latest setting supersedes the original ten-opportunity plan; current regression evidence is in the implementation report.
- Own memory and verified public board: `agents/retrieval.py`, `storage/projections.py`, `backends/coral/workspace.py`; no peer history, failure evidence catalog, cumulative H_t and independent repositories.
- Real CPU/wall/memory/pid/output/disk/network/inference containment: `execution/sandbox.py`, `agent_container.py`, `budgets.py`, `gateway.py`, `backends/coral/provider.py`; mock/contract validation and explicit live preflight. Physical qualification is separate.
- PACE graph/output adapter, verifier, score, whole-panel ticket: `tasks/pace/`; isolated verification worker, malformed/illegal/timeout cases, manifest hashes and disjoint normalized graphs.
- Configurable instance concurrency: `execution/cpu_pool.py`, `tasks/pace/runner.py`, `config.py`, `loop.py`; one solver/verifier pool per controller, frozen resource bindings checked by ledger/replay, ordered panel results and join-before-failure. Regression sources: `test_instance_workers.py`, `test_round_resources.py`, `test_container_quiescence.py`; tests are now authorized, actual Warm Start remains on hold.
- Strict tokenless Codex autonomy: `backends/codex_session/autonomous_backend.py`, `autonomous_client.py`, `quota_proxy.py`, `tools.py`; one/three actual request gates, native function-tool continuation, private/shared memory, cumulative debug CPU, no credential mounts and persistent unknown scopes. Source/native fake-upstream tests qualify local protocols, not live provider or physical sandbox operation.
- Final held-out: `tasks/pace/heldout.py`, `cli.py`; completed final champion only, separate authority, zero payment, no agent feedback.
- SQLite, immutable artifacts, precise money and crash recovery: `storage/`, `replay.py`, `loop.py`; replay anchors, idempotent result-before-settle recovery, UNKNOWN refusal and NOT_STARTED compensation.
- Costs, trajectories and provenance: `reports.py`; JSON/CSV/SVG exports, common prefix plus main plus heldout costs, funded-only calibration labels.
- Full-spec extensions: `market/full.py`, `reputation.py`, `screening.py`, `audits.py`, `batches.py`, `agents/forecast.py`, `strategy_doc.py`, `sharing/`; independent profile-guarded APIs and tests. The V1 CLI does not enable them.

Native Coral manager/daemon source patches are not part of the default composition backend. The plan documents the alternative hooks; the cloned Coral tracked sources remain unchanged.
