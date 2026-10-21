# Implementation coordination contract

Root owns `types.py`, `config.py`, `seeds.py`, `loop.py`, `cli.py`, `replay.py`, `reports.py`, `storage/` and top-level packaging/docs. Other owners must not edit those without asking root. All code is Python 3.12; standard library first. Optional packages are lazy imported. No implementation may silently replace real Coral/PACE with mock.

## Shared public data types (`maestro.types`)

All dataclasses are serializable with root `to_jsonable` helper. IDs/hashes strings; score floats in [0,1]; money Decimal.

- `Plan(plan_id, version_id, agent_id, parent_hash, target, mod_type, diagnosis, change, expected_effect, evidence_run_ids=(), category_detail='')` immutable.
- `HistoryRecord(experiment_id, agent_id, parent_score, champion_score, score, valid, target, mod_type, credit=Decimal(0), status='COMPLETED')` immutable. `.gain` is max(0,score-parent_score) if valid, else0. Every record is a formal outcome, never a precheck.
- `Submission(agent_id, plan_version_id, value, bid, eligible=True)` immutable.
- `AgentState(agent_id, parent_hash, parent_score, wealth=Decimal('2000'), history=[], lessons=[], pending_plan=None, last_planning_round=-999, needs_refresh=False, ucb_n=0, ucb_total=Decimal(0))` mutable, independent factories.
- `EvaluationResult(job_id,candidate_hash,score,valid,status='COMPLETED',cpu_seconds=0,wall_seconds=0,diagnostics={},instance_results=())` immutable.
- `RunConfig`: frozen with n_agents=4,slots=2,rounds=20,warm_opportunities_per_agent=10,refresh_interval=3,reward_scale=Decimal100,catchup_fraction=Decimal.25,bid_cap=Decimal100,initial_wealth=Decimal2000,seed=0,execution_mode='mock',benchmark='pace',profile='v1_cooperative',generation_calls=3,planning_calls=1,generation_window_seconds=300,resource limits, failures=dict. root will implement `config.to_dict()/from_dict()/load_config(path)`.

## Pure market/agents/bootstrap (owner market agent)

Implement stable signatures:
- `market.credit.reward(score,parent,champion,scale=Decimal('100'),catchup=Decimal('0.25'),valid=True)->Decimal`; `money(value)->Decimal` 6 decimal places half even.
- `agents.valuation.estimate_value(history,target,mod_type,parent,champion,scale=...,catchup=...)->tuple[Decimal,list[str]]`; no history raises BootstrapIncomplete.
- `agents.bidding.cooperative_bid(value,bid_cap=Decimal('100'))->Decimal`.
- `market.policies.rank(policy,submissions,states,round_index,seed)->list[str]` all eligible, seeded deterministic order, RR balanced, UCB warm stats.
- `agents.planner.needs_refresh(state,round_index,refresh_interval=3)->bool`; `mock_plan(state,round_index,seed)->Plan`; validate_plan(plan,parent_hash,allowed_evidence_ids); parse/repair schema helper.
- `agents.retrieval.build_context(state,board,token_budget=...)->dict`, own records only + manifest.
- `bootstrap.director.make_direction_briefs(agent_ids,seed)->list[dict]` deterministic offline; explicit provider injection for live once.
- `bootstrap.warm_start.balanced_schedule(agent_ids,slots,opportunities,seed)->list[list[str]]` exact opportunities each.
- `bootstrap.snapshot` serialize/clone state helpers, no SQLite dependency.
- memory functions and future extensions can be pure, fail closed when feature isn't profile-enabled.

## Execution / Coral backend (owner integration agent)

Implement generation/dispatcher/budgets/grants/phases/gateway/sandbox and `backends/base.py,mock.py,coral/*`. Root provides SQLite Ledger described below. Favor dependency injection for live runtime tests; don't need actual credentials. Hard native execution limits fail closed when unavailable; implement working Linux Docker/rootless runner if available and deterministic mocked containment for tests. Preserve Task metadata through real Coral classes when importable. Runtime-specific provisioning must be checked before launch; never run stock manager or daemon.

Core execution integration signatures:
- `dispatcher.Dispatcher(ledger, evaluator)`; `dispatch(ticket_id, job_id, expected_epoch=0)->EvaluationResult`. evaluator receives trusted ticket dict; enforce claim before evaluator invoked, verify candidate/panel/contract identities via ledger.
- `backends.mock.MockEvaluator(seed=0).evaluate(ticket:dict)->EvaluationResult`, policy-independent outcome from parent_score,target,mod_type and seed/round/agent. No SQLite dependency. CPU/wall deterministic simulated accounting.
- `generation.precheck_mock(agent_id,round_index,failures,revision=0)->tuple[bool,str]`; other generic controller/helper optional. Root loop owns invitation/backfill main loop.
- grant/budget functions/classes can use Ledger generic SQL/transaction or independent persistent local implementation; stable APIs documented in their docstrings and tests.

Root Ledger interface available shortly:
`Ledger(path)` context manager or `.close()`, `.connection` sqlite3 Row factory; `.transaction()` contextmanager; `.event(kind,payload,episode_id=None,round_index=None)`;
`.ticket(ticket_id)->dict` includes trusted binding fields + status;
`.claim_ticket(ticket_id,job_id,expected_epoch=0)->dict` checks ADMITTED,CAS claim,fence,returns ticket+fence;
`.record_result(result,fence)->None` validates ticket/job/candidate hash, idempotent same result, differing payload raises;
`.mark_running(job_id,fence,receipt)->None`; `.mark_unknown(job_id,reason)->None`;
`.existing_result(job_id)->EvaluationResult|None`.
Root will bind tickets to immutable artifact and contract/panel and store expected epoch. Dispatcher must not trust request-supplied ticket data; retries with completed result return it, running/claimed cannot relaunch.

## Task package (owner task agent)

Implement task base/mock aliases, `tasks/pace/{manifest,parser,verifier,runner,scoring}.py`, toy fixtures and focused tests. Standalone parser/scoring API choose clearly and document. Runner works via injected containment; real unsafe native subprocess is never default. `.gr` + dominating-set output strict; per-instance timeout0 vs aggregate invalid0 distinct. Formal 1 core,20x30 CPU sec+20 verification reserve. Actual heldout restricted, config missing prereqs fail closed. Can rely on `maestro.execution.sandbox` via lazy import once integration owner writes API; coordinate directly.

## Workspace and tests

All edits under this staging directory; final root copies controlled paths to D:/Becky_files/Documents/Personal_Repos/MAESTRO preserving existing old_docs and vendor checkout. Tests belong to your modules under tests/unit,contract,integration. Use `PYTHONPATH=src python -m pytest ...` or standard unittest if pytest unavailable. Avoid editing another owner's files. Report API deltas immediately. Root integrates entire V1 and records all unverified production prerequisites honestly.
