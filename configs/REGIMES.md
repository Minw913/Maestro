# Regime Registry

Current numbering (registry revision 5) for the four implemented Market regimes.
Warm Start is their common initialization stage.

| ID / display name | CLI policy | Allocation | Payment |
|---|---|---|---|
| R1 / 1 — Uniform | `uniform` | Seeded balanced round-robin; skip ineligible agents | Zero |
| R2 / 2 — Bandit-based allocation | `ucb` | `value + c * sqrt(2 * log(1 + total_n) / own_n)` | Zero |
| R3 / 3 — First-price naive | `first_price_naive` | Highest affordable bids: `min(value, wealth - holds)` | Own bid when formally admitted |
| R4 / 4 — Agent-adjusted valuation | `adjusted_valuation` | Highest affordable bids from assessed expected credit | Own bid when formally admitted |

R2 and R3 use exponentially weighted **actual settled credits** across all own
formal attempts, including Warm history and zero-credit terminal failures. Aging
advances only on another own settled attempt. UNKNOWN is not a zero sample.
For identical history and discount, R2 and R3 compute exactly the same valuation
`V`. R2's optional exploration bonus belongs to its allocation index, not `V`.
At `c=0`, its index equals `V`; R3 instead ranks the affordable bid and pays it.
R1 does not use value to rank. All share the existing reward/evaluation contract.
R4 starts with R3's settled-credit baseline. A fresh read-only Coral session
assesses each frozen proposal and returns probabilities and representative scores
for no-positive, catch-up and frontier outcomes. The controller applies the
frozen reward formula to calculate expected credit; the agent does not return
a free-form value. R4 then uses R3's affordable-bid and payment rules.
With the current `c=0`, R2 is greedy reward ranking rather than active UCB
exploration. R3 can differ because its bid is limited by wealth and it pays.

## Inspect the effective settings

```sh
python -m maestro regimes
python -m maestro regimes --config configs/experiments/pace_remote_deepseek.yaml
```

The first command lists IDs/rules. The second resolves actual Market settings and
prospective contracts without creating an experiment, constructing a backend or
calling a model. The list is not proof that a runtime/environment is qualified.
There is no second copy of tunable settings in this registry:

| Setting | Authoritative configuration |
|---|---|
| Reward discount (`valuation_discount_factor`, currently 0.8) | `experiments/<entry>.yaml` |
| R4 physical requests (`valuation_calls`, at most 4) and wall cap (`valuation_window_seconds`, currently 300) | `benchmarks/<benchmark>/market*.yaml` |
| R2 exploration (`ucb_exploration_coefficient`, currently 0) | `experiments/<entry>.yaml` |
| Market initial wallet (PACE currently 5 credits/agent) | `benchmarks/pace/market*.yaml` |
| Slots, rounds, reward scale, phase budgets | `benchmarks/<benchmark>/market*.yaml` |
| Model and runtime | `runtimes/<runtime>.yaml`, selected by the experiment entry |
| Candidate pipeline | `prebid_v2` for every executable regime; postbid archives are read-only search history |

Changing these settings keeps the regime ID, but changes the Market configuration
and contract hashes. Market credits are chosen at Market launch, independently
of the Warm checkpoint; resume cannot silently alter them. Other shared
Warm/Market changes still require a matching frozen experiment. Historical runs retain their
original settings, not the current defaults. Numbered families are not a claim
that all historical R2 runs used the same exploration coefficient.

`initial_wealth` is the configurable Market credits amount, also exposed as
`--credits` on run/resume. A positive value is finite. Zero means
sufficient funding: the ledger receives a finite balance equal to the frozen
round count times a conservative maximum per-round reward/bid. R3 and R4 still
pay their own bids; this setting makes wallet exhaustion impossible within the
frozen run. Choose it at Market launch; Warm has no wallet or credits setting.
Each Market arm freezes its own choice for resume.

## Select by number

```sh
python -m maestro --root artifacts/runs/<run> run \
  --bootstrap <checkpoint> --regime 3 --episode first_price_naive_seed0

python -m maestro --root artifacts/runs/<run> resume \
  --episode first_price_naive_seed0 --regime R3
```

`--regime 1` / `R1` select uniform; `2` / `R2` select bandit (`ucb`);
`3` / `R3` select first-price; `4` / `R4` select agent-adjusted valuation.
`--regime 3`, `--regime R3`, and `--policy first_price_naive` choose the same policy.
Choose either `--regime` or `--policy`, not both. Omit both to retain the existing
first-price default. `resume --regime` is an optional assertion against the stored
policy, not a request to switch policy. Listing/selection never authorizes a real
experiment on its own.

## Frozen per-episode contract

R4 requires real execution, `prebid_v2`, discounted settled-credit history,
and a native Coral backend with an actual request gate. After all agents freeze
proposals, independent read-only sessions share one assessment opening and
deadline. Each sees its proposed plan, frozen candidate and parent code, its
own settled trajectory and available historical code, screened technical
notes, current parent/champion scores, and baseline/reward formulas. It cannot
read the board, peer records, financial state or allocation rules. Read,
search and bounded batch tools are available; writes, shell, compilation and
evaluation are denied. Free-text screening is conservative and does not prove
semantic isolation from every possible coded reference.

Each assessment normally has at most `valuation_calls` physical model requests,
including its final structured answer. DeepSeek permits one extra tool-free
final-answer request only after the fourth response has proven output-cap
truncation or malformed JSON, terminal usage and local process recovery. A clean failure of that
regeneration removes the agent from this round; it may try again next round.
UNKNOWN usage or unproven cleanup still stops the episode. Other definite invalid
answers and clean timeouts use the frozen baseline. Usage, read traces, input
hashes, outcome and fallback reasons are stored for replay. The assessment
deadline is the earlier of its own wall cap and the original common generation
deadline: implementation, assessment, allocation, precheck, repair and cleanup
all share that generation window. The extra requests are separate from the
generation request allowance, and the 300-second cap is not a measured window
recommendation. Offline tests do not qualify a real provider or runtime.

New Market episodes store:

- `arms/<episode>/regime-contract.json`: readable contract and content hash.
- An immutable CAS copy, referenced by the `REGIME_CONTRACT_FROZEN` ledger event.
- The same contract in the episode report; replay verifies identity/hash/config binding.

The contract records ID, registry revision, allocation/payment rules, effective
settings, pipeline and complete config hash. Old episodes without such a contract
are reported with a null contract; no retrospective freeze is claimed.

Executable ID definitions live in `src/maestro/market/regimes.py`. Numbering
changes require a registry revision and version-aware replay. Revisions 1–3
used R1=first-price and R3=uniform; revision 4 applies the user's requested swap.
Frozen historical contracts, reports and ledgers keep their recorded IDs;
replay resolves them using their stored registry revision. CLI numeric aliases
use the current numbering. For older episodes, explicit policy names such as
omit `--regime` and use the stored policy; replay resolves the historical
registry revision. Revision 5 names current R3 `first_price_naive` and R4
`adjusted_valuation`, without changing allocation, payment or budgets. Old
policy strings remain in frozen records and are not accepted for new runs.

R2 display name is **Bandit-based allocation** (Bandit). Its internal `ucb` policy
identifier is retained for existing CLI/config/ledger compatibility. This label
change does not rewrite previously frozen contracts or change the formula.
