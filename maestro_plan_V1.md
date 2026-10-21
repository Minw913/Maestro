# MAESTRO on Coral：V1 接入与新增代码工程计划

状态：设计文档；本次只创建本文，不创建实现代码、修改 Coral 或修改 Overleaf。日期：2026-10-20。

## 1. 依据、实现范围与架构结论

规格依据来自 [Overleaf 项目](https://www.overleaf.com/project/6ac01a424a9a262f96ec2be9)，本地 Git 对象 revision `9ec29c82b48addf077137cd9b844e5ed4d9f51ff`：

- `specification_implement_V1.tex`：V1 实现与验收的直接依据，尤其 §2–§6、pending plans、common warm start、§4.3 的 pre-evaluation sanity check/replacement。
- `Notes/specification_1016_mingyi.tex`：全版模型与未来接口的依据。与 V1 有差异时，以所选 execution profile 明确规则，不能把两份规则混成一个实验。
- Coral 接入基线固定为 commit `0123dfb939b35228cf2c1fde224cd0561e727408`。下面的原生接口结论针对这个已检查版本，不代表所有后续版本。

**架构：MAESTRO 是外部实验调度和结算层；Coral 提供 coding-agent runtime、工具循环、session、版本与 grader 接口。** MAESTRO 决定何时允许规划、编辑、启动正式实验，以及如何结算；agent 在授权范围内自主读文件、检索、使用 memory、修改代码和调用工具。

V1 默认采用组合式 `ControlledCoralSupervisor`：新增适配层直接调用 Coral 的公开 runtime/sandbox/grader primitives，作为唯一的 agent 进程生命周期 owner。此路径不启动原生 `AgentManager.start_all()/monitor_loop` 和原生自动 grader daemon，因此可在 Coral 核心零修改的前提下建立硬授权边界。需要新增的进程、权限与预算控制仍是真实工程工作，不能用 prompt 或原生配置假装已经实现。

如果后续必须保留 Coral 原生 manager、自动恢复和 daemon，则采用 §12 的小型通用 hooks；市场算法仍放在 MAESTRO。两条路径不能同时管理同一 agent 的启动、重启与 sandbox。

### 1.1 对话要求与应对方案

1. **异步 harness 接同步 auction**：设置共同 phase deadline、冻结 round snapshot；agent 内部自主执行，phase 间统一 barrier。
2. **先交 plan，后改代码**：规划时 source/Git 只读；通过临时实施授权后才恢复 RW 环境；候选提交后重新冻结。
3. **控制小测试成本**：独立 preparation/generation budget；固定 toy smoke test；进程树 CPU、wall、内存、进程数与 LLM 配额一起限制。
4. **ticket 真正控制实验**：区分 EditGrant 与 EvalTicket；受信 dispatcher 在启动评估前核验、认领 ticket，候选程序无法直达 panel。
5. **自主共享适配**：私有 memory 与公开投影分离；V1 公开 verified board，全版再启用自愿 disclosure、验证与归因。
6. **顶层分配探索方向**：启动时 SearchDirector 生成不同 DirectionBrief，注入各 agent 的 role/context；它不参与每轮竞价或改写市场分配。
7. **market 前 warm start**：均衡非市场机会、标准正式评估，积累真实成功与失败；冻结一次共同 checkpoint，再克隆给三种 policy。
8. **结构化 artifact 与检索**：plan 的 target/mod_type、parent、evidence、版本必存；Git/CAS 存产物，SQLite 存授权、实验与市场账本。
9. **后续 bidding strategy doc**：保留私有 strategy artifact 接口；V1 的数值估值和出价依然由确定性公式生成。

### 1.2 规格要求补齐的骨架

- pending plan 的持久身份与逐轮 submission 必须分开，落选重投不产生新实验。
- provisional invitation、sanity pass、final admission 必须分开；正式启动前失败释放 hold 并递补，启动后失败消耗 slot。
- 同轮所有 parent/champion/history/public board 必须冻结，结果在共同结算时释放；不能边评估边更新后续 agent。
- warm checkpoint 必须包含历史 allocation/UCB 统计；每个 arm 的初始 wealth 相同。
- 共享 Git objects、原始日志和 Coral shared notes 需要真正的访问边界；独立 worktree 本身不能保护私有信息。
- 资源合同、真实 parser/verifier、身份绑定、恢复与幂等结算是正式实验的前置条件。
- 全版的 multi-plan、策略学习、reputation/audit、D2 disclosure/royalty 应有扩展点，并保持独立 profile。

## 2. Profiles 与 V1 固定合同

### 2.1 `v1_cooperative`

- `N=4`，`K=2`，warm 之后运行 `T=20` comparison rounds。
- 每 agent 每轮最多一个 active plan、一次 generation invitation、一个 final formal admission。
- 比较 `uniform`、`ucb`、`first_price`；前两者支付为零，first-price 支付自己的 bid。
- uniform 为 seeded balanced round-robin。UCB 使用 verified credit/正式 slot。三组采用相同 agent learning、资源合同与 warm prefix。
- planning 每 agent 每轮最多 1 次实际 LLM inference request；Coral native 路径每轮先执行独立 preparation，再提交单次 plan；旧诊断路径的 pending-plan 复用不用于该路径。每个被邀请的 agent，generation/repair 总共最多 3 次实际请求。
- `m_refresh=3`；每轮重新估值、重新出价；失败生成/precheck、实验后、parent 改变触发 refresh。
- 正式 slot：1 CPU core，20 个冻结 training instances，每个最多 30 CPU seconds，另有 20 seconds verification reserve；保留量 `620/3600 ≈ 0.172222 core-hour`。LLM、生成和 precheck 成本单列。
- 配置独立 wall、memory、pids、输出和文件大小限制。所有阶段的 CPU 和 wall 用量分别记录。
- `reward_scale=100`，`catchup_fraction=0.25`，`bid_cap=100`，main 初始 `wealth=20×100=2000 credits`。
- value 只来自本 agent 已正式评估的历史 gain；category 无样本时回退到本 agent 全部正式历史。
- 私有代码与 history 保留；公开 current champion 的只读代码及 evaluator-stamped summaries。

### 2.2 用户选择的 bootstrap 参数

从同一个 verified seed 出发，由 SearchDirector 给出不同探索方向，在 warm 阶段发展出不同 lineage；这是固定初始化流程，不提供初始化模式配置。

建议 `warm_opportunities_per_agent=10`，不是“总共 10 个全局 round”。N=4、K=2 时，均衡调度需 20 个 warm waves，合计 40 个 opportunities。机会、成功通过 precheck、正式 admission 和有效结果分别计数；每 agent 至少一个正式 evaluation record 是进入 main 的必要条件。

### 2.3 全版扩展边界

全版 profile 后续启用：多 plan/多 ticket、same-parent siblings、诊断与历史混合预测、自利/对抗策略、limited wealth、reputation、LLM screening、audits、D2 模块披露与 credit transfer。V1 的 schema 允许这些身份关系，但算法与表的扩展按后续里程碑实施。

需要显式版本化的规则差异：

- V1 的正式评估前失败释放 hold 并递补；全版文本的 failed/not-ready 实现仍可能消耗原 slot。未来 profile 要声明具体 failure policy。
- V1 的 `100[above + 0.25×catchup]` 对应全版 `alpha=100, beta_credit=25`，而不是 `beta_credit=0.25`。
- 全版 random uniform、策略 regulator、multi-plan batch reward 与 V1 balanced uniform、bid-only priority、single-plan 是不同机制。

## 3. 完整的拟建文件树

以下是将来创建的实现目录，全部位于当前 workspace 下的新 `maestro/`。现有 Overleaf checkout、Coral 参考源码和研究材料保留为参考；本次只创建根目录 `maestro_plan.md`。`__init__.py` 按 package 常规放置，下面集中声明，不重复占行。

```text
Project 2_ MAESTRO1/
|-- maestro_plan.md                         # 本文
|-- maestro-overleaf/                       # 已有规格来源
|-- tmp/coral-ticket-review/<coral-sha>/     # 已检查源码，只作参考
`-- maestro/                               # 后续实现根目录
    |-- pyproject.toml                      # CLI、依赖、Coral 固定 revision
    |-- README.md                           # mock / real / recovery 使用方法
    |-- .gitignore                          # artifacts、data、credentials
    |-- configs/
    |   |-- mock_smoke.yaml                 # 自包含 mock，完整 V1 默认参数
    |   |-- runtimes/coral_codex.yaml        # Coral runtime/model
    |   |-- deployments/remote.yaml         # 路径、资源与平台身份
    |   |-- benchmarks/pace/benchmark.yaml   # PACE 评估合同
    |   |-- benchmarks/pace/warm_start.yaml  # warm 调度与阶段预算
    |   |-- benchmarks/pace/market.yaml      # market 调度与阶段预算
    |   |-- experiments/pace_remote.yaml    # 组合入口
    |-- prompts/
    |   |-- search_director.md              # startup 方向分配
    |   |-- planning.md                     # retain/revise/withdraw + lessons
    |   |-- generation.md                   # 实施冻结 plan，受 EditGrant 约束
    |   `-- agent_contract.md               # phase、memory、禁止访问边界
    |-- src/maestro/
    |   |-- tasks/pace/taxonomy.py          # 冻结 PACE target/mod_type 词表
    |   |-- __init__.py                     # 各子包同样有 __init__.py
    |   |-- cli.py                         # bootstrap/run/report/replay
    |   |-- config.py                      # profile、schema、hash/preflight
    |   |-- types.py                       # 跨模块不可变数据合同
    |   |-- seeds.py                       # 分层 deterministic seed derivation
    |   |-- llm.py                         # 单次 planner 调用/受控 gateway client
    |   |-- loop.py                        # round state machine/barriers
    |   |-- reports.py                     # 成本、效果、fairness、market 输出
    |   |-- replay.py                      # 只读重算账本与一致性检查
    |   |-- bootstrap/
    |   |   |-- director.py                # DirectionBrief schema/分歧校验
    |   |   |-- warm_start.py              # 均衡非市场调度，真实经验收集
    |   |   `-- snapshot.py                # settled checkpoint/跨 arm 克隆
    |   |-- agents/
    |   |   |-- planner.py                 # plan schema、pending refresh
    |   |   |-- memory.py                  # lessons、5-store 视图、持久上下文
    |   |   |-- retrieval.py               # own target/mod_type 检索和 manifest
    |   |   |-- valuation.py               # history-only gain -> credit value
    |   |   `-- bidding.py                 # V1 cooperative deterministic bid
    |   |-- market/
    |   |   |-- eligibility.py             # plan/parent/evidence/余额校验
    |   |   |-- policies.py                # balanced uniform/UCB/first-price
    |   |   |-- credit.py                  # score gain、reward、Decimal
    |   |   `-- settlement.py              # holds/payment/reward/wealth 原子结算
    |   |-- execution/
    |   |   |-- phases.py                  # phase_epoch、deadline、转换协议
    |   |   |-- budgets.py                 # 累计 CPU/LLM/token 预留与计量
    |   |   |-- gateway.py                 # fail-closed LLM budget proxy
    |   |   |-- grants.py                  # EditGrant/EvalTicket/claim/fencing
    |   |   |-- generation.py              # invitation、precheck、去重、递补
    |   |   |-- dispatcher.py              # 唯一受信正式实验启动入口
    |   |   `-- sandbox.py                 # OS/container containment & teardown
    |   |-- backends/
    |   |   |-- base.py                    # AgentBackend/EvaluatorBackend 合同
    |   |   |-- mock.py                    # 无外部依赖、policy-independent dynamics
    |   |   `-- coral/
    |   |       |-- runtime.py             # delegate Coral runtime，phase prompt
    |   |       |-- supervisor.py          # 唯一进程 owner，stop/resume/limits
    |   |       |-- workspace.py           # 独立 repo、Coral memory 目录视图
    |   |       |-- submission.py          # plan/candidate API 和 CLI 兼容入口
    |   |       |-- grading.py             # TaskGrader wrapper / Task.metadata
    |   |       `-- results.py             # ScoreBundle -> verified result/public view
    |   |-- tasks/
    |   |   |-- base.py                    # task-agnostic task contract
    |   |   |-- mock.py                    # 可重复结果/故障模拟
    |   |   `-- pace/
    |   |       |-- manifest.py            # panel/reference/hash/seeds
    |   |       |-- parser.py              # .gr / solver output 严格解析
    |   |       |-- verifier.py            # dominating set 合法性
    |   |       |-- runner.py              # one-core 子进程与执行计量
    |   |       `-- scoring.py             # 逐实例与 aggregate internal score
    |   `-- storage/
    |       |-- schema.sql                 # SQLite versioned schema/constraints
    |       |-- ledger.py                  # 单一权威事务、events/outbox
    |       |-- artifacts.py               # immutable CAS + Git provenance
    |       `-- projections.py             # own/private 与 verified public exports
    |-- tests/
    |   |-- unit/
    |   |   |-- test_credit_value.py
    |   |   |-- test_policies.py
    |   |   |-- test_pending_plans.py
    |   |   |-- test_taxonomy_evidence.py
    |   |   `-- test_pace_parser_score.py
    |   |-- contract/
    |   |   |-- test_phase_permissions.py
    |   |   |-- test_budget_gateway.py
    |   |   |-- test_ticket_dispatch.py
    |   |   `-- test_coral_serialization.py
    |   `-- integration/
    |       |-- test_warm_clone.py
    |       |-- test_precheck_replacement.py
    |       |-- test_round_barriers.py
    |       |-- test_resume_settlement.py
    |       |-- test_private_views.py
    |       |-- test_v1_mock_episodes.py
    |       `-- test_pace_sandbox.py
    |-- fixtures/
    |   |-- toy_graphs/                    # 与正式/held-out 不重叠
    |   |-- schema_examples/               # synthetic，只有格式示例
    |   `-- mock_cases/                    # 失败、超时、重复、故障恢复
    |-- docs/
    |   |-- requirements.md                # 规格条款 -> 模块 -> 验收映射
    |   |-- coral_contract.md              # pinned API 与升级检查
    |   `-- operations.md                  # bootstrap/run/recovery/runbook
    |-- data/                              # ignored；真实数据仅受信 runner 可见
    |   |-- training/
    |   |-- references/
    |   `-- heldout/
    `-- artifacts/                         # ignored；见 §10 的运行产物树
```

这将规格中的 flat `storage.py/agents.py/regulator.py/evaluator.py/sandbox.py/scoring.py` 分为职责明确的子包：storage、agents、market、tasks、execution。CLI、公式、循环和验收语义保持一致。避免同时维护两份同名业务实现。

后续全版新增 `market/reputation.py`、`screening.py`、`audits.py`，`agents/strategy.py`，`sharing/disclosure.py`、`verification.py`、`attribution.py`。V1 不创建占位逻辑来改变实验行为。

## 4. 全生命周期 ASCII 流程

### 4.1 从 Strategy/SearchDirector 到 paired comparison

```text
[config + taxonomy + training contract + provider/runtime pin]
                            |
                  preflight / freeze hashes
                            |
            [verify initial solver(s); setup cost]
                            |
        +-------------------+---------------------+
        | SearchDirector (startup strategy agent) |
        | one shared call / fixed budget          |
        | output 4 different DirectionBriefs      |
        +-------------------+---------------------+
                            |
         common verified seed -> 4 private repos
         A: direction A   B: B   C: C   D: D
                            |
     +---------------- COMMON WARM START ----------------+
     | balanced RR, no market bidding/payment            |
     | 10 opportunities/agent => 20 global waves (K=2)    |
     | plan first -> edit grant -> precheck -> formal eval|
     | own histories + zero gains + errors + lessons      |
     | target/mod_type labels + measured retrieval data  |
     +---------------------------+------------------------+
                                 |
       verify equal opportunities + >=1 formal record/agent
                                 |
     [settled warm snapshot: parents / H / private memories /
      evidence / UCB statistics / artifact+contract manifests]
                                 |
                +----------------+----------------+
                |                |                |
         [uniform arm]      [UCB arm]      [first-price arm]
         same warm prefix   same prefix    same prefix
         wealth=2000        wealth=2000    wealth=2000
                |                |                |
         T=20 main rounds  T=20 rounds    T=20 rounds
                |                |                |
         final settled checkpoints / training trajectories
                                 |
          final-only held-out evaluation, separate authority
                                 |
      paired-seed reports: quality / cost / slots / calibration /
      generation failures / payments / rewards / wealth / replay
```

DirectionBrief 是算法探索方向；未来 BiddingStrategy 是私有市场策略，两者为不同 artifact。Director 不看到未来 arm 的结果，也不按 policy 分配不同起点。

### 4.2 一轮完整 iteration：plan -> reward -> settlement

```text
ROUND t: all agents use the SAME pre-round snapshot
  frozen: P_i, parent artifact, champion C, H_t, own history, wealth
                                  |
           +----------------------+----------------------+
           | pending plan eligible and refresh not due? |
           +-----------+----------------------+---------+
                      YES                     NO
                       |                       |
              reuse plan_version        PLAN_RO environment
              zero planning call        own memory + fixed toy diagnosis
                       |                <=1 LLM call: retain/revise/withdraw
                       +------------+----------+
                                    |
                    validate plan / evidence / parent
                  malformed or timeout -> ABSTAIN (pay 0)
                                    |
                 history-only value -> cooperative bid
                                    |
             planning deadline -> freeze ALL submissions
                                    |
       policy builds immutable priority order of eligible agents
                                    |
                invite up to K; first-price bid HOLD
                                    |
        +---------------- GENERATION WINDOW -----------------+
        | EditGrant(plan,parent,paths,budget,phase_epoch)     |
        | stop old tree -> RW sandbox -> resume/hydrate      |
        | invited agent implements frozen plan (<=3 calls)   |
        | freeze candidate -> compile + fixed toy pass/fail  |
        | precheck fail -> bounded repair -> new revision    |
        | exact content dedup; no training/held-out access   |
        +-------------+-------------------------+------------+
              FINAL FAILURE                    PASS
                      |                          |
        stop/revoke; release hold          freeze candidate hash
        payment 0; log gen costs           add to admission pool
        refresh plan next round                  |
                      |                          |
        next agent from frozen order             |
        at most one invitation/agent              |
        until K pass or common deadline ----------+
                                                 |
                 generation barrier: <= K final candidates
                                                 |
       atomic final ADMISSION: x=1, post own-bid DEBIT,
       issue EvalTicket bound to candidate+contract+job_id
                                                 |
       trusted dispatcher verifies ticket + atomic CLAIM
                                                 |
       TicketedGrader rechecks binding/fence before runner
                                                 |
       isolated formal runner: full frozen panel, 1 core
       failure/timeout/invalid AFTER start consumes slot
       NO refund, NO replacement after formal start
                                                 |
       verifier -> result + actual CPU/wall + diagnostics
       store immutable results, withhold agent feedback
                                                 |
              wait for ALL admitted jobs terminal
              (UNKNOWN => reconcile; do not guess)
                                                 |
       SIMULTANEOUS SETTLEMENT using frozen P_i and C:
       r_i=100([Y_i-C]+ + .25[min(Y_i,C)-P_i]+)
       w_i'=w_i - p_i + r_i; one settlement entry/agent
                                                 |
       update own best parent, global champion, UCB stats;
       project verified H_(t+1), release own results/memory
                                                 |
              durable SETTLED checkpoint -> ROUND t+1
```

未获邀请者不生成；被邀请但未正式 admitted 者可能发生 generation/precheck 成本，但 payment=0、没有正式 Y。最终 admitted 的不同 agents 至多 K，实际邀请数可能大于 K、最多 N。

## 5. 控制面、权限与同步语义

### 5.1 四种 owner

- `loop.py`：round/phase/snapshot 的唯一 owner，发 transition 请求。
- `ControlledCoralSupervisor`：agent 进程、session 恢复与 OS containment 的唯一 owner；按授权转换，不能自行进入下一轮。
- `dispatcher.py`：正式 candidate evaluation 的唯一 owner；只有这个服务拥有 panel 执行能力。
- `ledger.py`：plan、grant、ticket、payment、result、settlement 的权威持久状态。

agent 只持有自己的短期身份和受限 API；不读取 SQLite、其他 agent repo、原始全局日志或 grader credentials。受信服务拥有存储访问能力；market policy 通过窄查询接口读取必要记录，不扫描 private notes。

### 5.2 Phase 状态与读写范围

`PREPARE_RO -> PLAN_RO -> WAIT_ALLOCATION -> EDIT_AUTHORIZED_RW -> CANDIDATE_FROZEN -> WAIT_RESULT -> SETTLED`。未获配者从 WAIT_ALLOCATION 等到统一结算；precheck 失败者从实施阶段回到只读等待。

同一次 invitation 内允许 `CANDIDATE_FROZEN -> EDIT_AUTHORIZED_RW` 的 bounded repair 转换；调用数、CPU 与 deadline 继续累计，不能重置，旧候选 artifact 保持不可变。

- PLAN_RO：冻结 parent 的 solver、全部 Git 可达路径、public board 只读；自己的 scratch、notes 和 session 日志可写。只对冻结 parent 做固定 toy 诊断，禁止改 solver 后再称为 debug。
- EDIT_AUTHORIZED_RW：只有有效 EditGrant 覆盖的自身 source paths 可写；candidate 从绑定 parent 出发；scratch、私有 notes 可写。
- CANDIDATE_FROZEN/WAIT_RESULT：提交的 source tree、Git refs 和 artifact 冻结；阻断继续生成与新 LLM 请求，保留读取权限，进程可直接暂停/退出。
- SETTLED：harness 更新只读视图；下一轮获得新的 snapshot 后再启动。

权限转换协议：持久化 transition intent -> 暂停接收新工具/API 请求 -> interrupt 并确认旧 containment 内全部进程退出 -> 撤销旧 token/epoch -> 挂载新视图/权限 -> prepare_agent -> start/resume -> 记录转换完成。若中途失败，保持无编辑和无评估权限。已有进程不会因为一个数据库字段变化就自动失去 RW。

### 5.3 自主工具循环与严格调用上限

Coral/coding CLI 的工具与文件格式保留。V1 的一次 planning call 由 `planner.py + llm.py` 明确实现为单次结构化请求：Coral agent 先在独立只读 preparation 阶段自行读取 parent、memory、own history 和公开信息；单次 planning 请求接收其证据摘要，返回 retain/revise/withdraw 与 plan。这样不会把一个包含多次模型推理的 CLI session 冒充一次 call。

PLAN_RO 使用这个无任意 shell 的 PlannerClient；文件检索和固定 toy 诊断由 harness 预先执行，模型只返回结构化 plan/lesson 文本，不启动自主 coding CLI 工具循环。单独把 source 设为 RO 仍允许复制到 scratch 后偷偷写候选，因此 scratch 写入也由受限接口管理。Coral 的自主工具循环在获得 EditGrant 的 generation 阶段保留；memory/skills 的格式与 own/public 检索视图贯穿两个阶段。

generation 继续使用 Coral runtime 与 agent 的工具循环，但每个实际 provider inference request 都计数，总计最多三次；一次请求后的多个本地工具可以运行，后续模型请求仍须过配额。编译、toy precheck 和格式校验由确定性 harness 执行。若三次不足以生成候选，该邀请按 pre-evaluation failure 结束。

JSON repair 默认采用一次确定性本地语法修复，再做 schema 校验，不增加模型请求。需要模型修复时，必须仍在同一 planning cap 内；不能通过另开 session 增加一次隐含调用。这个解释写入 config/报告。

原生 `max_turns` 是 runtime/session 参数，不能当作真实 LLM call 或全轮 token 上限；原生 `agents.timeout` 是无输出 stall watchdog，也不能当作 phase wall deadline。预算跨 resume 累计，heartbeat 不产生额外免费请求。

### 5.4 屏障与截止

planning deadline 到达后冻结 submission 集合；超时者 abstain。generation 使用同一个绝对 deadline，递补者只使用剩余窗口，但保持自己的最多三次调用 cap；到期取消并确认回收所有未冻结生成环境。正式 evaluation 在 generation barrier 之后开始，避免结果影响递补和其他 candidate。

任何 agent 都不能提前看本轮正式结果。结果先进入受信 ledger，在所有 admitted jobs 达到明确 terminal 状态后共同结算并生成 H_(t+1)。没有 candidate 的轮次也做零支付、零奖励的正常 settlement。

## 6. Coral 复用边界与具体接入文件

下面标注 R=直接组合复用，A=新增 MAESTRO adapter，P=选择原生 orchestration 路径时才需 Coral 通用 patch。默认 V1 没有必须执行的 P。

### 6.1 Runtime、session 与 workspace：R + A

原生 [coral/agent/runtime.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/agent/runtime.py) 提供 `AgentRuntime.start(...) -> AgentHandle`、`extract_session_id(log_path)`、`AgentHandle.interrupt()/stop()`；[registry.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/agent/registry.py) 提供 `get_runtime(name)` 与自定义 runtime 注册。

新增 `backends/coral/runtime.py` 和 `supervisor.py`：

- 将 worktree、phase instructions、model/options、resume_session_id、gateway 和 sandbox 传给原 runtime，复用 coding CLI 内部工具循环。
- session ID、原生日志、CLI settings 放在各 agent 私有稳定目录。先 interrupt，等待 CLI 保存 session、日志 flush 和旧 containment 全部退出，再提取最终 session ID；无法可靠恢复时 fresh session hydrate。恢复时仍检查 phase/grant/budget。
- 同一 arm 内尽量原生 resume；跨 policy arm 先用 fresh session 从共同 semantic snapshot hydrate。原生 session 是否能安全 fork 要另做兼容验证，不能复制一个 session ID 就声称三组独立。
- workspace adapter 补齐原 manager 的 runtime-specific provisioning，包括 `setup_claude_settings`、`setup_codex_settings`、`setup_opencode_settings`、instruction 生成和 breadcrumbs。只调用 `runtime.start` 不代表 manager 的准备工作已完成。

[workspace/worktree.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/workspace/worktree.py) 的 `create_agent_worktree(repo_path,agent_id,agents_dir)` 可以复用，但先给每 agent 建独立 Git object store；同一 repo 下的 branches/worktrees 共享 objects，无法保证私密。禁止以 hardlink/alternates 方式把其他 agent 私有对象暴露给它。

`setup_shared_state(...)` 仅选择性复用目录约定。MAESTRO 自己生成 own/private 与 public verified projections，不直接采用所有 notes/skills/logs 共享的默认布局。原生 shared checkpoint repo 通常位于 `.coral/public/.git` 或 `.coral/islands/<id>/.git`，不应假设存在 `.coral/.git`。

### 6.2 SearchDirector 角色注入：R + A

新增 `bootstrap/director.py` 在启动前输出每 agent 的 DirectionBrief；使用 [config.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/config.py) assignment 的 `count=1`、不同 `runtime_options.role_file`，或在组合式 supervisor 中执行等价 role provisioning。

[workspace/project.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/workspace/project.py) 的 `seed_agent_role(coral_dir,agent_id,source,...)` 可复用；role 是公开方向信息，私有 strategy 不放在 role 文件。冻结 plan 的权威内容在 SQLite/CAS，不依赖 agent 可编辑的 role。

### 6.3 Sandbox 与预算：R 接口 + A enforcement

[sandbox/protocol.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/sandbox/protocol.py) 提供 `validate/start/prepare_agent(ctx)/stop`，返回 `AgentSandboxSpec(command_prefix,env)`。新增 `PhaseSandboxProvider` 依照持久 phase/epoch 构造容器或 OS wrapper，覆盖子进程，分别挂载 RO/RW 目录。

原生文件/网络 sandbox 不自动提供完整的 CPU、memory、pids、LLM quota。新增 `execution/budgets.py/gateway.py/sandbox.py` 落实这些约束；受控 gateway 需要请求前原子 reserve，不能只读事后 usage。Coral gateway 的归因/日志可选择复用，header callback 不能承担 fail-closed 授权。

### 6.4 提交命令：A；原生入口不能直接当授权

原生 [hooks/post_commit.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/hooks/post_commit.py) 的 `submit_eval(...)` 会 commit 并写 pending；它没有 ticket 参数。原生 [grader/daemon.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/grader/daemon.py) 会 drain pending 并提交 worker，不能直接充当市场 admission。

默认提供这些新增 agent-facing 命令，内部访问本地 authenticated API：

```text
maestro agent plan-submit --file plan.json
maestro agent candidate-submit --grant <id> --worktree <path>
maestro agent status
maestro eval ...       # candidate-submit 的兼容入口，不立即运行 panel
```

可选 PATH shim 将 `coral eval`、`coral eval --tune`、`coral wait` 路由到同一受控 adapter；查询类命令读取安全 projections。即使 agent 绕过 shim，原生 daemon 不运行、正式 panel 不可见、grader 无受信 claim，也无法启动正式评估。**PATH shim 是兼容入口，不是安全边界。** 默认提示词应使用明确的 MAESTRO 命令，避免原生 eval 写 pending 后无限等待。

原生 pending 的 commit/文件记录可作为 provenance 导出；不能把“文件存在”解释为获配。funding 状态保存在 MAESTRO，不随意给 Coral Attempt 新增 status，因为原生 poll/wait 的 terminal 判断依赖 `status != pending`。

### 6.5 Grader：R + A；避免 metadata 丢失

[grader/loader.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/grader/loader.py) 的 `load_grader(config,coral_dir)` 根据 `grader.entrypoint` 创建 grader；[task_grader.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/grader/task_grader.py) 的 `TaskGrader` 可派生。

新增 `TicketedGrader(TaskGrader)`，由受信 dispatcher 调用 `await grader.grade(codebase_path,tasks)`。使用可信方构造的 `Task.metadata` 显式传递 `episode_id/round_id/agent_id/plan_version_id/parent_hash/candidate_hash/job_id/ticket_id/phase_epoch/contract_hash/panel_hash`。

在 `load_grader` 前，调用或等价复用 `setup_grader_env`，创建隔离 grader venv，安装固定 Coral、MAESTRO wrapper 及任务依赖，准备受信 private inputs 并校验 hashes。loader 只负责加载，不会替代这些初始化；缺 worker interpreter 时明确失败。

[subprocess_grader.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/grader/subprocess_grader.py) 会序列化完整 Task，但任意自定义 `**kwargs` 不会自动全部传到 worker，所以 IDs 不能只塞 kwargs。metadata 只是声明，仍要与受信 ledger 核验。

wrapper 在昂贵 runner 启动前核验一次 fence 与 ticket；dispatcher 在启动 grader worker 前已做第一次认领。grader venv 是可信依赖隔离，运行候选程序仍需独立、不带 credentials 的 sandbox。结果经 `results.py` 转换为 VerifiedResult，完整原始 ScoreBundle 私有保存，公开内容由明确字段白名单生成；`is_public=False` 不是整个反馈对象的机密开关。

setup 初始 solver 验证和最终 held-out 也走受信 dispatcher，但使用独立 `purpose=SETUP/WARM/MAIN/HELDOUT` authority 与预算；它们不能成为 main ticket 的绕路入口。

## 7. 工程接口合同：全部为拟新增接口

下列接口名是设计合同，不声称 Coral 已有这些 methods。agent-facing 请求统一从短期身份解析 agent_id，不接受调用方任意填写 owner。拒绝时写可审计 reason code，但不泄露其他 agent 私有状态。

### 7.1 Round 与 phase

`RoundController.open_round(episode_id,t) -> RoundSnapshot`，位于 `loop.py`：在一个事务中冻结 parent artifact/score、champion、H_t、history cutoff、wealth、policy state、seeds、全部预算与截止。snapshot hash 是本轮所有请求的上游绑定。

`Supervisor.transition(agent_id,expected_epoch,new_phase,grant_id?) -> TransitionReceipt`，位于 `execution/phases.py` 和 Coral supervisor：采用 compare-and-swap epoch，串行停止和重新部署权限。过期请求拒绝；start/restart 必须再次读取当前状态。

`Budget.reserve(agent_id,round_id,phase,resource,amount,request_id)` 与 `record_usage(...)`，位于 `execution/budgets.py`：同一 request_id 幂等；同时考虑已用和在途 reservations，跨 crash/restart 不重置。

### 7.2 Plan 与逐轮 submission

`Planner.prepare_context(snapshot,agent_id) -> ContextManifest`：deterministic own retrieval、选取公开 H_t，记录包含和截断的 IDs、token 预算与所有输入 hash。

`submit_plan(snapshot_id,plan_payload,idempotency_key) -> PlanVersion`：校验 parent、taxonomy、字段、evidence 权限和 normalized uniqueness；冻结原文和规范化表示。字段至少包括：

```text
plan_id, plan_version_id, agent_id, created_at, parent_commit, parent_tree_hash,
target, mod_type, category_detail, diagnosis, change, expected_effect,
evidence_run_ids, prompt_hash, context_manifest_hash, content_hash
```

全版扩展 `depends_on`、word limits、diagnostic forecasts；V1 可在 schema 中记录空 depends_on，但不计算 royalty。plan 是修改假设，不包含已经实施的 candidate。共享标签词表由 config 冻结：

- target：`reduction, initialization, local_search, tie_break, restart, postprocess, data_structure, time_management, other`。
- mod_type：`parameter_change, rule_replacement, new_component, removal, reimplementation, other`。

`submit_for_round(plan_version_id,snapshot_id,refresh_reason) -> Submission`：保留 pending plan 版本身份，生成独立 `(episode,round,agent)` submission；harness 计算 value/bid/evidence manifest。eligible 未执行 plan 无需模型即可重投；一次 refresh call 可 retain、revise 或 withdraw。revision 必须新 version；parent 改变后不能静默把旧 plan 重绑到新 parent。

`eligibility.py` 校验：plan 属于当前 agent、parent 与 snapshot 一致、引用可解析、未被执行/作废、字段合法、未过期、可支付 hold。只对 active eligible submissions 排序，不能为 malformed plan 自动编一个替代方案。

### 7.3 临时实施授权与 hold

`Policy.rank(snapshot,submissions) -> PriorityOrder`：返回完整确定性顺序、ties 的 seed、分配分数与解释字段。整轮冻结；不能因某位 agent 的 toy 表现、rationale 或晚到结果重新排序。

`Generation.invite(next_agent,order_id) -> EditGrant`：同事务记录 invitation、first-price hold、预算上限与 grant。hold 只预留余额，尚不是 payment；uniform/UCB 的 hold 为零。

EditGrant 字段：`grant_id,owner,episode,round,plan_version,parent_tree_hash,allowed_paths,phase_epoch,expires_at,generation_budget_id,max_model_calls=3,status`。不给 training/heldout 权限。只有 supervisor 可将 grant 转成实际 RW sandbox。

`Candidate.submit(grant_id,tree_manifest) -> FrozenCandidate`：tree_manifest 只是调用方声明。authority 确认有效 phase/owner/parent，停止全部 writers 后自己捕获受限 source tree，校验 parent、拒绝越界 symlinks/路径、排除 credentials 与无关 scratch，并自己计算 deterministic source-tree SHA-256，同时保存 Git commit 与 diff。grader 只读取这份不可变 artifact，不再读取 agent 的可变 worktree。Git commit ID 与 candidate content hash 是两个字段，不能混用。

`Precheck.run(candidate_id,toy_contract) -> PrecheckResult`：固定 compile/syntax 与固定 toy，只有 pass/fail 和错误诊断；没有训练分数、gain 或正式 reward。每 candidate 的 toy/compile 上限同等预声明。

失败后若同一 invitation 仍有 generation 调用/CPU 余额与共同 deadline，可走 `Generation.repair(invitation_id,precheck_id)`：保持原 plan/parent 和 hold，重新发带新 epoch/revision 的 EditGrant，修复后捕获新的 immutable candidate。最多三次 LLM 请求是整次 invitation 的总和；另预声明 precheck/revision 尝试数上限，防止纯本地无限重试。旧 candidate 不原地修改；需要换研究假设时本轮终止，下一轮重新提交 plan。

`Generation.finalize_pool(order_id,deadline) -> AdmissionPool`：内容去重、保留 higher-priority copy，seeded ties。修复预算耗尽、无法修复、重复或到期的候选释放 hold、终止环境、记录 refresh reason，按 frozen order 递补。repair 不是第二次 invitation；每 agent 最多邀请一次，共同 deadline 后停止。历史已评估的相同 content 不再产生新 credit；复用同一 pending plan 并不等于重复 candidate。

### 7.4 Final admission、EvalTicket 与真实启动

`Admission.commit(pool,snapshot) -> EvalTickets`：generation barrier 后，确认通过者不超过 K，每 agent 至多一个，全部 candidate/contract/panel 已冻结；原子写 final allocation `x=1`、将 hold 转为 payment debit、生成稳定 job_id 和 dispatch outbox。余者 release hold，`x=0,p=0`。

EvalTicket 的可信记录包含：

```text
ticket_id, purpose=MAIN, episode_id, round_id, agent_id,
plan_version_id, candidate_id, parent_tree_hash, candidate_tree_hash,
contract_hash, panel_hash, task_seed_manifest_hash,
job_id, slot_index, resources, admitted_payment, phase_epoch,
status=ADMITTED, issued_at, execution_deadline
```

agent 可以读 ticket receipt，但不能通过复制 receipt 创造新的权限。authorization 的真相是受信 ledger 中的状态；同一 ticket/job 只能认领一次。

`Dispatcher.claim_and_launch(ticket_id,job_id,expected_epoch)`：

1. 受信事务校验当前 phase、purpose、owner、final admission、candidate/contract/panel hashes、额度和未撤销状态；CAS `ADMITTED -> CLAIMED`，生成单调 fencing token，写 launch intent。
2. 仅此服务构造 grader tasks、只读 candidate checkout、评估 sandbox 和 panel 挂载；agent CLI 不持有这些权限。
3. worker 启动后 TicketedGrader 对 claim/fence 和 hashes 再校验；通过后启动受限候选进程。
4. 更新 RUNNING 与 executor receipt，实际资源计量归同一个 job；完成后记录 RESULT_RECORDED，等待全轮结算。

**关键阻断点有三个**：agent 无 panel/执行 credentials；dispatcher 在 worker 前拒绝无有效 admission 的请求；grader 在昂贵 runner 前拒绝失配/重放 ticket。将一个字符串 ticket 放进 prompt 不构成上述任一保障。

final admission 是计费预提交边界：完整候选通过 precheck 后、正式启动之前记账。已实际开始的 solver crash/timeout/invalid 消耗 slot 和 payment。DB admission 不证明物理执行已经开始；dispatcher/基础设施启动异常另标 `NOT_STARTED/INFRA_FAILURE/UNKNOWN`，依据 §11 的恢复约定处理。不能借基础设施异常自动补 slot、改成生成失败或造一个有效分数。

### 7.5 Result 与 settlement

`Results.record(job_id,fence,result_hash,metrics) -> VerifiedResult`：同 job 幂等，重复相同 payload 返回既有记录，矛盾 payload 报错；结果包含 validity、status、逐实例 metric、aggregate、CPU/wall/limits、candidate/provenance。

`Settlement.settle_round(round_id,expected_snapshot_hash)`：只有所有正式 jobs 的状态可判定时执行；冻结基线计算每 agent 的 payment/reward/wealth、parent 提升、champion、UCB；一事务写 settlements、wallet entries、round=SETTLED 和 publication outbox。payment debit 已在 admission 记过，settle 只引用它，绝不能再扣一次。

`Projection.publish(settled_round_id)`：将私有结果送回各自 memory，将 verified public board 发布为同版本只读目录；先 durable settlement，后发布，重试幂等。新 round 只从完整 publication/checkpoint 开启。

## 8. 估值、分配与结算的确定性规则

### 8.1 History-only value

每个正式 funded evaluation 记录：`G=max(0,Y-P_at_that_experiment)`；invalid、failed formal evaluation、未改进时 G=0。未 admitted 的生成/precheck failure 不产生正式 Y/G 样本。

对于本轮 plan，先选本 agent 历史中 exact `(target,mod_type)` records，包括失败和零收益；为空则用本 agent 的全部正式 records。`other` 按普通标签精确匹配。数值估计使用完整可用样本集；prompt retrieval 的 token 截断不能改变估计样本集合。

用当前冻结 parent P 和 champion C，将每个历史 G 转成 `Y'=min(1,P+G)`，计算 `R(Y';P,C)`，取均值作为 `v_hat`。V1 `bid=min(100,v_hat)`。warm 后仍无正式样本属于 bootstrap 不完整，不能引入人工 prior 或调用 LLM 编造 gain。

fixture：P=.70，C=.75，历史 gains=[0,0,.08,.08]，hypothetical rewards=[0,0,4.25,4.25]，所以 v_hat=bid=2.125 credits。这里是历史样本，不是初始化 prior。

### 8.2 Policies

- first-price：按 bid 降序与冻结 tie seed 排序；记录 cutoff 供分析，但最终 payment 是自身 bid。rationale、预测误差和信誉不改变 V1 order。
- uniform：balanced round-robin，seed 决定初始顺序和预声明 rotation；skip ineligible 时记录原因，不能让失败 agent 额外多次生成。
- UCB：`mean_verified_credit_i + ucb_exploration_scale * sqrt(2*log(1+sum_j n_j)/n_i)`，`ucb_exploration_scale=1 credit`；n_i=0 的 eligible agent 为 infinity。n_i 是正式 slot 数，mean 包含正式失败的零 credit。所有统计在轮末更新，warm 统计作为共同 prefix 克隆。

### 8.3 Credit、wealth 与计量

`R(Y;P,C)=100*(max(0,Y-C)+0.25*max(0,min(Y,C)-P))`。本轮所有 agents 使用同一 pre-round C；parent P 是各自 best verified own lineage，不能每轮强制换成 champion。

- 有效 Y=.78、P=.70、C=.75：reward=4.25；Y=.74：reward=1。
- 未 admitted：Y 未定义，reward=payment=0；候选整体 invalid/整体执行失败：reward=0。单个 instance timeout 只令该 instance 得0，其他有效 instances 仍聚合，候选仍可能改进并获得 reward。parent/champion 只取有效改进，不倒退。
- `wealth_after=wealth_before-payment+reward`；已释放 hold 不参与最终 payment；主实验财富 ample，不能隐藏加入生存/破产机制。
- `credit_per_plan` 与 `wallet_reward_per_agent` 分开命名。V1 q=1 时相等；全版多 siblings 时 wallet 用 phi(best sibling)，不是所有 per-plan credit 的总和。
- `system_progress` 是全局 champion 的提升，不能把各 agent rewards 相加当作系统性能。

金额采用 Decimal，配置固定精度和 ROUND_HALF_EVEN，在固定记账边界量化；SQLite 以规范 decimal 字符串存储，Python Decimal 运算后写入，SQL 不以浮点 SUM 做权威重算。原始 gain/score 与量化 credit 同时保存。每个 policy 使用相同量化规则与 tie 规则。

## 9. Warm start、方向多样性与 few-shot

### 9.1 SearchDirector

输入：task contract、同一 seed 代码/verified diagnostics、允许 taxonomy 和总 agent 数。输出每个 DirectionBrief 的 `direction_id,agent_id,objective,allowed_focus,initial_hypotheses,diversity_rationale,seed_artifact,prompt_hash`。限制为启动期一次共享调用，单列 setup token/cost；失败时使用预声明的人工 direction set，不能按未来 outcomes 临时选择。

例如四个方向分别侧重 reduction/initialization、local_search、restart/tie_break、data_structure/time_management；这是方向示例，不是 taxonomy 的强制分区。agent 可以据证据修正路线。角色不同不保证实际代码多样性，应报告 target/mod_type 分布、候选 hash、lineage 和有效变更覆盖率。

### 9.2 非市场 warm protocol

1. 公共 verified seed 与四个 private repos 初始化完毕，记录 setup evaluations；initial solver record 与 modification history 分开。
2. 使用确定性 balanced schedule；`warm_opportunities_per_agent=10`，每 wave 最多 K 个邀请。warm 不竞价、不 hold、不产生 market payment；formal evaluation 的合同和验证器与 main 相同。
3. 每个 warm wave 仍先提交计划，再获得 EditGrant、实施、toy precheck、正式评估与共同可见性更新。按同等 opportunity budget 计数；precheck fail 仍计一次 opportunity，不记正式 outcome。warm 内不额外向某位 agent无限递补。
4. 完整记录实际 formal evaluations、零 gain、invalid、失败与 generation failures；预期能获得不同 lineage 和非空真实 gain history。
5. 结束时每 agent 必须有至少一个 formal record，机会数相等。若不满足，标记 BOOTSTRAP_INCOMPLETE；如配置允许补充，只增加预声明的等量 warm block，并单列成本，不能加入合成样本填洞。
6. warm 的每个 formal result 计算同一 credit rule，积累 n_i 与 reward sum 等 allocation/UCB 历史；没有发生的 bid 不伪造为 market bid。
7. 在 fully settled boundary 冻结共同 checkpoint；三种 policy 的 parent、private histories、lessons、public H、historical allocation statistics 和 config manifests 一致，main wealth 均为 2000。

warm 内可以串行/并行执行任务，只要满足固定合同、均衡机会、可重放的顺序和 checkpoint 一致性。本实现用明确 waves，便于构建共同经验与核算；main auction 一律走同步 barriers。

### 9.3 Taxonomy 与 few-shot

区分两类材料：

- `schema_examples`：人工/合成的 plan JSON 格式与 target/mod_type 分类示例，只教字段含义，不携带伪造 verified score/gain，不进入估值。
- `measured_examples`：warm 或既往允许历史里的真实 plan->candidate->formal outcome，包含失败/反例及 parent/champion 基线。默认只检索该 agent 私有 records，加公开 verified board 允许内容。

`retrieval.py` 按 exact labels、recency 和 evidence relevance 取有限 records；保留 source IDs、selection rule、omitted counts、token truncation、taxonomy version，形成 ContextManifest。不能只筛成功例来形成数值估计；few-shot token 限制只影响模型上下文，不删 ledger 中的失败。

历史自由文本 labels 的一次规范化在 warm snapshot 前完成，保留原值、mapped 值、mapper prompt/version 或人工规则和 provenance。缺失 target/mod_type 的真实 legacy record 可用于 own-all fallback，但在明确标注前不能伪装 exact-match 样本。合成说明与 measured outcome 清晰分区。

## 10. Artifact、SQLite 与信息边界

### 10.1 存储职责

Git 保存可重建的代码版本和 diff；CAS 保存大文件、prompt/context、stdout/stderr、结果和快照；SQLite 保存关系、授权、事件与账目。JSON/JSONL、Coral Attempt 和共享目录都是可重建 projections/exports，不是第二套市场真相。

只用 Coral 的单文件 Attempt JSON 无法原子提交 hold/debit/result/reward/wealth/round，SQLite 在本机单 scheduler 场景适合这个事务边界。V1 无需独立数据库服务器；远程多 scheduler 才评估替换数据库。DB 文件不挂载给 agents 或不可信 candidate。

### 10.2 Maestro artifact 清单

V1 active：

1. **DirectionBrief/DirectorRun**：启动策略、分工和调用 provenance。
2. **SubmittedPlan/PlanVersion**：规范化 target/mod_type、parent、diagnosis/change、evidence、版本与 hash。
3. **RoundSubmission**：本轮 plan 引用、reuse/refresh reason、value、bid、source sample IDs；与持久 plan 分离。
4. **Context/EvidenceManifest**：所有数值样本与 prompt-selected records 分开记录，含权限、cutoff 与截断信息。
5. **Lesson**：claim、支持/反对的 experiment IDs、hypothesis/supported/refuted、revision。属于解释，不覆盖不可变实验事实。
6. **PriorityOrder/Invitation/Hold/EditGrant**：临时获选、实施权限、预算和失败/递补链。
7. **FrozenCandidate/PrecheckResult**：tree hash、Git commit、parent diff、toy contract、编译/检查日志；没有正式分数时不补造。
8. **FinalAllocation/EvalTicket/DispatchClaim/ExecutionReceipt**：slot、计费、真实启动与 fencing。
9. **VerifiedExperiment/PerInstanceResult/ResourceUsage**：plan、parent、candidate、contract、score、validity、CPU/wall 与 executor provenance 的闭环。
10. **Settlement/WalletEntry**：hold release、payment、reward、wealth 及对应 transaction。
11. **RoundSnapshot/PublicBoardVersion/PublicationReceipt**：冻结信息集及统一反馈时点。
12. **WarmCheckpoint/ArmCloneManifest**：共同历史、parents、statistics、refs、初始化与数据合同。
13. **ProviderCall/BudgetReservation/PhaseTransition/Event**：实际调用、用量、取消、恢复与安全边界。
14. **ReportManifest**：paired seeds、全部成本、held-out purpose 和 replay 状态。

全版 extension：

- **BiddingStrategyDoc/StrategyRevision**：私有目标、市场观察、出价/quantity/disclosure 策略、支持证据和至多固定数量 standing lessons；不同于算法 DirectionBrief。
- **DiagnosticForecast/ReportOverride**：p_D/g_D、历史融合、agent reported forecast、真实 value 的区分。
- **PlanBatch/SiblingGroup**：多 plan、同 parent、dependence、per-plan credit 与 batch best reward。
- **DisclosurePackage/Module/Finding/VerificationReceipt**：agent 自选的可披露产物、证据、签名与 visibility。
- **Dependency/Attribution/CreditTransfer**：depends_on、代码血缘、receiver->source 的固定 eta 转移；transfer 守恒，不额外铸币。
- **ReputationSnapshot/AuditAssignment/ScreeningDecision**：A/B/N、mu/U/zeta、审计保留 slot、screening prompt/result 和规则版本。

### 10.3 运行产物树与访问范围

```text
artifacts/<experiment_id>/
|-- authority/                             # 仅受信服务
|   |-- ledger.sqlite
|   |-- config.resolved.json
|   |-- manifests/                         # contracts/panels/providers/seeds
|   |-- cas/sha256/<prefix>/<hash>          # 权威不可变对象
|   `-- executor/                          # receipts/fences/in-flight state
|-- bootstrap/
|   |-- director/                          # original run + public briefs
|   `-- warm/checkpoint-<hash>/             # settled semantic snapshot
|-- arms/<policy>/episode-<seed>/
|   |-- private/<agent_id>/                 # 只挂载给 owner
|   |   |-- repo/                          # 独立 .git objects/refs
|   |   |-- worktree/                      # RO/RW 随 phase 切换
|   |   |-- memory/
|   |   |   |-- experiments/               # verified own projections
|   |   |   |-- notes/                     # lessons/hypotheses
|   |   |   |-- skills/                    # 原生 SKILL.md + scripts 约定
|   |   |   `-- strategy/                  # full profile 私有策略文档
|   |   |-- plans/                         # submitted receipts，权威对象在 CAS
|   |   |-- scratch/                       # 有大小/生命周期限制
|   |   |-- sessions/                      # CLI-native session/home settings
|   |   `-- logs/                          # private agent/provider trace
|   |-- public/board-<round>/               # 同版本只读挂载给所有 agents
|   |   |-- champion/                      # verified code artifact
|   |   |-- summaries.json
|   |   `-- disclosures/                   # 仅 full D2 profile
|   `-- checkpoints/                       # fully settled only
|-- exports/                               # 重建用 JSONL/CSV/Coral views
`-- reports/                               # results + provenance
```

CAS 中原始文件不是默认公开的；projection 复制/挂载具体允许对象，不能给 agent 可枚举全局 CAS 的路径。私有 repo 不包含其他 agent 的 object history；public champion 只导出允许的只读 tree，不导出完整私有 Git 历史。候选执行环境连 own private memory 也不需要挂载。

保留 Coral 原生 notes/skills/session 的格式和发现方式，通过 adapter 指向 private store 或明确公开投影。原生协作 prompt 需要替换为 phase contract，避免其“每次都分享全部知识”的指令越过 V1 边界。raw provider logs 不放入 shared logs。

### 10.4 SQLite schema 与关键约束

`schema.sql` 初始包括以下表组；外键启用、WAL、本地单 writer、事务锁与 busy timeout 明确配置：

- `experiments, episodes, agents, profiles, contracts, artifact_refs, seed_manifests`。
- `rounds, round_snapshots, phase_transitions, events, dispatch_outbox, publication_outbox`。
- `plan_versions, round_submissions, evidence_links, context_manifests, lessons`。
- `priority_orders, invitations, holds, edit_grants, candidates, prechecks`。
- `final_allocations, eval_tickets, dispatch_claims, jobs, verified_results, instance_results`。
- `wallet_entries, settlements, policy_statistics`。
- `budget_accounts, budget_reservations, provider_calls, resource_usage`。
- `warm_checkpoints, arm_clones, public_board_versions, report_manifests`。

关键约束与事务：

1. `round_submissions/final_allocations/settlements UNIQUE(episode_id,round_id,agent_id)`；invitation 同轮 agent 唯一；plan_version 可跨轮引用。
2. `eval_tickets UNIQUE(job_id)`；`jobs UNIQUE(ticket_id)`；result 对 job 唯一；每 payment/reward wallet entry 有唯一 operation key。
3. admission 事务验证 K、各 agent 一次、grant/candidate/contract 对应和余额，将 hold 转为 debit；所有条目同一事务成功或回滚。
4. settle 事务只处理还未 settled 的 round，确保 frozen baseline 与结果集合完整；没有 payment 的 agent 也有 settlement 记录。
5. DB 外的大 artifact 先写临时文件、fsync、内容 hash 校验后原子 rename，再在事务中引用。未引用文件可后续 GC；缺失/失配的必需对象阻止执行/恢复。
6. events 是同事务的审计流，状态表是受控 materialized state；两者由同一 mutation 写入。JSONL 从 ledger 导出，不允许手改 JSONL 再静默覆盖 ledger。

### 10.5 自愿共享如何接入

V1 的强制发布仅为 verified H_t；private plans、numeric forecasts、rationale、strategy、失败实现细节不默认公开。全版可新增 `Disclosure.submit(owner,artifact_refs,claims,evidence,visibility)`，经过权限/证据验证后在下一 publication boundary 入公共目录，agent 自主决定是否调用。

mandatory evidence 与 optional disclosure 是不同通道：V1 数值计算使用本 agent 完整合法历史；全版要求提交同 `(target,mod_type)` 的全部记录含失败给 regulator 时，不能用“不想公开”拒绝该 mandatory evidence。披露只控制公开传播，不改变不可省略的实验记账。

全版的 module reuse 使用显式 depends_on 和 verified lineage；free-form “借鉴”不能直接触发 royalty。转移先计算 receiver 原始 credit，再按固定规则拆分来源和净 wallet reward，并检查 transfer 守恒。

## 11. 小测试、正式 runner 与故障恢复

### 11.1 小测试如何受控

所有组件属于一个本地 Python package。supervisor、dispatcher、market 和 ledger 是进程内职责；评估/agent 是受限子进程；本地 authenticated control endpoint 和 LLM proxy 只承担已有工具的窄入口。SQLite outbox 是可恢复 launch/publication intents，不引入外部消息队列、微服务或分布式调度框架。

每 agent 每 phase 的 BudgetAccount 至少包括：

```text
absolute_deadline, cpu_seconds_total, cpu_rate_limit, memory_bytes,
pids_max, scratch_bytes, stdout_bytes, file_write_limit,
llm_requests_max, input_tokens_max, output_tokens_max,
llm_inflight_max, permitted_network_routes, gpu_devices=[]
```

- CPU/wall：整个 containment 的所有 shell、编译和小测试子进程合计；fork/重启不会得到新的免费池。CPU rate 限制防止短时间占满机器，累计 CPU seconds 限制总消耗。
- preparation：agent 自主读取冻结 parent、自己的 memory/history 和公开信息；禁止 shell、toy 执行和所有写入。generation：仅受邀者可编辑；compile/toy 消耗 generation/precheck budget。
- LLM：gateway 在转发前原子预留 call、input/output tokens 和在途额度；输出上限传给 provider，超预算拒绝。重试、自动 compaction、subagent 和并发 provider 请求都计数；store/quota gate 不可用时 fail closed。由可信 usage 回填实际量，未知用量保守占用 reservation，不能在未知时恢复免费额度。
- network：仅允许受控 provider proxy 和必要本地 control endpoint，移除 alternate provider credentials/config 与 CLI auth fallback，阻断直接 provider/API、外部计算服务；子进程不得访问其他 GPU/容器运行时 socket。
- deadline：到期先拒绝新请求，再终止整个 containment，确认结束后释放物理 capacity。取消远程请求不等于 provider 不再收费，已发送请求单列其真实/unknown 成本。

可先 pilot 预注册 preparation 最多 4 次请求和共同 300s wall、planning 最多 1 次请求和 120s wall、generation 最多 3 次请求和共同 300s wall、工具 CPU 30s（固定 precheck 另计）、内存 1 GiB、pids 32 等候选值；这些是工程起测值，**不是两份规格已经确定的参数**。最终值在 paired arms 前冻结，保持相同；不得按某个 arm 的表现临时放宽。实际可行性以受控 runtime spike 为准。

严格同步的代价是慢 agent 可能 abstain/无法完成，不是所有 agent 都必须成功后才继续。利用 pending reuse、缓存合法 context 和固定小诊断降低等待；不能把未完成 work 搬到等待期无限继续执行。

### 11.2 正式 PACE evaluator

`manifest.py` 冻结 ordered panel、图 source hash、reference k*、seeds、toolchain/hardware、solver argv/stdin/stdout 合同。缺数据、reference 或真实 containment 时 `--execution-mode real --benchmark pace` 报错退出，mock 只能显式选择。

`parser.py/verifier.py`：

- 输入 `.gr` 的 `p ds n m`，1-index vertices；图 header、边和节点范围严格校验。
- output 首个非注释行是 k，后续非注释内容恰好 k 个 vertex IDs；拒绝重复、越界、数量不符和非 dominating set。
- 任一实例 verified invalid => aggregate invalid、F=0，不更新 parent。timeout 无有效解时该 instance=0；SIGTERM 后是否接受已完整输出的合法解由冻结合同明确，hard-kill grace 计入 wall 上限。
- require manifest `0<k*<u`，`u=min(n,2k*)`；`s=clip((u-k)/(u-k*),0,1)^2`，F 为固定 panel 上的 mean。better-than-reference clip 到1并 flag。

逐实例 timeout 与 aggregate invalid 分别编码：前者通常只贡献该实例0分，后者使整个候选F=0。保存 partial valid outputs 与终止方式，避免一个 timeout 抹掉其他实例已验证的有效得分。

`runner.py`：每 candidate 单独 containment，一次 slot 覆盖整个 panel，最多一 core/一线程。每实例 30 CPU seconds，验证 reserve 合计 20 seconds，总 CPU 上限620；独立 wall 上限为配置合同，不由 CPU 上限隐式推断。编译在 generation/precheck 完成，正式执行用冻结 binary/source 与 toolchain hash 核验，避免正式启动时再让 agent 修代码。

正式 candidate 进程：无 network、credentials、SQLite、agent homes、其他 repo、held-out；只见必要的只读实例输入、binary 和受限输出目录。可信 verifier 在 candidate 外执行；原始 stdout 先严格解析，再给 verified summaries，不能让 solver 自报 score。

setup/warm/main/heldout 的 costs 分开。heldout 的路径、reference 和详细反馈不进入 agent context；只对最终 champions 用独立 authority 执行。输出标记 `pace_style_internal` 与 contract hash，明确是短时内部合同。

### 11.3 操作系统支持

当前工作站是 Windows，已检查的 Coral process-group primitives 明显依赖 Unix 行为。真实运行优先使用受支持的 Linux/WSL2/container 执行环境，采用 cgroups/namespace/受限 mount 等机制；本地 Windows 可先运行 mock 和纯业务逻辑。

如后续支持 Windows native，应用 Job Objects、ACL/隔离执行器等实现同等进程树和文件约束，并通过独立 containment acceptance。不能把 Unix `killpg` 和 SRT 接口直接写在 Windows 流程里就声明 real mode 可用。preflight 检查实际能力，不以操作系统名称推测隔离成功。

### 11.4 Resume、幂等与执行未知状态

保证一次授权认领和一次记账，不声称普通 subprocess 具有物理 exactly-once。DB 提交与 OS 启动不是同一事务，必须显式处理其间 crash。

```text
NEW -> ADMITTED/debit -> CLAIMED/launch-intent -> RUNNING
                                |                   |
                                +---- UNKNOWN ------+
                                                    |
                       RESULT_RECORDED -> ROUND_SETTLED
```

- 完成 ledger/schema/hash 检查后，从 last fully settled checkpoint 重建 agent 可见 state；未完成 round 保持不可见，先 reconcile job/claim/containment。
- ADMITTED、尚未 claim：同一 job_id 可由 outbox 重试派发，不重复 debit。
- 可证明 NOT_STARTED：撤销该 final admission，保留原事件，以唯一 compensating wallet entry 抵销 admission debit，最终 `x=0,payment=0,Y未定义`，记录基础设施失败和已发生的管理成本。这个是规格未细化的“计费提交与物理启动间 crash”的工程恢复约定；有明确证据才能使用。正式 batch 已启动后不因此重开递补。
- CLAIMED 且无法判断是否已启动：标 UNKNOWN，冻结 ticket/capacity，查询执行器 receipt 与结果；不自动再次 launch，不退款、不重发票。
- 已 RUNNING：attach/query 同一 execution；确认失效后按实际基础设施状态记录。支持 `submit(job_id)` 幂等与查询的执行器才可自动安全重试。
- 结果已写、尚未 settle：重算同一 frozen snapshot、使用唯一 wallet operation keys，完成一次 settlement。
- settlement 已写、projection 未发布：重放 publication outbox；不得再次发 reward 或重复扣款。
- 无法消解 UNKNOWN 时中止该 episode，保留状态和所有 costs，报告 incomplete；不能把它算成一个已成功完成的 paired comparison。是否整组重跑由预注册的 rerun 规则决定，保留原 episode。

可确认的候选整体正式 solver failure 记零 reward、消耗原 slot；逐实例 timeout 按正常 scoring 聚合。已启动后的基础设施故障消耗原 slot，按预声明的有效性规则处理并另有 status，不与算法 failure 混报。恢复产生新 phase_epoch/fence，过期进程、ticket 和回调全部拒绝。

### 11.5 可重现性

从 `(experiment_seed, warm/main, round, agent_id, step)` 派生 seeds，分别用于 mock dynamics、policy ties、task 和 retrieval；不要把 policy 名加入共同 task random stream。mock outcomes 不应根据 policy 名生成。真实 LLM 即使设 seed 也不承诺 bitwise 相同，保存 provider/model、request/response、prompt/context hashes 与实际 usage 以供审计。

所有报告引用 config、taxonomy、panel、contract、toolchain、Coral SHA、provider、prompt、source 和 warm snapshot hashes。Replay 只重算 allocation/credit/wealth、验证引用与 artifact，不重新调用模型或 benchmark；只凭账本无法重建一个未保存的模型响应。

## 12. 若保留原生 Coral orchestration：可选源码改动

默认组合路径不需要以下 patches。选择完整 native manager/daemon 时，这些 hooks 才是接入其执行边界的必要改动；通过 dependency pin 和补丁测试维护，不把 market 逻辑塞进 Coral。

### 12.1 Manager 安全转换 hook：P

位置：[agent/manager.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/agent/manager.py) 的 `_setup_and_start_agent`、`_interrupt_and_resume`、`_sandbox_spec_for`、`monitor_loop`。

新增公开 `request_phase_transition(agent_id,expected_epoch,transition_spec)`，由 manager 串行处理 stop -> prepare sandbox -> resume，复用其 session 恢复。MAESTRO 不再直接操作 handles；manager 每次自动 restart/heartbeat 也查询持久 phase、grant、剩余额度。

bounded waiting 状态不当作“无输出卡死”反复自动恢复；它有自己的绝对 deadline。V1 migration 关闭；后续要迁移 parent，先撤旧 grant 再重新计划，不能把旧 plan/edit 权限搬到新代码。

### 12.2 Submission hook：P

位置：[hooks/post_commit.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/hooks/post_commit.py) `submit_eval`：commit 前校验 EditGrant/phase，commit 后绑定 plan/parent/candidate；eval/tune 使用同一控制路径。无 EditGrant 不提交待执行 candidate。

新增字段是 provenance，不是由 agent 自发填写即有效的授权。保留原生 public Attempt 的兼容格式，把 funding 详细状态存在 MAESTRO。

### 12.3 Daemon admission/metadata hook：P

位置：[grader/daemon.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/grader/daemon.py) `_drain_pending` 在 `pool.submit` 前查询 `admission.claim`，仅派发获准 job；`process_pending_once` 与长期 daemon 共用同一检查。`_grade_one` 显式传播 ticket/job/plan/contract metadata，并在昂贵执行前复核 fence。

默认 `_grade_one` 只构造部分 Task metadata；不能假设在 pending JSON 加了 ticket 字段就会自动传到 grader。仅在 TaskGrader 里阻塞等待会先占用 worker/checkout，不能替代 pool 前 admission。

### 12.4 Waiting 与其他入口：P/配置审计

位置：[cli/eval.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/cli/eval.py)、post_commit poll、manager pending 统计，以及 [task/validation.py](https://github.com/Human-Agent-Society/CORAL/blob/0123dfb939b35228cf2c1fde224cd0561e727408/coral/task/validation.py) 的直接 grader 调用。

保留 pending 的原始未完成语义，在 MAESTRO 层提供 auction deadline/status；若扩展原生 Attempt statuses，必须同时改完全部 terminal 判断、timeout 和 queue-cap 计数。baseline validation 只接受 SETUP authority，不能绕过 main admission；所有 grader 入口都有 purpose 验证。

这条路线增加少量核心 hooks，也增加上游耦合和回归面；只有明确要保留 native orchestration 时才实施。默认路径使用 §6–7 的新增 adapters 即可。

## 13. CLI、输出与完整验收

### 13.1 拟提供的本地命令

以下是将来的命令合同，本次没有执行这些命令，也不声称已实现：

```text
maestro bootstrap --config configs/mock_smoke.yaml --seed 0
maestro run --bootstrap <warm-checkpoint> --policy uniform --seed 0
maestro run --bootstrap <warm-checkpoint> --policy ucb --seed 0
maestro run --bootstrap <warm-checkpoint> --policy first_price --seed 0
maestro replay --episode <id> --verify-ledger
maestro report --experiment <id>

maestro bootstrap --config configs/experiments/pace_remote.yaml --execution-mode real --benchmark pace --seed 0
```

bootstrap 包含 config/preflight、Director/initialization、balanced warm 与 checkpoint。run 校验 snapshot 和 profile 兼容性，主阶段跑满 T=20 或以明确 incomplete 状态结束；不把 warm 轮数算入 T。report 由 ledger/CAS 生成，原始执行日志保持私有。

report 最少包括：champion curve/最终 training score、final held-out、每 agent formal slots/有效改进/失败、unused K、generation invitations 与 precheck failures、每阶段 reserved/actual CPU/wall、LLM requests/tokens、warm/setup/main/held-out 分项及总成本、payments/rewards/wealth、funded-only forecast MAE/MSE、pending reuse、diversity 与 seed-paired policy comparison。说明 funded-only calibration 的 winner-selection bias。

### 13.2 关键验收清单

实现阶段必须通过下面的行为测试；不是给单个函数写镜像测试。

1. **Accounting**：重试 admission/settlement 不重复 debit/reward；hold release、unused holds、wealth 守恒与 operation keys 正确。
2. **Value/credit units**：历史 gains fixture 得2.125；exact-label/fallback、失败零样本、当前 parent/champion 重算正确；LLM numeric claims 不进入 value。
3. **Reward**：4.25与1 fixture、catchup_fraction=0、invalid/nonimprove/duplicate=0、父版本不倒退；full 参数单位映射不混淆。
4. **Policies**：own-bid first-price、cutoff 仅日志、seeded ties；uniform 平衡；UCB cold start∞、distinct agents、warm prefix 与轮末更新。
5. **Frozen baselines**：同轮晚完成结果不会改变他人 reward 基线、plan、ranking 或 candidate；没有 intra-round feedback。
6. **Pre-eval replacement**：编译失败可在原 invitation 剩余预算内修复，形成新 candidate revision；最终失败才释放 hold，第三位递补；最多K正式评估、每 agent 最多一次邀请；共同 deadline 耗尽后留空而不超发。
7. **Post-start failure**：正式启动后 timeout/invalid 消耗 slot、自身 bid 不退款，不能继续递补；与 precheck failures 清楚区分。
8. **No-admission path**：无正式 Y/G、payment/reward=0，但受邀失败者 generation/CPU/tokens 成本保留；不制造 zero-gain 正式样本。
9. **Pending plan**：保留 ID/evidence，无调用重投；每轮重算 bid；每3轮/实验/失败/parent change refresh；revised version 新ID，reuse 不产生 outcome。
10. **Warm/clone**：4×10机会=20waves；至少一正式记录；缺失报 incomplete；克隆 private histories、parents、H、UCB stats，main equal wealth，无 synthetic prior。
11. **PACE adapter**：isolated vertex、dominated/non-dominated、重复/越界 IDs、comments、wrong count、malformed header、timeout/SIGTERM/no output、reference 缺失与 clipped better result；部分实例 timeout 不应被误当成全候选 invalid。
12. **授权**：无 ticket、伪 ticket、旧 epoch、错 candidate/contract/panel、重复 claim 均不启动 runner；无 grant 不能编辑；提交后旧进程已被回收。
13. **预算**：多子进程/背景 shell、runtime resume、并发 LLM 请求、旁路 provider、stderr 持续输出，均不能逃出累计 CPU/token/wall 限制。
14. **隐私**：agent A 无法经 Git objects/refs、sibling worktrees、global CAS、Coral logs/notes/skills 读取B的私有数据；公开仅白名单字段。
15. **Coral contract**：真实 runtime provisioning/resume、Task.metadata 经 subprocess 完整传递、eval shim 正确路由、没有 native daemon 旁路。
16. **Recovery**：admission前/后、claim后启动前、RUNNING、result后、settlement后publication前分别注入 crash；账本不重复、已证实 NOT_STARTED 的 compensation 幂等、UNKNOWN 不盲目 rerun/退款。
17. **Integrity/replay**：artifact 缺失/hash变动/schema不兼容明确失败；同一 seed 的 deterministic mock 可复算，JSONL projection 不成为权威。
18. **E2E**：mock 对三个 policies 分别跑4 agents×20 main rounds、每轮至多2个正式 slots；公共 warm prefix；reports/replay 完整。real pilot 独立验收，mock 通过不等于隔离/runtime已通过。

### 13.3 按依赖推进的实现顺序

**M0：规格合同与兼容性 spike。** 固定 requirements/config/IDs/Decimal；验证所选 Coral runtime 的 settings、sandbox、gateway、单次请求预算与 session 恢复。选择一个真实 runtime 先支持；其余 provider 在兼容测试后加入，不凭接口相同就宣称支持。

**M1：数学与事务内核。** types/config/seeds、CAS/SQLite、value/reward/policies/hold/admission/settlement/replay；完成数值、唯一性和恢复事务验收。

**M2：完整 mock loop。** Director mock、warm、pending plans、generation failures/递补、barriers、3 policies E2E、报告；用 policy-independent outcome dynamics 暴露调度差异。

**M3：Coral 控制适配。** workspace private repos、runtime provisioning、supervisor、phase-aware sandbox、LLM proxy、plan/candidate 命令。先证明 RO plan-first、RW grant、撤权、预算与无票不能启动；再启用真实 autonomous generation。

**M4：真实任务执行。** PACE manifest/parser/verifier/scoring、可信 dispatcher/TicketedGrader、资源 runner、安全与数据合同；缺 prerequisite 就阻止 real mode。先用 toy contract 验证全链路，再用冻结 training panel。

**M5：共同 warm 与 paired pilots。** Director 提交方向、每agent10机会，确认真实历史非空；冻结共同 checkpoint；按 paired seeds 运行三组20轮并做最终 held-out/ledger replay。

**M6：全版独立扩展。** 按 multi-plan -> strategy/forecaster -> reputation/screening/audits -> D2/attribution 顺序，每项新增独立 profile 和对应成本/信息/失败合同。不要一边跑 V1 比较一边动态启用这些机制。

### 13.4 完成定义

工程可交付需要同时满足：可运行 CLI、可复现 mock E2E、真实 Coral 接入测试、硬权限与资源验收、真实 PACE verifier/runner、完整 cost/artifact provenance、无双重收费的 resume/replay，以及共同 warm snapshot 的 paired comparison。

**当前交付是这份本地详细计划。上面的文件树、接口和测试是后续实现合同，尚未创建或执行。**
