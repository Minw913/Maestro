# MAESTRO V1 实现与验收记录

日期：2026-10-20。目标仓库：`D:\Becky_files\Documents\Personal_Repos\MAESTRO`。

## 当前验收：严格 V1 Codex 真实连接通过

使用本机已有 ChatGPT 登录、Codex CLI 0.156.1、`gpt-6-astra`、显式 `reasoning_effort=low`，不提供 external API token。最终单次真实连接验收 **PASSED，26.344 秒**：规划 1 次、generation 3 次、新进程恢复 memory 读取 1 次，共 **5 次实际模型请求、5 份可信终态及 usage 回执、0 个未决请求**。全部 15 项检查通过。公开脱敏证据见 [docs/validation/codex-live-20261020.json](docs/validation/codex-live-20261020.json)。

模型真实调用批量工具读取 private notes/shared board，再写入候选代码和 private notes；controller 对私有 nonce 的内容与文件 hash 做验证。独立 native 进程恢复同一会话并读取恢复后的 memory 快照。无 EditGrant 的 generation 在派发前拒绝，public memory 写入拒绝，generation 第 4 次请求返回本地 429，未转发上游。planning 禁止工具调用；continuity 使用 PLAN_RO 只读 broker，不属于主规划请求。native host 保持 read-only、无网络权限，文件修改通过受签名 EditGrant 控制的独立虚拟 workspace RPC 完成。

本次修复与工程边界：

- `quota_proxy.py`：复现上游 HTTP 200、已有 `response.completed`、但缺少可识别 Content-Type 的 SSE 被误当 JSON，造成已完成请求被报 UNKNOWN。改为有界 SSE/JSON 帧前缀识别，继续完整验证工具、终态与 usage 后才交付；持久保存脱敏 HTTP/帧类型证据。POST 之前可证明的连接失败与派发后不确定性分开，二者均不退还调用次数。
- `autonomous_client.py` / `autonomous_backend.py`：固定并验证 low effort，防止继承桌面 ultra 配置；持续使用 native 登录，不读取或复制用户 auth.json。planning 回执从 provider receipt 表获取 response ID，避免混用 native thread ID。
- `tools.py`：安装版 native 客户端实际设置 `parallel_tool_calls=false`，在 3 次 generation 请求内完成相关读写需要有界 file batch。每批最多 8 个操作，逐操作验证 grant/deadline、消耗工具配额；禁止 shell、嵌套 batch 与未知字段。先检查结构，顺序执行，首错停止；已经完成的写入保留。
- 指令说明 native host read-only 与 controller 授权的 workspace RPC 边界，避免模型在拥有合法 EditGrant 时误认所有文件修改均被禁止；没有放宽 sandbox 或 public-memory 权限。
- `loop.py`：候选注册与读取使用共享 SQLite 锁，修复并发 generation 的连接竞态；捕捉刚签发 grant 在首次验证前跨过共同 deadline 的情况，撤销 grant、记账退出，不发起迟到请求。非超时的非法 grant 仍抛错。
- `qualification.py` / `codex-qualify --live`：新增有明确 5 次请求包络、不可复用输出目录的真实连接验收。只验证连接、文件工具、memory 和配额，不调用 ExperimentEngine 或 evaluator；失败后不自动重试或轮换目录。

最终验收 5 次请求已知 input **21,889**、output **433**，共 **22,322 tokens**；cache-read input 3,072 已包含在 input 中。真实请求次数为硬配额。原生接口不支持硬 output-token 上限，因此 token 预留估计与真实回执分开，不声称硬 token 限额。

**历史 UNKNOWN 的结论：** 旧探针原始数据库 SHA-256 `389755ed8e10463faf9402a4ced1a01643f453673870a83619f219301148a2fd` 保持不变。含该旧探针和本次所有独立诊断，累计 17 次已收费调用，其中 3 次因缺少可信服务端终态仍为 UNKNOWN：旧探针、acceptance-1 generation 的响应头超时、acceptance-4 planning 的响应头超时。不能由新验收成功推导它们完成，也没有退款、清零或自动重发。另一个复现案例 diagnostic-1 由可信观察器捕获同一请求的 terminal frame 后得到完成证明，调用仍计费，缺失 token 用量继续标未知。显式授权的独立诊断保留了全部历史；它们不等于恢复未决实验。低 effort 也出现过一次上游响应头超时，故不把所有 UNKNOWN 都归因于 ultra 或解析错误。

当前完整无模型回归 **581 passed，0 failed/errors/skipped，171.18 秒**，启用安装版 CLI 的 fake-auth/fake-upstream wire 测试；覆盖真实请求 gate、batch/grant/privacy、ticket 启动、同步 auction、递补、settlement、共享 SQLite 和迟到回调。完整证据保存在 `artifacts/codex-live-qualification-20261020/`：`acceptance-5/result.json`、各次 `requests.sqlite`、`case-accounting.json`、`historical-audit.json` 与 `tests-release.xml`。公开 JSON 只投影安全 hash、响应 ID、计数与检查结果；原始 auth、会话内容与运行数据库不入 Git。

**Warm Start、main、heldout 均未启动。** 此次验收只使用文件工具，没有执行候选代码、shell、小测试或正式评估。Linux/rootless Docker、镜像和真实 PACE 数据仍需独立资格验收，后续才能启动实际实验。Coral submodule 源码与固定版本保持不变。

## 此前实现记录（历史，以下 515 项结果由当前验收取代）

## 严格 Codex 自主接入更新

严格工厂已由占位拒绝替换为 `autonomous_backend.py`。原生 `autonomous_client.py` 通过已登录Codex app-server运行工具循环，`quota_proxy.py` 在每个上游请求前认领持久配额，`tools.py` 将文件/memory与隔离小测试绑定到实际EditGrant。Coral的私有Git/CAS、memory快照、ticket、同步auction、递补和settlement继续复用。没有修改上游Coral tracked源码。

规划1次请求、generation整个邀请最多3次请求；续推理、重试、compaction和修复共享账户。工具CPU预算也跨修复累计；中断后不能给同一邀请重开CPU预算。原生ChatGPT接口不支持硬output-token参数，因此只对真实请求次数声明硬限额，token预留与实际计量分别记录。固定toy precheck单独限1 CPU秒；其用量加入严格backend的generation回执。

最后审查补齐了两处退出/资源边界：controller关闭前等待迟到generation回调完成用量记录和grant清理，未完成则保留数据库连接；固定toy precheck与debug shell共用CPU池，等待和启动均受generation截止时间限制。Windows的CAS发布只对已确认的临时sharing/access错误有限重试，仍核验并发winner的内容hash。

安装版0.156.1把工具定义嵌在 `input.additional_tools` 中，已在网关移除原生工具并注入可信 `maestro_tool`；返回的工具调用也在交给Codex执行前检查。继承的MCP、插件、hooks、额外凭据和主机原生工具被禁用；Coral私有/共享memory仍通过broker提供。`configs/warm_start.yaml` 固定4 agents／20 CPU workers／5 instance workers／5 waves，CLI版本一并冻结。

**此前实机连接记录（已由上文新验收取代）：** 本机 `codex-check --strict` 确认ChatGPT登录与原生协议可用、无需external API token。另一次只读官方models元数据GET返回200，未发起模型推理。最多3次调用的真实连接探针实际只转发了1次请求，未获得可信完成回执，没有工具执行，已保留为UNKNOWN（active=1），没有重试、退款或新建scope重跑它。旧版网关当时缺少安全HTTP状态诊断，因此不能从本地进程退出推导上游完成；本次已补状态/错误类别诊断。证据为 `artifacts/strict-codex-validation/live-20261020T153650/result.json` 和同目录 `requests.sqlite`。当时未通过真实连接资格；该旧请求至今仍缺少可信完成/取消证据，不能原样恢复或重试。真实模型调用成功与Docker物理资格均不能由模拟测试推出。

本轮最终在目标D盘仓库重跑完整回归：**515 passed，0 failed，0 skipped，145.39秒**。包含安装版Codex实际进程的两项fake-upstream wire测试、真实请求gate、工具/memory、repair累计配额、ticket、同步auction、递补、settlement、迟到回调清理与CPU池竞争测试。命令为 `MAESTRO_RUN_NATIVE_WIRE=1 python -m pytest -q --basetemp artifacts/strict-codex-validation/pytest-final --junitxml artifacts/strict-codex-validation/tests.xml --tb=short`（PowerShell中该变量通过 `$env:MAESTRO_RUN_NATIVE_WIRE='1'` 设置）。原生wire测试使用独立CODEX_HOME里的虚构登录，没有读取或复制用户auth文件；完整回归不转发真实模型推理。

本轮只执行回归、原生协议和连接检查；没有启动真实Warm Start、main、heldout或实验候选程序。最终测试报告、无推理登录检查、本次源码清单和连接探针证据保存于 `artifacts/strict-codex-validation/`；机器可读结论见 `validation-summary.json`。下面的276项与320条settlement仍是此前版本的历史证据。

## 此前 instance-worker 更新记录（当时未运行测试）

- 新增 `cpu_workers`（默认4）、`instance_workers`（默认1）、独立的 `warm_concurrency`（默认4）；按用户最新设置，每agent warm机会为5次。CPU容量至少4时，共5个warm waves；main仍每轮2个正式slots。
- round snapshot、ticket和执行合同冻结worker参数；admission和replay核验。每candidate实例并发上限为 `min(instance_workers, evaluation_instance_count, ceil(cpu_workers / evaluation_batch_width))`，同一controller的solver、verifier及heldout共享CPU池，总并发不超过 `cpu_workers`。
- 子线程继承dispatcher授权，取得CPU名额后重新核对ticket/phase/fence；失败停止后续instance启动并等待已启动任务结束。结果按固定panel顺序验证，CPU累计、付款、reward和settlement barrier保持原定义。
- Docker终止无法确认时标记执行不确定并封锁新CPU启动；独立controller需划分CPU容量，进程内pool不提供跨controller机器级调度。
- 当时已编写并发、授权、资源绑定和回收异常回归用例，遵循“先不要开起跑测试”未执行。本轮用户已授权重跑相关测试；最新结果见严格Codex验收记录，下面276项与320条settlement仍为此前历史证据。新默认完整simulation的预期settlement数为260，相关集成回归包含在本轮测试中。

## 交付范围

已建立本地 Git 仓库，分支 `codex/maestro-v1`；Coral 为 `vendor/coral` submodule，固定到 `0123dfb939b35228cf2c1fde224cd0561e727408`。原有 `old_docs/` 保留，Coral tracked 源码保持原样。计划原文保存为 `maestro_plan_V1.md`。

V1 的代码包括：共同方向分配与 warm start、三种资源分配 policy、history-only 估值、pending plan、plan-first generation、bounded repair/递补、EditGrant/EvalTicket、受信 dispatcher、精确账目和同步 settlement、私有/公开 memory、可恢复 SQLite/CAS、replay、CLI 与成本/轨迹报告。

真实接入代码包括：固定 Coral runtime/provenance、单次无工具 planning、独立私有 Git、session/phase supervisor、配额 gateway、Linux rootless Docker containment、受限 tmpfs/export、PACE parser/verifier/scoring、独立硬预算 verifier 和 final-only heldout。默认真实 factory 已随包提供，缺环境时拒绝执行。

全版的 forecaster、strategy doc、reputation、screening、audits、多 plan best-sibling credit、disclosure 与 attribution 已实现为**独立、受 profile 约束的算法 API**并附测试。严格 V1 为 `v1_cooperative`。另提供明确区分预算单位的 `codex_session_pilot`，复用 ChatGPT 登录，无需 external API token。全版实验 loop 和原生 Coral manager/daemon hooks 是后续/替代接入。

## 验证证据

此前版本在目标 D 盘仓库及其 `.venv` 中完成的验收（本次worker更新前）：

- **276 passed，0 failed，0 skipped，101.35 seconds**。命令为 `python -m pytest -q --basetemp .test-temp-review-release --junitxml artifacts/review-validation/tests.xml`。
- Coral 的实际 Task、TaskGrader、SubprocessGrader worker 与 ScoreBundle 子进程兼容测试包含在上述276项内。
- 独立完整 smoke：**4 agents；每agent10次 warm 机会；20个 warm waves；uniform/UCB/first_price 各20个 main rounds；每轮最多2个正式 slots**。
- 共同 warm 加三个 main arms 的 **320条 settlement 通过账本 replay**，`verified=true`。完整响应保存在 `artifacts/review-validation/smoke-final.json`。
- smoke 报告为 `artifacts/review-smoke-final/reports/summary.json`，同时输出 CSV、SVG 与 events JSONL；全部明确标记 simulation。
- 本机 Codex 登录与真实 JSON probe 成功，回执保存在 `artifacts/review-validation/codex-session.json`；没有提供 external API token。CLI 为0.156.1，完成的 session turn 为1，真实 inference request 数未知。
- 另通过 Codex app-server 的只读 model/list 核验本机可用模型，默认项为 gpt-6-astra，已固定进 session pilot 示例配置；该 metadata 查询没有发起 inference。
- 另有完整 session adapter 接口测试，覆盖结构化 plan→first-price auction→EditGrant→host edit→ticket→simulated result→settlement；它验证接线与账目，真实模型调用证据来自独立 probe。
- 实际 Python import 路径已核验为本仓库 `vendor/coral/coral/__init__.py`，commit 与计划固定SHA一致；Coral tracked 源码无修改。

测试分三层：

1. 市场/存储/任务单元及完整 simulated episodes，包括四个 agents、每agent十次 warm 机会、三种 policies 各二十轮、每轮至多两次正式评估。
2. 模拟 executor/FakeRuntime 的权限与生命周期测试，以及真实固定 Coral Task/TaskGrader/SubprocessGrader/ScoreBundle 的子进程兼容测试。
3. Linux/rootless Docker + 指定镜像 + 已认证模型 + 真实 PACE 数据的实机 qualification。当前环境没有完成这一层，不能由前两层推导其成功。

仓库 `.venv` 已安装 MAESTRO 和固定 Coral 的 editable 源码包；测试使用本机已有依赖。Coral 的完整可选/原生 proxy 依赖未在此 Windows 环境全面安装，不把源码接口安装当作完整原生 Coral 服务部署。

## 实现中发现并修正的问题

- **授权不应只看 ticket 字符串**：admission 复核 frozen submission、plan/owner/parent、score baselines、panel/contract、当前 precheck、配置 K 与 own-bid payment；实际 candidate 在 dispatcher 启动前再次核验。
- **不能直接把 ticket dict 传给 PACE 执行**：Dispatcher 原子 claim 后打开一次性执行授权，每个 solver 与 verifier 启动前重验 phase/fence、注册路径与 hash；先写 LAUNCHING，异常保留 UNKNOWN。跨连接竞争只有一个 claim 可运行。
- **同步 generation 需要并行启动**：首 K 个邀请同时进入 wave，失败后按冻结 priority 递补；去重不依赖完成顺序。无确定回执的 generation 不重试、不递补、不进入正式评估。
- **并发 generation 的共享资源需保护**：新 workspace 只初始化自己的 public board；gateway 首次绑定与 credentials 发放/撤销串行；CAS 仅接受 hash 一致的并发发布 winner；每 worker 的 usage receipt 独立。
- **进程停止和远端完成是两个事实**：未决 provider reservation、无法确认终止的进程 scope 都冻结 generation；Docker daemon 断联不能作为容器不存在的证据。已完成但缺 token 计量的请求仅标记成本未知，不误判为仍在执行。
- **settlement 必须等完整本轮结果**：缺失、重复、跨轮、篡改结果或冻结基线漂移全部拒绝；在 detached states 上计算，事务回滚不会先污染 history/UCB；replay 重算 priority、邀请链与累计 UCB。
- **并发 SQLite 共享连接读写竞态**：所有 dispatcher 使用的连接读取与 mutation 共享串行锁，避免并行评估时返回错误 row 或 API misuse。
- **临时生成获选不等于付费正式获选**：hold、修复/递补、final debit 和正式失败分开；未启动有证据才 compensation；UNKNOWN 保守隔离。
- **真实成本不能以 revision 次数替代**：记录 provider request/usage；input/output/cache、reservation ceiling 和未知用量分开，失败/compaction/subagent 都经过同一 gate。
- **异步 planning 不能串行累加四个超时**：同一 deadline 并行 single-call；独立 receipt/context；迟到 decision 丢弃，无确定回执的请求保持 unknown。
- **warm 不能仅复制 parent**：共同复制 histories、UCB stats、lessons、私有 docs/skills 和 public board；main 与 resume 的 memory manifests 持久化。
- **公开 H_t 不能每轮丢失历史摘要**：公开累计 verified summaries；prompt 可截断，但完整历史和 evidence ID catalog 保留。
- **同代码不能重复领 credit**：同轮保留优先 copy；历史去重仅限本 arm 和共同 warm prefix，避免跨 policy arm 污染公平比较。
- **容器内不同 UID 的监控需实际 kill 权限**：trusted monitor 与候选分离，进程树回收、cgroup CPU、tmpfs 及限量导出均有独立合同测试。
- **逐实例 timeout 与整体 invalid 不同**：前者该实例得0，后者 aggregate=0；可信验证器在独立硬限额 worker 执行。
- **检查单个表 hash 不足以 replay**：将冻结 snapshot、admission、result 和 settled checkpoint 对回同事务提交的事件锚点，并验证 CAS、候选源码和 memory references。

## 尚需外部条件或进一步决定的点及 next action

### 1. 真实隔离环境尚未资格验收

本机有 Docker Desktop/WSL，但 Docker Linux engine 当前不可连接；WSL 可见的是 Docker Desktop distribution。代码默认要求受支持 Linux、rootless Docker/cgroup-v2，不在本机绕过限制运行不可信代码。

**Next action：** 在指定 Linux 执行机启动 rootless Docker，按 `docker/README.md` 构建并固定两个镜像 digest，执行 CPU/fork/setsid、memory/pids、磁盘/输出、路径/Git、network/credentials 以及取消/重启的 qualification matrix。通过后才启用真实候选。

### 2. 真实 PACE 数据、reference 与初始 solver 未提供

当前只有用于 parser/verifier/contract 的 toy fixtures，没有假装为研究数据集。真实模式需要冻结 training 与 heldout manifests、合法 reference k* 和可执行 seed。

**Next action：** 填入 `data/training`、`data/references`、`data/heldout` 与 seed solver；固定20个training instances、checksums、normalized graph split、hardware/toolchain/verification-source hashes，再运行真实 bootstrap。

### 3. Codex 严格原生连接已通过；历史未知用量保留

已登录 Codex 的严格自主连接通过上文 5 次真实请求验收。工具调用、私有/shared memory 读取、受 grant 控制的代码和 private memory 写入、跨进程恢复及 1/3 请求硬配额均有实际证据。旧 JSON proposal pilot 的 turn 计量不属于该验收，也不能替代严格 backend。

**Next action：** 保留并独立追查 3 个历史 UNKNOWN，只有可信提供商完成/取消证据才能 reconcile；不得退款、清零或自动重发。后续部署按已验收 CLI/model/effort 固定配置完成 Linux executor、镜像与真实数据的独立资格验收，实验中的 UNKNOWN 仍阻止恢复。此次用户未授权启动 Warm Start。

### 4. 基础设施 UNKNOWN 需要可信 executor 证据

数据库认领与物理启动不是一个原子操作。Docker daemon 断联、无可信 metering receipt、远端模型取消不确定时，代码保持 unknown，不自动重跑、退款或制造0分。

**Next action：** 运维根据 job/fence、容器 ID、provider request/receipt 和 budget reservations reconcile；确认未启动时使用幂等补偿接口。为指定执行器定义可证明的 OOM/终止/never-start 判据；若需要自动恢复，接入幂等 submit(job_id)+query 的执行器。

### 5. 全版算法与实验 profile 的衔接

全版算法模块已测试，V1 的固定实验语义保持封闭。原生 Coral manager/daemon 的替代 hooks 没有修改上游源码。

**Next action：** 若启动 V2，单独实现/启用 full-profile loop，预注册多 ticket、失败计费、策略目标、审计与披露合同；若需要原生 Coral orchestration，再按计划加入通用 phase/admission hooks。

### 6. 多来源 attribution 与 uniform-price 边界

全版文本未明确多个来源如何分配 eta：当前独立 API 使用一个总 eta 池，对 distinct verified sources 均分并固定小数余数，保证守恒。uniform-price API 仅在无 audit、财富不绑定的纯 top-K 情况开放，避免声称一般情形的临界价格。

**Next action：** V2 实验前确认并预注册这两个机制；扩大 uniform-price 的约束情形时，补充对应定价定义与测试。

### 7. 初始化与恢复支持范围

本版固定从共同 verified seed 和各 agent 的预声明方向开始，不提供初始化模式配置；中断在 preparation/generation 的 episode 也不会未经 containment reconciliation 就自动重做。

**Next action：** 如研究对照需要 predefined solver paradigms，增加真实多 seed 初始化分支及等成本 setup。对中断 generation 先回收旧 scope、保留原记录，再按预注册 rerun/recovery 规则处理，避免重置预算获得免费计算。

## 运行入口

```powershell
Set-Location 'D:\Becky_files\Documents\Personal_Repos\MAESTRO'
.\.venv\Scripts\python.exe -m maestro --root artifacts/demo smoke --config configs/mock_smoke.yaml
.\.venv\Scripts\python.exe -m maestro --root artifacts/demo replay --verify-ledger
.\.venv\Scripts\python.exe -m maestro --root artifacts/demo report
.\.venv\Scripts\python.exe -m maestro --root artifacts/session-check codex-check
```

前三个命令运行显式 simulation，最后一个仅检查已有 Codex 登录；追加 `--strict` 可检查原生协议，不发起推理。已登录Codex的严格自主入口为 `configs/warm_start.yaml`；旧proposal模式为 `configs/codex_session_pilot.yaml`，旧API-backed Coral配置为 `configs/pace_pilot.yaml`。严格真实连接现已通过；历史 UNKNOWN 保留隔离，当前尚未启动 Warm Start，后续实验仍要求实际数据与 Linux executor 资格通过。操作步骤见 `docs/operations.md`。

## Configuration interface (2026-10-21)

Execution selection is now split into `execution_mode: mock | real` and
`benchmark: pace`. CLI overrides use `--execution-mode` and `--benchmark`.
`evaluation_instance_count` counts benchmark inputs per candidate, and
`cpu_seconds_per_instance` retains the per-input CPU limit and reservation
semantics. Initialization uses the fixed common-seed procedure.

Unknown and duplicate configuration keys are rejected. Ledger and warm checkpoint
schemas are version 2; previous records are explicitly unsupported and are not
migrated. Resume configuration conflicts are checked before backend startup.
Maintained documents and presets use the executable names. No real inference or
PACE experiment was run for this change. `implementation_1020.tex` was absent
from this checkout; the interface follows the explicit task specification.
