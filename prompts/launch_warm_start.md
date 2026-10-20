# Warm Start 启动 prompt

将下文发送给负责执行的AI agent。现在只保存prompt；后续用户发送正文才是启动指令。

---

请在 clone 后的 MAESTRO 仓库根目录（`git rev-parse --show-toplevel`）核查条件，并在满足条件后启动一次真实Warm Start。只执行共同seed评估及5个warm waves，不执行main或heldout。

先阅读仓库指令、`maestro_plan_V1.md`、`IMPLEMENTATION_REPORT.md`、`docs/operations.md`、`docs/codex_session.md`、`docs/PACE_ADAPTER.md`、`configs/warm_start.yaml`和实际backend/dispatcher/evaluator源码。

设置固定为：4个agents从共同seed沿4个不同方向探索；每人5次warm机会，共5 waves、20次机会；`warm_concurrency=4`、`cpu_workers=20`、`instance_workers=5`。每candidate固定20个instances，5个workers并行；solver和verifier共用CPU池。main的 `slots=2` 不限制warm并发。保留每instance 30 CPU秒/45 wall秒、整panel验证20 CPU秒/30 wall秒、规划120秒窗口；改代码和小测试共用300秒窗口，最多3次generation，包含首次修改及后续修复。单实例仍限1核，candidate预留仍为620 CPU秒。

启动前必须确认：

1. 使用已登录Codex/ChatGPT，无external API token。`strict_v1_factory` 已接入原生app-server、请求代理和隔离工具broker；核对实际源码、CLI版本pin及最新验收证据。严格V1要求每次实际inference发送前持久认领配额：规划1次、generation最多3次（包括续推理、压缩、重试与所有修复）。CLI turn不等于一次inference。原生工具循环、memory与配额的本地协议测试不能替代真实模型上游资格验收。**当前真实连接探针有1次UNKNOWN，见 `IMPLEMENTATION_REPORT.md` 和 `artifacts/strict-codex-validation/`；须先获得可信完成/取消证据，不能按等待时间判定已完成。** 完成后才能进行新的连接资格检查；保留旧调用成本，不退款或清零。只有JSON提案pilot、存在未决请求或没有真实连接通过证据时，停止启动并列出next action，不自动改跑mock、pilot或外部API backend。
2. 真实training panel/reference、可执行seed及checksums已冻结；toy precheck与正式panel分离。填写YAML中实际镜像digest、hardware/toolchain标识，不能制造数据或分数。执行机必须支持Linux rootless Docker/cgroup-v2，已分配20个评估CPU及并发容器内存。Windows登录成功不能替代隔离资格验收；在Linux上使用真实仓库路径及Python环境。
3. 阅读最新回归报告，代码或环境变更后运行相关无模型测试：`tests/contract/test_codex_quota_proxy.py`、`test_codex_autonomous_client.py`、`test_codex_tool_broker.py`、`test_instance_workers.py`、`test_round_resources.py`、`test_container_quiescence.py`、`test_ticket_dispatcher.py`及 `tests/integration/test_strict_codex_backend.py`、`test_v1_episodes.py`、`test_heldout.py`。安装版CLI的 `test_codex_native_wire.py` 通过 `MAESTRO_RUN_NATIVE_WIRE=1` 启用，仍使用虚构登录和模拟上游。模拟测试成功不代表物理平台qualification；真实资格验收缺失时报告缺口，不直接用正式warm试跑代替。不要另跑旧proposal probe来代替严格接入检查。
4. 检查未决模型请求、容器及 `CLAIMED/LAUNCHING/RUNNING/UNKNOWN` jobs。需要可信执行器证据才能reconcile；新建目录不能绕过未决执行。不可复用资源配置不同的旧warm checkpoint。

任一必要条件不满足，报告具体阻塞、文件/接口与next action，不发起warm模型调用或正式评估，不把未启动写成成功。条件全部满足后保存解析后的配置，在唯一artifact root运行以下命令。`python`必须指向已安装MAESTRO和固定Coral的目标Linux环境：

```bash
run_root="artifacts/warm-start-20cpu-5iw-$(date -u +%Y%m%dT%H%M%SZ)"
python -m maestro --root "$run_root" bootstrap --config configs/warm_start.yaml --seed 0
```

只有bootstrap成功退出且checkpoint完整后，才使用同一 `run_root` 执行：

```bash
python -m maestro --root "$run_root" replay --verify-ledger
python -m maestro --root "$run_root" report
```

禁止使用 `smoke`，它会启动模拟及三组main。保留plan-first/EditGrant、实际启动前ticket/phase/fence检查、固定panel评分顺序、计量及每wave统一settlement。warm不竞价收费，但仍需ticket和记账；取得本wave结果后才进入下一wave。遇到执行不确定性保留状态，不盲目重跑、退款或重置预算。

结束后报告：实际开始/结束时间、4个方向、5 waves及每agent机会数/真实完成评估数、首次seed和每wave规划/改代码小测试/评估/settlement的wall时间、累计CPU及模型请求用量、实际并发证据、checkpoint和报告绝对路径、ledger replay及异常。并行阶段wall时间按最早开始到最晚结束计算，不能求和4个agent的时间。缺测量时标明未知，不能用timeout预算填成实测；机会不等于成功实验。先前30–35分钟只是排期假设，56分钟是含seed的名义阶段timeout预算，均不是端到端硬上限。
