# Durable Workflow Orchestrator

基于 FastAPI + SQLite 的本地工作流编排服务：定义一组有依赖关系的任务，由服务按依赖顺序调度执行，支持失败重试、重启续跑、并发控制，以及**多进程/多实例共享同一份数据时的执行者互斥、失联接管与失败影响面管理**。所有状态持久化在 SQLite 中。

## 快速开始

```bash
uv run python main.py          # 默认 127.0.0.1:8000，数据库 ./orchestrator.db
# 或用环境变量配置：
ORCH_DB_PATH=/tmp/orch.db ORCH_MAX_CONCURRENT_TASKS=2 PORT=8000 uv run python main.py
```

服务内置三个演示动作（`app/builtins.py`）：`echo`（返回 message）、`sleep`（等待 N 秒）、`fail`（按 kind=retryable/fatal 抛错）。自定义动作用 `app.handlers.register("名字", 函数)` 注册，函数接收任务 `params` 字典，可同步或异步。

### 多实例 / 多进程部署

多个服务进程只需指向**同一个数据库文件**（生产建议放在共享/网络盘或直接用 NFS/SMB 上的 SQLite，SQLite 以 WAL 模式 + busy_timeout 协调跨进程读写），各自用独立的 `ORCH_EXECUTOR_ID`（不配置会自动生成 `exec-<uuid>`）即可组成多执行者：

```bash
# 实例 A
ORCH_DB_PATH=/shared/orch.db ORCH_EXECUTOR_ID=node-a \
  ORCH_HEARTBEAT_INTERVAL_SECONDS=1 ORCH_HEARTBEAT_TIMEOUT_SECONDS=30 \
  uv run python main.py
# 实例 B（另一个进程/机器，同一份 DB）
ORCH_DB_PATH=/shared/orch.db ORCH_EXECUTOR_ID=node-b \
  ORCH_HEARTBEAT_INTERVAL_SECONDS=1 ORCH_HEARTBEAT_TIMEOUT_SECONDS=30 \
  PORT=8001 uv run python main.py
```

失联判定相关配置（同时也是 `Orchestrator(...)` 的构造参数）：

| 环境变量 | 含义 | 默认 |
|---|---|---|
| `ORCH_HEARTBEAT_INTERVAL_SECONDS` | 执行者/任务心跳上报间隔 | `1.0` |
| `ORCH_HEARTBEAT_TIMEOUT_SECONDS` | 心跳超过该时长未更新即判定执行者失联，任务可被接管（必须大于上报间隔） | `30.0` |
| `ORCH_MAINTENANCE_INTERVAL_SECONDS` | 后台维护周期：重排失联任务、接管没人调度的运行 | `1.0` |
| `ORCH_EXECUTOR_ID` | 执行者稳定身份；留空自动生成 | 自动 |
| `ORCH_MAX_CONCURRENT_TASKS` | 单实例内同时执行的任务数上限 | `4` |

## 工作流与任务的描述方式

```json
POST /workflows
{
  "name": "demo",
  "tasks": [
    {"name": "fetch",   "action": "echo",  "params": {"message": "data"}},
    {"name": "process", "action": "sleep", "depends_on": ["fetch"], "params": {"seconds": 1}},
    {"name": "notify",  "action": "echo",  "depends_on": ["process"], "max_retries": 3, "retry_delay_seconds": 0.5}
  ]
}
```

任务字段：

| 字段 | 含义 | 默认 |
|---|---|---|
| `name` | 任务名，工作流内唯一 | 必填 |
| `action` | 已注册的动作名 | 必填 |
| `depends_on` | 前置任务名列表，全部成功后本任务才可执行 | `[]` |
| `max_retries` | 可重试失败的最大重试次数 | `0` |
| `retry_delay_seconds` | 每次重试前的等待秒数 | `0` |
| `params` | 传给动作函数的参数字典 | `{}` |

定义时校验（不合法返回 422 及 `reason`）：任务名重复（`duplicate_task_name`）、依赖不存在的任务（`unknown_task_dependency`）、**循环依赖**（`cyclic_dependency`，含自依赖）、action 未注册（`unknown_action`）。

### 运行与查询

```bash
# 提交运行：idempotency_key 相同则返回同一 run_id（created=false），不会产生并行执行
curl -X POST localhost:8000/workflows/demo/runs -d '{"idempotency_key": "run-1"}' -H 'content-type: application/json'
# 查询状态
curl localhost:8000/runs/<run_id>
# 重跑一个 FAILED 的任务（仅允许 FAILED -> PENDING）
curl -X POST localhost:8000/runs/<run_id>/tasks/<task_name>/rerun
```

## 重试与恢复规则

- 动作抛出 `RetryableTaskError` 视为**可重试失败**：任务回到 PENDING 重新排队，直到成功或重试次数超过 `max_retries`，此后标记为 FAILED。
- 抛出 `FatalTaskError` 或任何其他异常视为**不可重试失败**：立即标记 FAILED，即使配置了重试次数也不重跑。
- 任一任务 FAILED 后，它的**直接和间接下游**被明确标记为 **`BLOCKED`**（错误信息为 `blocked by upstream failure`），与普通 `PENDING` 排队等待区分开；整个运行标记为 FAILED。等这个 FAILED 任务通过 rerun 接口重跑成功后，被挡住的下游会沿依赖**自动解阻（BLOCKED -> PENDING）并继续跑完，无需重新提交整个工作流**；若某下游还有另一个上游仍失败，则该下游继续保持 BLOCKED，直到所有上游恢复。
- 状态机：`PENDING -> RUNNING -> SUCCEEDED/FAILED`，重试时为 `RUNNING -> PENDING`，失败影响面为 `PENDING -> BLOCKED`（恢复时 `BLOCKED -> PENDING`）。所有迁移都是 SQLite 层面的 compare-and-swap，非法迁移（如把 SUCCEEDED 的任务再跑一遍）被拒绝并返回 409，`reason=invalid_state_transition`，并同时携带：
  - `current`/`target`：当前/目标状态；
  - `code`：细粒度原因码，区分到底动了哪种不该动的任务——`task_already_succeeded`（动已成功的）、`task_running`（动执行中的）、`task_blocked`（动被上游失败挡住的）、`task_pending`（动排队中的）、`task_missing`（任务不存在）。
- **重启恢复**：运行与任务状态全部落盘（SQLite WAL）。服务启动与运行期的后台维护循环都会扫描未完成运行，但**只接管确实失联的任务**（见下）；已 SUCCEEDED 的任务在任何情况下都不会再跑；被接管任务的 `attempts` 等历史计数继续累计，不会清零或串到别的任务。

## 多执行者安全：租约、失联判定、接管与迟到写回

每个实例是一个执行者（executor），在数据库中登记并按 `ORCH_HEARTBEAT_INTERVAL_SECONDS` 周期上报心跳。每个任务在被认领（claim）时持有一份**租约**：

- `executor_id`：当前持有者；
- `claim_epoch`：每次（重新）认领单调递增的整数，充当**栅栏令牌（fencing token）**；
- `heartbeat_at`：该任务最近一次收到持有者心跳的时间。

规则：

1. **同一时刻只有一个执行者真正在跑一个任务。** 认领是数据库内原子 CAS：只有 `PENDING` 任务、或持有者心跳已超过 `ORCH_HEARTBEAT_TIMEOUT_SECONDS` 的 `RUNNING` 任务能被认领。某个执行者正在处理、心跳仍新鲜时，别的执行者看到“有人在跑”**不会**把它抢过来重复执行。
2. **只有确实失联才接管。** 进程崩溃/被 kill/卡死导致心跳超时后，任务才会被重排为 PENDING，随后由任一存活实例（含新启动实例或已在运行实例的后台维护循环）重新认领；接管后 `claim_epoch` +1，`attempts` 历史保留。正常停机会主动释放自己仍持有的任务（回到 PENDING），因此滚动重启也能立即交接。
3. **失联执行者“复活”后的迟到写回被忽略。** 完成/失败/重试写回都必须携带认领时拿到的 `(executor_id, claim_epoch)`；任务已被别人接管（纪元已增长）后，旧执行者的写回会被拒绝（存储层抛 `stale_executor_write`，引擎记录告警后忽略），**绝不会覆盖或干扰新执行者的进度、结果或尝试计数**。执行长任务期间还会有任务级心跳续期，一旦发现自己的租约已被接管，会立即取消本地执行，避免继续产生副作用。
4. 心跳间隔与失联超时均可配置；约束是 `heartbeat_timeout_seconds > heartbeat_interval_seconds`，避免一次正常心跳抖动就被误判失联。间隔越短发现失联越快，但写心跳更频繁，请按任务平均时长权衡。

## 并发控制

- `ORCH_MAX_CONCURRENT_TASKS`（默认 4）限制单实例内同时执行的任务数，超出的任务在信号量上排队等待，不会被丢弃。
- 同一 `(workflow_name, idempotency_key)` 的重复提交是幂等的：数据库唯一约束保证只创建一次运行。
- 同一运行不会在同一实例内被并行调度两次；不同实例可同时观察同一运行，但任务实例的认领是数据库级原子 CAS，配合租约与栅栏令牌，保证多实例下也不产生重复副作用。

## 怎么验证

```bash
uv run pytest tests/ -v
```

测试按业务域分组，结尾会输出“业务覆盖汇总”，列出每个业务下的用例与结果：

- **依赖顺序与定义校验**（8 例）：拓扑序执行、循环/自依赖/未知依赖/重名/未知 action 拒绝、重复提交幂等。
- **重试与失败处理**（7 例）：可重试直到成功、重试耗尽标记 FAILED、fatal 不重试、下游 BLOCKED、FAILED 任务重跑、SUCCEEDED 重跑被拒（409）。
- **服务重启后的续跑恢复**（2 例）：中断运行续跑且已成功任务不重跑、已完成运行不被恢复。
- **重启恢复：区分活执行者与失联**（1 例）：恢复时活执行者的 RUNNING 任务保持原样，仅心跳超时的任务被重排队，已成功任务不碰，attempts 保留。
- **并发控制与幂等**（5 例）：并发上限遵守、重复调度不重复执行、并发认领 CAS 拒绝、非法迁移拒绝、并发下幂等创建唯一。
- **多执行者安全：租约/认领/接管/迟到写回**（14 例）：PENDING 单次认领且纪元递增、心跳保活不被抢、超时后才可接管、接管保留 attempts、成功任务永不认领、迟到成功/失败写回被栅栏挡下且不破坏新结果、旧纪元心跳无效、恢复只重排失联任务、可重试失败后再失联接管的 attempts 连续累计，以及多线程真实并发下的“唯一认领 / 唯一接管 / 迟到写回与新写回竞争”。
- **多执行者安全：保活/接管/迟到结果忽略（引擎级）**（6 例）：长任务持续心跳绝不被抢、崩溃者被另一实例接管跑完（attempts 接续）、已运行实例靠后台维护循环免重启接管、引擎层忽略失联复活者的结果、两实例真实异步并发调度同一运行不重复执行、两实例并发 rerun 同一 FAILED 任务只恢复一次。
- **多执行者安全：真实多进程接管**（1 例）：真正拉起两个独立 OS 进程共享同一 SQLite——持有者心跳新鲜时抢不走，`SIGKILL` 强杀持有者后由另一进程在失联超时后接管并完成，attempts=2、结果归接管者。
- **失败影响面：BLOCKED 级联与恢复续跑**（8 例）：直接/间接下游 BLOCKED、无关分支不受影响、rerun 后整链自动续跑且 attempts 接续、多上游部分恢复时继续阻塞、四种非法 rerun 各返回可区分的 `code`（含 API）。

多进程/并发用例基于数据库中的明确状态与屏障（barrier）同步，反复运行结果确定，不依赖时序巧合。可重复执行验证稳定性：

```bash
# 连跑多轮，观察全部通过且耗时稳定
for i in 1 2 3 4 5; do uv run pytest tests/ -q 2>&1 | grep -E "passed|failed"; done
```

也可以手动起两个进程对同一份数据库做端到端验证：用上面“多实例/多进程部署”的命令分别启动 node-a、node-b，向任一实例提交工作流后 `kill -9` 正在执行任务的那个进程，观察约一个失联超时周期后另一个实例接管并把运行推进到 SUCCEEDED，`GET /runs/<run_id>` 可看到任务的 `executor_id` 变为接管者、`claim_epoch` 增长、`attempts` 接续。

## 接口兼容性

- 现有接口路径、请求/响应字段保持不变；`TaskRunInfo` 仅**新增**了 `executor_id`、`claim_epoch` 两个带默认值的字段，老调用方不受影响。
- 错误响应在原有 `reason`/`current`/`target` 基础上**新增**可选 `code` 字段；只依赖旧字段的判断方式继续有效。
- `Store.transition_task` / `reset_running_tasks` 等旧方法保留；多实例场景请优先使用带租约的 `claim_task` / `complete_task` / `requeue_stale_tasks`。
