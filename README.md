# Durable Workflow Orchestrator

基于 FastAPI + SQLite 的本地工作流编排服务：定义一组有依赖关系的任务，由服务按依赖顺序调度执行，支持失败重试、重启续跑与并发控制，所有状态持久化在 SQLite 中。支持多进程/多实例部署：多个服务进程共用同一数据库文件时，通过租约与栅栏令牌保证同一任务同一时刻只有一个执行者在跑。

## 快速开始

```bash
uv run python main.py          # 默认 127.0.0.1:8000，数据库 ./orchestrator.db
# 或用环境变量配置：
ORCH_DB_PATH=/tmp/orch.db ORCH_MAX_CONCURRENT_TASKS=2 PORT=8000 uv run python main.py
```

服务内置三个演示动作（`app/builtins.py`）：`echo`（返回 message）、`sleep`（等待 N 秒）、`fail`（按 kind=retryable/fatal 抛错）。自定义动作用 `app.handlers.register("名字", 函数)` 注册，函数接收任务 `params` 字典，可同步或异步。

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
# 查询状态（任务上会带 owner 字段，表示当前持有租约的执行者）
curl localhost:8000/runs/<run_id>
# 重跑一个 FAILED 的任务（仅允许 FAILED -> PENDING；被阻塞的下游会自动解除阻塞）
curl -X POST localhost:8000/runs/<run_id>/tasks/<task_name>/rerun
# 查看执行者列表与租约配置（多实例可观测性）
curl localhost:8000/executors
```

## 重试与恢复规则

- 动作抛出 `RetryableTaskError` 视为**可重试失败**：任务回到 PENDING 重新排队，直到成功或重试次数超过 `max_retries`，此后标记为 FAILED。
- 抛出 `FatalTaskError` 或任何其他异常视为**不可重试失败**：立即标记 FAILED，即使配置了重试次数也不重跑。
- 状态机：`PENDING -> RUNNING -> SUCCEEDED/FAILED`，重试时为 `RUNNING -> PENDING`，失败级联时下游为 `PENDING -> BLOCKED`，上游恢复后 `BLOCKED -> PENDING`。所有迁移都是 SQLite 层面的 compare-and-swap。
- **重启恢复**：运行与任务状态全部落盘（SQLite WAL）。服务启动时自动扫描未完成的运行：已 SUCCEEDED 的任务任何情况下都不重跑；确认失联（租约过期）的执行者留下的 RUNNING 任务被接管重新排队；仍被存活执行者持有的任务保持原样。

## 失败影响面与恢复续跑

- 某个任务彻底失败（FAILED）后，它的**直接和间接下游**中尚未开始的任务会被级联标记为 `BLOCKED`（`error` 字段注明被哪个上游阻塞），不会混在普通的 PENDING 等待里；整个运行标记 FAILED。与失败任务无关的分支不受影响，照常执行。
- 通过 rerun 接口把 FAILED 任务重新排队（`FAILED -> PENDING`）后，所有不再有失败祖先的 `BLOCKED` 任务会**自动解除阻塞**回到 PENDING，顺着依赖继续跑完，不需要重新提交整个工作流；菱形依赖下，只有当全部失败的上游都恢复后下游才解除阻塞。
- 对任务的非法操作返回 409 及**可区分**的 `reason`，便于调用方分别处理：
  - `task_already_succeeded`：任务已成功，任何情况下都不会再跑；
  - `task_currently_running`：任务正在被执行者处理中；
  - `task_blocked_by_upstream`：任务被上游失败阻塞，需先恢复失败的上游；
  - `invalid_state_transition`：其他非法迁移（携带 `current`/`target`）。

## 多实例 / 多执行者部署

多个服务进程（或多台机器上的实例）可以共用同一个 SQLite 数据库文件并行工作。每个进程是一个**执行者（executor）**，行为由两个参数控制：

| 环境变量 | 含义 | 默认 |
|---|---|---|
| `ORCH_EXECUTOR_ID` | 执行者标识，缺省自动生成；多实例部署时建议显式设为进程唯一值 | 自动生成 |
| `ORCH_LEASE_SECONDS` | **失联判定超时**：任务租约超过该时长未续期，即认为持有它的执行者已失联 | `30` |
| `ORCH_HEARTBEAT_INTERVAL` | **心跳间隔**：执行者定期上报存活并续期自己持有的任务租约，必须小于租约时长 | `5` |

规则：

- **认领即加租**：执行者认领任务（`PENDING -> RUNNING`）时获得一个租约（到期时间 = 当前时间 + `ORCH_LEASE_SECONDS`）和一个单调递增的栅栏令牌（fencing token）。租约未过期时，其他执行者不能认领该任务——不会看着“有人在跑”就抢过来重复执行。
- **心跳续期**：存活执行者每隔 `ORCH_HEARTBEAT_INTERVAL` 上报心跳并续期自己全部在跑任务的租约，因此正常执行（即使任务本身跑很久）不会被误判失联。
- **失联接管**：只有租约过期（进程崩溃、卡死、网络隔离等导致心跳停止）的任务才会被其他执行者接管，重新排队执行；接管时 `attempts` 等记录接着算，不丢失、不串。每个实例后台都有接管循环，无需重启即可完成接管。
- **迟到写回被忽略**：被接管后，原失联执行者如果又活过来往回写结果，因为它持有的栅栏令牌已过期（令牌随每次认领递增），写入会被直接忽略——不会覆盖、也不会干扰新执行者的进度和结果。日志中会出现 `stale write of lost executor ignored`。
- **恢复分清“真失联”和“还在跑”**：服务重启或新实例启动时（以及后台接管循环每次扫描时），只接管租约已过期的任务；仍被其他存活执行者正常处理的任务保持原样，不会被重置重跑。
- `GET /executors` 可以查看本实例 id、租约配置以及所有上报过心跳的执行者，用于观测集群状态。

注意：多实例共用 SQLite 文件要求各实例能访问同一文件路径（如共享卷）；SQLite 单写者特性意味着写吞吐有限，但认领/写回均为单语句 CAS，正确性不依赖实例间的任何额外协调。

## 并发控制

- `ORCH_MAX_CONCURRENT_TASKS`（默认 4）限制单实例全局同时执行的任务数，超出的任务在信号量上排队等待，不会被丢弃。
- 同一 `(workflow_name, idempotency_key)` 的重复提交是幂等的：数据库唯一约束保证只创建一次运行。
- 同一运行不会被并行调度两次；同一任务实例的认领是原子的（租约 CAS），并发下只有一个执行者能进入，保证不产生重复副作用。

## 怎么验证

```bash
uv run pytest tests/ -v
```

测试按业务域分组，结尾会输出“业务覆盖汇总”，列出每个业务下的用例与结果：

- **依赖顺序与定义校验**（8 例）：拓扑序执行、循环/自依赖/未知依赖/重名/未知 action 拒绝、重复提交幂等。
- **重试与失败处理**（7 例）：可重试直到成功、重试耗尽标记 FAILED、fatal 不重试、下游阻塞标记 BLOCKED、FAILED 任务重跑、SUCCEEDED 重跑被拒（409，reason 可区分）。
- **服务重启后的续跑恢复**（2 例）：中断运行续跑且已成功任务不重跑、已完成运行不被恢复。
- **并发控制与幂等**（5 例）：并发上限遵守且排队任务不丢弃、重复调度不重复执行、并发认领 CAS 拒绝、非法状态迁移拒绝、并发下幂等创建唯一。
- **多执行者安全与失联接管**（6 例）：存活执行者的任务不可被抢、租约过期接管且 attempts 连续计数、**8 线程真实并发认领恰有一个成功**、失联执行者迟到写回被忽略（双执行者引擎级场景）、存储层栅栏拒绝、心跳阻止存活同伴被接管。
- **失败影响面标记与恢复续跑**（5 例）：直接+间接下游级联 BLOCKED 且无关分支不受影响、上游恢复后下游自动续跑（不重提交流程）、菱形依赖全部上游恢复才解除阻塞、对 BLOCKED/RUNNING 任务的非法操作返回可区分 reason。

### 本地手动验证多实例行为

```bash
# 终端 1：实例 A（短租约便于观察）
ORCH_DB_PATH=/tmp/orch.db ORCH_EXECUTOR_ID=A ORCH_LEASE_SECONDS=5 ORCH_HEARTBEAT_INTERVAL=1 PORT=8000 uv run python main.py
# 终端 2：实例 B，共用同一数据库
ORCH_DB_PATH=/tmp/orch.db ORCH_EXECUTOR_ID=B ORCH_LEASE_SECONDS=5 ORCH_HEARTBEAT_INTERVAL=1 PORT=8001 uv run python main.py

# 向 A 提交一个会跑一段时间的工作流（sleep 动作），查询可见任务的 owner 是 A
curl -X POST localhost:8000/workflows -H 'content-type: application/json' -d \
  '{"name":"demo","tasks":[{"name":"t","action":"sleep","params":{"seconds":30}}]}'
curl -X POST localhost:8000/workflows/demo/runs -H 'content-type: application/json' -d '{"idempotency_key":"r1"}'
curl localhost:8001/runs/<run_id>   # owner 为 A 期间，B 不会重复执行

# 杀掉 A（模拟崩溃）：约 5 秒（租约超时）后 B 的后台接管循环会把任务重新排队并执行完
kill -9 <A 的 pid>
curl localhost:8001/runs/<run_id>   # owner 变为 B，attempts 接着计数，最终 SUCCEEDED

# 查看两个实例的心跳上报
curl localhost:8001/executors
```

也可以按“快速开始”启动单个服务后用上面的 curl 命令手动验证基础能力。
