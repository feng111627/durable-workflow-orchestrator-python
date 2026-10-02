# Durable Workflow Orchestrator

基于 FastAPI + SQLite 的本地工作流编排服务：定义一组有依赖关系的任务，由服务按依赖顺序调度执行，支持失败重试、重启续跑与并发控制，所有状态持久化在 SQLite 中。

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
# 查询状态
curl localhost:8000/runs/<run_id>
# 重跑一个 FAILED 的任务（仅允许 FAILED -> PENDING）
curl -X POST localhost:8000/runs/<run_id>/tasks/<task_name>/rerun
```

## 重试与恢复规则

- 动作抛出 `RetryableTaskError` 视为**可重试失败**：任务回到 PENDING 重新排队，直到成功或重试次数超过 `max_retries`，此后标记为 FAILED。
- 抛出 `FatalTaskError` 或任何其他异常视为**不可重试失败**：立即标记 FAILED，即使配置了重试次数也不重跑。
- 任一任务 FAILED 后，其下游任务保持 PENDING 不再执行，整个运行标记 FAILED；可通过 rerun 接口把 FAILED 任务重新排队恢复运行。
- 状态机：`PENDING -> RUNNING -> SUCCEEDED/FAILED`，重试时为 `RUNNING -> PENDING`。所有迁移都是 SQLite 层面的 compare-and-swap，非法迁移（如把 SUCCEEDED 的任务再跑一遍）被拒绝并返回 409，`reason=invalid_state_transition`，且携带 `current`/`target` 便于区分原因。
- **重启恢复**：运行与任务状态全部落盘（SQLite WAL）。服务启动时自动扫描未完成的运行：已 SUCCEEDED 的任务不重跑；中断时处于 RUNNING 的任务重置为 PENDING 重新排队（不丢失）；随后继续调度直到终态。

## 并发控制

- `ORCH_MAX_CONCURRENT_TASKS`（默认 4）限制全局同时执行的任务数，超出的任务在信号量上排队等待，不会被丢弃。
- 同一 `(workflow_name, idempotency_key)` 的重复提交是幂等的：数据库唯一约束保证只创建一次运行。
- 同一运行不会被并行调度两次；同一任务实例的 `PENDING -> RUNNING` 认领是原子的，并发下只有一个执行者能进入，保证不产生重复副作用。

## 怎么验证

```bash
uv run pytest tests/ -v
```

测试按业务域分组，结尾会输出“业务覆盖汇总”，列出每个业务下的用例与结果：

- **依赖顺序与定义校验**（8 例）：拓扑序执行、循环/自依赖/未知依赖/重名/未知 action 拒绝、重复提交幂等。
- **重试与失败处理**（7 例）：可重试直到成功、重试耗尽标记 FAILED、fatal 不重试、下游阻塞、FAILED 任务重跑、SUCCEEDED 重跑被拒（409）。
- **服务重启后的续跑恢复**（2 例）：中断运行续跑且已成功任务不重跑、已完成运行不被恢复。
- **并发控制与幂等**（5 例）：并发上限遵守且排队任务不丢弃、重复调度不重复执行、并发认领 CAS 拒绝、非法状态迁移拒绝、并发下幂等创建唯一。

也可以按“快速开始”启动服务后用上面的 curl 命令手动验证。
