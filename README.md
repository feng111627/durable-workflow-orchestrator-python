# Durable Workflow Orchestrator

一个可在本地运行的、支持持久化的工作流编排服务（FastAPI + SQLite + 线程池）。
用于调度和跟踪一组**有依赖关系**的任务，具备循环依赖检测、失败重试、
服务重启后续跑、并发上限控制与全链路幂等能力。

## 1. 能力总览

| 能力 | 行为 |
| --- | --- |
| 依赖调度 | 任务按 DAG 拓扑顺序执行；所有前驱成功后后继才会解锁 |
| 循环检测 | 提交时检测（含自依赖），返回 `409 cycle_detected` 且不落任何数据 |
| 提交幂等 | 同一 `run_id` 重复提交返回 `409 duplicate_run`，不会产生两个并行执行 |
| 失败分类 | 可重试失败按退避策略重试，超过 `max_attempts` 后标记失败；不可重试失败立即终止 |
| 级联失败 | 一个任务最终失败后，其直接/间接下游全部标记 `failed` |
| 重启续跑 | 状态全部落在 SQLite；进行中任务靠**租约（lease）**过期后被新实例接管，已成功任务绝不重跑 |
| 并发控制 | 全局 `max_concurrency` 上限，超过上限的任务保持排队，不会被丢弃 |
| 执行幂等 | 任务认领/完成均为数据库条件更新；同一任务不会被两个执行者同时完成；旧执行者的迟到结果会因租约不匹配被丢弃 |
| 非法状态 | 已成功/已失败/被依赖阻塞/退避中的任务再次执行，返回**可区分的 reason 码** |

## 2. 快速开始

```bash
uv sync                       # 安装依赖（已配置阿里云镜像）
uv run uvicorn main:app --reload
# 或 uv run python main.py  （默认监听 127.0.0.1:8000）
```

服务启动即内置 `noop` / `echo` / `sleep` / `flaky` / `boom` 五个示例处理器，
打开 <http://127.0.0.1:8000/docs> 可直接在 Swagger UI 调用。

环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WF_DB_PATH` | `./.workflow.db` | SQLite 数据库路径（自动建库，WAL 模式） |
| `WF_MAX_CONCURRENCY` | `4` | 全局同时执行任务数上限 |
| `WF_LEASE_SECONDS` | `30` | 任务租约时长；超时未续租视为执行节点死亡 |

## 3. 工作流与任务的描述方式

通过 `POST /workflows/{run_id}` 提交一个 JSON：

```json
{
  "retry": {"max_attempts": 3, "backoff_seconds": 0.5},
  "tasks": [
    {"id": "download", "handler": "echo", "payload": {"url": "https://example.com/a"}},
    {"id": "parse",     "handler": "sleep", "dependencies": ["download"],
     "payload": {"seconds": 0.1}, "retry": {"max_attempts": 5, "backoff_seconds": 1}},
    {"id": "load",      "handler": "echo", "dependencies": ["parse"]}
  ]
}
```

字段说明：

- 顶层 `retry`：工作流默认重试策略。
- `tasks[].id`：工作流内唯一的任务标识。
- `tasks[].handler`：已注册的处理器名称；未注册会在提交时被拒绝（`invalid_task`）。
- `tasks[].dependencies`：前驱任务 id 列表；前驱全部成功后该任务才变为 `ready`。
- `tasks[].payload`：传给处理器的任意 JSON 对象。
- `tasks[].retry`：**任务级**重试策略，提供即覆盖工作流默认值（哪怕数值与默认相同也算显式覆盖）。
  - `max_attempts`：最大尝试次数（含首次），最小为 1。
  - `backoff_seconds`：两次尝试之间的固定退避秒数。

### 编写自定义处理器

处理器签名为 `(payload: dict) -> Any`，返回值需可 JSON 序列化：

```python
from app.handlers import HandlerRegistry
from app.errors import RetryableTaskError, FatalTaskError

registry = HandlerRegistry()

def my_handler(payload: dict) -> dict:
    if is_transient_problem():
        raise RetryableTaskError("稍后可恢复，按策略重试")
    if is_bad_input():
        raise FatalTaskError("数据非法，重试没有意义")   # 立即失败，不再重试
    return {"ok": True}

registry.register("my_handler", my_handler)
```

抛出 `RetryableTaskError` 会按策略重试；抛出 `FatalTaskError` 立即失败；
抛出其它任意异常（如 `ValueError`）默认**按可重试处理**，错误信息会记录在
任务的 `last_error` 中。

## 4. 重试与恢复规则

### 4.1 重试

1. 任务首次失败若为可重试失败且 `attempt < max_attempts`，进入 `retry_wait`，
   `not_before = now + backoff_seconds`。
2. 退避到期后自动重新变为 `ready`，由调度器再次认领执行，`attempt +1`。
3. 达到 `max_attempts` 仍失败：任务标记 `failed`，`failure_kind = retryable`，
   `last_error` 带有 `retry budget exhausted` 说明。
4. 不可重试失败：第一次就标记 `failed`，`failure_kind = fatal`，`attempt` 保持 1。
5. 任务最终失败后，其所有直接/间接依赖者被级联标记为 `failed`，
   工作流整体变为 `failed`。

任务状态机：

```
pending ──(前驱全部成功)──▶ ready ──(认领)──▶ running
                              ▲                  │
                              └────(退避到期)────┤
                                          可重试失败 → retry_wait
                                          成功 → succeeded（终态）
                                          致命/次数耗尽 → failed（终态）
```

### 4.2 租约与重启续跑（at-least-once）

- 任务被认领后进入 `running`，记录 `lease_owner`（节点 id）与 `lease_expires_at`。
- 执行期间有**心跳线程**定期续租；节点存活时租约不会过期。
- 进程崩溃/被杀死 → 心跳停止 → 租约到期 → 新实例的 `recover()`/调度循环
  会原子地接管这些任务：`lease_owner` 改为新节点、`attempt +1`，然后继续执行。
- **已经 `succeeded` 的任务永远不会被重跑**；崩溃前还没开始的任务照常排队。
- 如果旧节点其实没死、在租约过期后才写回结果，该写回会因为
  `lease_owner` 已变更被数据库拒绝（`LeaseMismatchError`），
  绝不覆盖新执行产生的状态。

> 语义说明：接管意味着同一任务的处理器**可能被执行超过一次**（at-least-once）。
> 因此处理器应当设计为**幂等**（例如以 `(run_id, task_id)` 作为副作用去重键）。
> 状态层面的重复副作用已被条件更新杜绝，处理器层面的幂等由业务保证。

## 5. 并发相关配置

- `EngineConfig.max_concurrency`（或 `WF_MAX_CONCURRENCY`）：全局同时执行的任务上限，
  对所有工作流生效。超过上限时，就绪任务**留在队列里**，等有空槽立即被领取。
- 实现上由两层共同保证：
  1. 调度器只在空闲槽位 > 0 时才领取任务；
  2. 数据库认领是单条条件 `UPDATE ... WHERE status='ready'`，跨线程/跨进程也安全。
- `EngineConfig.poll_interval`：调度轮询间隔（默认 0.02s），同时每次任务完成会立即唤醒调度。
- `EngineConfig.heartbeat_interval`：续租间隔，应明显小于 `lease_seconds`。

## 6. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /workflows/{run_id}` | 提交工作流（201）；重复/循环/非法定义返回 409 |
| `GET  /workflows/{run_id}` | 查询工作流与全部任务状态（404 `not_found`） |
| `GET  /workflows/{run_id}/tasks/{task_id}` | 查询单个任务（含 attempt、last_error、result） |
| `POST /workflows/{run_id}/tasks/{task_id}/run` | 同步认领并执行单个任务（含重试直到终态），用于手动驱动/排障 |

### 非法状态变更的 reason 码（HTTP 409）

| reason | 含义 |
| --- | --- |
| `duplicate_run` | 该 `run_id` 已存在，拒绝重复提交 |
| `cycle_detected` | 依赖图存在环（错误信息含环路径） |
| `invalid_task` | 任务 id 重复/为空、依赖不存在、处理器未注册等 |
| `task_blocked_by_dependency` | 前驱还没全部成功，任务不能执行 |
| `task_already_succeeded` | 已成功任务不能再跑 |
| `task_already_failed` | 已失败任务不能再跑 |
| `task_already_running` | 任务正在被其它执行者执行（并发重复触发） |
| `task_retry_waiting` | 任务处于退避等待中，尚未到下次执行时间 |
| `concurrency_limit_reached` | 并发槽位已满（自动调度下任务会排队等待） |

## 7. 怎么验证

### 7.1 跑单元测试

```bash
uv run pytest            # 等价 uv run pytest -s
```

测试日志按**业务分组**打印，每个业务下列出全部用例与结果，结尾有覆盖汇总。
共 5 个业务分组、44 个用例（另含 1 个冒烟用例）：

- `tests/test_dependency.py` —— **依赖顺序与拓扑校验**：拓扑序、多前驱、
  直接/间接/自循环检测、未知依赖、重复 id、空工作流。
- `tests/test_retry_recovery.py` —— **重试策略与失败恢复**：预算内重试成功、
  致命失败不重试、预算耗尽、退避耗时、任务级覆盖、未分类异常、级联失败、
  退避中拒绝执行、自动调度下重试成功。
- `tests/test_restart.py` —— **重启后续跑**：已成功任务不重跑、进行中任务被接管、
  未完成工作流续跑、retry_wait 跨重启、recover 幂等（用两个引擎实例 + 同一文件库
  模拟进程崩溃与重启）。
- `tests/test_concurrency_idempotency.py` —— **并发控制与幂等**：超额排队不丢弃、
  峰值并发不超限、重复提交（含 8 线程并发提交同一 run_id）、并发手动执行同一任务、
  非法状态原因码可区分、成功任务零重复副作用、旧租约写回被拒。
- `tests/test_api.py` —— **HTTP API**：201/409/404/422 与各 reason 码。

日志样式：

```
▶ 业务：依赖顺序与拓扑校验
    ✓ test_manual_execution_respects_dependency_order
    ✓ test_direct_cycle_is_rejected
    ...
【重启后续跑】用例 5 个，通过 5，失败 0
    [PASS] test_succeeded_tasks_are_not_rerun_after_restart
    ...
合计：44 个用例，通过 44，失败 0
```

### 7.2 curl 手工验证

```bash
curl -s -X POST localhost:8000/workflows/demo -H 'Content-Type: application/json' -d '{
  "tasks": [
    {"id": "a", "handler": "echo", "payload": {"hello": "world"}},
    {"id": "b", "handler": "sleep", "dependencies": ["a"], "payload": {"seconds": 0.2}}
  ]
}'
curl -s localhost:8000/workflows/demo | python3 -m json.tool
# 重复提交 -> 409 duplicate_run
curl -s -i -X POST localhost:8000/workflows/demo -H 'Content-Type: application/json' -d '{"tasks":[{"id":"a","handler":"noop"}]}'
```

### 7.3 验证重启续跑

```bash
# 1) 提交一个含长任务的工作流后立刻 Ctrl-C 杀掉服务
WF_LEASE_SECONDS=2 uv run uvicorn main:app
# 2) 等超过租约时间后用同一个 WF_DB_PATH 重新启动，观察工作流自动跑到 succeeded；
#    GET /workflows/{run_id} 可看到接管任务的 attempt 递增且已成功任务不重复执行。
```

## 8. 代码结构

```
app/
  models.py    领域模型与状态枚举（TaskStatus / WorkflowStatus / FailureKind ...）
  errors.py    领域错误与可区分 reason 码；RetryableTaskError / FatalTaskError
  store.py     SQLite 持久化：条件 UPDATE 驱动的原子状态转换、租约、级联、对账
  handlers.py  处理器注册表
  engine.py    调度引擎：校验/提交、调度循环、执行、心跳续租、重试、崩溃恢复
  api.py       FastAPI 路由与错误到 HTTP 状态码的映射
main.py        本地运行入口与示例处理器
tests/         五个业务分组的单元测试 + conftest 分组日志
```
