"""SQLite 持久化层：工作流定义、运行实例、任务实例的状态存储。

多执行者安全模型
----------------
每个执行者（服务进程/实例）在 ``executors`` 表中登记并定期上报心跳
（``heartbeat_at``）。任务被认领时写入：

* ``executor_id``：当前持有者；
* ``claim_epoch``：每次（重新）认领单调递增的栅栏令牌（fencing token）；
* ``heartbeat_at``：该任务上一次收到持有者心跳的时间。

- 认领（claim）是数据库内的 compare-and-swap：PENDING 或"持有者心跳已超时"
  的 RUNNING 才能被认领，因此正在被活执行者处理的任务不会被别人抢走；
- 执行者完成任务时必须同时带上自己的 ``(executor_id, claim_epoch)``，
  失联后又活过来的旧执行者写回会因令牌不匹配抛 :class:`StaleExecutorWrite`，
  调用方应当忽略，绝不允许覆盖新执行者的进度；
- 只有心跳超过配置阈值的任务才会被接管重排队，接管后 attempts 继续累计。

所有时间均为 UTC ISO-8601 字符串。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from .errors import (
    InvalidStateTransition,
    StaleExecutorWrite,
    WorkflowNotFound,
)
from .models import RunInfo, RunStatus, TaskRunInfo, TaskStatus, WorkflowSpec

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    name TEXT PRIMARY KEY,
    spec TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (workflow_name, idempotency_key)
);
CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    result TEXT,
    executor_id TEXT,
    claim_epoch INTEGER NOT NULL DEFAULT 0,
    heartbeat_at TEXT,
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    UNIQUE (run_id, task_name)
);
CREATE TABLE IF NOT EXISTS executors (
    executor_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    last_heartbeat_at TEXT NOT NULL
);
"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Store:
    """基于 SQLite 的持久化存储（WAL 模式，支持多进程共享与重启恢复）。"""

    def __init__(
        self,
        db_path: str = "orchestrator.db",
        *,
        clock: Callable[[], datetime] | None = None,
    ):
        self.db_path = db_path
        # clock 仅用于让时间相关行为在测试中可确定性地驱动，生产用真实 UTC 时钟
        self._clock = clock or _utcnow
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        # 多进程/多线程并发写时等待锁而不是立刻报 database is locked
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """对旧版数据库补齐新列（老数据平滑升级，不破坏既有调用方式）。"""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(task_runs)")}
        if "executor_id" not in cols:
            self._conn.execute("ALTER TABLE task_runs ADD COLUMN executor_id TEXT")
        if "claim_epoch" not in cols:
            self._conn.execute(
                "ALTER TABLE task_runs ADD COLUMN claim_epoch INTEGER NOT NULL DEFAULT 0"
            )
        if "heartbeat_at" not in cols:
            self._conn.execute("ALTER TABLE task_runs ADD COLUMN heartbeat_at TEXT")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _now_iso(self) -> str:
        return self._clock().astimezone(timezone.utc).isoformat()

    # ---- 工作流定义 ----
    def save_workflow(self, spec: WorkflowSpec) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO workflows (name, spec) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET spec = excluded.spec",
                (spec.name, spec.model_dump_json()),
            )

    def get_workflow(self, name: str) -> WorkflowSpec | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT spec FROM workflows WHERE name = ?", (name,)
            ).fetchone()
        return WorkflowSpec.model_validate_json(row["spec"]) if row else None

    # ---- 运行实例 ----
    def create_run_idempotent(
        self, workflow_name: str, idempotency_key: str
    ) -> tuple[str, bool]:
        """按 (workflow_name, idempotency_key) 幂等创建运行，返回 (run_id, created)。

        重复提交同一 key 返回已存在的 run_id，created=False，不会产生并行执行。
        """
        spec = self.get_workflow(workflow_name)
        if spec is None:
            raise WorkflowNotFound(f"workflow '{workflow_name}' not defined")
        with self._lock, self._conn:
            run_id = uuid.uuid4().hex
            cur = self._conn.execute(
                "INSERT INTO runs (run_id, workflow_name, idempotency_key, status, created_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(workflow_name, idempotency_key) DO NOTHING",
                (
                    run_id,
                    workflow_name,
                    idempotency_key,
                    RunStatus.PENDING.value,
                    self._now_iso(),
                ),
            )
            if cur.rowcount == 0:
                row = self._conn.execute(
                    "SELECT run_id FROM runs WHERE workflow_name = ? AND idempotency_key = ?",
                    (workflow_name, idempotency_key),
                ).fetchone()
                return row["run_id"], False
            for task in spec.tasks:
                self._conn.execute(
                    "INSERT INTO task_runs (run_id, task_name, status) VALUES (?, ?, ?)",
                    (run_id, task.name, TaskStatus.PENDING.value),
                )
            return run_id, True

    def get_run(self, run_id: str) -> RunInfo | None:
        with self._lock:
            run = self._conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                return None
            tasks = self._conn.execute(
                "SELECT * FROM task_runs WHERE run_id = ? ORDER BY seq", (run_id,)
            ).fetchall()
        return RunInfo(
            run_id=run["run_id"],
            workflow_name=run["workflow_name"],
            idempotency_key=run["idempotency_key"],
            status=RunStatus(run["status"]),
            tasks=[
                TaskRunInfo(
                    task_name=t["task_name"],
                    status=TaskStatus(t["status"]),
                    attempts=t["attempts"],
                    error=t["error"],
                    result=json.loads(t["result"]) if t["result"] is not None else None,
                    executor_id=t["executor_id"],
                    claim_epoch=t["claim_epoch"],
                )
                for t in tasks
            ],
        )

    def list_unfinished_runs(self) -> list[RunInfo]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id FROM runs WHERE status IN (?, ?)",
                (RunStatus.PENDING.value, RunStatus.RUNNING.value),
            ).fetchall()
        return [self.get_run(r["run_id"]) for r in rows]

    def set_run_status(self, run_id: str, status: RunStatus) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE runs SET status = ? WHERE run_id = ?", (status.value, run_id)
            )

    # ---- 执行者登记与心跳 ----
    def register_executor(self, executor_id: str) -> None:
        """登记一个执行者（幂等）。"""
        now = self._now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO executors (executor_id, started_at, last_heartbeat_at) "
                "VALUES (?, ?, ?) ON CONFLICT(executor_id) DO NOTHING",
                (executor_id, now, now),
            )

    def heartbeat_executor(self, executor_id: str) -> None:
        """上报执行者存活；若该执行者此前已被注销（记录不存在）则忽略。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE executors SET last_heartbeat_at = ? WHERE executor_id = ?",
                (self._now_iso(), executor_id),
            )

    def deregister_executor(self, executor_id: str) -> None:
        """执行者正常退出时注销自己（崩溃场景无需调用，靠心跳超时判定）。"""
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM executors WHERE executor_id = ?", (executor_id,)
            )

    # ---- 任务状态迁移（通用 CAS，保持原有接口兼容） ----
    def transition_task(
        self,
        run_id: str,
        task_name: str,
        target: TaskStatus,
        *,
        allowed_from: tuple[TaskStatus, ...],
        error: str | None = None,
        result: object = None,
        increment_attempts: bool = False,
    ) -> None:
        """CAS 状态迁移；当前状态不在 allowed_from 中时抛 InvalidStateTransition。"""
        placeholders = ",".join("?" for _ in allowed_from)
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE task_runs SET status = ?, error = ?, result = ?, "
                f"attempts = attempts + ? "
                f"WHERE run_id = ? AND task_name = ? AND status IN ({placeholders})",
                [target.value, error, json.dumps(result) if result is not None else None,
                 1 if increment_attempts else 0, run_id, task_name]
                + [s.value for s in allowed_from],
            )
            if cur.rowcount == 0:
                row = self._conn.execute(
                    "SELECT status FROM task_runs WHERE run_id = ? AND task_name = ?",
                    (run_id, task_name),
                ).fetchone()
                current = row["status"] if row else "MISSING"
                raise InvalidStateTransition(
                    f"task '{task_name}' in run {run_id} cannot go "
                    f"{current} -> {target.value}",
                    current=current,
                    target=target.value,
                )

    def reset_running_tasks(self, run_id: str) -> int:
        """把 RUNNING 状态的任务重置为 PENDING 并释放租约（用于正常停机交接）。

        注意：多实例场景下不应无条件调用本方法——活执行者持有的任务不能被
        重置。失联接管请使用 :meth:`requeue_stale_tasks`。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ?, executor_id = NULL, heartbeat_at = NULL "
                "WHERE run_id = ? AND status = ?",
                (TaskStatus.PENDING.value, run_id, TaskStatus.RUNNING.value),
            )
            return cur.rowcount

    # ---- 认领 / 栅栏写回 / 心跳（多执行者核心） ----
    def claim_task(
        self,
        run_id: str,
        task_name: str,
        executor_id: str,
        *,
        heartbeat_timeout_seconds: float,
    ) -> int | None:
        """原子认领一个任务。

        可认领的条件（数据库内 CAS，二选一）：

        1. 当前为 PENDING（从未开始或等待重试）；
        2. 当前为 RUNNING，但持有者心跳距今已超过 ``heartbeat_timeout_seconds``
           ——即原执行者确实失联，此时发生**接管**。

        正在被活执行者处理（心跳新鲜）的任务不会被认领。认领成功后
        attempts + 1（接管时历史尝试次数自然继续累计）、claim_epoch + 1，
        返回新的 claim_epoch（栅栏令牌）；无可认领任务返回 None。
        """
        now = self._clock().astimezone(timezone.utc)
        cutoff = (now - timedelta(seconds=heartbeat_timeout_seconds)).isoformat()
        now_iso = now.isoformat()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT status, heartbeat_at FROM task_runs "
                "WHERE run_id = ? AND task_name = ?",
                (run_id, task_name),
            ).fetchone()
            if row is None:
                return None
            if row["status"] == TaskStatus.PENDING.value:
                eligible = True
            elif row["status"] == TaskStatus.RUNNING.value:
                # 没有心跳记录或心跳早于截止时间 => 判定失联，可接管
                hb = row["heartbeat_at"]
                eligible = hb is None or hb < cutoff
            else:
                eligible = False
            if not eligible:
                return None
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ?, attempts = attempts + 1, "
                "executor_id = ?, claim_epoch = claim_epoch + 1, "
                "error = NULL, heartbeat_at = ? "
                "WHERE run_id = ? AND task_name = ? AND status = ?",
                (
                    TaskStatus.RUNNING.value,
                    executor_id,
                    now_iso,
                    run_id,
                    task_name,
                    row["status"],
                ),
            )
            if cur.rowcount == 0:
                return None
            out = self._conn.execute(
                "SELECT claim_epoch FROM task_runs WHERE run_id = ? AND task_name = ?",
                (run_id, task_name),
            ).fetchone()
            return int(out["claim_epoch"])

    def heartbeat_task(
        self, run_id: str, task_name: str, executor_id: str, claim_epoch: int
    ) -> bool:
        """执行者为自己持有的任务续心跳。

        只有 (executor_id, claim_epoch) 都匹配才刷新；任务已被接管（令牌过期）
        时返回 False，调用方应自行中止本次执行。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET heartbeat_at = ? "
                "WHERE run_id = ? AND task_name = ? AND status = ? "
                "AND executor_id = ? AND claim_epoch = ?",
                (
                    self._now_iso(),
                    run_id,
                    task_name,
                    TaskStatus.RUNNING.value,
                    executor_id,
                    claim_epoch,
                ),
            )
            return cur.rowcount > 0

    def complete_task(
        self,
        run_id: str,
        task_name: str,
        executor_id: str,
        claim_epoch: int,
        target: TaskStatus,
        *,
        error: str | None = None,
        result: object = None,
    ) -> None:
        """带栅栏令牌的任务写回（成功 / 失败 / 回到 PENDING 重试）。

        旧执行者失联后任务已被别人接管（claim_epoch 已增长）时，旧执行者的
        迟到写回抛 :class:`StaleExecutorWrite`，必须被忽略，不能覆盖新执行者
        的状态、结果或尝试计数。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT status, executor_id, claim_epoch FROM task_runs "
                "WHERE run_id = ? AND task_name = ?",
                (run_id, task_name),
            ).fetchone()
            if row is None:
                raise InvalidStateTransition(
                    f"task '{task_name}' in run {run_id} missing",
                    current="MISSING",
                    target=target.value,
                )
            if (
                row["status"] != TaskStatus.RUNNING.value
                or row["executor_id"] != executor_id
                or row["claim_epoch"] != claim_epoch
            ):
                # 任务已被接管或已被别的流程终结：本次写回是迟到写回
                raise StaleExecutorWrite(
                    f"stale write from executor '{executor_id}' for task "
                    f"'{task_name}' in run {run_id} ignored: current holder is "
                    f"'{row['executor_id']}' epoch {row['claim_epoch']}",
                    current=row["status"],
                    claim_epoch=int(row["claim_epoch"]),
                )
            self._conn.execute(
                "UPDATE task_runs SET status = ?, error = ?, result = ? "
                "WHERE run_id = ? AND task_name = ?",
                (
                    target.value,
                    error,
                    json.dumps(result) if result is not None else None,
                    run_id,
                    task_name,
                ),
            )

    def release_my_tasks(self, executor_id: str) -> int:
        """执行者正常停机：把自己仍持有的 RUNNING 任务释放回 PENDING（不含 epoch
        变更，等待被重新认领；attempts 保留）。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ?, executor_id = NULL, heartbeat_at = NULL "
                "WHERE executor_id = ? AND status = ?",
                (TaskStatus.PENDING.value, executor_id, TaskStatus.RUNNING.value),
            )
            return cur.rowcount

    def requeue_stale_tasks(
        self, *, heartbeat_timeout_seconds: float, executor_id: str | None = None
    ) -> list[tuple[str, str]]:
        """扫描并接管心跳超时的 RUNNING 任务，返回被重排队的 (run_id, task_name)。

        - 心跳新鲜（持有者还活着）的任务保持 RUNNING 原样不动；
        - 心跳超时（持有者崩溃/卡死）的任务回到 PENDING，保留 attempts 历史，
          随后可被任意执行者（含新启动实例）重新认领；
        - 已 SUCCEEDED 的任务永远不会被这里碰到。

        可选 ``executor_id`` 仅用于日志，不影响选择逻辑。
        """
        cutoff = (
            self._clock().astimezone(timezone.utc)
            - timedelta(seconds=heartbeat_timeout_seconds)
        ).isoformat()
        requeued: list[tuple[str, str]] = []
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT run_id, task_name FROM task_runs "
                "WHERE status = ? AND (heartbeat_at IS NULL OR heartbeat_at < ?)",
                (TaskStatus.RUNNING.value, cutoff),
            ).fetchall()
            for r in rows:
                self._conn.execute(
                    "UPDATE task_runs SET status = ?, executor_id = NULL, heartbeat_at = NULL "
                    "WHERE run_id = ? AND task_name = ? AND status = ?",
                    (
                        TaskStatus.PENDING.value,
                        r["run_id"],
                        r["task_name"],
                        TaskStatus.RUNNING.value,
                    ),
                )
                requeued.append((r["run_id"], r["task_name"]))
        return requeued

    # ---- 失败影响面：阻塞与恢复续跑 ----
    def block_tasks(self, run_id: str, task_names_to_block: set[str]) -> list[str]:
        """把给定集合中的 PENDING 任务标记为 BLOCKED（状态/原因可区分）。"""
        blocked: list[str] = []
        with self._lock, self._conn:
            for name in task_names_to_block:
                cur = self._conn.execute(
                    "UPDATE task_runs SET status = ?, error = 'blocked by upstream failure' "
                    "WHERE run_id = ? AND task_name = ? AND status = ?",
                    (
                        TaskStatus.BLOCKED.value,
                        run_id,
                        name,
                        TaskStatus.PENDING.value,
                    ),
                )
                if cur.rowcount > 0:
                    blocked.append(name)
            return blocked

    def unblock_tasks(self, run_id: str, names: list[str]) -> list[str]:
        """把指定的 BLOCKED 任务解阻回 PENDING（保留 attempts/result 历史）。"""
        unblocked: list[str] = []
        with self._lock, self._conn:
            for name in names:
                cur = self._conn.execute(
                    "UPDATE task_runs SET status = ?, error = NULL "
                    "WHERE run_id = ? AND task_name = ? AND status = ?",
                    (
                        TaskStatus.PENDING.value,
                        run_id,
                        name,
                        TaskStatus.BLOCKED.value,
                    ),
                )
                if cur.rowcount > 0:
                    unblocked.append(name)
            return unblocked

    def rerun_failed_task(self, run_id: str, task_name: str) -> None:
        """手动重跑一个 FAILED 任务：仅允许 FAILED -> PENDING。

        其他状态给出可区分的细粒度 code（task_already_succeeded /
        task_running / task_blocked / task_pending），方便调用方区分到底
        动了哪种不该动的任务。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT status FROM task_runs WHERE run_id = ? AND task_name = ?",
                (run_id, task_name),
            ).fetchone()
            if row is None:
                raise InvalidStateTransition(
                    f"task '{task_name}' in run {run_id} not found",
                    current="MISSING",
                    target=TaskStatus.PENDING.value,
                    code="task_missing",
                )
            current = row["status"]
            if current == TaskStatus.FAILED.value:
                self._conn.execute(
                    "UPDATE task_runs SET status = ?, error = NULL, executor_id = NULL, "
                    "heartbeat_at = NULL WHERE run_id = ? AND task_name = ?",
                    (TaskStatus.PENDING.value, run_id, task_name),
                )
                return
            code = {
                TaskStatus.SUCCEEDED.value: "task_already_succeeded",
                TaskStatus.RUNNING.value: "task_running",
                TaskStatus.BLOCKED.value: "task_blocked",
                TaskStatus.PENDING.value: "task_pending",
            }.get(current, "task_not_runnable")
            raise InvalidStateTransition(
                f"task '{task_name}' in run {run_id} cannot be rerun from {current}",
                current=current,
                target=TaskStatus.PENDING.value,
                code=code,
            )
