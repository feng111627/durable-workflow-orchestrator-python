"""SQLite 持久化层：工作流定义、运行实例、任务实例的状态存储。

所有状态变更都通过 compare-and-swap 完成，非法变更抛 InvalidStateTransition。
多执行者安全：任务认领基于租约（lease）+ 栅栏令牌（fencing token），
失联执行者的迟到写回会被忽略；失败任务的下游被级联标记为 BLOCKED。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

from .errors import (
    InvalidStateTransition,
    TaskAlreadySucceeded,
    TaskBlockedByUpstream,
    TaskCurrentlyRunning,
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
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    UNIQUE (run_id, task_name)
);
CREATE TABLE IF NOT EXISTS executors (
    executor_id TEXT PRIMARY KEY,
    last_heartbeat REAL NOT NULL
);
"""

# 老版本数据库的 task_runs 表缺少租约相关列，启动时按需补齐
_TASK_RUN_COLUMNS = {
    "owner": "ALTER TABLE task_runs ADD COLUMN owner TEXT",
    "lease_expires_at": "ALTER TABLE task_runs ADD COLUMN lease_expires_at REAL",
    "fencing_token": (
        "ALTER TABLE task_runs ADD COLUMN fencing_token INTEGER NOT NULL DEFAULT 0"
    ),
}


class Store:
    """基于 SQLite 的持久化存储（WAL 模式，支持服务重启后恢复）。"""

    def __init__(self, db_path: str = "orchestrator.db"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)
            existing = {
                row["name"] for row in self._conn.execute("PRAGMA table_info(task_runs)")
            }
            for column, ddl in _TASK_RUN_COLUMNS.items():
                if column not in existing:
                    self._conn.execute(ddl)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

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
                    datetime.now(timezone.utc).isoformat(),
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
                    owner=t["owner"],
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

    def list_runs_needing_reconciliation(self) -> list[RunInfo]:
        """列出仍需要调度推进的运行。

        包含两类：
        1. 状态为 PENDING/RUNNING 的正常未完成运行；
        2. 运行已被标记 FAILED，但其中仍存在非终态任务
           （RUNNING/PENDING）的运行——这是旧版本“一个任务失败就整体
           停止调度”留下的不一致状态：运行判了失败，无关分支上的任务
           却还卡在执行中、没人推进。这些任务与失败任务没有依赖关系，
           应当继续跑完；SUCCEEDED 运行的任务全部终态，不会被列出。
        """
        nonterminal = (TaskStatus.PENDING.value, TaskStatus.RUNNING.value)
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id FROM runs WHERE status IN (?, ?) "
                "OR (status = ? AND EXISTS ("
                "  SELECT 1 FROM task_runs WHERE task_runs.run_id = runs.run_id "
                "  AND status IN (?, ?)))",
                (
                    RunStatus.PENDING.value,
                    RunStatus.RUNNING.value,
                    RunStatus.FAILED.value,
                    *nonterminal,
                ),
            ).fetchall()
        return [self.get_run(r["run_id"]) for r in rows]

    def set_run_status(self, run_id: str, status: RunStatus) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE runs SET status = ? WHERE run_id = ?", (status.value, run_id)
            )

    def mark_run_active(self, run_id: str) -> None:
        """把运行标记为进行中（PENDING/FAILED -> RUNNING）；已成功的不回退。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE runs SET status = ? WHERE run_id = ? AND status != ?",
                (RunStatus.RUNNING.value, run_id, RunStatus.SUCCEEDED.value),
            )

    # ---- 任务实例 ----
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
        params: list[object] = [
            target.value,
            error,
            json.dumps(result) if result is not None else None,
            run_id,
            task_name,
            *[s.value for s in allowed_from],
        ]
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE task_runs SET status = ?, error = ?, result = ?, "
                f"attempts = attempts + ? "
                f"WHERE run_id = ? AND task_name = ? AND status IN ({placeholders})",
                [params[0], params[1], params[2], 1 if increment_attempts else 0]
                + params[3:],
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

    # ---- 多执行者：心跳与租约 ----
    def heartbeat(self, executor_id: str, lease_seconds: float) -> None:
        """上报执行者存活，并续期其持有的全部 RUNNING 任务租约。"""
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO executors (executor_id, last_heartbeat) VALUES (?, ?) "
                "ON CONFLICT(executor_id) DO UPDATE SET last_heartbeat = excluded.last_heartbeat",
                (executor_id, now),
            )
            self._conn.execute(
                "UPDATE task_runs SET lease_expires_at = ? "
                "WHERE owner = ? AND status = ?",
                (now + lease_seconds, executor_id, TaskStatus.RUNNING.value),
            )

    def list_executors(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT executor_id, last_heartbeat FROM executors ORDER BY executor_id"
            ).fetchall()
        return [dict(r) for r in rows]

    def claim_task(
        self, run_id: str, task_name: str, executor_id: str, lease_seconds: float
    ) -> int:
        """认领任务：PENDING -> RUNNING，或接管租约已过期的 RUNNING 任务。

        成功返回本次认领的栅栏令牌（fencing token）；任务被其他存活执行者
        持有或状态不允许时抛 InvalidStateTransition。认领会推进 attempts，
        被接管的任务在原有尝试次数上继续累计。
        """
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ?, owner = ?, lease_expires_at = ?, "
                "fencing_token = fencing_token + 1, attempts = attempts + 1, "
                "error = NULL "
                "WHERE run_id = ? AND task_name = ? AND ("
                "  status = ? OR (status = ? AND lease_expires_at < ?)"
                ")",
                (
                    TaskStatus.RUNNING.value,
                    executor_id,
                    now + lease_seconds,
                    run_id,
                    task_name,
                    TaskStatus.PENDING.value,
                    TaskStatus.RUNNING.value,
                    now,
                ),
            )
            if cur.rowcount == 0:
                row = self._conn.execute(
                    "SELECT status, owner FROM task_runs WHERE run_id = ? AND task_name = ?",
                    (run_id, task_name),
                ).fetchone()
                current = row["status"] if row else "MISSING"
                raise InvalidStateTransition(
                    f"task '{task_name}' in run {run_id} cannot be claimed "
                    f"by '{executor_id}': current={current} owner={row['owner'] if row else None}",
                    current=current,
                    target=TaskStatus.RUNNING.value,
                )
            row = self._conn.execute(
                "SELECT fencing_token FROM task_runs WHERE run_id = ? AND task_name = ?",
                (run_id, task_name),
            ).fetchone()
            return row["fencing_token"]

    def complete_task(
        self,
        run_id: str,
        task_name: str,
        executor_id: str,
        fencing_token: int,
        target: TaskStatus,
        *,
        error: str | None = None,
        result: object = None,
    ) -> bool:
        """带栅栏令牌的写回：只有租约仍属于该执行者时才生效。

        返回 True 表示写入成功；返回 False 表示这是失联执行者的迟到写回
        （租约已被接管），写入被忽略，不影响新执行者的进度与结果。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ?, error = ?, result = ?, "
                "owner = CASE WHEN ? IN (?, ?) THEN NULL ELSE owner END, "
                "lease_expires_at = CASE WHEN ? IN (?, ?) THEN NULL ELSE lease_expires_at END "
                "WHERE run_id = ? AND task_name = ? AND status = ? "
                "AND owner = ? AND fencing_token = ?",
                (
                    target.value,
                    error,
                    json.dumps(result) if result is not None else None,
                    target.value,
                    TaskStatus.SUCCEEDED.value,
                    TaskStatus.FAILED.value,
                    target.value,
                    TaskStatus.SUCCEEDED.value,
                    TaskStatus.FAILED.value,
                    run_id,
                    task_name,
                    TaskStatus.RUNNING.value,
                    executor_id,
                    fencing_token,
                ),
            )
            return cur.rowcount > 0

    def reclaim_expired_leases(self, now: float | None = None) -> int:
        """接管失联执行者：租约已过期的 RUNNING 任务重置为 PENDING 重新排队。

        仍被存活执行者持有（租约未过期）的任务保持原样。attempts 与
        fencing_token 都保留：尝试次数接着算，旧执行者的写回依然无效。
        """
        now = time.time() if now is None else now
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ?, owner = NULL, lease_expires_at = NULL "
                "WHERE status = ? AND lease_expires_at < ?",
                (TaskStatus.PENDING.value, TaskStatus.RUNNING.value, now),
            )
            return cur.rowcount

    # ---- 失败影响面：级联阻塞与恢复 ----
    def fail_task_and_block_downstream(
        self,
        run_id: str,
        task_name: str,
        executor_id: str,
        fencing_token: int,
        error: str,
    ) -> bool:
        """栅栏写回 FAILED，并把直接+间接下游中未开始的任务级联标记为 BLOCKED。

        返回 False 表示迟到写回被忽略（此时不做任何级联）。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ?, error = ?, owner = NULL, "
                "lease_expires_at = NULL "
                "WHERE run_id = ? AND task_name = ? AND status = ? "
                "AND owner = ? AND fencing_token = ?",
                (
                    TaskStatus.FAILED.value,
                    error,
                    run_id,
                    task_name,
                    TaskStatus.RUNNING.value,
                    executor_id,
                    fencing_token,
                ),
            )
            if cur.rowcount == 0:
                return False
            downstream = self._downstream_closure(run_id, task_name)
            if downstream:
                placeholders = ",".join("?" for _ in downstream)
                self._conn.execute(
                    f"UPDATE task_runs SET status = ?, "
                    f"error = ? "
                    f"WHERE run_id = ? AND task_name IN ({placeholders}) "
                    f"AND status = ?",
                    [
                        TaskStatus.BLOCKED.value,
                        f"blocked by failed upstream '{task_name}'",
                        run_id,
                        *downstream,
                        TaskStatus.PENDING.value,
                    ],
                )
            return True

    def requeue_failed_task(self, run_id: str, task_name: str) -> None:
        """把 FAILED 任务重新排队（FAILED -> PENDING），并自动解除下游阻塞。

        对已成功 / 正在执行 / 被上游阻塞的任务抛出各自可区分的错误。
        解除阻塞的规则：BLOCKED 任务的所有祖先都不再 FAILED 时回到 PENDING。
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
                )
            current = TaskStatus(row["status"])
            if current == TaskStatus.SUCCEEDED:
                raise TaskAlreadySucceeded(
                    f"task '{task_name}' already SUCCEEDED and cannot be rerun",
                    current=current.value,
                    target=TaskStatus.PENDING.value,
                )
            if current == TaskStatus.RUNNING:
                raise TaskCurrentlyRunning(
                    f"task '{task_name}' is currently RUNNING",
                    current=current.value,
                    target=TaskStatus.PENDING.value,
                )
            if current == TaskStatus.BLOCKED:
                raise TaskBlockedByUpstream(
                    f"task '{task_name}' is blocked by a failed upstream task; "
                    f"recover the failed upstream first",
                    current=current.value,
                    target=TaskStatus.PENDING.value,
                )
            if current != TaskStatus.FAILED:
                raise InvalidStateTransition(
                    f"task '{task_name}' cannot go {current.value} -> PENDING",
                    current=current.value,
                    target=TaskStatus.PENDING.value,
                )
            self._conn.execute(
                "UPDATE task_runs SET status = ?, error = NULL "
                "WHERE run_id = ? AND task_name = ? AND status = ?",
                (TaskStatus.PENDING.value, run_id, task_name, TaskStatus.FAILED.value),
            )
            self._unblock_recovered(run_id)

    # ---- 内部工具 ----
    def _downstream_closure(self, run_id: str, task_name: str) -> list[str]:
        """从工作流定义计算 task_name 的全部直接+间接下游任务名。"""
        run = self._conn.execute(
            "SELECT workflow_name FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        spec = self.get_workflow(run["workflow_name"])
        children: dict[str, list[str]] = {}
        for t in spec.tasks:
            for dep in t.depends_on:
                children.setdefault(dep, []).append(t.name)
        seen: set[str] = set()
        stack = list(children.get(task_name, []))
        while stack:
            name = stack.pop()
            if name in seen:
                continue
            seen.add(name)
            stack.extend(children.get(name, []))
        return sorted(seen)

    def _unblock_recovered(self, run_id: str) -> None:
        """把不再有 FAILED 祖先的 BLOCKED 任务解除阻塞（调用方须持有锁且在事务中）。"""
        run = self._conn.execute(
            "SELECT workflow_name FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        spec = self.get_workflow(run["workflow_name"])
        rows = self._conn.execute(
            "SELECT task_name, status FROM task_runs WHERE run_id = ?", (run_id,)
        ).fetchall()
        status_by_name = {r["task_name"]: r["status"] for r in rows}
        failed = {n for n, s in status_by_name.items() if s == TaskStatus.FAILED.value}
        ancestors: dict[str, set[str]] = {t.name: set() for t in spec.tasks}
        for t in spec.tasks:  # 拓扑展开传递闭包（spec 已保证无环）
            for dep in t.depends_on:
                ancestors[t.name].add(dep)
                ancestors[t.name] |= ancestors[dep]
        for t in spec.tasks:
            if status_by_name[t.name] != TaskStatus.BLOCKED.value:
                continue
            if not (ancestors[t.name] & failed):
                self._conn.execute(
                    "UPDATE task_runs SET status = ?, error = NULL "
                    "WHERE run_id = ? AND task_name = ? AND status = ?",
                    (TaskStatus.PENDING.value, run_id, t.name, TaskStatus.BLOCKED.value),
                )
