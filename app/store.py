"""SQLite 持久化层：工作流/任务记录的存取与原子状态转换。

设计要点：
- 单连接 + 进程内锁串行化所有写操作；状态转换用单条条件 UPDATE 保证原子性
  （只在 status / lease_owner 符合预期时才生效），避免并发下重复执行同一任务。
- running 任务带租约（lease_owner + lease_expires_at），进程崩溃后租约过期可被接管。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from .errors import (
    ConcurrencyLimitReached,
    LeaseMismatchError,
    TaskAlreadyRunningError,
    TaskNotReadyError,
)
from .models import (
    FailureKind,
    TaskRecord,
    TaskStatus,
    WorkflowRecord,
    WorkflowStatus,
)

_TASK_COLUMNS = (
    "run_id, task_id, status, dependencies, payload, attempt, "
    "max_attempts, backoff_seconds, not_before, lease_owner, lease_expires_at, "
    "last_error, failure_kind, result"
)


class Store:
    def __init__(self, db_path: str) -> None:
        self._db = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,  # 显式管理事务
            timeout=30.0,
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        self._init_schema()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflows (
                    run_id     TEXT PRIMARY KEY,
                    status     TEXT NOT NULL,
                    definition TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    run_id           TEXT NOT NULL,
                    task_id          TEXT NOT NULL,
                    status           TEXT NOT NULL,
                    dependencies     TEXT NOT NULL DEFAULT '[]',
                    payload          TEXT NOT NULL DEFAULT '{}',
                    attempt          INTEGER NOT NULL DEFAULT 1,
                    max_attempts     INTEGER NOT NULL DEFAULT 3,
                    backoff_seconds  REAL NOT NULL DEFAULT 0,
                    not_before       REAL NOT NULL DEFAULT 0,
                    lease_owner      TEXT,
                    lease_expires_at REAL,
                    last_error       TEXT,
                    failure_kind     TEXT,
                    result           TEXT,
                    PRIMARY KEY (run_id, task_id),
                    FOREIGN KEY (run_id) REFERENCES workflows(run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_run_status
                    ON tasks(run_id, status);
                """
            )

    @contextmanager
    def _tx(self) -> Iterator[None]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self._db.execute("ROLLBACK")
                raise
            else:
                self._db.execute("COMMIT")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ------------------------------------------------------------- row mapping
    def _row_to_task(self, row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            run_id=row["run_id"],
            task_id=row["task_id"],
            status=TaskStatus(row["status"]),
            dependencies=tuple(json.loads(row["dependencies"])),
            payload=json.loads(row["payload"]),
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            backoff_seconds=row["backoff_seconds"],
            not_before=row["not_before"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            last_error=row["last_error"],
            failure_kind=(
                FailureKind(row["failure_kind"]) if row["failure_kind"] else None
            ),
            result=(json.loads(row["result"]) if row["result"] is not None else None),
        )

    # -------------------------------------------------------------- workflows
    def insert_workflow_if_absent(self, run_id: str, definition: dict) -> bool:
        """插入工作流；若 run_id 已存在则返回 False（提交幂等）。"""
        with self._tx():
            cur = self._db.execute(
                "INSERT OR IGNORE INTO workflows(run_id, status, definition) "
                "VALUES (?, ?, ?)",
                (run_id, WorkflowStatus.PENDING.value, json.dumps(definition)),
            )
            return cur.rowcount == 1

    def get_workflow(self, run_id: str) -> WorkflowRecord | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM workflows WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            return None
        return WorkflowRecord(
            run_id=row["run_id"],
            status=WorkflowStatus(row["status"]),
            definition=json.loads(row["definition"]),
        )

    def update_workflow_status(
        self, run_id: str, status: WorkflowStatus
    ) -> None:
        with self._tx():
            self._db.execute(
                "UPDATE workflows SET status = ? WHERE run_id = ?",
                (status.value, run_id),
            )

    def list_workflow_runs(self) -> list[WorkflowRecord]:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM workflows ORDER BY run_id"
            ).fetchall()
        return [
            WorkflowRecord(
                run_id=r["run_id"],
                status=WorkflowStatus(r["status"]),
                definition=json.loads(r["definition"]),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------ tasks
    def insert_task(self, task: TaskRecord) -> None:
        with self._tx():
            self._db.execute(
                f"INSERT INTO tasks ({_TASK_COLUMNS}) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task.run_id,
                    task.task_id,
                    task.status.value,
                    json.dumps(list(task.dependencies)),
                    json.dumps(task.payload),
                    task.attempt,
                    task.max_attempts,
                    task.backoff_seconds,
                    task.not_before,
                    task.lease_owner,
                    task.lease_expires_at,
                    task.last_error,
                    task.failure_kind.value if task.failure_kind else None,
                    (
                        json.dumps(task.result, ensure_ascii=False)
                        if task.result is not None
                        else None
                    ),
                ),
            )

    def _fetch_task(self, run_id: str, task_id: str) -> TaskRecord | None:
        row = self._db.execute(
            "SELECT * FROM tasks WHERE run_id = ? AND task_id = ?",
            (run_id, task_id),
        ).fetchone()
        return self._row_to_task(row) if row else None

    def get_task(self, run_id: str, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._fetch_task(run_id, task_id)

    def list_tasks(self, run_id: str) -> list[TaskRecord]:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM tasks WHERE run_id = ? ORDER BY task_id",
                (run_id,),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def list_ready_tasks(self, run_id: str) -> list[TaskRecord]:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM tasks WHERE run_id = ? AND status = ? "
                "ORDER BY task_id",
                (run_id, TaskStatus.READY.value),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def count_running(self, run_id: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) AS c FROM tasks "
                "WHERE run_id = ? AND status = ?",
                (run_id, TaskStatus.RUNNING.value),
            ).fetchone()
        return row["c"]

    # ------------------------------------------------------------- state moves
    def _claim(
        self,
        run_id: str | None,
        task_id: str | None,
        owner: str,
        lease_seconds: float,
        now: float,
        max_running: int | None,
    ) -> TaskRecord:
        """在单个事务里完成“检查并发上限 -> 原子认领”。

        - task_id 为 None 时挑选 run_id 工作流中任意一个 ready 任务；
        - run_id 为 None 时跨全部工作流挑选（调度器用）；
        - 条件 UPDATE 保证同一任务只会被一个执行者抢到。
        """
        with self._tx():
            if max_running is not None:
                q = "SELECT COUNT(*) AS c FROM tasks WHERE status = ?"
                params: tuple[object, ...] = (TaskStatus.RUNNING.value,)
                if run_id is not None:
                    q += " AND run_id = ?"
                    params = (TaskStatus.RUNNING.value, run_id)
                n = self._db.execute(q, params).fetchone()["c"]
                if n >= max_running:
                    raise ConcurrencyLimitReached
            if task_id is None:
                row = self._db.execute(
                    "SELECT * FROM tasks WHERE status = ? "
                    + ("AND run_id = ? " if run_id is not None else "")
                    + "AND not_before <= ? ORDER BY run_id, task_id LIMIT 1",
                    (
                        (TaskStatus.READY.value, now)
                        if run_id is None
                        else (TaskStatus.READY.value, run_id, now)
                    ),
                ).fetchone()
                if row is None:
                    raise TaskNotReadyError("no_ready_task")
                run_id, task_id = row["run_id"], row["task_id"]
            else:
                row = self._db.execute(
                    "SELECT * FROM tasks WHERE run_id = ? AND task_id = ?",
                    (run_id, task_id),
                ).fetchone()
                if row is None:
                    raise TaskNotReadyError("task_not_found")
                status = TaskStatus(row["status"])
                if status is TaskStatus.SUCCEEDED:
                    raise TaskNotReadyError("task_already_succeeded")
                if status is TaskStatus.FAILED:
                    raise TaskNotReadyError("task_already_failed")
                if status is TaskStatus.RUNNING:
                    raise TaskAlreadyRunningError("task_already_running")
                if status is TaskStatus.PENDING:
                    raise TaskNotReadyError("task_blocked_by_dependency")
                if status is TaskStatus.RETRY_WAIT:
                    raise TaskNotReadyError("task_retry_waiting")
                if row["not_before"] > now:
                    raise TaskNotReadyError("task_retry_waiting")
            cur = self._db.execute(
                "UPDATE tasks SET status = ?, attempt = attempt, "
                "lease_owner = ?, lease_expires_at = ? "
                "WHERE run_id = ? AND task_id = ? AND status = ?",
                (
                    TaskStatus.RUNNING.value,
                    owner,
                    now + lease_seconds,
                    run_id,
                    task_id,
                    TaskStatus.READY.value,
                ),
            )
            if cur.rowcount != 1:
                # 被其它线程/进程抢先认领
                raise TaskAlreadyRunningError("task_already_running")
            return self._fetch_task(run_id, task_id)  # type: ignore[arg-type]

    def claim_next_task(
        self,
        owner: str,
        lease_seconds: float,
        now: float,
        max_running: int | None = None,
        run_id: str | None = None,
    ) -> TaskRecord | None:
        try:
            return self._claim(
                run_id, None, owner, lease_seconds, now, max_running
            )
        except (TaskNotReadyError, ConcurrencyLimitReached):
            # 没有就绪任务或达到并发上限：任务继续排队，等待下一轮调度
            return None

    def claim_task(
        self,
        run_id: str,
        task_id: str,
        owner: str,
        lease_seconds: float,
        now: float,
        max_running: int | None = None,
    ) -> TaskRecord:
        return self._claim(
            run_id, task_id, owner, lease_seconds, now, max_running
        )

    def renew_lease(
        self, run_id: str, task_id: str, owner: str, expires_at: float
    ) -> bool:
        with self._tx():
            cur = self._db.execute(
                "UPDATE tasks SET lease_expires_at = ? "
                "WHERE run_id = ? AND task_id = ? AND status = ? "
                "AND lease_owner = ?",
                (
                    expires_at,
                    run_id,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            return cur.rowcount == 1

    def complete_task(
        self, run_id: str, task_id: str, owner: str, result: object, now: float
    ) -> TaskRecord:
        with self._tx():
            cur = self._db.execute(
                "UPDATE tasks SET status = ?, result = ?, lease_owner = NULL, "
                "lease_expires_at = NULL, last_error = NULL, failure_kind = NULL, "
                "not_before = 0 "
                "WHERE run_id = ? AND task_id = ? AND status = ? AND lease_owner = ?",
                (
                    TaskStatus.SUCCEEDED.value,
                    json.dumps(result, ensure_ascii=False),
                    run_id,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            if cur.rowcount != 1:
                if self._fetch_task(run_id, task_id) is None:
                    raise TaskNotReadyError("task_not_found")
                raise LeaseMismatchError
            # 解锁所有仅依赖该任务的后继
            self._db.execute(
                "UPDATE tasks SET status = ? "
                "WHERE run_id = ? AND status = ? AND dependencies = ?",
                (
                    TaskStatus.READY.value,
                    run_id,
                    TaskStatus.PENDING.value,
                    json.dumps([task_id]),
                ),
            )
            # 依赖多个前驱的后继：所有前驱都成功才解锁
            rows = self._db.execute(
                "SELECT task_id, dependencies FROM tasks "
                "WHERE run_id = ? AND status = ?",
                (run_id, TaskStatus.PENDING.value),
            ).fetchall()
            for r in rows:
                deps = json.loads(r["dependencies"])
                if len(deps) <= 1:
                    continue
                done = self._db.execute(
                    "SELECT COUNT(*) AS c FROM tasks "
                    "WHERE run_id = ? AND task_id IN (%s) AND status = ?"
                    % ",".join("?" for _ in deps),
                    (run_id, *deps, TaskStatus.SUCCEEDED.value),
                ).fetchone()["c"]
                if done == len(deps):
                    self._db.execute(
                        "UPDATE tasks SET status = ? "
                        "WHERE run_id = ? AND task_id = ?",
                        (TaskStatus.READY.value, run_id, r["task_id"]),
                    )
            return self._fetch_task(run_id, task_id)  # type: ignore[return-value]

    def retry_task(
        self,
        run_id: str,
        task_id: str,
        owner: str,
        attempt: int,
        not_before: float,
        backoff: float,
        error: str,
    ) -> TaskRecord:
        with self._tx():
            cur = self._db.execute(
                "UPDATE tasks SET status = ?, lease_owner = NULL, "
                "lease_expires_at = NULL, attempt = ?, not_before = ?, "
                "backoff_seconds = ?, last_error = ?, failure_kind = ? "
                "WHERE run_id = ? AND task_id = ? AND status = ? AND lease_owner = ?",
                (
                    TaskStatus.RETRY_WAIT.value,
                    attempt,
                    not_before,
                    backoff,
                    error,
                    FailureKind.RETRYABLE.value,
                    run_id,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            if cur.rowcount != 1:
                if self._fetch_task(run_id, task_id) is None:
                    raise TaskNotReadyError("task_not_found")
                raise LeaseMismatchError
            return self._fetch_task(run_id, task_id)  # type: ignore[return-value]

    def fail_task(
        self,
        run_id: str,
        task_id: str,
        owner: str,
        kind: FailureKind,
        error: str,
    ) -> TaskRecord:
        with self._tx():
            cur = self._db.execute(
                "UPDATE tasks SET status = ?, lease_owner = NULL, "
                "lease_expires_at = NULL, last_error = ?, failure_kind = ? "
                "WHERE run_id = ? AND task_id = ? AND status = ? AND lease_owner = ?",
                (
                    TaskStatus.FAILED.value,
                    error,
                    kind.value,
                    run_id,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            if cur.rowcount != 1:
                if self._fetch_task(run_id, task_id) is None:
                    raise TaskNotReadyError("task_not_found")
                raise LeaseMismatchError
            return self._fetch_task(run_id, task_id)  # type: ignore[return-value]

    def mark_dependents_failed(self, run_id: str, failed_task: str) -> int:
        """把直接或间接依赖失败任务的未完成任务标记为 failed（级联）。"""
        changed = 0
        with self._tx():
            while True:
                rows = self._db.execute(
                    "SELECT task_id, dependencies FROM tasks "
                    "WHERE run_id = ? AND status IN (?, ?, ?)",
                    (
                        run_id,
                        TaskStatus.PENDING.value,
                        TaskStatus.READY.value,
                        TaskStatus.RETRY_WAIT.value,
                    ),
                ).fetchall()
                hit = 0
                for r in rows:
                    deps = json.loads(r["dependencies"])
                    failed = self._db.execute(
                        "SELECT COUNT(*) AS c FROM tasks "
                        "WHERE run_id = ? AND task_id IN (%s) AND status = ?"
                        % ",".join("?" for _ in deps or [""]),
                        (run_id, *(deps or ["_"]), TaskStatus.FAILED.value),
                    ).fetchone()["c"]
                    if failed:
                        self._db.execute(
                            "UPDATE tasks SET status = ?, failure_kind = ?, "
                            "last_error = ? WHERE run_id = ? AND task_id = ?",
                            (
                                TaskStatus.FAILED.value,
                                FailureKind.FATAL.value,
                                "upstream task failed",
                                run_id,
                                r["task_id"],
                            ),
                        )
                        hit += 1
                changed += hit
                if hit == 0:
                    break
        return changed

    def requeue_due_tasks(self, now: float) -> int:
        """退避结束的 retry_wait 任务重新进入 ready。"""
        with self._tx():
            cur = self._db.execute(
                "UPDATE tasks SET status = ? "
                "WHERE status = ? AND not_before <= ?",
                (TaskStatus.READY.value, TaskStatus.RETRY_WAIT.value, now),
            )
            return cur.rowcount

    def requeue_task_if_due(
        self, run_id: str, task_id: str, now: float
    ) -> bool:
        """把单个退避到期的任务重新置为 ready；返回是否发生了状态转换。"""
        with self._tx():
            cur = self._db.execute(
                "UPDATE tasks SET status = ? "
                "WHERE run_id = ? AND task_id = ? AND status = ? "
                "AND not_before <= ?",
                (
                    TaskStatus.READY.value,
                    run_id,
                    task_id,
                    TaskStatus.RETRY_WAIT.value,
                    now,
                ),
            )
            return cur.rowcount == 1

    def reclaim_expired_leases(
        self, owner: str, lease_seconds: float, now: float
    ) -> list[TaskRecord]:
        """接管其它 owner 租约过期的 running 任务（进程重启/崩溃恢复）。"""
        with self._tx():
            rows = self._db.execute(
                "SELECT * FROM tasks WHERE status = ? AND lease_expires_at <= ?",
                (TaskStatus.RUNNING.value, now),
            ).fetchall()
            tasks = [self._row_to_task(r) for r in rows]
            if rows:
                self._db.execute(
                    "UPDATE tasks SET lease_owner = ?, lease_expires_at = ?, "
                    "attempt = attempt + 1 "
                    "WHERE status = ? AND lease_expires_at <= ?",
                    (
                        owner,
                        now + lease_seconds,
                        TaskStatus.RUNNING.value,
                        now,
                    ),
                )
                tasks = [
                    self._fetch_task(t.run_id, t.task_id) for t in tasks
                ]
        return [t for t in tasks if t is not None]

    def has_terminal_workflow(self, run_id: str) -> bool:
        rec = self.get_workflow(run_id)
        return rec is not None and rec.status in (
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.FAILED,
        )

    def reconcile_workflow(self, run_id: str) -> WorkflowStatus:
        """根据任务当前状态推导工作流终态；非终态则返回当前状态。"""
        with self._tx():
            wf = self._db.execute(
                "SELECT status FROM workflows WHERE run_id = ?", (run_id,)
            ).fetchone()
            if wf is None:
                raise KeyError(run_id)
            rows = self._db.execute(
                "SELECT status FROM tasks WHERE run_id = ?", (run_id,)
            ).fetchall()
            statuses = [TaskStatus(r["status"]) for r in rows]
            any_failed = any(s is TaskStatus.FAILED for s in statuses)
            all_succeeded = bool(statuses) and all(
                s is TaskStatus.SUCCEEDED for s in statuses
            )
            if any_failed:
                new_status = WorkflowStatus.FAILED
            elif all_succeeded:
                new_status = WorkflowStatus.SUCCEEDED
            else:
                new_status = WorkflowStatus.RUNNING
            if WorkflowStatus(wf["status"]) != new_status:
                self._db.execute(
                    "UPDATE workflows SET status = ? WHERE run_id = ?",
                    (new_status.value, run_id),
                )
            return new_status
