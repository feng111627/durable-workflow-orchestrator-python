"""SQLite 持久化层：工作流定义、运行实例、任务实例的状态存储。

所有状态变更都通过 compare-and-swap 完成，非法变更抛 InvalidStateTransition。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

from .errors import InvalidStateTransition, WorkflowNotFound
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
"""


class Store:
    """基于 SQLite 的持久化存储（WAL 模式，支持服务重启后恢复）。"""

    def __init__(self, db_path: str = "orchestrator.db"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

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

    def reset_running_tasks(self, run_id: str) -> int:
        """把 RUNNING 状态的任务重置为 PENDING（用于重启恢复），返回重置数量。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET status = ? WHERE run_id = ? AND status = ?",
                (TaskStatus.PENDING.value, run_id, TaskStatus.RUNNING.value),
            )
            return cur.rowcount
