"""调度引擎：提交去重、循环检测、调度、租约、重试与重启恢复。

线程模型：
- 单个调度线程周期执行：退避到期重排队 -> 接管过期租约 -> 在并发上限内认领任务 -> 投递执行。
- ThreadPoolExecutor 执行任务处理器；running 任务执行期间由心跳线程续租，
  进程存活时租约不会过期；stop() 后心跳停止，租约到期可被新实例接管。
- 所有状态变更最终都落到 SQLite 的条件 UPDATE 上，保证重复提交/重复执行幂等。
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from .errors import (
    CycleDetectedError,
    DuplicateRunError,
    InvalidTaskError,
    LeaseMismatchError,
    TaskFailure,
)
from .handlers import HandlerRegistry
from .models import (
    FailureKind,
    TaskRecord,
    TaskStatus,
    WorkflowDef,
    WorkflowStatus,
)
from .store import Store

logger = logging.getLogger("workflow.engine")


@dataclass
class EngineConfig:
    max_concurrency: int = 2
    lease_seconds: float = 30.0
    poll_interval: float = 0.02
    # 租约续租间隔（小于 lease_seconds，保证存活时任务不会被别人接管）
    heartbeat_interval: float = 5.0
    # 可选的外部停止信号（测试用）：任务处理器可据此感知"进程被杀死"并立即返回
    stop_event: threading.Event | None = None


class Orchestrator:
    def __init__(
        self,
        store: Store,
        handlers: HandlerRegistry,
        config: EngineConfig | None = None,
        owner: str | None = None,
    ) -> None:
        self.store = store
        self.handlers = handlers
        self.config = config or EngineConfig()
        self.owner = owner or f"node-{uuid.uuid4().hex[:8]}"
        self._pool = ThreadPoolExecutor(
            max_workers=max(1, self.config.max_concurrency),
            thread_name_prefix="wf-task",
        )
        self._inflight: dict[tuple[str, str], Future[Any]] = {}
        self._inflight_lock = threading.Lock()
        # 本机在执行的任务 -> 心跳停止信号（stop 时统一置位，让租约尽快到期）
        self._heartbeat_stops: dict[tuple[str, str], threading.Event] = {}
        self._wakeup = threading.Event()
        self._stop = threading.Event()
        self._closed = False
        self._scheduler: threading.Thread | None = None

    # ============================================================ 校验与提交
    @staticmethod
    def validate(definition: WorkflowDef) -> None:
        if not definition.tasks:
            raise InvalidTaskError("workflow must contain at least one task")
        ids = [t.id for t in definition.tasks]
        if len(ids) != len(set(ids)):
            raise InvalidTaskError("duplicate task id")
        idset = set(ids)
        graph: dict[str, set[str]] = {}
        for t in definition.tasks:
            unknown = [d for d in t.dependencies if d not in idset]
            if unknown:
                raise InvalidTaskError(
                    f"task {t.id} depends on unknown task(s): {unknown}"
                )
            graph[t.id] = set(t.dependencies)
        Orchestrator._detect_cycle(graph)

    @staticmethod
    def _detect_cycle(graph: dict[str, set[str]]) -> None:
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in graph}

        def visit(node: str, stack: list[str]) -> None:
            color[node] = GRAY
            stack.append(node)
            for dep in sorted(graph[node]):
                if color[dep] == GRAY:
                    cycle = stack[stack.index(dep):] + [dep]
                    raise CycleDetectedError(" -> ".join(cycle))
                if color[dep] == WHITE:
                    visit(dep, stack)
            stack.pop()
            color[node] = BLACK

        for n in sorted(graph):
            if color[n] == WHITE:
                visit(n, [])

    def submit(self, run_id: str, definition: WorkflowDef) -> WorkflowStatus:
        self.validate(definition)
        # 处理器必须提前注册，提交时快速失败，不等到执行才报错
        known = set(self.handlers.names())
        for t in definition.tasks:
            if t.handler not in known:
                raise InvalidTaskError(f"task {t.id}: unknown handler {t.handler}")
        created = self.store.insert_workflow_if_absent(
            run_id, self._def_to_dict(definition)
        )
        if not created:
            # 重复提交同一次运行：拒绝再起一份，返回已有运行的状态信息
            rec = self.store.get_workflow(run_id)
            assert rec is not None
            raise DuplicateRunError(
                f"run_id={run_id} already exists with status "
                f"{rec.status.value}"
            )
        for t in definition.tasks:
            retry = t.retry if t.retry_set else definition.retry
            status = (
                TaskStatus.READY if not t.dependencies else TaskStatus.PENDING
            )
            self.store.insert_task(
                TaskRecord(
                    run_id=run_id,
                    task_id=t.id,
                    status=status,
                    dependencies=t.dependencies,
                    payload={"handler": t.handler, "data": t.payload},
                    max_attempts=max(1, retry.max_attempts),
                    backoff_seconds=max(0.0, retry.backoff_seconds),
                )
            )
        self.store.update_workflow_status(run_id, WorkflowStatus.RUNNING)
        self._wakeup.set()
        return WorkflowStatus.RUNNING

    @staticmethod
    def _def_to_dict(definition: WorkflowDef) -> dict:
        return {
            "retry": {
                "max_attempts": definition.retry.max_attempts,
                "backoff_seconds": definition.retry.backoff_seconds,
            },
            "tasks": [
                {
                    "id": t.id,
                    "handler": t.handler,
                    "dependencies": list(t.dependencies),
                    "payload": t.payload,
                    "retry": {
                        "max_attempts": t.retry.max_attempts,
                        "backoff_seconds": t.retry.backoff_seconds,
                    },
                }
                for t in definition.tasks
            ],
        }

    # ================================================================ 查询
    def get_workflow_status(self, run_id: str) -> WorkflowStatus:
        rec = self.store.get_workflow(run_id)
        if rec is None:
            raise KeyError(run_id)
        return rec.status

    def get_task_status(self, run_id: str, task_id: str) -> TaskStatus:
        task = self.store.get_task(run_id, task_id)
        if task is None:
            raise KeyError(task_id)
        return task.status

    def get_task(self, run_id: str, task_id: str) -> TaskRecord:
        task = self.store.get_task(run_id, task_id)
        if task is None:
            raise KeyError(task_id)
        return task

    # ====================================================== 手动触发一个任务
    def run_task(self, run_id: str, task_id: str) -> TaskStatus:
        """同步认领并执行单个任务，按退避策略持续重试直到到达终态。

        非法状态变更由 store 层抛出带 reason 的 TaskNotReadyError：
        task_already_succeeded / task_already_running / task_blocked_by_dependency
        / task_retry_waiting / concurrency_limit_reached 等。
        """
        if self.store.get_workflow(run_id) is None:
            raise KeyError(run_id)
        task = self.store.claim_task(
            run_id,
            task_id,
            self.owner,
            self.config.lease_seconds,
            time.time(),
            max_running=self.config.max_concurrency,
        )
        while True:
            self._execute(task)
            task = self.get_task(run_id, task_id)
            if task.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
                return task.status
            # RETRY_WAIT：等待退避结束后重新认领执行（并发上限内）
            delay = task.not_before - time.time()
            if delay > 0:
                time.sleep(delay)
            self.store.requeue_task_if_due(run_id, task_id, time.time())
            task = self.store.claim_task(
                run_id,
                task_id,
                self.owner,
                self.config.lease_seconds,
                time.time(),
                max_running=self.config.max_concurrency,
            )

    # ============================================================== 执行任务
    def _submit_execution(self, task: TaskRecord) -> None:
        key = (task.run_id, task.task_id)
        with self._inflight_lock:
            if key in self._inflight:
                return  # 本机已在执行，不重复投递
            fut = self._pool.submit(self._run_guarded, task)
            self._inflight[key] = fut
        fut.add_done_callback(lambda _f: self._drop_inflight(key))

    def _drop_inflight(self, key: tuple[str, str]) -> None:
        with self._inflight_lock:
            self._inflight.pop(key, None)
        self._wakeup.set()

    def _run_guarded(self, task: TaskRecord) -> None:
        try:
            self._execute(task)
        except Exception:
            if self._closed:
                # 关闭后旧工作线程的写回（含库已关闭）属预期丢弃，不刷错误栈
                logger.info(
                    "task %s/%s write-back ignored after shutdown",
                    task.run_id, task.task_id,
                )
            else:  # pragma: no cover - 防御性兜底
                logger.exception("task %s/%s execution crashed", task.run_id, task.task_id)

    def _execute(self, task: TaskRecord) -> None:
        run_id, task_id = task.run_id, task.task_id
        handler_name = task.payload.get("handler", "noop")
        data = task.payload.get("data", {})
        stop_beat = threading.Event()
        with self._inflight_lock:
            self._heartbeat_stops[(run_id, task_id)] = stop_beat
        beat = threading.Thread(
            target=self._heartbeat,
            args=(run_id, task_id, stop_beat),
            name=f"hb-{run_id}-{task_id}",
            daemon=True,
        )
        beat.start()
        try:
            try:
                fn = self.handlers.get(handler_name)
                result = fn(data)
            except TaskFailure as exc:
                self._handle_failure(task, exc.kind, exc.message or str(exc))
                return
            except Exception as exc:  # 未显式分类的异常：按可重试处理
                self._handle_failure(
                    task, FailureKind.RETRYABLE, f"{type(exc).__name__}: {exc}"
                )
                return
            try:
                self.store.complete_task(
                    run_id, task_id, self.owner, result, time.time()
                )
            except LeaseMismatchError:
                # 租约已被其它实例接管：本结果作废，绝不覆盖新执行的状态
                logger.warning(
                    "task %s/%s completed but lease lost; result discarded",
                    run_id, task_id,
                )
                return
            self.store.reconcile_workflow(run_id)
        finally:
            stop_beat.set()
            with self._inflight_lock:
                self._heartbeat_stops.pop((run_id, task_id), None)

    def _heartbeat(
        self, run_id: str, task_id: str, stop: threading.Event
    ) -> None:
        interval = max(0.01, self.config.heartbeat_interval)
        while not stop.wait(interval):
            try:
                ok = self.store.renew_lease(
                    run_id,
                    task_id,
                    self.owner,
                    time.time() + self.config.lease_seconds,
                )
            except Exception:
                break  # 库已关闭等情况：心跳线程安静退出
            if not ok:
                break  # 任务已不在自己名下（被接管或状态已变更）

    def _handle_failure(
        self, task: TaskRecord, kind: FailureKind, message: str
    ) -> None:
        run_id, task_id = task.run_id, task.task_id
        try:
            if kind is FailureKind.FATAL:
                self.store.fail_task(
                    run_id, task_id, self.owner, FailureKind.FATAL, message
                )
            elif task.attempt < task.max_attempts:
                now = time.time()
                self.store.retry_task(
                    run_id,
                    task_id,
                    self.owner,
                    attempt=task.attempt + 1,
                    not_before=now + task.backoff_seconds,
                    backoff=task.backoff_seconds,
                    error=message,
                )
            else:
                self.store.fail_task(
                    run_id,
                    task_id,
                    self.owner,
                    FailureKind.RETRYABLE,
                    f"{message} [retry budget exhausted: "
                    f"{task.max_attempts} attempt(s)]",
                )
                kind = FailureKind.FATAL  # 重试耗尽视同终结，级联下游
            if kind is FailureKind.FATAL:
                self.store.mark_dependents_failed(run_id, task_id)
        except LeaseMismatchError:
            logger.warning(
                "task %s/%s failure handling skipped: lease lost",
                run_id, task_id,
            )
            return
        self.store.reconcile_workflow(run_id)

    # ============================================================ 调度与恢复
    def recover(self) -> int:
        """重启恢复：接管过期租约、修正工作流状态。返回接管的任务数。"""
        now = time.time()
        n = 0
        for t in self.store.reclaim_expired_leases(
            self.owner, self.config.lease_seconds, now
        ):
            key = (t.run_id, t.task_id)
            with self._inflight_lock:
                local_running = key in self._inflight
            if not local_running:
                self._submit_execution(t)
                n += 1
        for wf in self.store.list_workflow_runs():
            self.store.reconcile_workflow(wf.run_id)
        self._wakeup.set()
        return n

    def start(self) -> None:
        if self._scheduler is not None and self._scheduler.is_alive():
            return
        self._stop.clear()
        self._wakeup.clear()
        self.recover()
        self._scheduler = threading.Thread(
            target=self._scheduler_loop, name="wf-scheduler", daemon=True
        )
        self._scheduler.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wakeup.set()
        # 停掉所有心跳：租约将到期，允许新实例接管（模拟崩溃后恢复）
        with self._inflight_lock:
            stops = list(self._heartbeat_stops.values())
        for ev in stops:
            ev.set()
        if self._scheduler is not None:
            self._scheduler.join(timeout=timeout)
            self._scheduler = None
        # 不再领取新任务；已在跑的任务线程自然结束（其写回会因租约失效被拒）
        self._closed = True
        self._pool.shutdown(wait=False, cancel_futures=True)

    def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:  # pragma: no cover - 调度循环不能因单次异常退出
                logger.exception("scheduler tick failed")
            self._wakeup.wait(self.config.poll_interval)
            self._wakeup.clear()

    def _tick(self) -> None:
        now = time.time()
        self.store.requeue_due_tasks(now)
        # 过期租约接管（本实例之外的崩溃节点）
        for t in self.store.reclaim_expired_leases(
            self.owner, self.config.lease_seconds, now
        ):
            key = (t.run_id, t.task_id)
            with self._inflight_lock:
                local_running = key in self._inflight or key in self._heartbeat_stops
            if not local_running:
                self._submit_execution(t)
        # 在并发上限内持续认领就绪任务
        while True:
            with self._inflight_lock:
                slots = self.config.max_concurrency - len(self._inflight)
            if slots <= 0:
                break
            task = self.store.claim_next_task(
                self.owner,
                self.config.lease_seconds,
                time.time(),
                max_running=self.config.max_concurrency,
            )
            if task is None:
                break
            self._submit_execution(task)
