"""调度引擎：按依赖拓扑序执行任务，控制并发，处理重试、失败与多执行者接管。

多执行者（多进程/多实例共享同一数据库）要点
-------------------------------------------
* 每个 Orchestrator 实例是一个“执行者”，拥有唯一 executor_id；
* 任务通过 :meth:`Store.claim_task` 原子认领：PENDING 或持有者心跳超时的
  RUNNING 才能被认领，活执行者手里的任务不会被抢；
* 认领返回单调递增的 claim_epoch（栅栏令牌）；执行期间定期续心跳，
  完成时必须带令牌写回，失联复活后的迟到写回抛 StaleExecutorWrite 被忽略；
* 后台维护循环定期：上报执行者心跳、把失联任务重排队、接管未完成运行；
* 某任务彻底 FAILED 后，其直接/间接下游被标成 BLOCKED（区别于普通 PENDING）；
  该 FAILED 任务被重跑成功后，被挡住的下游按依赖自动解阻续跑。
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from typing import Any

from . import handlers
from .errors import (
    InvalidStateTransition,
    RetryableTaskError,
    RunNotFound,
    StaleExecutorWrite,
    WorkflowNotFound,
)
from .models import RunInfo, RunStatus, TaskSpec, TaskStatus, WorkflowSpec
from .store import Store

logger = logging.getLogger("orchestrator.engine")

TERMINAL_RUN_STATES = (RunStatus.SUCCEEDED, RunStatus.FAILED)


class Orchestrator:
    def __init__(
        self,
        store: Store,
        max_concurrent_tasks: int = 4,
        *,
        executor_id: str | None = None,
        heartbeat_interval_seconds: float = 1.0,
        heartbeat_timeout_seconds: float = 30.0,
        maintenance_interval_seconds: float = 1.0,
        poll_interval_seconds: float = 0.1,
    ):
        if max_concurrent_tasks < 1:
            raise ValueError("max_concurrent_tasks must be >= 1")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be > 0")
        if heartbeat_timeout_seconds <= heartbeat_interval_seconds:
            # 超时阈值必须大于心跳间隔，否则一次正常的心跳抖动就会被误判失联
            raise ValueError(
                "heartbeat_timeout_seconds must be greater than "
                "heartbeat_interval_seconds"
            )
        if maintenance_interval_seconds <= 0 or poll_interval_seconds <= 0:
            raise ValueError("maintenance/poll interval must be > 0")
        self.store = store
        self.max_concurrent_tasks = max_concurrent_tasks
        self.executor_id = executor_id or f"exec-{uuid.uuid4().hex}"
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.maintenance_interval_seconds = maintenance_interval_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self._semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._run_tasks: dict[str, asyncio.Task] = {}
        self._bg_tasks: set[asyncio.Task] = set()
        self._started = False

    # ---- 生命周期 ----
    async def start(self) -> None:
        """登记执行者并启动心跳/接管维护循环（幂等）。"""
        if self._started:
            return
        self._started = True
        self.store.register_executor(self.executor_id)
        self._bg_tasks.add(asyncio.create_task(self._heartbeat_loop()))
        self._bg_tasks.add(asyncio.create_task(self._maintenance_loop()))
        logger.info("executor %s started", self.executor_id)

    def _ensure_started(self) -> None:
        # 兼容不调用 start() 的老用法：首次调度时惰性拉起后台循环
        if not self._started:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            self._started = True
            self.store.register_executor(self.executor_id)
            self._bg_tasks.add(asyncio.create_task(self._heartbeat_loop()))
            self._bg_tasks.add(asyncio.create_task(self._maintenance_loop()))

    async def shutdown(self) -> None:
        """正常停机：取消本实例的调度，把自己持有的任务释放回 PENDING 供他人
        接手，并注销执行者。崩溃场景没有机会执行本方法，靠心跳超时接管。"""
        for task in self._run_tasks.values():
            task.cancel()
        if self._run_tasks:
            await asyncio.gather(*self._run_tasks.values(), return_exceptions=True)
        self._run_tasks.clear()
        for task in self._bg_tasks:
            task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()
        if self._started:
            try:
                released = self.store.release_my_tasks(self.executor_id)
                if released:
                    logger.info(
                        "executor %s released %d task(s) for takeover",
                        self.executor_id, released,
                    )
                self.store.deregister_executor(self.executor_id)
            except Exception:
                logger.exception("error during executor deregistration")
            self._started = False

    # ---- 运行调度 ----
    async def start_run(self, run_id: str) -> None:
        """为一次运行启动（或恢复）调度循环；同一 run 在本实例不会并行调度两次。

        不同实例可以同时调度同一 run：任务认领是数据库级 CAS，不会重复执行。
        """
        self._ensure_started()
        existing = self._run_tasks.get(run_id)
        if existing is not None and not existing.done():
            logger.info("run %s already scheduled, skip duplicate start", run_id)
            return
        self._run_tasks[run_id] = asyncio.create_task(self._run_loop(run_id))

    async def recover_unfinished(self) -> list[str]:
        """启动/重启后接管未完成的运行，返回接管调度的 run_id 列表。

        关键区分：**只接管确实失联的任务**——心跳超过 heartbeat_timeout 的
        RUNNING 任务被重排为 PENDING（attempts 历史保留）；仍被活着的执行者
        正常持有（心跳新鲜）的任务保持 RUNNING 原样；已 SUCCEEDED 的任务
        在任何情况下都不会再跑。
        """
        self._ensure_started()
        requeued = self.store.requeue_stale_tasks(
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
            executor_id=self.executor_id,
        )
        for run_id, task_name in requeued:
            logger.warning(
                "executor %s takes over stale task %s in run %s (previous "
                "executor missed heartbeat for >%.1fs)",
                self.executor_id, task_name, run_id, self.heartbeat_timeout_seconds,
            )
        recovered: list[str] = []
        for run in self.store.list_unfinished_runs():
            await self.start_run(run.run_id)
            recovered.append(run.run_id)
        return recovered

    async def wait_run(self, run_id: str, timeout: float | None = None) -> RunInfo:
        """等待某次运行到达终态并返回其信息（测试与 API 用）。"""
        async def _poll() -> RunInfo:
            while True:
                info = self.store.get_run(run_id)
                if info is None:
                    raise RunNotFound(f"run {run_id} not found")
                if info.status in TERMINAL_RUN_STATES:
                    return info
                await asyncio.sleep(0.01)

        if timeout is None:
            return await _poll()
        return await asyncio.wait_for(_poll(), timeout=timeout)

    async def rerun_task(self, run_id: str, task_name: str) -> RunInfo:
        """重跑 FAILED 任务并自动恢复失败影响面。

        * 只允许 FAILED -> PENDING，其他状态抛带细粒度 code 的
          InvalidStateTransition（task_already_succeeded / task_running /
          task_blocked / task_pending）；
        * 原先被这次失败挡住的 BLOCKED 下游，在依赖重新满足后自动解阻回
          PENDING 并继续调度，无需重新提交整个工作流；attempts 等历史保留。
        """
        info = self.store.get_run(run_id)
        if info is None:
            raise RunNotFound(f"run {run_id} not found")
        spec = self.store.get_workflow(info.workflow_name)
        if spec is None:
            raise WorkflowNotFound(f"workflow '{info.workflow_name}' not defined")
        # 仅 FAILED 可重跑；其余状态在此抛出可区分的错误
        self.store.rerun_failed_task(run_id, task_name)
        # 运行可能正停在失败固定点（FAILED）。立即翻回 RUNNING，保证：
        # 1) 等待方（wait_run）不会立刻读到旧 FAILED 而提前返回；
        # 2) 即使本实例旧调度循环已退出、由别的实例接手，恢复也能被观察到。
        self.store.set_run_status(run_id, RunStatus.RUNNING)
        await self._unblock_recovered_downstream(run_id, spec)
        await self.start_run(run_id)
        return self.store.get_run(run_id)

    # ---- 后台循环 ----
    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval_seconds)
                try:
                    self.store.heartbeat_executor(self.executor_id)
                except Exception:
                    logger.exception("executor heartbeat failed")
        except asyncio.CancelledError:
            raise

    async def _maintenance_loop(self) -> None:
        """定期：重排失联任务 + 接管没人调度的未完成运行。"""
        try:
            while True:
                await asyncio.sleep(self.maintenance_interval_seconds)
                try:
                    requeued = self.store.requeue_stale_tasks(
                        heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
                        executor_id=self.executor_id,
                    )
                    for run_id, task_name in requeued:
                        logger.warning(
                            "executor %s takes over stale task %s in run %s",
                            self.executor_id, task_name, run_id,
                        )
                        await self.start_run(run_id)
                    # 新实例/漏网的运行也在这里被持续接管
                    for run in self.store.list_unfinished_runs():
                        await self.start_run(run.run_id)
                except Exception:
                    logger.exception("maintenance cycle failed")
        except asyncio.CancelledError:
            raise

    # ---- 内部实现 ----
    async def _run_loop(self, run_id: str) -> None:
        try:
            await self._run_loop_inner(run_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("run %s loop crashed", run_id)

    async def _run_loop_inner(self, run_id: str) -> None:
        info = self.store.get_run(run_id)
        if info is None:
            raise RunNotFound(f"run {run_id} not found")
        spec = self.store.get_workflow(info.workflow_name)
        if spec is None:
            raise WorkflowNotFound(f"workflow '{info.workflow_name}' not defined")
        deps = {t.name: set(t.depends_on) for t in spec.tasks}
        task_specs = {t.name: t for t in spec.tasks}
        self.store.set_run_status(run_id, RunStatus.RUNNING)

        executing: set[asyncio.Task] = set()
        inflight: set[str] = set()  # 本循环已派发、尚未落地认领的任务名
        at_failure_fixedpoint = False
        try:
            while True:
                info = self.store.get_run(run_id)
                by_name = {t.task_name: t for t in info.tasks}
                # 全部成功即真正完成。
                if all(t.status == TaskStatus.SUCCEEDED for t in info.tasks):
                    self.store.set_run_status(run_id, RunStatus.SUCCEEDED)
                    break
                # 找出可以开始的任务：PENDING 且所有依赖都已 SUCCEEDED。
                # 是否真的能归我执行由 claim_task 的 CAS 决定（别的实例可能已认领）。
                ready = [
                    t for t in info.tasks
                    if t.status == TaskStatus.PENDING
                    and t.task_name not in inflight
                    and all(
                        by_name[d].status == TaskStatus.SUCCEEDED
                        for d in deps[t.task_name]
                    )
                ]
                if ready:
                    # 失败固定点之后因 rerun 恢复了推进：把运行状态翻回 RUNNING，
                    # 让 wait_run 等调用方继续等待终态，而不是立刻读到旧的 FAILED
                    if at_failure_fixedpoint:
                        self.store.set_run_status(run_id, RunStatus.RUNNING)
                        at_failure_fixedpoint = False
                    for t in ready:
                        inflight.add(t.task_name)
                        task = asyncio.create_task(
                            self._execute_task(
                                run_id, task_specs[t.task_name]
                            )
                        )
                        task.add_done_callback(lambda _tk, n=t.task_name: inflight.discard(n))
                        executing.add(task)
                if executing:
                    done, executing = await asyncio.wait(
                        executing, return_when=asyncio.FIRST_COMPLETED
                    )
                    for d in done:
                        exc = d.exception()
                        if exc is not None:
                            logger.error("task execution error in run %s: %r", run_id, exc)
                    continue
                # 本实例没有在跑的任务：若 run 内还有 RUNNING（别的执行者持有），
                # 安静等待其写回。
                if any(t.status == TaskStatus.RUNNING for t in info.tasks):
                    await asyncio.sleep(self.poll_interval_seconds)
                    continue
                # 失败固定点：存在 FAILED/BLOCKED 且当前无可推进任务。
                # 循环不退出而是空闲等待——FAILED 任务可能随后被 rerun 复活，
                # 届时本循环（或别的实例）自动恢复下游调度。
                if not at_failure_fixedpoint:
                    logger.info(
                        "run %s reached failure fixed-point: %s",
                        run_id,
                        {t.task_name: t.status.value for t in info.tasks},
                    )
                    self.store.set_run_status(run_id, RunStatus.FAILED)
                    at_failure_fixedpoint = True
                await asyncio.sleep(self.poll_interval_seconds)
        finally:
            if executing:
                # 调度循环退出（含被取消）时取消仍在执行的任务；
                # 正常停机由 shutdown 统一释放租约，崩溃则靠心跳超时被接管
                for t in executing:
                    t.cancel()
                await asyncio.gather(*executing, return_exceptions=True)

    async def _execute_task(self, run_id: str, spec: TaskSpec) -> None:
        # 信号量限制全局并发，超过上限的任务在此排队等待而不是被丢弃
        async with self._semaphore:
            claim_epoch = self.store.claim_task(
                run_id, spec.name, self.executor_id,
                heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
            )
            if claim_epoch is None:
                # 已被别的执行者认领（或持有者心跳仍新鲜）：不重复执行
                logger.info(
                    "task %s in run %s not claimed by %s (held elsewhere)",
                    spec.name, run_id, self.executor_id,
                )
                return
            logger.info(
                "executor %s claimed task %s in run %s (epoch %d, attempt)",
                self.executor_id, spec.name, run_id, claim_epoch,
            )
            # 任务级心跳刷新：保证长任务不被误判失联；一旦发现令牌失效
            # （任务已被接管），立刻取消本地执行，避免继续产生副作用
            stop_heartbeat = asyncio.Event()
            lease_lost = False

            async def _refresh() -> None:
                nonlocal lease_lost
                try:
                    while not stop_heartbeat.is_set():
                        await asyncio.sleep(self.heartbeat_interval_seconds)
                        ok = self.store.heartbeat_task(
                            run_id, spec.name, self.executor_id, claim_epoch
                        )
                        if not ok:
                            lease_lost = True
                            logger.warning(
                                "executor %s lost lease of task %s in run %s; "
                                "cancelling local execution",
                                self.executor_id, spec.name, run_id,
                            )
                            worker.cancel()
                            return
                except asyncio.CancelledError:
                    raise

            handler = handlers.get(spec.action)
            if handler is None:
                await self._write_terminal(
                    run_id, spec, claim_epoch,
                    TaskStatus.FAILED, error=f"unknown action '{spec.action}'",
                )
                return

            worker = asyncio.create_task(self._invoke_handler(handler, spec.params))
            refresher = asyncio.create_task(_refresh())
            try:
                result: Any
                try:
                    result = await worker
                except RetryableTaskError as exc:
                    stop_heartbeat.set()
                    refresher.cancel()
                    await self._handle_retryable(run_id, spec, claim_epoch, exc)
                    return
                except asyncio.CancelledError:
                    if lease_lost:
                        # 因租约被接管而取消：本地执行应立即停止，
                        # 任何结果都属于迟到写回，绝不能落库
                        logger.info(
                            "task %s in run %s aborted locally because lease was lost",
                            spec.name, run_id,
                        )
                        return
                    # 调度循环/服务停机触发的取消：正常向上传播
                    raise
                except Exception as exc:  # 不可重试失败（含 FatalTaskError）
                    logger.info(
                        "task %s in run %s fatal failure: %r",
                        spec.name, run_id, exc,
                    )
                    stop_heartbeat.set()
                    refresher.cancel()
                    await self._write_terminal(
                        run_id, spec, claim_epoch,
                        TaskStatus.FAILED, error=f"fatal: {exc}",
                    )
                    return
                stop_heartbeat.set()
                refresher.cancel()
                await self._write_terminal(
                    run_id, spec, claim_epoch,
                    TaskStatus.SUCCEEDED, result=result,
                )
            finally:
                stop_heartbeat.set()
                refresher.cancel()
                try:
                    await refresher
                except (asyncio.CancelledError, Exception):
                    pass

    @staticmethod
    async def _invoke_handler(handler: Any, params: dict[str, Any]) -> Any:
        result = handler(params)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _write_terminal(
        self,
        run_id: str,
        spec: TaskSpec,
        claim_epoch: int,
        target: TaskStatus,
        *,
        error: str | None = None,
        result: Any = None,
    ) -> None:
        """带栅栏令牌的写回；失联复活后的迟到写回在此被挡下并忽略。"""
        try:
            self.store.complete_task(
                run_id, spec.name, self.executor_id, claim_epoch, target,
                error=error, result=result,
            )
        except StaleExecutorWrite:
            logger.warning(
                "late write for task %s in run %s from stale executor %s "
                "(epoch %d) ignored",
                spec.name, run_id, self.executor_id, claim_epoch,
            )
            return
        if target == TaskStatus.FAILED:
            # 标注失败影响面：直接与间接下游全部标 BLOCKED（只收敛 PENDING）
            await self._cascade_block(run_id, spec.name)

    async def _handle_retryable(
        self,
        run_id: str,
        spec: TaskSpec,
        claim_epoch: int,
        exc: RetryableTaskError,
    ) -> None:
        info = self.store.get_run(run_id)
        attempts = next(t.attempts for t in info.tasks if t.task_name == spec.name)
        if attempts <= spec.max_retries:
            logger.info(
                "task %s in run %s retryable failure (attempt %d/%d): %s",
                spec.name, run_id, attempts, spec.max_retries, exc,
            )
            if spec.retry_delay_seconds > 0:
                await asyncio.sleep(spec.retry_delay_seconds)
            try:
                # 重新排队等待下一次调度（保留 attempts 历史）
                self.store.complete_task(
                    run_id, spec.name, self.executor_id, claim_epoch,
                    TaskStatus.PENDING, error=str(exc),
                )
            except StaleExecutorWrite:
                logger.warning(
                    "late retry write for task %s in run %s ignored",
                    spec.name, run_id,
                )
        else:
            logger.info(
                "task %s in run %s exhausted %d retries, marking FAILED",
                spec.name, run_id, spec.max_retries,
            )
            await self._write_terminal(
                run_id, spec, claim_epoch,
                TaskStatus.FAILED, error=f"retries exhausted: {exc}",
            )

    # ---- 失败影响面 ----
    async def _cascade_block(self, run_id: str, failed_task: str) -> None:
        """把失败任务的直接/间接下游标成 BLOCKED。"""
        info = self.store.get_run(run_id)
        spec = self.store.get_workflow(info.workflow_name)
        if spec is None:
            return
        # 传递闭包：沿 depends_on 反向传播（受影响的是“依赖了失败任务”的任务）
        affected: set[str] = set()
        frontier = {failed_task}
        while frontier:
            new_frontier: set[str] = set()
            for t in spec.tasks:
                if t.name in affected or t.name == failed_task:
                    continue
                if frontier & set(t.depends_on):
                    new_frontier.add(t.name)
            affected |= new_frontier
            frontier = new_frontier
        if affected:
            blocked = self.store.block_tasks(run_id, affected)
            logger.warning(
                "task %s failed in run %s; downstream marked BLOCKED: %s",
                failed_task, run_id, blocked,
            )

    async def _unblock_recovered_downstream(self, run_id: str, spec: WorkflowSpec) -> None:
        """失败任务被重跑后，把重新满足依赖条件的 BLOCKED 下游解阻回 PENDING。

        迭代到固定点：只有当其所有上游当前都不是 FAILED/BLOCKED 时才解阻
        （另一个上游仍失败的任务必须继续被挡住）。attempts/result 历史保留。
        """
        deps = {t.name: set(t.depends_on) for t in spec.tasks}
        while True:
            info = self.store.get_run(run_id)
            by_name = {t.task_name: t for t in info.tasks}
            ready: list[str] = []
            for t in info.tasks:
                if t.status != TaskStatus.BLOCKED:
                    continue
                if all(
                    by_name[d].status not in (TaskStatus.FAILED, TaskStatus.BLOCKED)
                    for d in deps[t.task_name]
                ):
                    ready.append(t.task_name)
            if not ready:
                return
            self.store.unblock_tasks(run_id, ready)
            logger.info(
                "run %s: unblocked downstream after upstream recovery: %s",
                run_id, ready,
            )
