"""调度引擎：按依赖拓扑序执行任务，控制并发，处理重试与失败。"""
from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

from . import handlers
from .errors import InvalidStateTransition, RetryableTaskError, RunNotFound, WorkflowNotFound
from .models import RunInfo, RunStatus, TaskSpec, TaskStatus, WorkflowSpec
from .store import Store

logger = logging.getLogger("orchestrator.engine")

TERMINAL_RUN_STATES = (RunStatus.SUCCEEDED, RunStatus.FAILED)


class Orchestrator:
    def __init__(self, store: Store, max_concurrent_tasks: int = 4):
        if max_concurrent_tasks < 1:
            raise ValueError("max_concurrent_tasks must be >= 1")
        self.store = store
        self.max_concurrent_tasks = max_concurrent_tasks
        self._semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._run_tasks: dict[str, asyncio.Task] = {}

    async def start_run(self, run_id: str) -> None:
        """为一次运行启动（或恢复）调度循环；同一 run 不会并行调度两次。"""
        existing = self._run_tasks.get(run_id)
        if existing is not None and not existing.done():
            logger.info("run %s already scheduled, skip duplicate start", run_id)
            return
        self._run_tasks[run_id] = asyncio.create_task(self._run_loop(run_id))

    async def recover_unfinished(self) -> list[str]:
        """服务重启后恢复所有未完成的运行，返回恢复的 run_id 列表。

        已成功（SUCCEEDED）的任务保持原状不会重跑；中断时处于 RUNNING 的
        任务被重置为 PENDING 重新排队，不会丢失。
        """
        recovered: list[str] = []
        for run in self.store.list_unfinished_runs():
            reset = self.store.reset_running_tasks(run.run_id)
            logger.info(
                "recover run %s (%s): reset %d interrupted task(s)",
                run.run_id, run.workflow_name, reset,
            )
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
        return await asyncio.wait_for(_poll(), timeout)

    async def shutdown(self) -> None:
        for task in self._run_tasks.values():
            task.cancel()
        if self._run_tasks:
            await asyncio.gather(*self._run_tasks.values(), return_exceptions=True)
        self._run_tasks.clear()

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
        try:
            while True:
                info = self.store.get_run(run_id)
                by_name = {t.task_name: t for t in info.tasks}
                if any(t.status == TaskStatus.FAILED for t in info.tasks):
                    self.store.set_run_status(run_id, RunStatus.FAILED)
                    break
                if all(t.status == TaskStatus.SUCCEEDED for t in info.tasks):
                    self.store.set_run_status(run_id, RunStatus.SUCCEEDED)
                    break
                # 找出可以开始的任务：PENDING 且所有依赖都已 SUCCEEDED
                for t in info.tasks:
                    if t.status != TaskStatus.PENDING:
                        continue
                    if all(by_name[d].status == TaskStatus.SUCCEEDED for d in deps[t.task_name]):
                        executing.add(
                            asyncio.create_task(
                                self._execute_task(run_id, task_specs[t.task_name])
                            )
                        )
                if not executing:
                    # 没有可推进的任务也未达终态：剩余任务因依赖失败被阻塞
                    self.store.set_run_status(run_id, RunStatus.FAILED)
                    break
                done, executing = await asyncio.wait(
                    executing, return_when=asyncio.FIRST_COMPLETED
                )
                for d in done:
                    exc = d.exception()
                    if exc is not None:
                        logger.error("task execution error in run %s: %r", run_id, exc)
        finally:
            if executing:
                # 调度循环退出（含被取消/宕机）时，取消仍在执行的任务，
                # 它们在数据库中保持 RUNNING，由重启恢复逻辑重置为 PENDING
                for t in executing:
                    t.cancel()
                await asyncio.gather(*executing, return_exceptions=True)

    async def _execute_task(self, run_id: str, spec: TaskSpec) -> None:
        # 信号量限制全局并发，超过上限的任务在此排队等待而不是被丢弃
        async with self._semaphore:
            try:
                self.store.transition_task(
                    run_id, spec.name, TaskStatus.RUNNING,
                    allowed_from=(TaskStatus.PENDING,), increment_attempts=True,
                )
            except InvalidStateTransition:
                # 并发下同一任务只允许一个执行者进入 RUNNING，其余直接退出，
                # 保证不会产生重复副作用
                logger.info("task %s in run %s already claimed, skip", spec.name, run_id)
                return
            handler = handlers.get(spec.action)
            if handler is None:
                self.store.transition_task(
                    run_id, spec.name, TaskStatus.FAILED,
                    allowed_from=(TaskStatus.RUNNING,),
                    error=f"unknown action '{spec.action}'",
                )
                return
            try:
                result = handler(spec.params)
                if inspect.isawaitable(result):
                    result = await result
            except RetryableTaskError as exc:
                await self._handle_retryable(run_id, spec, exc)
                return
            except Exception as exc:  # 不可重试失败（含 FatalTaskError）
                logger.info("task %s in run %s fatal failure: %r", spec.name, run_id, exc)
                self.store.transition_task(
                    run_id, spec.name, TaskStatus.FAILED,
                    allowed_from=(TaskStatus.RUNNING,), error=f"fatal: {exc}",
                )
                return
            self.store.transition_task(
                run_id, spec.name, TaskStatus.SUCCEEDED,
                allowed_from=(TaskStatus.RUNNING,), result=result,
            )

    async def _handle_retryable(
        self, run_id: str, spec: TaskSpec, exc: RetryableTaskError
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
            # 重新排队等待下一次调度
            self.store.transition_task(
                run_id, spec.name, TaskStatus.PENDING,
                allowed_from=(TaskStatus.RUNNING,), error=str(exc),
            )
        else:
            logger.info(
                "task %s in run %s exhausted %d retries, marking FAILED",
                spec.name, run_id, spec.max_retries,
            )
            self.store.transition_task(
                run_id, spec.name, TaskStatus.FAILED,
                allowed_from=(TaskStatus.RUNNING,), error=f"retries exhausted: {exc}",
            )
