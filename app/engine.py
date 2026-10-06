"""调度引擎：按依赖拓扑序执行任务，控制并发，处理重试与失败。

多执行者安全：每个 Orchestrator 实例是一个执行者（executor），通过租约
认领任务、定期心跳续期；只有租约过期（执行者失联）的任务才会被其他
执行者接管；失联执行者的迟到写回因栅栏令牌不匹配而被忽略。

失败隔离：任务彻底失败只影响它自己的下游（级联标记 BLOCKED）；与失败
无关的分支照常调度、照常跑完。只有当运行里再没有任何任务在推进时
（无本地执行任务、无任何 RUNNING 任务、无可调度的 PENDING 任务），
运行才被判定终态，不会出现"运行已判失败却还有任务永远卡在执行中"。
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import uuid

from . import handlers
from .errors import InvalidStateTransition, RetryableTaskError, RunNotFound, WorkflowNotFound
from .models import RunInfo, RunStatus, TaskSpec, TaskStatus
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
        lease_seconds: float = 30.0,
        heartbeat_interval: float = 5.0,
    ):
        if max_concurrent_tasks < 1:
            raise ValueError("max_concurrent_tasks must be >= 1")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be > 0")
        if heartbeat_interval >= lease_seconds:
            raise ValueError("heartbeat_interval must be < lease_seconds")
        self.store = store
        self.max_concurrent_tasks = max_concurrent_tasks
        # 执行者身份：多实例部署时每个进程唯一，用于租约归属与失联判定
        self.executor_id = executor_id or f"executor-{uuid.uuid4().hex[:12]}"
        # 失联判定超时：租约超过该时长未续期即视为执行者失联，可被接管
        self.lease_seconds = lease_seconds
        # 心跳间隔：定期上报存活并续期自己持有的任务租约
        self.heartbeat_interval = heartbeat_interval
        self._semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._run_tasks: dict[str, asyncio.Task] = {}
        self._heartbeat_task: asyncio.Task | None = None
        self._reaper_task: asyncio.Task | None = None

    async def start_run(self, run_id: str) -> None:
        """为一次运行启动（或恢复）调度循环；同一 run 不会并行调度两次。"""
        self._ensure_background_loops()
        existing = self._run_tasks.get(run_id)
        if existing is not None and not existing.done():
            logger.info("run %s already scheduled, skip duplicate start", run_id)
            return
        # 同步先把运行标记为进行中：恢复失败运行时，避免调用方在调度循环
        # 生效前读到旧的 FAILED 终态；已成功的运行不会被回退
        self.store.mark_run_active(run_id)
        self._run_tasks[run_id] = asyncio.create_task(self._run_loop(run_id))

    async def recover_unfinished(self) -> list[str]:
        """恢复未完成的运行，返回恢复的 run_id 列表。

        只接管确实失联（租约已过期）的任务；仍被其他存活执行者正常处理
        的任务保持原样；已成功的任务任何情况下都不会重跑。
        """
        self._ensure_background_loops()
        reclaimed = self.store.reclaim_expired_leases()
        if reclaimed:
            logger.info("reclaimed %d task(s) from lost executors", reclaimed)
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
        return await asyncio.wait_for(_poll(), timeout)

    async def shutdown(self) -> None:
        for task in (self._heartbeat_task, self._reaper_task):
            if task is not None:
                task.cancel()
        for task in self._run_tasks.values():
            task.cancel()
        background = [t for t in (self._heartbeat_task, self._reaper_task) if t]
        if background:
            await asyncio.gather(*background, return_exceptions=True)
        if self._run_tasks:
            await asyncio.gather(*self._run_tasks.values(), return_exceptions=True)
        self._run_tasks.clear()
        self._heartbeat_task = None
        self._reaper_task = None

    # ---- 后台循环：心跳与失联接管 ----

    def _ensure_background_loops(self) -> None:
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        if self._reaper_task is None or self._reaper_task.done():
            self._reaper_task = asyncio.create_task(self._reaper_loop())

    async def _heartbeat_loop(self) -> None:
        while True:
            self.store.heartbeat(self.executor_id, self.lease_seconds)
            await asyncio.sleep(self.heartbeat_interval)

    async def _reaper_loop(self) -> None:
        """定期接管失联执行者的任务，并推动有就绪任务的运行继续调度。"""
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            reclaimed = self.store.reclaim_expired_leases()
            if reclaimed:
                logger.info(
                    "executor %s reclaimed %d task(s) from lost executors",
                    self.executor_id, reclaimed,
                )
            for run in self.store.list_unfinished_runs():
                await self.start_run(run.run_id)

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
        self.store.mark_run_active(run_id)

        executing: set[asyncio.Task] = set()
        try:
            while True:
                info = self.store.get_run(run_id)
                by_name = {t.task_name: t for t in info.tasks}
                if all(t.status == TaskStatus.SUCCEEDED for t in info.tasks):
                    self.store.set_run_status(run_id, RunStatus.SUCCEEDED)
                    break
                # 找出可以开始的任务：PENDING 且所有依赖都已 SUCCEEDED。
                # 失败只影响它自己的下游：失败任务的下游已被级联标记为
                # BLOCKED（不再是 PENDING），与失败无关的分支照常调度、
                # 照常跑完，不被牵连。
                for t in info.tasks:
                    if t.status != TaskStatus.PENDING:
                        continue
                    if all(by_name[d].status == TaskStatus.SUCCEEDED for d in deps[t.task_name]):
                        executing.add(
                            asyncio.create_task(
                                self._execute_task(run_id, task_specs[t.task_name])
                            )
                        )
                any_running = any(t.status == TaskStatus.RUNNING for t in info.tasks)
                if not executing and not any_running:
                    # 没有任何任务还在推进时才判终态：剩余任务要么 FAILED，
                    # 要么因上游失败被 BLOCKED，运行判失败。只要还有任务在
                    # 执行（含其他执行者持有的 RUNNING），就继续等它们跑完，
                    # 避免无关分支被提前取消、永远卡在执行中。
                    self.store.set_run_status(run_id, RunStatus.FAILED)
                    break
                # 被其他执行者持有的任务（RUNNING）不属于本实例的 executing，
                # 通过带超时的等待定期重新评估数据库状态
                if executing:
                    done, executing = await asyncio.wait(
                        executing, return_when=asyncio.FIRST_COMPLETED,
                        timeout=self.heartbeat_interval,
                    )
                else:
                    # 本实例没有在执行的任务但运行未结束（任务被其他执行者
                    # 持有）：休眠一个心跳周期后重新评估
                    await asyncio.sleep(self.heartbeat_interval)
                    done = set()
                for d in done:
                    exc = d.exception()
                    if exc is not None:
                        logger.error("task execution error in run %s: %r", run_id, exc)
        finally:
            if executing:
                # 调度循环退出（含被取消/宕机）时，取消仍在执行的任务，
                # 它们在数据库中保持 RUNNING，租约过期后由其他执行者接管
                for t in executing:
                    t.cancel()
                await asyncio.gather(*executing, return_exceptions=True)

    async def _execute_task(self, run_id: str, spec: TaskSpec) -> None:
        # 信号量限制全局并发，超过上限的任务在此排队等待而不是被丢弃
        async with self._semaphore:
            try:
                token = self.store.claim_task(
                    run_id, spec.name, self.executor_id, self.lease_seconds
                )
            except InvalidStateTransition:
                # 并发下同一任务只允许一个执行者认领成功，其余直接退出，
                # 保证不会产生重复副作用
                logger.info(
                    "task %s in run %s already claimed by another executor, skip",
                    spec.name, run_id,
                )
                return
            handler = handlers.get(spec.action)
            if handler is None:
                self.store.complete_task(
                    run_id, spec.name, self.executor_id, token, TaskStatus.FAILED,
                    error=f"unknown action '{spec.action}'",
                )
                return
            try:
                result = handler(spec.params)
                if inspect.isawaitable(result):
                    result = await result
            except RetryableTaskError as exc:
                await self._handle_retryable(run_id, spec, token, exc)
                return
            except Exception as exc:  # 不可重试失败（含 FatalTaskError）
                logger.info("task %s in run %s fatal failure: %r", spec.name, run_id, exc)
                ok = self.store.fail_task_and_block_downstream(
                    run_id, spec.name, self.executor_id, token, f"fatal: {exc}"
                )
                if not ok:
                    logger.info(
                        "stale write of lost executor ignored for task %s in run %s",
                        spec.name, run_id,
                    )
                return
            ok = self.store.complete_task(
                run_id, spec.name, self.executor_id, token,
                TaskStatus.SUCCEEDED, result=result,
            )
            if not ok:
                # 执行期间租约过期被别的执行者接管，本次迟到写回被忽略
                logger.info(
                    "stale write of lost executor ignored for task %s in run %s",
                    spec.name, run_id,
                )

    async def _handle_retryable(
        self, run_id: str, spec: TaskSpec, token: int, exc: RetryableTaskError
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
            # 重新排队等待下一次调度（带栅栏令牌，失联时写回被忽略）
            ok = self.store.complete_task(
                run_id, spec.name, self.executor_id, token,
                TaskStatus.PENDING, error=str(exc),
            )
            if not ok:
                logger.info(
                    "stale requeue of lost executor ignored for task %s in run %s",
                    spec.name, run_id,
                )
        else:
            logger.info(
                "task %s in run %s exhausted %d retries, marking FAILED",
                spec.name, run_id, spec.max_retries,
            )
            self.store.fail_task_and_block_downstream(
                run_id, spec.name, self.executor_id, token,
                f"retries exhausted: {exc}",
            )
