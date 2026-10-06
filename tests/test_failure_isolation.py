"""业务：失败隔离——失败只影响自己的下游，无关分支照常跑完。"""
from __future__ import annotations

import asyncio

from app import handlers
from app.engine import Orchestrator
from app.errors import FatalTaskError
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec


class TestFailureIsolation:
    def test_unrelated_branch_completes_while_parallel_branch_fails(self, store):
        """真实并发：失败任务与跑得更久的无关任务同时进行。

        断言无关任务（及其下游）正常跑完，失败任务的下游被阻塞，
        运行最终判失败，且没有任何任务卡在 RUNNING/PENDING。
        整个场景重复多轮，验证结果稳定。
        """
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="bad", action="fail_now"),
                TaskSpec(name="bad_child", action="ok", depends_on=["bad"]),
                TaskSpec(name="slow", action="slow_ok"),  # 无关分支
                TaskSpec(name="slow_child", action="ok", depends_on=["slow"]),
            ],
        )
        store.save_workflow(spec)

        async def go(round_no: int):
            slow_started = asyncio.Event()
            release_slow = asyncio.Event()

            def fail_now(params):
                raise FatalTaskError("boom")

            async def slow_ok(params):
                slow_started.set()
                await release_slow.wait()
                return "slow-done"

            handlers.register("fail_now", fail_now)
            handlers.register("slow_ok", slow_ok)
            handlers.register("ok", lambda p: "ok")

            orch = Orchestrator(store, lease_seconds=0.3, heartbeat_interval=0.1)
            run_id, _ = store.create_run_idempotent("w", f"k{round_no}")
            await orch.start_run(run_id)

            # 等慢任务真正开始执行，保证与失败任务真实并发
            await asyncio.wait_for(slow_started.wait(), timeout=5)
            # 等失败落库：此刻慢任务仍在执行中
            while True:
                info = store.get_run(run_id)
                by_name = {t.task_name: t for t in info.tasks}
                if by_name["bad"].status == TaskStatus.FAILED:
                    break
                await asyncio.sleep(0.01)
            # 失败已发生，但无关的慢任务必须仍在正常执行，不能被牵连停掉
            assert by_name["slow"].status == TaskStatus.RUNNING
            assert by_name["bad_child"].status == TaskStatus.BLOCKED
            # 运行里还有任务在推进，不应提前判失败
            assert info.status == RunStatus.RUNNING

            # 放行慢任务：它和它的下游都应正常跑完
            release_slow.set()
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        for round_no in range(3):
            info = asyncio.run(go(round_no))
            by_name = {t.task_name: t for t in info.tasks}
            # 失败影响面：只波及自己的下游
            assert info.status == RunStatus.FAILED
            assert by_name["bad"].status == TaskStatus.FAILED
            assert by_name["bad_child"].status == TaskStatus.BLOCKED
            # 无关分支：正常跑完，不被牵连
            assert by_name["slow"].status == TaskStatus.SUCCEEDED
            assert by_name["slow"].result == "slow-done"
            assert by_name["slow_child"].status == TaskStatus.SUCCEEDED
            # 终态下不允许还有任务挂在执行中或等待中
            for t in info.tasks:
                assert t.status not in (TaskStatus.RUNNING, TaskStatus.PENDING), t
