"""业务：服务重启后的续跑恢复。"""
from __future__ import annotations

import asyncio

from app import handlers
from app.engine import Orchestrator
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec
from app.store import Store


class TestRecoveryAfterRestart:
    def test_unfinished_run_resumes_after_restart(self, db_path):
        calls = {"a": 0, "b": 0}
        gate = asyncio.Event()

        def task_a(params):
            calls["a"] += 1
            return "A"

        async def task_b(params):
            calls["b"] += 1
            await gate.wait()  # 第一次执行时会被“宕机”打断
            return "B"

        handlers.register("a", task_a)
        handlers.register("b", task_b)
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="a", action="a"),
                TaskSpec(name="b", action="b", depends_on=["a"]),
            ],
        )

        async def first_boot():
            store = Store(db_path)
            store.save_workflow(spec)
            orch = Orchestrator(store)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            # 等 a 完成、b 进入 RUNNING（被 gate 卡住）
            for _ in range(1000):
                info = store.get_run(run_id)
                by_name = {t.task_name: t for t in info.tasks}
                if by_name["b"].status == TaskStatus.RUNNING:
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("task b never started")
            # 模拟服务宕机：直接取消所有调度，不等待任务完成
            await orch.shutdown()
            store.close()
            return run_id

        run_id = asyncio.run(first_boot())
        assert calls == {"a": 1, "b": 1}

        async def second_boot():
            store = Store(db_path)  # 同一个数据库文件 = 重启后状态还在
            orch = Orchestrator(store)
            recovered = await orch.recover_unfinished()
            assert recovered == [run_id]
            # 中断时 RUNNING 的 b 被重置为 PENDING，没有丢失
            info = store.get_run(run_id)
            by_name = {t.task_name: t for t in info.tasks}
            assert by_name["a"].status == TaskStatus.SUCCEEDED
            gate.set()
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            store.close()
            return info

        info = asyncio.run(second_boot())
        assert info.status == RunStatus.SUCCEEDED
        # 已成功的不重跑；被中断的 b 重新执行并完成
        assert calls["a"] == 1
        assert calls["b"] == 2
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["b"].status == TaskStatus.SUCCEEDED
        assert by_name["b"].result == "B"

    def test_succeeded_run_not_recovered(self, db_path):
        calls = {"n": 0}

        def task(params):
            calls["n"] += 1

        handlers.register("t", task)

        async def boot_and_finish():
            store = Store(db_path)
            store.save_workflow(WorkflowSpec(
                name="w", tasks=[TaskSpec(name="t", action="t")]
            ))
            orch = Orchestrator(store)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            store.close()
            return run_id, info

        run_id, info = asyncio.run(boot_and_finish())
        assert info.status == RunStatus.SUCCEEDED

        async def reboot():
            store = Store(db_path)
            orch = Orchestrator(store)
            recovered = await orch.recover_unfinished()
            await orch.shutdown()
            store.close()
            return recovered

        assert asyncio.run(reboot()) == []  # 已完成运行不会被恢复
        assert calls["n"] == 1  # 也不会被重跑
