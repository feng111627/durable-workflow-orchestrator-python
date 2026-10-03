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


from datetime import datetime, timedelta, timezone  # noqa: E402


class _Clock:
    def __init__(self):
        self.now = datetime(2026, 10, 3, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


class TestRecoveryDistinguishesAliveFromDead:
    def test_restart_keeps_alive_executor_tasks_and_requeues_stale(self, db_path):
        """重启/新实例启动恢复时：活执行者仍在处理的任务保持 RUNNING 原样；
        只有确实失联（心跳超时）的任务才被接管重排队；已成功的永不重跑。"""
        clock = _Clock()
        done_calls = {"n": 0}

        def done_work(params):
            done_calls["n"] += 1
            return "DONE"

        handlers.register("alive_action", lambda p: None)
        handlers.register("stale_action", lambda p: None)
        handlers.register("done_action", done_work)

        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="done", action="done_action"),
                TaskSpec(name="alive", action="alive_action"),
                TaskSpec(name="stale", action="stale_action"),
            ],
        )

        async def scenario():
            store = Store(db_path, clock=clock)
            store.save_workflow(spec)
            run_id, _ = store.create_run_idempotent("w", "k")
            # 直接落盘构造三类任务：已完成 / A 正在跑且心跳新鲜 / A 正在跑但失联
            e_done = store.claim_task(
                run_id, "done", "A", heartbeat_timeout_seconds=30
            )
            store.complete_task(
                run_id, "done", "A", e_done, TaskStatus.SUCCEEDED, result="DONE"
            )
            e_alive = store.claim_task(
                run_id, "alive", "A", heartbeat_timeout_seconds=30
            )
            e_stale = store.claim_task(
                run_id, "stale", "A", heartbeat_timeout_seconds=30
            )
            # 时间推进超过阈值：alive 仍上报心跳（活），stale 再无心跳（崩）
            clock.advance(45)
            assert store.heartbeat_task(run_id, "alive", "A", e_alive) is True

            # 恢复第一阶段（确定性）：只重排队失联任务。
            # 活执行者任务不动、已成功任务不碰；不启动调度循环，避免刚重排队
            # 的 stale 立刻被本实例认领执行（接管后的实际执行由
            # test_engine_takeover / test_multi_process 覆盖）。
            requeued = store.requeue_stale_tasks(heartbeat_timeout_seconds=30)
            assert sorted(t for _, t in requeued) == ["stale"]
            by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
            # 活执行者的任务保持 RUNNING、仍归 A、纪元不变
            assert by_name["alive"].status == TaskStatus.RUNNING
            assert by_name["alive"].executor_id == "A"
            assert by_name["alive"].claim_epoch == e_alive
            # 失联任务被重排队，历史尝试次数保留
            assert by_name["stale"].status == TaskStatus.PENDING
            assert by_name["stale"].attempts == 1
            # 已成功任务纹丝不动
            assert by_name["done"].status == TaskStatus.SUCCEEDED
            assert by_name["done"].result == "DONE"
            store.close()
            return run_id

        asyncio.run(scenario())
        # 整个恢复过程没有执行任何任务处理器（纯恢复判定），已成功任务更不会重跑
        assert done_calls["n"] == 0
