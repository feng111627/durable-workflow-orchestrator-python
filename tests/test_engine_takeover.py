"""业务：引擎层的执行者保活、失联接管与迟到结果忽略（真实并发）。"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from app import handlers
from app.engine import Orchestrator
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec
from app.store import Store


class FakeClock:
    def __init__(self):
        self.now = datetime(2026, 10, 3, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@pytest.fixture()
def clock_store(db_path):
    clock = FakeClock()
    s = Store(db_path, clock=clock)
    yield s, clock
    s.close()


def _workflow(action="work"):
    return WorkflowSpec(name="w", tasks=[TaskSpec(name="t", action=action)])


class TestHeartbeatKeepsLongTaskAlive:
    def test_long_running_task_with_heartbeat_not_taken_over(self, clock_store):
        """任务执行很久，但持有者持续上报心跳：另一个实例绝不能抢走它，
        任务在原执行者手里恰好执行一次。"""
        store, _clock = clock_store
        calls = {"n": 0}
        entered = asyncio.Event()
        release = asyncio.Event()

        async def long_work(params):
            calls["n"] += 1
            entered.set()
            await release.wait()
            return "done-by-A"

        handlers.register("work", long_work)
        store.save_workflow(_workflow())

        async def go():
            # A：心跳很快、超时阈值 60s
            orch_a = Orchestrator(
                store, executor_id="exec-A",
                heartbeat_interval_seconds=0.02,
                heartbeat_timeout_seconds=60,
                maintenance_interval_seconds=5,
            )
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch_a.start_run(run_id)
            await asyncio.wait_for(entered.wait(), timeout=2)
            # 让任务带着心跳持续运行一段时间（远超接管判定所需的时间尺度）
            await asyncio.sleep(0.3)
            # B 实例同时在场并周期性尝试接管；因 A 心跳新鲜，不应抢走
            orch_b = Orchestrator(
                store, executor_id="exec-B",
                heartbeat_interval_seconds=0.02,
                heartbeat_timeout_seconds=60,
                maintenance_interval_seconds=0.05,
            )
            await orch_b.start()
            await asyncio.sleep(0.3)
            task = store.get_run(run_id).tasks[0]
            assert task.status == TaskStatus.RUNNING
            assert task.executor_id == "exec-A"
            release.set()
            info = await orch_a.wait_run(run_id, timeout=5)
            await orch_b.shutdown()
            await orch_a.shutdown()
            return info

        info = asyncio.run(go())
        assert info.status == RunStatus.SUCCEEDED
        assert info.tasks[0].result == "done-by-A"
        assert calls["n"] == 1  # 没有被接管重跑，只执行一次


class TestTakeoverAfterDeath:
    def test_dead_executor_task_taken_over_and_completed(self, clock_store):
        """A 认领后彻底失联（不再有心跳），B 通过 requeue+claim 接管并把任务
        真正跑完；attempts 历史保留。"""
        store, clock = clock_store
        outcomes = {"n": 0}

        async def work(params):
            outcomes["n"] += 1
            return f"result-{outcomes['n']}"

        handlers.register("work", work)
        store.save_workflow(_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")

        async def a_claims_then_dies():
            # 不启动后台循环、不续心跳，认领后直接“崩溃”（不 shutdown）
            orch_a = Orchestrator(
                store, executor_id="exec-A",
                heartbeat_interval_seconds=1, heartbeat_timeout_seconds=30,
                maintenance_interval_seconds=60,
            )
            epoch = store.claim_task(
                run_id, "t", "exec-A", heartbeat_timeout_seconds=30
            )
            assert epoch == 1
            return epoch

        asyncio.run(a_claims_then_dies())
        assert outcomes["n"] == 0  # A 没产出结果就死了

        # 时钟推进超过失联阈值（确定性，不依赖真实等待）
        clock.advance(45)

        async def b_recovers():
            orch_b = Orchestrator(
                store, executor_id="exec-B",
                heartbeat_interval_seconds=0.02,
                heartbeat_timeout_seconds=30,
                maintenance_interval_seconds=5,
            )
            recovered = await orch_b.recover_unfinished()
            assert recovered == [run_id]
            info = await orch_b.wait_run(run_id, timeout=5)
            await orch_b.shutdown()
            return info

        info = asyncio.run(b_recovers())
        assert info.status == RunStatus.SUCCEEDED
        task = info.tasks[0]
        assert task.result == "result-1"
        assert task.executor_id == "exec-B"
        assert task.attempts == 2  # A 的第 1 次 + B 接管的第 2 次
        assert outcomes["n"] == 1

    def test_takeover_via_maintenance_loop_without_restart(self, clock_store):
        """B 早已在运行（不重启）：A 崩溃后，B 的维护循环自动发现失联任务、
        重排队并接管跑完，无需任何实例重启。"""
        store, clock = clock_store
        done_flag = {"b": 0}

        async def work(params):
            done_flag["b"] += 1
            return "B-finished"

        handlers.register("work", work)
        store.save_workflow(_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")

        async def scenario():
            # B 先启动并在场，维护周期很短
            orch_b = Orchestrator(
                store, executor_id="exec-B",
                heartbeat_interval_seconds=0.02,
                heartbeat_timeout_seconds=30,
                maintenance_interval_seconds=0.05,
            )
            await orch_b.start()
            # A 直接在库里认领（模拟 A 拿到任务后崩溃）
            store.claim_task(run_id, "t", "exec-A", heartbeat_timeout_seconds=30)
            clock.advance(45)  # A 心跳超时
            # 不调用任何 recover/restart，仅等待 B 的后台维护循环接管
            deadline = 0
            info = store.get_run(run_id)
            while info.status not in (RunStatus.SUCCEEDED, RunStatus.FAILED) and deadline < 200:
                await asyncio.sleep(0.02)
                deadline += 1
                info = store.get_run(run_id)
            await orch_b.shutdown()
            return info

        info = asyncio.run(scenario())
        assert info.status == RunStatus.SUCCEEDED
        assert info.tasks[0].result == "B-finished"
        assert info.tasks[0].executor_id == "exec-B"
        assert done_flag["b"] == 1


class TestLateResultIgnoredByEngine:
    def test_engine_ignores_result_from_executor_that_lost_lease(self, clock_store):
        """A 失联被 B 接管后，A “复活”并尝试用旧令牌写回结果，引擎层必须忽略，
        最终结果以 B 为准。"""
        store, clock = clock_store
        store.save_workflow(_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch_a = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        clock.advance(45)
        epoch_b = store.claim_task(
            run_id, "t", "exec-B", heartbeat_timeout_seconds=30
        )
        assert epoch_b == epoch_a + 1

        async def go():
            orch = Orchestrator(
                store, executor_id="exec-B",
                heartbeat_interval_seconds=0.02, heartbeat_timeout_seconds=30,
                maintenance_interval_seconds=5,
            )
            # A 复活后的迟到成功写回：引擎写回路径直接抛 StaleExecutorWrite，
            # 由 _write_terminal 吞掉并记录，绝不落库
            from app.errors import StaleExecutorWrite
            with pytest.raises(StaleExecutorWrite):
                store.complete_task(
                    run_id, "t", "exec-A", epoch_a,
                    TaskStatus.SUCCEEDED, result="zombie-A",
                )
            # B 正常完成
            store.complete_task(
                run_id, "t", "exec-B", epoch_b,
                TaskStatus.SUCCEEDED, result="legit-B",
            )
            await orch.shutdown()

        asyncio.run(go())
        task = store.get_run(run_id).tasks[0]
        assert task.status == TaskStatus.SUCCEEDED
        assert task.result == "legit-B"
        assert task.executor_id == "exec-B"
        assert task.claim_epoch == epoch_b


class TestTwoEnginesOneDatabase:
    def test_two_engines_concurrently_schedule_same_run_without_dup(self, store):
        """两个 Orchestrator 实例（同一事件循环、同一 Store，模拟两个执行者）
        同时接管并调度同一运行；所有任务恰好执行一次，最终全部成功。"""
        counter = {"n": 0}

        async def work(params):
            counter["n"] += 1
            await asyncio.sleep(0.01)  # 放大交错窗口
            return params["name"]

        handlers.register("work", work)
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name=f"t{i}", action="work", params={"name": f"t{i}"})
                for i in range(6)
            ],
        )
        store.save_workflow(spec)
        run_id, _ = store.create_run_idempotent("w", "k")

        async def go():
            a = Orchestrator(
                store, executor_id="EA",
                heartbeat_interval_seconds=0.02, heartbeat_timeout_seconds=30,
                maintenance_interval_seconds=60,
            )
            b = Orchestrator(
                store, executor_id="EB",
                heartbeat_interval_seconds=0.02, heartbeat_timeout_seconds=30,
                maintenance_interval_seconds=60,
            )
            # 两者都认为自己需要调度该运行（真实并发，不是顺序循环）
            await asyncio.gather(
                a.recover_unfinished(),
                b.recover_unfinished(),
            )
            info_a = await a.wait_run(run_id, timeout=10)
            info_b = await b.wait_run(run_id, timeout=10)
            await a.shutdown()
            await b.shutdown()
            return info_a, info_b

        info_a, info_b = asyncio.run(go())
        assert info_a.status == info_b.status == RunStatus.SUCCEEDED
        assert len(info_a.tasks) == 6
        assert all(t.status == TaskStatus.SUCCEEDED for t in info_a.tasks)
        assert counter["n"] == 6  # 6 个任务各执行一次，无跨实例重复


class TestConcurrentRerun:
    def test_concurrent_rerun_same_failed_task_executes_once(self, store):
        """两个执行者实例同时 rerun 同一个 FAILED 任务：只有一个能把
        FAILED -> PENDING，恢复后的任务只执行一次。"""
        attempts = {"n": 0}

        def flaky(params):
            attempts["n"] += 1
            return "recovered"

        handlers.register("work", flaky)
        spec = WorkflowSpec(
            name="w",
            tasks=[TaskSpec(name="t", action="work")],
        )
        store.save_workflow(spec)

        async def go():
            a = Orchestrator(store, executor_id="EA",
                             heartbeat_interval_seconds=0.02,
                             heartbeat_timeout_seconds=30,
                             maintenance_interval_seconds=60)
            b = Orchestrator(store, executor_id="EB",
                             heartbeat_interval_seconds=0.02,
                             heartbeat_timeout_seconds=30,
                             maintenance_interval_seconds=60)
            run_id, _ = store.create_run_idempotent("w", "k")
            # 直接布置 FAILED 终态（避免依赖处理器抛错）
            e = store.claim_task(run_id, "t", "EA", heartbeat_timeout_seconds=30)
            store.complete_task(run_id, "t", "EA", e,
                                TaskStatus.FAILED, error="boom")
            store.set_run_status(run_id, RunStatus.FAILED)
            # 两个实例并发发起 rerun（真实并发）
            results = await asyncio.gather(
                a.rerun_task(run_id, "t"),
                b.rerun_task(run_id, "t"),
                return_exceptions=True,
            )
            invalid = [r for r in results if isinstance(r, Exception)]
            info = await a.wait_run(run_id, timeout=10)
            await a.shutdown()
            await b.shutdown()
            return info, invalid

        info, invalid = asyncio.run(go())
        assert info.status == RunStatus.SUCCEEDED
        assert attempts["n"] == 1  # 恢复执行恰好一次
        # 恰好一个 rerun 因状态已不是 FAILED 被拒绝（可区分），不产生重复恢复
        assert len(invalid) == 1
