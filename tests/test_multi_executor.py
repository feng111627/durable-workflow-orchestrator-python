"""业务：多执行者安全与失联接管。

并发用例均为真实并发：多个执行者（独立数据库连接 / 独立 Orchestrator
实例）同时对同一份数据动手，用事件与条件轮询保证可稳定复现。
"""
from __future__ import annotations

import asyncio
import threading

import pytest

from app import handlers
from app.engine import Orchestrator
from app.errors import InvalidStateTransition
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec
from app.store import Store


def _make_run(store: Store) -> str:
    store.save_workflow(WorkflowSpec(
        name="w", tasks=[TaskSpec(name="t", action="x")]
    ))
    run_id, _ = store.create_run_idempotent("w", "k")
    return run_id


class TestMultiExecutorSafety:
    def test_live_executor_task_cannot_be_claimed_by_other(self, db_path):
        store = Store(db_path)
        run_id = _make_run(store)
        token_a = store.claim_task(run_id, "t", "executor-A", lease_seconds=30.0)
        # 租约未过期：另一个执行者不能看着“有人在跑”就抢过来
        with pytest.raises(InvalidStateTransition) as exc:
            store.claim_task(run_id, "t", "executor-B", lease_seconds=30.0)
        assert exc.value.current == "RUNNING"
        info = store.get_run(run_id)
        task = info.tasks[0]
        assert task.owner == "executor-A" and task.attempts == 1
        store.close()

    def test_takeover_after_lease_expiry_preserves_attempts(self, db_path):
        store = Store(db_path)
        run_id = _make_run(store)
        token_a = store.claim_task(run_id, "t", "executor-A", lease_seconds=0.2)
        # A 失联（不再心跳续期），租约过期后被接管
        import time
        end = time.time() + 5
        reclaimed = 0
        while time.time() < end:
            reclaimed = store.reclaim_expired_leases()
            if reclaimed:
                break
            time.sleep(0.02)
        assert reclaimed == 1
        token_b = store.claim_task(run_id, "t", "executor-B", lease_seconds=30.0)
        assert token_b > token_a  # 栅栏令牌单调递增
        task = store.get_run(run_id).tasks[0]
        # 尝试次数接着算，不丢失、不串
        assert task.attempts == 2 and task.owner == "executor-B"
        store.close()

    def test_concurrent_claim_only_one_winner_across_connections(self, db_path):
        """真实多线程并发：8 个执行者用独立连接同时认领同一任务，恰有一个成功。"""
        setup = Store(db_path)
        run_id = _make_run(setup)
        n = 8
        barrier = threading.Barrier(n)
        results: list[tuple[str, int]] = []
        results_lock = threading.Lock()

        def worker(i: int) -> None:
            store = Store(db_path)  # 每个执行者独立连接，模拟多进程
            try:
                barrier.wait(timeout=5)  # 同时出手
                store.claim_task(run_id, "t", f"executor-{i}", lease_seconds=30.0)
                outcome = "ok"
            except InvalidStateTransition:
                outcome = "rejected"
            finally:
                store.close()
            with results_lock:
                results.append((outcome, i))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        winners = [i for outcome, i in results if outcome == "ok"]
        assert len(results) == n
        assert len(winners) == 1  # 并发下恰有一个执行者认领成功
        task = setup.get_run(run_id).tasks[0]
        assert task.attempts == 1  # 只执行了一次认领，没有重复计数
        setup.close()

    def test_late_write_from_lost_executor_ignored(self, db_path):
        """失联执行者被接管后又活过来写回结果，必须被忽略。"""
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        calls = {"n": 0}

        async def blocker(params):
            calls["n"] += 1
            if calls["n"] == 1:
                # 第一个执行者的执行被卡住（模拟进程卡死），之后迟到返回
                first_entered.set()
                await release_first.wait()
                return "stale-result"
            return "fresh-result"

        handlers.register("blocker", blocker)

        async def go():
            store_a = Store(db_path)
            store_b = Store(db_path)
            store_a.save_workflow(WorkflowSpec(
                name="w", tasks=[TaskSpec(name="t", action="blocker")]
            ))
            run_id, _ = store_a.create_run_idempotent("w", "k")
            orch_a = Orchestrator(
                store_a, executor_id="executor-A",
                lease_seconds=0.3, heartbeat_interval=0.1,
            )
            orch_b = Orchestrator(
                store_b, executor_id="executor-B",
                lease_seconds=0.3, heartbeat_interval=0.1,
            )
            await orch_a.start_run(run_id)
            await asyncio.wait_for(first_entered.wait(), timeout=5)
            # A 彻底失联（进程卡死）：心跳与接管循环都冻结，但卡住的
            # handler 还活着，之后会被放回来触发迟到写回
            orch_a._reaper_task.cancel()
            orch_a._heartbeat_task.cancel()
            # B 上线：只应在租约过期后接管
            await orch_b.recover_unfinished()
            # 等 B 接管并完成（attempts 接着算到 2）
            deadline = asyncio.get_running_loop().time() + 5
            while True:
                task = store_b.get_run(run_id).tasks[0]
                if task.status == TaskStatus.SUCCEEDED:
                    break
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.02)
            assert calls["n"] == 2
            # 此刻 A 的 handler 还在卡着；放它返回，触发迟到写回
            release_first.set()
            await asyncio.sleep(0.3)  # 给 A 足够时间尝试写回
            task = store_b.get_run(run_id).tasks[0]
            await orch_a.shutdown()
            await orch_b.shutdown()
            store_a.close()
            store_b.close()
            return task

        task = asyncio.run(go())
        # 迟到写回被忽略：结果仍是新执行者的，attempts 不被干扰
        assert task.status == TaskStatus.SUCCEEDED
        assert task.result == "fresh-result"
        assert task.attempts == 2

    def test_stale_complete_rejected_at_store_level(self, db_path):
        """存储层栅栏：失联执行者醒来后的写回直接返回 False（被忽略）。"""
        store = Store(db_path)
        run_id = _make_run(store)
        token_a = store.claim_task(run_id, "t", "executor-A", lease_seconds=0.2)
        # A 失联，租约过期，B 接管并完成
        import time
        end = time.time() + 5
        while time.time() < end:
            if store.reclaim_expired_leases():
                break
            time.sleep(0.02)
        token_b = store.claim_task(run_id, "t", "executor-B", lease_seconds=30.0)
        assert store.complete_task(
            run_id, "t", "executor-B", token_b, TaskStatus.SUCCEEDED,
            result="fresh-result",
        ) is True
        # A 又活过来往回写：必须被忽略
        assert store.complete_task(
            run_id, "t", "executor-A", token_a, TaskStatus.SUCCEEDED,
            result="stale-result",
        ) is False
        task = store.get_run(run_id).tasks[0]
        assert task.result == "fresh-result"
        assert task.attempts == 2
        store.close()

    def test_heartbeat_prevents_takeover_by_live_peer(self, db_path):
        """执行者正常心跳时，别的执行者（含新启动实例）不能接管它的任务。"""
        gate = asyncio.Event()
        calls = {"n": 0}

        async def slow(params):
            calls["n"] += 1
            await gate.wait()
            return "done"

        handlers.register("slow", slow)

        async def go():
            store_a = Store(db_path)
            store_b = Store(db_path)
            store_a.save_workflow(WorkflowSpec(
                name="w", tasks=[TaskSpec(name="t", action="slow")]
            ))
            run_id, _ = store_a.create_run_idempotent("w", "k")
            orch_a = Orchestrator(
                store_a, executor_id="executor-A",
                lease_seconds=0.3, heartbeat_interval=0.1,
            )
            orch_b = Orchestrator(
                store_b, executor_id="executor-B",
                lease_seconds=0.3, heartbeat_interval=0.1,
            )
            await orch_a.start_run(run_id)
            # 等 A 认领成功
            deadline = asyncio.get_running_loop().time() + 5
            while store_b.get_run(run_id).tasks[0].status != TaskStatus.RUNNING:
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.02)
            # B 作为新实例做恢复：A 活着且持续心跳，任务必须保持原样
            await orch_b.recover_unfinished()
            await asyncio.sleep(0.8)  # 远超一个租约周期
            task = store_b.get_run(run_id).tasks[0]
            assert task.status == TaskStatus.RUNNING
            assert task.owner == "executor-A"
            assert calls["n"] == 1  # 没有被 B 重复执行
            gate.set()
            info = await orch_a.wait_run(run_id, timeout=5)
            await orch_a.shutdown()
            await orch_b.shutdown()
            store_a.close()
            store_b.close()
            return info, calls["n"]

        info, total_calls = asyncio.run(go())
        assert info.status == RunStatus.SUCCEEDED
        assert total_calls == 1  # 全程只有 A 执行了一次
