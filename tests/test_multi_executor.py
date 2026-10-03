"""业务：多执行者安全（认领排他、心跳保活、失联接管、迟到写回忽略）。"""
from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone

import pytest

from app import handlers
from app.engine import Orchestrator
from app.errors import StaleExecutorWrite
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec
from app.store import Store


class FakeClock:
    """可手动推进的 UTC 时钟，让失联判定确定、快速、不依赖真实时序。"""

    def __init__(self, start: datetime | None = None):
        self.now = start or datetime(2026, 10, 3, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _single_task_workflow(action: str = "work", name: str = "w") -> WorkflowSpec:
    return WorkflowSpec(name=name, tasks=[TaskSpec(name="t", action=action)])


@pytest.fixture()
def clock_store(db_path):
    clock = FakeClock()
    s = Store(db_path, clock=clock)
    yield s, clock
    s.close()


class TestClaimAndLease:
    def test_pending_task_claimed_once_epoch_increments(self, store):
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        e1 = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        assert e1 == 1  # 首次认领，纪元为 1
        # 第二个执行者立刻来抢：持有者心跳新鲜，不能抢
        assert (
            store.claim_task(run_id, "t", "exec-B", heartbeat_timeout_seconds=30)
            is None
        )

    def test_heartbeat_keeps_fresh_lease_from_takeover(self, clock_store):
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        # 时间推进 20s（未超 30s 阈值），期间 A 持续续心跳
        clock.advance(20)
        assert store.heartbeat_task(run_id, "t", "exec-A", epoch) is True
        clock.advance(20)
        assert store.heartbeat_task(run_id, "t", "exec-A", epoch) is True
        # 累计 40s 但心跳一直新鲜：B 仍不能接管
        assert (
            store.claim_task(run_id, "t", "exec-B", heartbeat_timeout_seconds=30)
            is None
        )

    def test_stale_lease_taken_over_only_after_timeout(self, clock_store):
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch_a = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        assert epoch_a == 1
        # 未超时：不可接管
        clock.advance(29)
        assert (
            store.claim_task(run_id, "t", "exec-B", heartbeat_timeout_seconds=30)
            is None
        )
        # A 失联超过阈值：B 接管，纪元递增
        clock.advance(2)
        epoch_b = store.claim_task(
            run_id, "t", "exec-B", heartbeat_timeout_seconds=30
        )
        assert epoch_b == 2
        info = store.get_run(run_id)
        task = info.tasks[0]
        assert task.status == TaskStatus.RUNNING
        assert task.executor_id == "exec-B"
        assert task.claim_epoch == 2

    def test_takeover_keeps_attempts_history(self, clock_store):
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        store.claim_task(run_id, "t", "exec-A", heartbeat_timeout_seconds=30)
        # A 已尝试 2 次（模拟重试后再次认领），每次认领 attempts +1
        store.complete_task(
            run_id, "t", "exec-A", 1, TaskStatus.PENDING, error="retry"
        )
        store.claim_task(run_id, "t", "exec-A", heartbeat_timeout_seconds=30)
        assert store.get_run(run_id).tasks[0].attempts == 2
        # 失联被 B 接管：attempts 接着算，不丢不串
        clock.advance(60)
        epoch_b = store.claim_task(
            run_id, "t", "exec-B", heartbeat_timeout_seconds=30
        )
        assert epoch_b == 3
        assert store.get_run(run_id).tasks[0].attempts == 3

    def test_succeeded_task_never_claimable(self, store):
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        store.complete_task(
            run_id, "t", "exec-A", epoch, TaskStatus.SUCCEEDED, result="ok"
        )
        # 已成功：任何执行者、无论过多久都不能再认领
        assert (
            store.claim_task(run_id, "t", "exec-B", heartbeat_timeout_seconds=0.01)
            is None
        )


class TestFencingLateWrites:
    def test_late_success_write_from_dead_executor_ignored(self, clock_store):
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch_a = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        # A 失联，B 接管并成功完成
        clock.advance(60)
        epoch_b = store.claim_task(
            run_id, "t", "exec-B", heartbeat_timeout_seconds=30
        )
        store.complete_task(
            run_id, "t", "exec-B", epoch_b, TaskStatus.SUCCEEDED, result="B"
        )
        # A 复活，用旧令牌往回写：必须被拒绝，不能覆盖 B 的结果
        with pytest.raises(StaleExecutorWrite) as exc:
            store.complete_task(
                run_id, "t", "exec-A", epoch_a, TaskStatus.SUCCEEDED, result="A"
            )
        assert exc.value.claim_epoch == epoch_b
        task = store.get_run(run_id).tasks[0]
        assert task.status == TaskStatus.SUCCEEDED
        assert task.result == "B"  # 新执行者结果未被覆盖
        assert task.executor_id == "exec-B"

    def test_late_failure_write_does_not_corrupt_new_attempt(self, clock_store):
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch_a = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        clock.advance(60)
        epoch_b = store.claim_task(
            run_id, "t", "exec-B", heartbeat_timeout_seconds=30
        )
        # A 迟到的“失败”写回不能把 B 正在执行的任务打成 FAILED
        with pytest.raises(StaleExecutorWrite):
            store.complete_task(
                run_id, "t", "exec-A", epoch_a,
                TaskStatus.FAILED, error="late failure",
            )
        task = store.get_run(run_id).tasks[0]
        assert task.status == TaskStatus.RUNNING
        assert task.executor_id == "exec-B"
        # B 正常成功
        store.complete_task(
            run_id, "t", "exec-B", epoch_b, TaskStatus.SUCCEEDED, result="ok"
        )
        assert store.get_run(run_id).tasks[0].status == TaskStatus.SUCCEEDED

    def test_heartbeat_with_old_epoch_rejected(self, clock_store):
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch_a = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        clock.advance(60)
        epoch_b = store.claim_task(
            run_id, "t", "exec-B", heartbeat_timeout_seconds=30
        )
        assert epoch_b > epoch_a
        # 旧执行者续心跳无效；新执行者续心跳有效
        assert store.heartbeat_task(run_id, "t", "exec-A", epoch_a) is False
        assert store.heartbeat_task(run_id, "t", "exec-B", epoch_b) is True


class TestRecoveryDistinguishesAliveAndDead:
    def test_recover_requeues_only_stale_tasks(self, clock_store):
        store, clock = clock_store
        store.save_workflow(
            WorkflowSpec(
                name="w",
                tasks=[
                    TaskSpec(name="alive", action="x"),
                    TaskSpec(name="dead", action="x"),
                ],
            )
        )
        run_id, _ = store.create_run_idempotent("w", "k")
        store.claim_task(run_id, "alive", "exec-A", heartbeat_timeout_seconds=30)
        store.claim_task(run_id, "dead", "exec-B", heartbeat_timeout_seconds=30)
        # A 持续上报心跳（活着），B 没有心跳（崩溃）
        clock.advance(60)
        store.heartbeat_task(run_id, "alive", "exec-A", 1)
        requeued = store.requeue_stale_tasks(heartbeat_timeout_seconds=30)
        assert requeued == [(run_id, "dead")]  # 只有失联的被重排队
        by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert by_name["alive"].status == TaskStatus.RUNNING  # 活执行者任务原样
        assert by_name["alive"].executor_id == "exec-A"
        assert by_name["dead"].status == TaskStatus.PENDING
        assert by_name["dead"].attempts == 1  # 历史尝试保留

    def test_restart_with_live_peer_does_not_touch_its_tasks(self, clock_store):
        """新实例启动时恢复：活执行者的任务保持 RUNNING，不重置重跑。"""
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        store.register_executor("exec-A")
        store.claim_task(run_id, "t", "exec-A", heartbeat_timeout_seconds=30)
        clock.advance(5)  # 只过了 5s，A 心跳仍然新鲜

        async def recover():
            other = Orchestrator(
                store, executor_id="exec-new",
                heartbeat_interval_seconds=1,
                heartbeat_timeout_seconds=30,
                maintenance_interval_seconds=60,
            )
            recovered = await other.recover_unfinished()
            await other.shutdown()
            return recovered

        recovered = asyncio.run(recover())
        assert recovered == [run_id]  # 运行被调度接管观察
        task = store.get_run(run_id).tasks[0]
        assert task.status == TaskStatus.RUNNING  # 没有被重置成 PENDING
        assert task.executor_id == "exec-A"


class TestConcurrentExecutors:
    def test_concurrent_claim_from_threads_only_one_wins(self, store):
        """真实并发：多线程同时对同一份数据认领同一任务，只有一个成功。"""
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        winners: list[str] = []
        barrier = threading.Barrier(8)

        def try_claim(exec_id: str) -> None:
            barrier.wait()  # 所有线程同时发起，最大化真实竞争
            epoch = store.claim_task(
                run_id, "t", exec_id, heartbeat_timeout_seconds=30
            )
            if epoch is not None:
                winners.append(exec_id)

        threads = [
            threading.Thread(target=try_claim, args=(f"exec-{i}",))
            for i in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(winners) == 1
        task = store.get_run(run_id).tasks[0]
        assert task.executor_id == winners[0]
        assert task.attempts == 1  # 没有因竞争产生重复执行/重复计数

    def test_concurrent_takeover_race_only_one_takes_over(self, clock_store):
        """真实并发：任务失联后多个执行者同时接管，恰好一个接管成功且
        attempts 只增加一次。"""
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        store.claim_task(run_id, "t", "dead-exec", heartbeat_timeout_seconds=30)
        clock.advance(60)
        epochs: list[int | None] = []
        barrier = threading.Barrier(6)

        def try_takeover(exec_id: str) -> None:
            barrier.wait()
            epochs.append(
                store.claim_task(run_id, "t", exec_id, heartbeat_timeout_seconds=30)
            )

        threads = [
            threading.Thread(target=try_takeover, args=(f"exec-{i}",))
            for i in range(6)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        successes = [e for e in epochs if e is not None]
        assert successes == [2]  # 只有一个接管者，纪元恰为 2
        assert store.get_run(run_id).tasks[0].attempts == 2

    def test_concurrent_late_writes_and_new_writes_keep_new_result(self, clock_store):
        """真实并发：旧执行者的迟到写回与新执行者的正确写回同时发生，
        数据最终一定是新执行者的结果，旧写回全部被忽略。"""
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        epoch_a = store.claim_task(
            run_id, "t", "exec-A", heartbeat_timeout_seconds=30
        )
        clock.advance(60)
        epoch_b = store.claim_task(
            run_id, "t", "exec-B", heartbeat_timeout_seconds=30
        )
        errors: list[Exception] = []
        barrier = threading.Barrier(4)

        def late_write() -> None:
            barrier.wait()
            try:
                store.complete_task(
                    run_id, "t", "exec-A", epoch_a,
                    TaskStatus.SUCCEEDED, result="STALE",
                )
            except StaleExecutorWrite as exc:
                errors.append(exc)

        def new_write() -> None:
            barrier.wait()
            store.complete_task(
                run_id, "t", "exec-B", epoch_b,
                TaskStatus.SUCCEEDED, result="FRESH",
            )

        threads = [threading.Thread(target=late_write) for _ in range(3)]
        threads.append(threading.Thread(target=new_write))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 3  # 所有迟到写回都被挡下
        task = store.get_run(run_id).tasks[0]
        assert task.status == TaskStatus.SUCCEEDED
        assert task.result == "FRESH"


class TestRetryAcrossTakeover:
    def test_retry_then_death_then_takeover_attempts_continuous(self, clock_store):
        """A 上可重试失败回到 PENDING（attempts=1）；A 再次认领跑到一半失联，
        B 接管重跑：attempts 连续累计为 3，最终成功。"""
        store, clock = clock_store
        store.save_workflow(_single_task_workflow())
        run_id, _ = store.create_run_idempotent("w", "k")
        # 第一次尝试：A 认领后可重试失败，回 PENDING
        e1 = store.claim_task(run_id, "t", "exec-A", heartbeat_timeout_seconds=30)
        store.complete_task(
            run_id, "t", "exec-A", e1, TaskStatus.PENDING, error="retryable"
        )
        assert store.get_run(run_id).tasks[0].attempts == 1
        # 第二次尝试：A 重新认领后崩溃（没写回）
        e2 = store.claim_task(run_id, "t", "exec-A", heartbeat_timeout_seconds=30)
        assert e2 == 2
        clock.advance(60)
        # B 接管：第三次尝试
        e3 = store.claim_task(run_id, "t", "exec-B", heartbeat_timeout_seconds=30)
        assert e3 == 3
        task = store.get_run(run_id).tasks[0]
        assert task.attempts == 3
        assert task.executor_id == "exec-B"
        store.complete_task(
            run_id, "t", "exec-B", e3, TaskStatus.SUCCEEDED, result="ok"
        )
        assert store.get_run(run_id).tasks[0].status == TaskStatus.SUCCEEDED
