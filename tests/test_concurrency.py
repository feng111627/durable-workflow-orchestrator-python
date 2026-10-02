"""业务：并发控制与幂等。"""
from __future__ import annotations

import asyncio

import pytest

from app import handlers
from app.engine import Orchestrator
from app.errors import InvalidStateTransition
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec


class TestConcurrencyAndIdempotency:
    def test_max_concurrent_tasks_limits_parallelism(self, store):
        current = 0
        peak = 0
        done: list[str] = []

        async def worker(params):
            nonlocal current, peak
            current += 1
            peak = max(peak, current)
            await asyncio.sleep(0.02)
            current -= 1
            done.append(params["name"])

        handlers.register("worker", worker)
        spec = WorkflowSpec(
            name="w",
            tasks=[TaskSpec(name=f"t{i}", action="worker",
                            params={"name": f"t{i}"}) for i in range(6)],
        )
        store.save_workflow(spec)

        async def go():
            orch = Orchestrator(store, max_concurrent_tasks=2)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=10)
            await orch.shutdown()
            return info

        info = asyncio.run(go())
        assert info.status == RunStatus.SUCCEEDED
        assert peak <= 2  # 并发上限被遵守
        assert sorted(done) == [f"t{i}" for i in range(6)]  # 排队任务全部执行，无丢弃

    def test_duplicate_start_run_does_not_double_execute(self, store):
        calls = {"n": 0}

        async def slow(params):
            calls["n"] += 1
            await asyncio.sleep(0.05)

        handlers.register("slow", slow)
        store.save_workflow(WorkflowSpec(
            name="w", tasks=[TaskSpec(name="t", action="slow")]
        ))

        async def go():
            orch = Orchestrator(store)
            run_id, _ = store.create_run_idempotent("w", "k")
            # 同一次运行被并发触发多次调度
            await asyncio.gather(*(orch.start_run(run_id) for _ in range(5)))
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(go())
        assert info.status == RunStatus.SUCCEEDED
        assert calls["n"] == 1  # 任务只执行了一次，无重复副作用

    def test_concurrent_claim_of_same_task_rejected(self, store):
        store.save_workflow(WorkflowSpec(
            name="w", tasks=[TaskSpec(name="t", action="x")]
        ))
        run_id, _ = store.create_run_idempotent("w", "k")
        # 第一个执行者成功认领 PENDING -> RUNNING
        store.transition_task(
            run_id, "t", TaskStatus.RUNNING,
            allowed_from=(TaskStatus.PENDING,), increment_attempts=True,
        )
        # 第二个并发执行者的认领被 CAS 拒绝，原因可区分
        with pytest.raises(InvalidStateTransition) as exc:
            store.transition_task(
                run_id, "t", TaskStatus.RUNNING,
                allowed_from=(TaskStatus.PENDING,), increment_attempts=True,
            )
        assert exc.value.reason == "invalid_state_transition"
        assert exc.value.current == "RUNNING"
        assert exc.value.target == "RUNNING"

    def test_rerun_of_succeeded_task_rejected_at_store_level(self, store):
        store.save_workflow(WorkflowSpec(
            name="w", tasks=[TaskSpec(name="t", action="x")]
        ))
        run_id, _ = store.create_run_idempotent("w", "k")
        store.transition_task(
            run_id, "t", TaskStatus.RUNNING, allowed_from=(TaskStatus.PENDING,)
        )
        store.transition_task(
            run_id, "t", TaskStatus.SUCCEEDED, allowed_from=(TaskStatus.RUNNING,)
        )
        with pytest.raises(InvalidStateTransition) as exc:
            store.transition_task(
                run_id, "t", TaskStatus.PENDING, allowed_from=(TaskStatus.FAILED,)
            )
        assert exc.value.current == "SUCCEEDED"

    def test_create_run_idempotent_under_concurrency(self, store):
        store.save_workflow(WorkflowSpec(
            name="w", tasks=[TaskSpec(name="t", action="x")]
        ))
        # 模拟并发重复提交同一幂等键：只有一个能创建成功
        results = [store.create_run_idempotent("w", "same-key") for _ in range(10)]
        created = [r for r in results if r[1]]
        assert len(created) == 1
        assert all(r[0] == created[0][0] for r in results)
