"""业务：失败影响面管理（BLOCKED 级联）与上游恢复后的自动续跑。"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app import handlers
from app.api import create_app
from app.engine import Orchestrator
from app.errors import InvalidStateTransition
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec
from app.store import Store


class FakeClock:
    def __init__(self):
        self.now = datetime(2026, 10, 3, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _diamond_workflow():
    #          up
    #        /     \
    #     left     right
    #        \     /
    #         bottom
    return WorkflowSpec(
        name="w",
        tasks=[
            TaskSpec(name="up", action="up"),
            TaskSpec(name="left", action="ok", depends_on=["up"]),
            TaskSpec(name="right", action="ok", depends_on=["up"]),
            TaskSpec(name="bottom", action="ok", depends_on=["left", "right"]),
        ],
    )


class TestBlockedCascade:
    def test_direct_and_indirect_downstream_marked_blocked(self, store):
        def fail(params):
            raise RuntimeError("boom")

        handlers.register("up", fail)
        handlers.register("ok", lambda p: "ok")
        spec = _diamond_workflow()

        async def go():
            orch = Orchestrator(store)
            store.save_workflow(spec)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(go())
        assert info.status == RunStatus.FAILED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["up"].status == TaskStatus.FAILED
        # 直接与间接下游都被明确标记为 BLOCKED，而不是混在 PENDING 里
        for n in ("left", "right", "bottom"):
            assert by_name[n].status == TaskStatus.BLOCKED
            assert "blocked by upstream failure" in (by_name[n].error or "")

    def test_independent_branch_not_blocked(self, store):
        """不在失败传递闭包内的任务不受影响，照常跑完。"""
        order: list[str] = []

        def fail(params):
            raise RuntimeError("boom")

        handlers.register("bad", fail)
        handlers.register("fine", lambda p: order.append("fine") or "done")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="bad", action="bad"),
                TaskSpec(name="fine", action="fine"),
            ],
        )

        async def go():
            orch = Orchestrator(store)
            store.save_workflow(spec)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(go())
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["bad"].status == TaskStatus.FAILED
        assert by_name["fine"].status == TaskStatus.SUCCEEDED
        assert order == ["fine"]

    def test_blocked_status_distinguished_from_pending(self, store):
        blocked = store.block_tasks
        # 枚举值本身即可被调用方区分
        assert TaskStatus.BLOCKED.value == "BLOCKED"
        assert TaskStatus.BLOCKED != TaskStatus.PENDING


class TestRecoveryRerun:
    def test_rerun_failed_upstream_unblocks_and_completes_chain(self, store):
        # up 第一次失败（fatal），下游全部 BLOCKED；修复后 rerun up，
        # 整条链自动跑完，无需重新提交工作流
        attempts = {"up": 0}

        def flaky_up(params):
            attempts["up"] += 1
            if attempts["up"] == 1:
                raise RuntimeError("first fails")
            return "UP"

        handlers.register("up", flaky_up)
        handlers.register("ok", lambda p: "ok")
        spec = _diamond_workflow()

        async def go():
            orch = Orchestrator(store)
            store.save_workflow(spec)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            assert info.status == RunStatus.FAILED
            # 恢复失败任务：引擎负责按依赖固定点解阻并续跑
            await orch.rerun_task(run_id, "up")
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info, attempts["up"]

        info, up_attempts = asyncio.run(go())
        assert info.status == RunStatus.SUCCEEDED
        assert up_attempts == 2  # 之前尝试历史接着算，up 只再跑一次
        by_name = {t.task_name: t for t in info.tasks}
        for n in ("up", "left", "right", "bottom"):
            assert by_name[n].status == TaskStatus.SUCCEEDED
        assert by_name["up"].attempts == 2

    def test_rerun_keeps_attempts_accumulated(self, store):
        # up 有 max_retries：首次执行+2 次重试耗尽后 FAILED（attempts=3），
        # 手动 rerun 后第 4 次尝试成功，历史计数不丢
        from app.errors import RetryableTaskError

        calls = {"n": 0}

        def flaky(params):
            calls["n"] += 1
            if calls["n"] < 4:
                raise RetryableTaskError("glitch")
            return "ok"

        handlers.register("up", flaky)
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="up", action="up", max_retries=2,
                         retry_delay_seconds=0),
                TaskSpec(name="down", action="down", depends_on=["up"]),
            ],
        )
        handlers.register("down", lambda p: "d")

        async def go():
            orch = Orchestrator(store)
            store.save_workflow(spec)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            by_name = {t.task_name: t for t in info.tasks}
            assert by_name["up"].status == TaskStatus.FAILED
            assert by_name["up"].attempts == 3
            assert by_name["down"].status == TaskStatus.BLOCKED
            await orch.rerun_task(run_id, "up")
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(go())
        by_name = {t.task_name: t for t in info.tasks}
        assert info.status == RunStatus.SUCCEEDED
        assert calls["n"] == 4
        assert by_name["up"].attempts == 4
        assert by_name["down"].status == TaskStatus.SUCCEEDED

    def test_partial_unblock_when_other_upstream_still_failed(self, store):
        # left 依赖 up1、up2；up1 恢复但 up2 仍失败时，left 必须继续 BLOCKED；
        # 等 up2 也恢复后才解阻
        outcomes = {"up1": RuntimeError("x"), "up2": RuntimeError("x")}

        def make_handler(name):
            def _h(params):
                exc = outcomes[name]
                if isinstance(exc, Exception):
                    raise exc
                return exc
            return _h

        handlers.register("up1", make_handler("up1"))
        handlers.register("up2", make_handler("up2"))
        handlers.register("join", lambda p: "j")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="up1", action="up1"),
                TaskSpec(name="up2", action="up2"),
                TaskSpec(name="join", action="join", depends_on=["up1", "up2"]),
            ],
        )

        async def go():
            orch = Orchestrator(store)
            store.save_workflow(spec)
            run_id, _ = store.create_run_idempotent("w", "k")
            await orch.start_run(run_id)
            await orch.wait_run(run_id, timeout=5)
            outcomes["up1"] = "ok1"
            await orch.rerun_task(run_id, "up1")
            await asyncio.sleep(0.1)
            info1 = store.get_run(run_id)
            by1 = {t.task_name: t for t in info1.tasks}
            assert by1["up1"].status == TaskStatus.SUCCEEDED
            assert by1["join"].status == TaskStatus.BLOCKED  # up2 仍失败，继续挡
            outcomes["up2"] = "ok2"
            await orch.rerun_task(run_id, "up2")
            info2 = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info2

        info = asyncio.run(go())
        by_name = {t.task_name: t for t in info.tasks}
        assert info.status == RunStatus.SUCCEEDED
        assert by_name["join"].status == TaskStatus.SUCCEEDED


class TestIllegalOperationReasons:
    def test_rerun_each_illegal_state_has_distinct_code(self, store):
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="succ", action="x"),
                TaskSpec(name="run", action="x"),
                TaskSpec(name="blkd", action="x", depends_on=["run"]),
                TaskSpec(name="pend", action="x"),
                TaskSpec(name="fail", action="x"),
            ],
        )
        store.save_workflow(spec)
        run_id, _ = store.create_run_idempotent("w", "k")
        # 手工布置各状态
        store.transition_task(run_id, "succ", TaskStatus.RUNNING,
                              allowed_from=(TaskStatus.PENDING,))
        store.transition_task(run_id, "succ", TaskStatus.SUCCEEDED,
                              allowed_from=(TaskStatus.RUNNING,))
        store.transition_task(run_id, "run", TaskStatus.RUNNING,
                              allowed_from=(TaskStatus.PENDING,))
        store.block_tasks(run_id, {"blkd"})
        store.transition_task(run_id, "fail", TaskStatus.RUNNING,
                              allowed_from=(TaskStatus.PENDING,))
        store.transition_task(run_id, "fail", TaskStatus.FAILED,
                              allowed_from=(TaskStatus.RUNNING,))

        cases = {
            "succ": "task_already_succeeded",
            "run": "task_running",
            "blkd": "task_blocked",
            "pend": "task_pending",
        }
        for task_name, code in cases.items():
            with pytest.raises(InvalidStateTransition) as exc:
                store.rerun_failed_task(run_id, task_name)
            assert exc.value.reason == "invalid_state_transition"
            assert exc.value.code == code, task_name
            assert exc.value.current == {
                "succ": "SUCCEEDED", "run": "RUNNING",
                "blkd": "BLOCKED", "pend": "PENDING",
            }[task_name]
        # 不存在的任务也有可区分 code
        with pytest.raises(InvalidStateTransition) as exc:
            store.rerun_failed_task(run_id, "ghost")
        assert exc.value.code == "task_missing"

    def test_api_returns_distinct_code_for_each_illegal_rerun(self, db_path):
        handlers.register("ok", lambda p: "fine")
        app = create_app(db_path)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w",
                "tasks": [{"name": "t", "action": "ok"}],
            })
            run_id = client.post(
                "/workflows/w/runs", json={"idempotency_key": "k"}
            ).json()["run_id"]
            asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            resp = client.post(f"/runs/{run_id}/tasks/t/rerun")
            assert resp.status_code == 409
            body = resp.json()
            assert body["reason"] == "invalid_state_transition"
            assert body["current"] == "SUCCEEDED"
            assert body["code"] == "task_already_succeeded"
