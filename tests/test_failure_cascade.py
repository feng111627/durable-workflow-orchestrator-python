"""业务：失败影响面标记与恢复续跑。"""
from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app import handlers
from app.api import create_app
from app.engine import Orchestrator
from app.errors import FatalTaskError
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec


def _run_until_terminal(store, spec: WorkflowSpec, key="k"):
    store.save_workflow(spec)

    async def go():
        orch = Orchestrator(store, lease_seconds=0.3, heartbeat_interval=0.1)
        run_id, _ = store.create_run_idempotent(spec.name, key)
        await orch.start_run(run_id)
        info = await orch.wait_run(run_id, timeout=5)
        await orch.shutdown()
        return run_id, info

    return asyncio.run(go())


class TestFailureBlastRadius:
    def test_direct_and_indirect_downstream_marked_blocked(self, store):
        def fail(params):
            raise FatalTaskError("broken")

        handlers.register("fail", fail)
        handlers.register("ok", lambda p: "fine")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="a", action="fail"),
                TaskSpec(name="b", action="ok", depends_on=["a"]),   # 直接下游
                TaskSpec(name="c", action="ok", depends_on=["b"]),   # 间接下游
                TaskSpec(name="d", action="ok", depends_on=["a", "c"]),  # 多依赖
                TaskSpec(name="e", action="ok"),                     # 无关分支
            ],
        )
        _, info = _run_until_terminal(store, spec)
        assert info.status == RunStatus.FAILED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["a"].status == TaskStatus.FAILED
        # 直接+间接下游都被明确标成 BLOCKED，而不是混在普通 PENDING 里
        for name in ("b", "c", "d"):
            assert by_name[name].status == TaskStatus.BLOCKED, name
            assert "blocked by failed upstream" in by_name[name].error
        # 无关分支不受影响，正常跑完
        assert by_name["e"].status == TaskStatus.SUCCEEDED

    def test_blocked_downstream_resumes_after_upstream_recovery(self, store):
        calls = {"a": 0, "b": 0, "c": 0}

        def flaky(params):
            calls["a"] += 1
            if calls["a"] == 1:
                raise FatalTaskError("first attempt fails")
            return "recovered"

        handlers.register("flaky", flaky)
        handlers.register("b", lambda p: calls.__setitem__("b", calls["b"] + 1) or "B")
        handlers.register("c", lambda p: calls.__setitem__("c", calls["c"] + 1) or "C")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="a", action="flaky"),
                TaskSpec(name="b", action="b", depends_on=["a"]),
                TaskSpec(name="c", action="c", depends_on=["b"]),
            ],
        )
        run_id, info = _run_until_terminal(store, spec)
        assert info.status == RunStatus.FAILED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["b"].status == TaskStatus.BLOCKED
        assert by_name["c"].status == TaskStatus.BLOCKED

        async def recover():
            orch = Orchestrator(store, lease_seconds=0.3, heartbeat_interval=0.1)
            # 恢复失败的上游：重新排队，不用重新提交整个工作流
            store.requeue_failed_task(run_id, "a")
            # 被挡住的下游随依赖自动解除阻塞
            mid = {t.task_name: t for t in store.get_run(run_id).tasks}
            assert mid["b"].status == TaskStatus.PENDING
            assert mid["c"].status == TaskStatus.PENDING
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(recover())
        assert info.status == RunStatus.SUCCEEDED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["a"].attempts == 2  # 尝试次数接着算
        assert by_name["b"].result == "B" and by_name["c"].result == "C"
        assert calls == {"a": 2, "b": 1, "c": 1}

    def test_unblock_only_when_all_failed_ancestors_recovered(self, store):
        """菱形依赖：两个上游都失败时，恢复一个不足以解除下游阻塞。"""
        handlers.register("ok", lambda p: "fine")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="a1", action="ok"),
                TaskSpec(name="a2", action="ok"),
                TaskSpec(name="b", action="ok", depends_on=["a1", "a2"]),
            ],
        )
        store.save_workflow(spec)
        run_id, _ = store.create_run_idempotent("w", "k")
        # 直接构造终态：a1、a2 失败，b 被阻塞
        for name in ("a1", "a2"):
            token = store.claim_task(run_id, name, "exec", lease_seconds=30.0)
            assert store.fail_task_and_block_downstream(
                run_id, name, "exec", token, "fatal: boom"
            ) is True
        by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert by_name["b"].status == TaskStatus.BLOCKED
        # 只恢复 a1：b 仍有失败的祖先 a2，保持 BLOCKED
        store.requeue_failed_task(run_id, "a1")
        by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert by_name["b"].status == TaskStatus.BLOCKED
        # 再恢复 a2：所有失败祖先都已恢复，b 自动解除阻塞
        store.requeue_failed_task(run_id, "a2")
        by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert by_name["b"].status == TaskStatus.PENDING

    def test_rerun_blocked_task_rejected_with_distinct_reason(self, db_path):
        def fail(params):
            raise FatalTaskError("no")

        handlers.register("fail", fail)
        handlers.register("ok", lambda p: "fine")
        app = create_app(db_path, lease_seconds=0.3, heartbeat_interval=0.1)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w",
                "tasks": [
                    {"name": "a", "action": "fail"},
                    {"name": "b", "action": "ok", "depends_on": ["a"]},
                ],
            })
            run_id = client.post(
                "/workflows/w/runs", json={"idempotency_key": "k"}
            ).json()["run_id"]
            info = asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            assert info.status == RunStatus.FAILED
            resp = client.post(f"/runs/{run_id}/tasks/b/rerun")
            assert resp.status_code == 409
            body = resp.json()
            # 可区分：是“被上游失败阻塞”，不是笼统的非法迁移
            assert body["reason"] == "task_blocked_by_upstream"
            assert body["current"] == "BLOCKED"

    def test_rerun_running_task_rejected_with_distinct_reason(self, db_path):
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow(params):
            started.set()
            await release.wait()
            return "done"

        handlers.register("slow", slow)
        app = create_app(db_path, lease_seconds=0.3, heartbeat_interval=0.1)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w", "tasks": [{"name": "t", "action": "slow"}],
            })
            run_id = client.post(
                "/workflows/w/runs", json={"idempotency_key": "k"}
            ).json()["run_id"]

            async def wait_running():
                while True:
                    info = app.state.store.get_run(run_id)
                    if info.tasks[0].status == TaskStatus.RUNNING:
                        return
                    await asyncio.sleep(0.02)

            asyncio.run(asyncio.wait_for(wait_running(), timeout=5))
            resp = client.post(f"/runs/{run_id}/tasks/t/rerun")
            assert resp.status_code == 409
            body = resp.json()
            # 可区分：是“正在执行中”
            assert body["reason"] == "task_currently_running"
            assert body["current"] == "RUNNING"
            release.set()
            info = asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            assert info.status == RunStatus.SUCCEEDED
