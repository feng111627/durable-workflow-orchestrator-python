"""业务：重试与失败处理。"""
from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app import handlers
from app.api import create_app
from app.engine import Orchestrator
from app.errors import FatalTaskError, RetryableTaskError
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec


def _run_workflow(store, spec: WorkflowSpec, key="k"):
    store.save_workflow(spec)

    async def go():
        orch = Orchestrator(store)
        run_id, _ = store.create_run_idempotent(spec.name, key)
        await orch.start_run(run_id)
        info = await orch.wait_run(run_id, timeout=5)
        await orch.shutdown()
        return info

    return asyncio.run(go())


class TestRetryAndFailure:
    def test_retryable_error_retried_until_success(self, store):
        calls = {"n": 0}

        def flaky(params):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RetryableTaskError("temporary glitch")
            return "done"

        handlers.register("flaky", flaky)
        info = _run_workflow(store, WorkflowSpec(
            name="w",
            tasks=[TaskSpec(name="t", action="flaky", max_retries=3)],
        ))
        assert info.status == RunStatus.SUCCEEDED
        assert calls["n"] == 3
        task = info.tasks[0]
        assert task.status == TaskStatus.SUCCEEDED and task.attempts == 3
        assert task.result == "done"

    def test_retryable_error_exhausts_retries_then_failed(self, store):
        calls = {"n": 0}

        def always(params):
            calls["n"] += 1
            raise RetryableTaskError("still broken")

        handlers.register("always", always)
        info = _run_workflow(store, WorkflowSpec(
            name="w",
            tasks=[TaskSpec(name="t", action="always", max_retries=2)],
        ))
        assert info.status == RunStatus.FAILED
        task = info.tasks[0]
        # 首次执行 + 2 次重试 = 3 次尝试
        assert calls["n"] == 3 and task.attempts == 3
        assert task.status == TaskStatus.FAILED
        assert "retries exhausted" in task.error

    def test_fatal_error_not_retried(self, store):
        calls = {"n": 0}

        def fatal(params):
            calls["n"] += 1
            raise FatalTaskError("bad input, will never work")

        handlers.register("fatal", fatal)
        info = _run_workflow(store, WorkflowSpec(
            name="w",
            tasks=[TaskSpec(name="t", action="fatal", max_retries=5)],
        ))
        assert info.status == RunStatus.FAILED
        assert calls["n"] == 1  # 不可重试：即使配置了重试也不重跑
        assert "fatal" in info.tasks[0].error

    def test_unexpected_exception_treated_as_fatal(self, store):
        def boom(params):
            raise ValueError("unexpected")

        handlers.register("boom", boom)
        info = _run_workflow(store, WorkflowSpec(
            name="w",
            tasks=[TaskSpec(name="t", action="boom", max_retries=3)],
        ))
        assert info.status == RunStatus.FAILED
        assert info.tasks[0].attempts == 1

    def test_failed_task_blocks_downstream(self, store):
        ran: list[str] = []

        def fail(params):
            raise FatalTaskError("no")

        handlers.register("fail", fail)
        handlers.register("down", lambda p: ran.append("down"))
        info = _run_workflow(store, WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="up", action="fail"),
                TaskSpec(name="down", action="down", depends_on=["up"]),
            ],
        ))
        assert info.status == RunStatus.FAILED
        assert ran == []  # 下游任务不被执行
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["down"].status == TaskStatus.PENDING

    def test_rerun_failed_task_via_api(self, db_path):
        attempts = {"n": 0}

        def flaky(params):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise FatalTaskError("first try fails")
            return "recovered"

        handlers.register("flaky", flaky)
        app = create_app(db_path)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w", "tasks": [{"name": "t", "action": "flaky"}],
            })
            run_id = client.post(
                "/workflows/w/runs", json={"idempotency_key": "k"}
            ).json()["run_id"]
            info = asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            assert info.status == RunStatus.FAILED
            resp = client.post(f"/runs/{run_id}/tasks/t/rerun")
            assert resp.status_code == 200
            info = asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            assert info.status == RunStatus.SUCCEEDED
            assert attempts["n"] == 2

    def test_rerun_succeeded_task_rejected_with_reason(self, db_path):
        handlers.register("ok", lambda p: "fine")
        app = create_app(db_path)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w", "tasks": [{"name": "t", "action": "ok"}],
            })
            run_id = client.post(
                "/workflows/w/runs", json={"idempotency_key": "k"}
            ).json()["run_id"]
            info = asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            assert info.status == RunStatus.SUCCEEDED
            resp = client.post(f"/runs/{run_id}/tasks/t/rerun")
            assert resp.status_code == 409
            body = resp.json()
            assert body["reason"] == "invalid_state_transition"
            assert body["current"] == "SUCCEEDED"
            assert body["target"] == "PENDING"
