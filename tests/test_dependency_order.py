"""业务：依赖顺序与定义校验。"""
from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from app import handlers
from app.api import create_app, validate_workflow
from app.engine import Orchestrator
from app.errors import (
    CyclicDependencyError,
    DuplicateTaskName,
    UnknownAction,
    UnknownTaskDependency,
)
from app.models import RunStatus, TaskSpec, WorkflowSpec


def _spec(**kwargs):
    return WorkflowSpec(**kwargs)


class TestDependencyOrdering:
    def test_tasks_run_in_dependency_order(self, store):
        order: list[str] = []
        for name in ("a", "b", "c", "d"):
            handlers.register(name, lambda p, n=name: order.append(n))

        store.save_workflow(_spec(
            name="dag",
            tasks=[
                TaskSpec(name="a", action="a"),
                TaskSpec(name="b", action="b", depends_on=["a"]),
                TaskSpec(name="c", action="c", depends_on=["a"]),
                TaskSpec(name="d", action="d", depends_on=["b", "c"]),
            ],
        ))

        async def run():
            orch = Orchestrator(store)
            run_id, _ = store.create_run_idempotent("dag", "k1")
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(run())
        assert info.status == RunStatus.SUCCEEDED
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("d") > order.index("b")
        assert order.index("d") > order.index("c")

    def test_cyclic_dependency_rejected(self):
        with pytest.raises(CyclicDependencyError) as exc:
            validate_workflow(_spec(
                name="cyc",
                tasks=[
                    TaskSpec(name="a", action="a", depends_on=["b"]),
                    TaskSpec(name="b", action="b", depends_on=["a"]),
                ],
            ))
        assert exc.value.reason == "cyclic_dependency"

    def test_self_dependency_rejected_as_cycle(self):
        with pytest.raises(CyclicDependencyError):
            validate_workflow(_spec(
                name="self", tasks=[TaskSpec(name="a", action="a", depends_on=["a"])]
            ))

    def test_unknown_dependency_rejected(self):
        with pytest.raises(UnknownTaskDependency) as exc:
            validate_workflow(_spec(
                name="bad", tasks=[TaskSpec(name="a", action="a", depends_on=["ghost"])]
            ))
        assert exc.value.reason == "unknown_task_dependency"

    def test_duplicate_task_name_rejected(self):
        with pytest.raises(DuplicateTaskName):
            validate_workflow(_spec(
                name="dup",
                tasks=[TaskSpec(name="a", action="a"), TaskSpec(name="a", action="a")],
            ))

    def test_unknown_action_rejected(self):
        with pytest.raises(UnknownAction):
            validate_workflow(_spec(
                name="noact", tasks=[TaskSpec(name="a", action="not-registered")]
            ))

    def test_api_rejects_cyclic_workflow_with_reason(self, db_path):
        handlers.register("a", lambda p: None)
        client = TestClient(create_app(db_path))
        resp = client.post("/workflows", json={
            "name": "cyc",
            "tasks": [
                {"name": "a", "action": "a", "depends_on": ["b"]},
                {"name": "b", "action": "a", "depends_on": ["a"]},
            ],
        })
        assert resp.status_code == 422
        assert resp.json()["reason"] == "cyclic_dependency"

    def test_duplicate_run_submission_is_idempotent(self, db_path):
        started: list[str] = []

        async def slow(params):
            started.append("x")
            await asyncio.sleep(0.05)

        handlers.register("slow", slow)
        app = create_app(db_path)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w", "tasks": [{"name": "t", "action": "slow"}],
            })
            r1 = client.post("/workflows/w/runs", json={"idempotency_key": "same"})
            r2 = client.post("/workflows/w/runs", json={"idempotency_key": "same"})
            assert r1.status_code == 201 and r2.status_code == 201
            assert r1.json()["created"] is True
            assert r2.json()["created"] is False
            assert r1.json()["run_id"] == r2.json()["run_id"]
            run_id = r1.json()["run_id"]
            asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
        # 同一次运行重复提交不会引发第二次并行执行
        assert started == ["x"]
