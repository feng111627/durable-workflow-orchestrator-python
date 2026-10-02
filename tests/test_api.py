"""业务：HTTP API 行为（提交、查询、重复提交、循环拒绝、非法状态、404）。"""

import os
import tempfile
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.engine import EngineConfig, Orchestrator
from app.handlers import HandlerRegistry
from app.store import Store


@pytest.fixture
def client() -> TestClient:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    store = Store(path)
    reg = HandlerRegistry()
    reg.register("noop", lambda _p: None)
    reg.register("echo", lambda p: dict(p))
    reg.register("sleep", lambda p: time.sleep(p.get("seconds", 0.02)) or p)
    engine = Orchestrator(
        store,
        reg,
        EngineConfig(max_concurrency=4, poll_interval=0.01),
    )
    engine.start()
    app = create_app(engine)
    with TestClient(app) as c:
        yield c
    engine.stop()
    store.close()


@pytest.fixture
def gated_client() -> tuple[TestClient, threading.Event]:
    """带阻塞处理器 block 的 client：任务进入执行后会挂起，直到事件置位。"""
    gate = threading.Event()
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    store = Store(path)
    reg = HandlerRegistry()
    reg.register("block", lambda _p: gate.wait(5.0))
    engine = Orchestrator(
        store, reg, EngineConfig(max_concurrency=4, poll_interval=0.01)
    )
    engine.start()
    app = create_app(engine)
    with TestClient(app) as c:
        yield c, gate
    gate.set()
    engine.stop()
    store.close()


DIAMOND = {
    "retry": {"max_attempts": 2, "backoff_seconds": 0},
    "tasks": [
        {"id": "a", "handler": "echo", "payload": {"v": 1}},
        {"id": "b", "handler": "sleep", "dependencies": ["a"], "payload": {"seconds": 0.02}},
        {"id": "c", "handler": "sleep", "dependencies": ["a"], "payload": {"seconds": 0.02}},
        {"id": "d", "handler": "echo", "dependencies": ["b", "c"], "payload": {"v": 2}},
    ],
}


def _wait_done(client, run_id: str, timeout: float = 8.0) -> dict:
    deadline = time.time() + timeout
    body = {}
    while time.time() < deadline:
        body = client.get(f"/workflows/{run_id}").json()
        if body["status"] in ("succeeded", "failed"):
            return body
        time.sleep(0.01)
    return body


def test_submit_returns_201_and_initial_states(gated_client):
    client, gate = gated_client
    body_def = {
        "tasks": [
            {"id": "a", "handler": "block"},
            {"id": "b", "handler": "block", "dependencies": ["a"]},
            {"id": "d", "handler": "block", "dependencies": ["b"]},
        ]
    }
    r = client.post("/workflows/r-init", json=body_def)
    assert r.status_code == 201
    body = r.json()
    assert body["run_id"] == "r-init"
    by_id = {t["task_id"]: t for t in body["tasks"]}
    # a 被阻塞处理器挂住：此刻只能是 ready 或 running
    assert by_id["a"]["status"] in ("ready", "running")
    # 后继还没解锁，必然 pending
    assert by_id["b"]["status"] == "pending"
    assert by_id["d"]["status"] == "pending"
    assert by_id["d"]["dependencies"] == ["b"]
    assert body["status"] == "running"
    gate.set()


def test_workflow_runs_to_success(client):
    client.post("/workflows/r2", json=DIAMOND)
    body = _wait_done(client, "r2")
    assert body["status"] == "succeeded"
    assert all(t["status"] == "succeeded" for t in body["tasks"])


def test_duplicate_submit_returns_409_with_reason(client):
    client.post("/workflows/r3", json=DIAMOND)
    r = client.post("/workflows/r3", json=DIAMOND)
    assert r.status_code == 409
    assert r.json()["reason"] == "duplicate_run"


def test_cycle_returns_409_with_cycle_reason(client):
    cyclic = {"tasks": [
        {"id": "a", "handler": "noop", "dependencies": ["b"]},
        {"id": "b", "handler": "noop", "dependencies": ["a"]},
    ]}
    r = client.post("/workflows/r4", json=cyclic)
    assert r.status_code == 409
    assert r.json()["reason"] == "cycle_detected"
    assert "a" in r.json()["error"]


def test_unknown_dependency_returns_409(client):
    bad = {"tasks": [{"id": "a", "handler": "noop", "dependencies": ["ghost"]}]}
    r = client.post("/workflows/r5", json=bad)
    assert r.status_code == 409
    assert r.json()["reason"] == "invalid_task"


def test_unknown_handler_returns_409(client):
    bad = {"tasks": [{"id": "a", "handler": "not-registered"}]}
    r = client.post("/workflows/r6", json=bad)
    assert r.status_code == 409
    assert r.json()["reason"] == "invalid_task"


def test_rerun_succeeded_task_returns_distinct_409(client):
    body = {"tasks": [{"id": "a", "handler": "noop"}]}
    client.post("/workflows/r7", json=body)
    final = _wait_done(client, "r7")
    assert final["status"] == "succeeded"
    r = client.post("/workflows/r7/tasks/a/run")
    assert r.status_code == 409
    assert r.json()["reason"] == "task_already_succeeded"


def test_blocked_task_run_returns_blocked_reason(gated_client):
    client, gate = gated_client
    body = {"tasks": [
        {"id": "a", "handler": "block"},
        {"id": "b", "handler": "block", "dependencies": ["a"]},
    ]}
    client.post("/workflows/r8", json=body)
    # 等 a 被调度器认领（它会阻塞在 gate 上），b 必然仍被依赖阻塞
    deadline = time.time() + 5
    while time.time() < deadline:
        t = client.get("/workflows/r8/tasks/a").json()
        if t["status"] == "running":
            break
        time.sleep(0.005)
    r = client.post("/workflows/r8/tasks/b/run")
    assert r.status_code == 409
    assert r.json()["reason"] == "task_blocked_by_dependency"
    gate.set()


def test_task_query_and_404(client):
    client.post("/workflows/r9", json={"tasks": [{"id": "a", "handler": "noop"}]})
    assert client.get("/workflows/r9/tasks/a").status_code == 200
    r1 = client.get("/workflows/missing")
    assert r1.status_code == 404 and r1.json()["reason"] == "not_found"
    r2 = client.get("/workflows/r9/tasks/missing")
    assert r2.status_code == 404 and r2.json()["reason"] == "not_found"


def test_validation_error_on_empty_tasks(client):
    r = client.post("/workflows/r10", json={"tasks": []})
    assert r.status_code == 422  # pydantic min_length
