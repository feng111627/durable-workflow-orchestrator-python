"""业务：服务重启后的续跑。

用"同一文件数据库 + 先后两个 Orchestrator 实例"模拟进程重启：
- 旧实例 stop() 后心跳停止，租约到期即可被新实例接管；
- 已成功任务保持 succeeded，绝不重跑；
- 进行中的任务不会被丢掉（租约接管后继续执行）。
"""

import threading
import time

import pytest

from app.engine import EngineConfig, Orchestrator
from app.handlers import HandlerRegistry
from app.models import (
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowDef,
    WorkflowStatus,
)
from app.store import Store

CRASH_CFG = dict(
    max_concurrency=1,
    lease_seconds=0.3,
    heartbeat_interval=0.08,
    poll_interval=0.01,
)

# 记录每个用例里用于阻塞"旧实例"任务的 gate，teardown 时统一释放，
# 让残留工作线程立即退出（它随后对已关闭库的写回会被引擎安静丢弃）。
_GATES: list[threading.Event] = []


@pytest.fixture(autouse=True)
def _release_gated_workers():
    _GATES.clear()
    yield
    # 顺序很重要：先放行旧工作线程，等它写回（租约已失效，必然被 store 拒绝），
    # 最后才由 db_path fixture 关闭文件；避免对已关闭库操作产生噪声。
    for g in _GATES:
        g.set()
    time.sleep(0.15)


def _registry(side_effects: list[str], gate: threading.Event | None = None) -> HandlerRegistry:
    if gate is not None:
        _GATES.append(gate)
    reg = HandlerRegistry()

    def work(payload):
        side_effects.append(payload["n"])
        if gate is not None and payload.get("gated"):
            gate.wait(60.0)  # 由测试 teardown 释放，模拟崩溃期间任务一直挂着
        return payload["n"]

    reg.register("work", work)
    reg.register("noop", lambda _p: None)
    return reg


def test_succeeded_tasks_are_not_rerun_after_restart(db_path):
    """已成功的任务在重启后绝不执行第二次。"""
    gate = threading.Event()
    store1 = Store(db_path)
    o1 = Orchestrator(store1, _registry([], gate), EngineConfig(**CRASH_CFG), owner="A")
    o1.submit(
        "wf",
        WorkflowDef(tasks=(
            TaskDef("a", "work", payload={"n": "a"}),
            TaskDef("b", "work", dependencies=("a",),
                    payload={"n": "b", "gated": True}),
            TaskDef("c", "work", dependencies=("b",), payload={"n": "c"}),
        )),
    )
    o1.start()

    deadline = time.time() + 5
    while time.time() < deadline:
        if store1.get_task("wf", "b").status is TaskStatus.RUNNING:
            break
        time.sleep(0.005)
    assert store1.get_task("wf", "a").status is TaskStatus.SUCCEEDED
    assert store1.get_task("wf", "b").status is TaskStatus.RUNNING
    o1.stop()
    time.sleep(0.45)  # 租约过期

    store2 = Store(db_path)
    side2: list[str] = []
    o2 = Orchestrator(store2, _registry(side2), EngineConfig(**CRASH_CFG), owner="B")
    recovered = o2.recover()
    assert recovered == 1
    o2.start()
    deadline = time.time() + 8
    while time.time() < deadline and o2.get_workflow_status("wf") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    o2.stop()

    assert o2.get_workflow_status("wf") is WorkflowStatus.SUCCEEDED
    # a 在重启前已成功，新实例绝不能再跑它
    assert side2.count("a") == 0
    for tid in ("a", "b", "c"):
        assert store2.get_task("wf", tid).status is TaskStatus.SUCCEEDED
    store1.close()
    store2.close()


def test_inflight_task_is_reclaimed_after_restart(db_path):
    """进行中（租约过期）的任务不会丢，会被新实例接管继续跑。"""
    side_effects: list[str] = []
    gate = threading.Event()
    store1 = Store(db_path)
    o1 = Orchestrator(store1, _registry(side_effects, gate), EngineConfig(**CRASH_CFG), owner="A")
    o1.submit(
        "wf",
        WorkflowDef(tasks=(
            TaskDef("a", "work", payload={"n": "a"}),
            TaskDef("b", "work", dependencies=("a",), payload={"n": "b", "gated": True}),
        )),
    )
    o1.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if store1.get_task("wf", "b").status is TaskStatus.RUNNING:
            break
        time.sleep(0.005)
    assert store1.get_task("wf", "b").status is TaskStatus.RUNNING
    o1.stop()
    time.sleep(0.45)  # 旧实例租约过期

    store2 = Store(db_path)
    side2: list[str] = []
    gate2 = threading.Event()  # 新实例不再阻塞
    o2 = Orchestrator(store2, _registry(side2), EngineConfig(**CRASH_CFG), owner="B")
    n = o2.recover()
    assert n == 1
    # 接管后 running 任务归新 owner，attempt +1
    task_b = store2.get_task("wf", "b")
    assert task_b.lease_owner == "B"
    assert task_b.attempt == 2
    o2.start()
    deadline = time.time() + 8
    while time.time() < deadline and o2.get_workflow_status("wf") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    o2.stop()
    assert side2.count("b") == 1
    assert store2.get_task("wf", "b").status is TaskStatus.SUCCEEDED
    store1.close()
    store2.close()


def test_pending_workflow_resumes_after_restart(db_path):
    """还没跑完的工作流在重启后自动接着跑直到完成。"""
    gate = threading.Event()
    store1 = Store(db_path)
    o1 = Orchestrator(store1, _registry([], gate), EngineConfig(**CRASH_CFG), owner="A")
    o1.submit(
        "wf",
        WorkflowDef(tasks=(
            TaskDef("t0", "work", payload={"n": "t0"}),
            # t1 进入执行后阻塞，确保停止时 t0 已成功、t1 在跑、t2..t4 未完成
            TaskDef("t1", "work", dependencies=("t0",),
                    payload={"n": "t1", "gated": True}),
            TaskDef("t2", "work", dependencies=("t1",), payload={"n": "t2"}),
            TaskDef("t3", "work", dependencies=("t2",), payload={"n": "t3"}),
            TaskDef("t4", "work", dependencies=("t3",), payload={"n": "t4"}),
        )),
    )
    o1.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if store1.get_task("wf", "t1").status is TaskStatus.RUNNING:
            break
        time.sleep(0.005)
    succeeded_before = [
        f"t{i}" for i in range(5)
        if store1.get_task("wf", f"t{i}").status is TaskStatus.SUCCEEDED
    ]
    assert succeeded_before == ["t0"]
    o1.stop()
    time.sleep(0.45)

    store2 = Store(db_path)
    side2: list[str] = []
    o2 = Orchestrator(store2, _registry(side2), EngineConfig(**CRASH_CFG), owner="B")
    o2.start()
    deadline = time.time() + 10
    while time.time() < deadline and o2.get_workflow_status("wf") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    o2.stop()
    assert o2.get_workflow_status("wf") is WorkflowStatus.SUCCEEDED
    # 重启前成功的任务不再执行
    for tid in succeeded_before:
        assert tid not in side2, (tid, side2)
    store1.close()
    store2.close()
    # 旧实例工作线程由 autouse fixture 在 teardown 释放


def test_retry_wait_survives_restart(db_path):
    """退避中的任务在重启后，退避到期仍会继续重试。"""
    from app.errors import RetryableTaskError

    side_effects: list[str] = []
    gate = threading.Event()
    store1 = Store(db_path)
    reg1 = HandlerRegistry()

    def always_fail(p):
        side_effects.append(p["n"])
        if p.get("gated"):
            gate.wait(5.0)
        raise RetryableTaskError("transient")

    reg1.register("work", always_fail)
    o1 = Orchestrator(
        store1, reg1,
        EngineConfig(max_concurrency=1, lease_seconds=0.3,
                     heartbeat_interval=0.08, poll_interval=0.01),
        owner="A",
    )
    o1.submit(
        "wf",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=3, backoff_seconds=0.05),
            tasks=(TaskDef("a", "work", payload={"n": "a"}),),
        ),
    )
    o1.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if store1.get_task("wf", "a").status is TaskStatus.RETRY_WAIT:
            break
        time.sleep(0.005)
    assert store1.get_task("wf", "a").status is TaskStatus.RETRY_WAIT
    o1.stop()

    # 新实例：让处理器第二次起成功
    store2 = Store(db_path)
    reg2 = HandlerRegistry()
    calls2 = {"n": 0}

    def succeeds_after_one(p):
        calls2["n"] += 1
        if calls2["n"] == 1:
            raise RetryableTaskError("still transient")
        return "ok"

    reg2.register("work", succeeds_after_one)
    o2 = Orchestrator(
        store2, reg2,
        EngineConfig(max_concurrency=1, lease_seconds=0.3,
                     heartbeat_interval=0.08, poll_interval=0.01),
        owner="B",
    )
    o2.start()
    deadline = time.time() + 8
    while time.time() < deadline and o2.get_workflow_status("wf") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    o2.stop()
    assert o2.get_workflow_status("wf") is WorkflowStatus.SUCCEEDED
    assert store2.get_task("wf", "a").status is TaskStatus.SUCCEEDED
    store1.close()
    store2.close()
    # 旧实例工作线程由 autouse fixture 在 teardown 释放


def test_recover_is_idempotent(db_path):
    """重复调用 recover 不会重复接管/重复执行已在本机执行的任务。"""
    side_effects: list[str] = []
    gate = threading.Event()
    store1 = Store(db_path)
    o1 = Orchestrator(store1, _registry(side_effects, gate), EngineConfig(**CRASH_CFG), owner="A")
    o1.submit("wf", WorkflowDef(tasks=(TaskDef("a", "work", payload={"n": "a", "gated": True}),)))
    o1.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if store1.get_task("wf", "a").status is TaskStatus.RUNNING:
            break
        time.sleep(0.005)
    assert store1.get_task("wf", "a").status is TaskStatus.RUNNING
    o1.stop()
    time.sleep(0.45)

    store2 = Store(db_path)
    side2: list[str] = []
    o2 = Orchestrator(store2, _registry(side2), EngineConfig(**CRASH_CFG), owner="B")
    first = o2.recover()
    second = o2.recover()
    assert first == 1
    assert second == 0  # 已经在本机执行，不能再投递一次
    deadline = time.time() + 5
    while time.time() < deadline and side2.count("a") < 1:
        time.sleep(0.01)
    o2.stop()
    assert side2.count("a") == 1
    store1.close()
    store2.close()
    # 旧实例工作线程由 autouse fixture 在 teardown 释放
