"""业务：依赖顺序与拓扑校验。"""

import threading

import pytest

from app.errors import CycleDetectedError, InvalidTaskError, TaskNotReadyError
from app.models import (
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowDef,
    WorkflowStatus,
)
from app.engine import Orchestrator
from tests.conftest import make_config


def _wf(*tasks: TaskDef) -> WorkflowDef:
    return WorkflowDef(tasks=tasks)


def test_initial_ready_and_pending_states(store):
    """无依赖的任务初始即 ready；有依赖的必须 pending。"""
    engine = Orchestrator(store, _registry(), make_config())
    engine.submit(
        "r",
        _wf(
            TaskDef("a", "noop"),
            TaskDef("b", "noop", dependencies=("a",)),
        ),
    )
    assert store.get_task("r", "a").status is TaskStatus.READY
    assert store.get_task("r", "b").status is TaskStatus.PENDING


def test_manual_execution_respects_dependency_order(store):
    """手动按序执行：前驱没完成时后继不可执行，完成后自动解锁。"""
    order: list[str] = []
    reg = _registry(order)
    engine = Orchestrator(store, reg, make_config())
    engine.submit(
        "r",
        _wf(
            TaskDef("a", "record", payload={"n": "a"}),
            TaskDef("b", "record", dependencies=("a",), payload={"n": "b"}),
            TaskDef("c", "record", dependencies=("b",), payload={"n": "c"}),
        ),
    )
    with pytest.raises(TaskNotReadyError) as ei:
        engine.run_task("r", "c")
    assert ei.value.reason == "task_blocked_by_dependency"
    engine.run_task("r", "a")
    engine.run_task("r", "b")
    engine.run_task("r", "c")
    assert order == ["a", "b", "c"]
    assert engine.get_workflow_status("r") is WorkflowStatus.SUCCEEDED


def test_multi_dependency_waits_for_all_predecessors(store):
    """多前驱任务必须等所有前驱成功才解锁。"""
    engine = Orchestrator(store, _registry(), make_config())
    engine.submit(
        "r",
        _wf(
            TaskDef("x", "noop"),
            TaskDef("y", "noop"),
            TaskDef("z", "noop", dependencies=("x", "y")),
        ),
    )
    engine.run_task("r", "x")
    assert store.get_task("r", "z").status is TaskStatus.PENDING
    engine.run_task("r", "y")
    assert store.get_task("r", "z").status is TaskStatus.READY


def test_auto_scheduler_runs_diamond_in_topological_order(store):
    """菱形 DAG 经自动调度：a 先于 b/c，d 最后执行。"""
    done: list[str] = []
    lock = threading.Lock()
    reg = _registry()

    def record(p):
        with lock:
            done.append(p["n"])

    reg.register("record", record)
    engine = Orchestrator(store, reg, make_config(max_concurrency=4))
    engine.start()
    engine.submit(
        "r",
        _wf(
            TaskDef("a", "record", payload={"n": "a"}),
            TaskDef("b", "record", dependencies=("a",), payload={"n": "b"}),
            TaskDef("c", "record", dependencies=("a",), payload={"n": "c"}),
            TaskDef("d", "record", dependencies=("b", "c"), payload={"n": "d"}),
        ),
    )
    import time

    deadline = time.time() + 8
    while time.time() < deadline and engine.get_workflow_status("r") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    engine.stop()
    assert done.index("a") < done.index("b")
    assert done.index("a") < done.index("c")
    assert done.index("d") > done.index("b")
    assert done.index("d") > done.index("c")
    assert sorted(done) == ["a", "b", "c", "d"]


def test_direct_cycle_is_rejected(store):
    """直接循环依赖必须明确拒绝并给出 cycle_detected 原因。"""
    engine = Orchestrator(store, _registry(), make_config())
    with pytest.raises(CycleDetectedError) as ei:
        engine.submit(
            "r",
            _wf(
                TaskDef("a", "noop", dependencies=("b",)),
                TaskDef("b", "noop", dependencies=("a",)),
            ),
        )
    assert ei.value.reason == "cycle_detected"
    assert store.get_workflow("r") is None  # 拒绝后不留任何运行记录


def test_self_cycle_is_rejected(store):
    """自依赖也算循环。"""
    engine = Orchestrator(store, _registry(), make_config())
    with pytest.raises(CycleDetectedError):
        engine.submit("r", _wf(TaskDef("x", "noop", dependencies=("x",))))


def test_three_node_cycle_is_rejected(store):
    """三节点环 a->b->c->a 必须被检测到。"""
    engine = Orchestrator(store, _registry(), make_config())
    with pytest.raises(CycleDetectedError) as ei:
        engine.submit(
            "r",
            _wf(
                TaskDef("a", "noop", dependencies=("c",)),
                TaskDef("b", "noop", dependencies=("a",)),
                TaskDef("c", "noop", dependencies=("b",)),
            ),
        )
    assert ei.value.reason == "cycle_detected"
    assert "a" in str(ei.value)  # 错误信息里带环路


def test_unknown_dependency_rejected(store):
    """依赖了不存在的任务要拒绝。"""
    engine = Orchestrator(store, _registry(), make_config())
    with pytest.raises(InvalidTaskError) as ei:
        engine.submit("r", _wf(TaskDef("a", "noop", dependencies=("ghost",))))
    assert ei.value.reason == "invalid_task"


def test_duplicate_task_id_rejected(store):
    """同一工作流内任务 id 重复要拒绝。"""
    engine = Orchestrator(store, _registry(), make_config())
    with pytest.raises(InvalidTaskError):
        engine.submit("r", _wf(TaskDef("a", "noop"), TaskDef("a", "noop")))


def test_empty_workflow_rejected(store):
    """空工作流无意义，拒绝。"""
    engine = Orchestrator(store, _registry(), make_config())
    with pytest.raises(InvalidTaskError):
        engine.submit("r", WorkflowDef(tasks=()))


def _registry(order: list[str] | None = None):
    from app.handlers import HandlerRegistry

    reg = HandlerRegistry()
    reg.register("noop", lambda _p: None)
    reg.register("echo", lambda p: dict(p))
    if order is not None:
        reg.register("record", lambda p: order.append(p["n"]) or p["n"])
    else:
        reg.register("record", lambda p: p["n"])
    return reg
