"""业务：并发控制与幂等。"""

import threading
import time

import pytest

from app.engine import Orchestrator
from app.errors import (
    ConcurrencyLimitReached,
    DuplicateRunError,
    TaskAlreadyRunningError,
    TaskNotReadyError,
)
from app.handlers import HandlerRegistry
from app.models import (
    TaskDef,
    TaskStatus,
    WorkflowDef,
    WorkflowStatus,
)
from tests.conftest import make_config


def _wait_task(engine, run_id, task_id, status, timeout=5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if engine.get_task(run_id, task_id).status is status:
            return True
        time.sleep(0.005)
    return engine.get_task(run_id, task_id).status is status


def test_excess_tasks_queue_instead_of_dropped(store):
    """并发上限=2 时，4 个独立任务中超过上限的排队，最终全部成功。"""
    observed: list[int] = []
    lock = threading.Lock()

    def slow(_p):
        with lock:
            observed.append(1)
            peak = len(observed)
        time.sleep(0.12)
        with lock:
            observed.pop()
        return peak

    reg = HandlerRegistry()
    reg.register("slow", slow)
    engine = Orchestrator(store, reg, make_config(max_concurrency=2))
    engine.start()
    engine.submit(
        "r",
        WorkflowDef(tasks=tuple(
            TaskDef(f"t{i}", "slow") for i in range(4)
        )),
    )
    deadline = time.time() + 8
    while time.time() < deadline and engine.get_workflow_status("r") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    engine.stop()
    statuses = [engine.get_task("r", f"t{i}").status for i in range(4)]
    assert all(s is TaskStatus.SUCCEEDED for s in statuses)


def test_observed_concurrency_never_exceeds_limit(store):
    """峰值并发不超过配置的上限。"""
    peaks: list[int] = []
    current = 0
    lock = threading.Lock()

    def slow(_p):
        nonlocal current
        with lock:
            current += 1
            peaks.append(current)
        time.sleep(0.1)
        with lock:
            current -= 1

    reg = HandlerRegistry()
    reg.register("slow", slow)
    engine = Orchestrator(store, reg, make_config(max_concurrency=3))
    engine.start()
    engine.submit(
        "r",
        WorkflowDef(tasks=tuple(
            TaskDef(f"t{i}", "slow") for i in range(9)
        )),
    )
    deadline = time.time() + 10
    while time.time() < deadline and engine.get_workflow_status("r") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    engine.stop()
    assert max(peaks) <= 3
    assert max(peaks) == 3


def test_duplicate_submission_does_not_start_parallel_run(store):
    """同一 run_id 重复提交被拒绝，且只有一份工作流记录/一组任务。"""
    reg = HandlerRegistry()
    reg.register("noop", lambda _p: None)
    engine = Orchestrator(store, reg, make_config())
    wf = WorkflowDef(tasks=(TaskDef("a", "noop"),))
    engine.submit("dup", wf)
    with pytest.raises(DuplicateRunError) as ei:
        engine.submit("dup", wf)
    assert ei.value.reason == "duplicate_run"
    assert len(store.list_tasks("dup")) == 1
    runs = store.list_workflow_runs()
    assert [r.run_id for r in runs] == ["dup"]


def test_concurrent_duplicate_submission_single_insert(store):
    """多线程同时提交同一 run_id：只有一个成功，绝不产生并行执行。"""
    reg = HandlerRegistry()
    entered = threading.Event()
    release = threading.Event()

    def slow_noop(_p):
        entered.set()
        release.wait(3.0)

    reg.register("slow", slow_noop)
    reg.register("noop", lambda _p: None)
    engine = Orchestrator(store, reg, make_config(max_concurrency=4))
    wf = WorkflowDef(tasks=(TaskDef("a", "slow"),))
    results: list[object] = []

    def submit():
        try:
            engine.submit("race", wf)
            results.append("ok")
        except DuplicateRunError:
            results.append("dup")

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    release.set()
    assert results.count("ok") == 1
    assert results.count("dup") == 7
    assert len(store.list_tasks("race")) == 1


def test_parallel_manual_run_of_same_task_executes_once(store):
    """并发手动触发同一 ready 任务：只有一个线程进入执行，另一个拿到明确原因。"""
    started = threading.Event()
    finish = threading.Event()

    def slow(_p):
        started.set()
        finish.wait(3.0)
        return "done"

    reg = HandlerRegistry()
    reg.register("slow", slow)
    engine = Orchestrator(store, reg, make_config(max_concurrency=4))
    engine.submit("r", WorkflowDef(tasks=(TaskDef("a", "slow"),)))

    errors: list[TaskNotReadyError] = []

    def runner(claim_only: bool):
        try:
            if claim_only:
                engine.store.claim_task(
                    "r", "a", f"t-{threading.get_ident()}",
                    engine.config.lease_seconds, time.time(),
                    max_running=engine.config.max_concurrency,
                )
            else:
                engine.run_task("r", "a")
        except (TaskNotReadyError, ConcurrencyLimitReached) as e:  # type: ignore[attr-defined]
            errors.append(e)

    t_run = threading.Thread(target=runner, args=(False,))
    t_claim = threading.Thread(target=runner, args=(True,))
    t_run.start()
    assert started.wait(3.0)
    t_claim.start()
    t_claim.join()
    finish.set()
    t_run.join()

    assert len(errors) == 1
    # 已在运行的重复执行被拒绝，原因是 task_already_running
    assert getattr(errors[0], "reason", "") == "task_already_running"
    assert engine.get_task("r", "a").status is TaskStatus.SUCCEEDED


def test_rerun_succeeded_task_rejected_with_distinct_reason(store):
    """已成功任务再跑被拒，错误码与"依赖未满足"等原因可区分。"""
    reg = HandlerRegistry()
    reg.register("noop", lambda _p: None)
    engine = Orchestrator(store, reg, make_config())
    engine.submit("r", WorkflowDef(tasks=(
        TaskDef("a", "noop"),
        TaskDef("b", "noop", dependencies=("a",)),
    )))
    engine.run_task("r", "a")
    with pytest.raises(TaskNotReadyError) as ei:
        engine.run_task("r", "a")
    assert ei.value.reason == "task_already_succeeded"
    # 失败任务的重跑原因也不同
    from app.errors import FatalTaskError
    from app.models import RetryPolicy

    reg.register("boom", lambda _p: (_ for _ in ()).throw(FatalTaskError("x")))
    engine.submit(
        "r2",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=1),
            tasks=(TaskDef("z", "boom"),),
        ),
    )
    engine.run_task("r2", "z")
    with pytest.raises(TaskNotReadyError) as ei_failed:
        engine.run_task("r2", "z")
    assert ei_failed.value.reason == "task_already_failed"
    assert ei_failed.value.reason != ei.value.reason


def test_blocked_task_has_distinct_reason_from_succeeded(store):
    """被依赖阻塞与已成功两种拒绝原因必须可区分。"""
    reg = HandlerRegistry()
    reg.register("noop", lambda _p: None)
    engine = Orchestrator(store, reg, make_config())
    engine.submit("r", WorkflowDef(tasks=(
        TaskDef("a", "noop"),
        TaskDef("b", "noop", dependencies=("a",)),
    )))
    with pytest.raises(TaskNotReadyError) as blocked:
        engine.run_task("r", "b")
    assert blocked.value.reason == "task_blocked_by_dependency"
    engine.run_task("r", "a")
    engine.run_task("r", "b")
    with pytest.raises(TaskNotReadyError) as done:
        engine.run_task("r", "b")
    assert done.value.reason == "task_already_succeeded"
    assert blocked.value.reason != done.value.reason


def test_handler_side_effect_runs_once_per_success(store):
    """同一任务成功完成后，任何重复触发都不会再产生副作用。"""
    calls = {"n": 0}

    def handler(_p):
        calls["n"] += 1
        return calls["n"]

    reg = HandlerRegistry()
    reg.register("h", handler)
    engine = Orchestrator(store, reg, make_config())
    engine.submit("r", WorkflowDef(tasks=(TaskDef("a", "h"),)))
    engine.run_task("r", "a")
    assert calls["n"] == 1
    # 连续 5 次非法重跑，副作用次数保持 1
    for _ in range(5):
        with pytest.raises(TaskNotReadyError):
            engine.run_task("r", "a")
    assert calls["n"] == 1


def test_lease_mismatch_blocks_stale_worker_write(store):
    """租约被接管后，旧执行者的完成写回必须被拒（防止覆盖新执行结果）。"""
    reg = HandlerRegistry()
    reg.register("noop", lambda _p: None)
    engine = Orchestrator(store, reg, make_config(lease_seconds=30))
    engine.submit("r", WorkflowDef(tasks=(TaskDef("a", "noop"),)))
    now = time.time()
    t = store.claim_task("r", "a", "old-owner", 30, now)
    assert t.lease_owner == "old-owner"
    # 旧租约过期，被新 owner 接管
    expired = [
        x for x in store.reclaim_expired_leases("new-owner", 30, now + 31)
        if x.run_id == "r" and x.task_id == "a"
    ]
    assert len(expired) == 1
    from app.errors import LeaseMismatchError

    with pytest.raises(LeaseMismatchError):
        store.complete_task("r", "a", "old-owner", {"stale": True}, now + 32)
    current = store.get_task("r", "a")
    assert current.lease_owner == "new-owner"
    assert current.status is TaskStatus.RUNNING
    assert current.result is None
