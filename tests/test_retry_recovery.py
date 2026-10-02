"""业务：重试策略与失败恢复。"""

import threading
import time

import pytest

from app.engine import Orchestrator
from app.errors import FatalTaskError, RetryableTaskError, TaskNotReadyError
from app.handlers import HandlerRegistry
from app.models import (
    FailureKind,
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowDef,
    WorkflowStatus,
)
from tests.conftest import make_config


class CountingHandler:
    def __init__(self, fail_times: int, fatal: bool = False, message: str = "boom"):
        self.calls = 0
        self.fail_times = fail_times
        self.fatal = fatal
        self.message = message

    def __call__(self, payload):
        self.calls += 1
        if self.calls <= self.fail_times:
            if self.fatal:
                raise FatalTaskError(self.message)
            raise RetryableTaskError(f"{self.message} #{self.calls}")
        return {"attempt": self.calls}


def _engine(store, registry, **cfg):
    return Orchestrator(store, registry, make_config(**cfg))


def _registry() -> HandlerRegistry:
    return HandlerRegistry()


def test_retryable_failure_succeeds_within_budget(store):
    """前两次可重试失败、第三次成功：attempt=3，工作流成功。"""
    reg = _registry()
    h = CountingHandler(fail_times=2)
    reg.register("flaky", h)
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=5),
            tasks=(TaskDef("t", "flaky"),),
        ),
    )
    status = engine.run_task("r", "t")
    assert status is TaskStatus.SUCCEEDED
    task = engine.get_task("r", "t")
    assert task.attempt == 3
    assert task.last_error is None
    assert engine.get_workflow_status("r") is WorkflowStatus.SUCCEEDED


def test_fatal_failure_never_retried(store):
    """不可重试失败立即终止，即便预算充足也只执行一次。"""
    reg = _registry()
    reg.register("boom", CountingHandler(fail_times=99, fatal=True))
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=5),
            tasks=(TaskDef("t", "boom"),),
        ),
    )
    status = engine.run_task("r", "t")
    task = engine.get_task("r", "t")
    assert status is TaskStatus.FAILED
    assert task.failure_kind is FailureKind.FATAL
    assert task.attempt == 1


def test_retry_budget_exhausted_marks_failed(store):
    """可重试失败次数用尽：任务标记失败，failure_kind=retryable 且错误信息说明用尽。"""
    reg = _registry()
    reg.register("always", CountingHandler(fail_times=99))
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=2),
            tasks=(TaskDef("t", "always"),),
        ),
    )
    status = engine.run_task("r", "t")
    task = engine.get_task("r", "t")
    assert status is TaskStatus.FAILED
    assert task.attempt == 2
    assert task.failure_kind is FailureKind.RETRYABLE
    assert "exhausted" in (task.last_error or "")
    assert engine.get_workflow_status("r") is WorkflowStatus.FAILED


def test_backoff_delays_next_attempt(store):
    """退避时间必须被遵守：max_attempts=3、backoff=0.2 时总耗时 >= 0.4s。"""
    reg = _registry()
    reg.register("always", CountingHandler(fail_times=99))
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=3, backoff_seconds=0.2),
            tasks=(TaskDef("t", "always"),),
        ),
    )
    start = time.time()
    engine.run_task("r", "t")
    elapsed = time.time() - start
    assert elapsed >= 0.4
    assert engine.get_task("r", "t").status is TaskStatus.FAILED


def test_task_level_retry_overrides_workflow_default(store):
    """任务级 retry 覆盖工作流默认策略。"""
    reg = _registry()
    h = CountingHandler(fail_times=2)
    reg.register("flaky", h)
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=1),  # 默认不允许重试
            tasks=(
                TaskDef("t", "flaky", retry=RetryPolicy(max_attempts=3), retry_set=True),
            ),
        ),
    )
    status = engine.run_task("r", "t")
    assert status is TaskStatus.SUCCEEDED
    assert h.calls == 3


def test_unknown_exception_defaults_to_retryable(store):
    """处理器抛出未分类异常（如 ValueError）默认按可重试处理。"""
    reg = _registry()

    def raise_value_error(_p):
        raise ValueError("unexpected but transient")

    reg.register("weird", raise_value_error)
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=2, backoff_seconds=0),
            tasks=(TaskDef("t", "weird"),),
        ),
    )
    engine.run_task("r", "t")
    task = engine.get_task("r", "t")
    assert task.status is TaskStatus.FAILED
    assert task.failure_kind is FailureKind.RETRYABLE
    assert "ValueError" in (task.last_error or "")


def test_failed_task_cascades_to_downstream(store):
    """一个任务不可重试失败，直接/间接下游全部标记 failed，工作流失败。"""
    reg = _registry()
    reg.register("boom", CountingHandler(fail_times=1, fatal=True))
    reg.register("noop", lambda _p: None)
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=1),
            tasks=(
                TaskDef("a", "boom"),
                TaskDef("b", "noop", dependencies=("a",)),
                TaskDef("c", "noop", dependencies=("b",)),
            ),
        ),
    )
    engine.run_task("r", "a")
    assert engine.get_task("r", "b").status is TaskStatus.FAILED
    assert engine.get_task("r", "c").status is TaskStatus.FAILED
    assert engine.get_workflow_status("r") is WorkflowStatus.FAILED


def test_retry_wait_task_cannot_be_run_early(store):
    """退避中的任务提前执行要被拒绝，原因可区分。"""
    reg = _registry()
    reg.register("always", CountingHandler(fail_times=99))
    engine = _engine(store, reg)
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=3, backoff_seconds=10),
            tasks=(TaskDef("t", "always"),),
        ),
    )
    # 直接构造一次失败，进入 retry_wait
    task = store.claim_task("r", "t", engine.owner, 30, time.time())
    engine._execute(task)
    assert engine.get_task("r", "t").status is TaskStatus.RETRY_WAIT
    with pytest.raises(TaskNotReadyError) as ei:
        engine.run_task("r", "t")
    assert ei.value.reason == "task_retry_waiting"


def test_auto_scheduler_retries_and_then_succeeds(store):
    """自动调度下：可重试失败会按退避重试并最终成功。"""
    reg = _registry()
    h = CountingHandler(fail_times=2)
    reg.register("flaky", h)
    engine = _engine(store, reg, poll_interval=0.01)
    engine.start()
    engine.submit(
        "r",
        WorkflowDef(
            retry=RetryPolicy(max_attempts=4, backoff_seconds=0.05),
            tasks=(TaskDef("t", "flaky"),),
        ),
    )
    deadline = time.time() + 8
    while time.time() < deadline and engine.get_workflow_status("r") is not WorkflowStatus.SUCCEEDED:
        time.sleep(0.01)
    engine.stop()
    assert engine.get_workflow_status("r") is WorkflowStatus.SUCCEEDED
    assert engine.get_task("r", "t").attempt == 3
    assert h.calls == 3
