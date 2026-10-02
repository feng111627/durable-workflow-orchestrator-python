"""领域错误：非法状态变更等场景抛出，并携带可区分的原因码。"""

from __future__ import annotations

from .models import FailureKind


class OrchestratorError(Exception):
    reason = "orchestrator_error"


class DuplicateRunError(OrchestratorError):
    reason = "duplicate_run"


class CycleDetectedError(OrchestratorError):
    reason = "cycle_detected"


class InvalidTaskError(OrchestratorError):
    reason = "invalid_task"


class TaskNotReadyError(OrchestratorError):
    """任务当前不可执行。reason 给出可区分的具体原因。"""

    def __init__(self, reason: str = "task_not_ready", message: str = "") -> None:
        super().__init__(message or reason)
        self.reason = reason


class TaskAlreadySucceededError(TaskNotReadyError):
    def __init__(self, message: str = "") -> None:
        super().__init__("task_already_succeeded", message or "task already succeeded")


class TaskAlreadyRunningError(TaskNotReadyError):
    def __init__(self, message: str = "") -> None:
        super().__init__("task_already_running", message or "task is already running")


class TaskNotRunningError(OrchestratorError):
    reason = "task_not_running"


class LeaseMismatchError(OrchestratorError):
    reason = "lease_mismatch"


class ConcurrencyLimitReached(Exception):
    reason = "concurrency_limit_reached"


class TaskFailure(Exception):
    """任务处理器抛出的失败基类，用于区分可重试与不可重试失败。"""

    kind: FailureKind = FailureKind.RETRYABLE

    def __init__(self, message: str = "") -> None:
        super().__init__(message)
        self.message = message


class RetryableTaskError(TaskFailure):
    """可重试失败：按任务的 RetryPolicy 退避后重试。"""

    kind = FailureKind.RETRYABLE


class FatalTaskError(TaskFailure):
    """不可重试失败：立即将任务（及工作流）标记为失败。"""

    kind = FailureKind.FATAL
