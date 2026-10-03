"""业务异常定义：每种非法情况都有可区分的原因码。"""


class OrchestratorError(Exception):
    reason = "internal_error"


class WorkflowNotFound(OrchestratorError):
    reason = "workflow_not_found"


class RunNotFound(OrchestratorError):
    reason = "run_not_found"


class CyclicDependencyError(OrchestratorError):
    reason = "cyclic_dependency"


class UnknownTaskDependency(OrchestratorError):
    reason = "unknown_task_dependency"


class DuplicateTaskName(OrchestratorError):
    reason = "duplicate_task_name"


class UnknownAction(OrchestratorError):
    reason = "unknown_action"


class InvalidStateTransition(OrchestratorError):
    """非法状态变更，例如重跑已成功的任务。"""

    reason = "invalid_state_transition"

    def __init__(self, message: str, *, current: str, target: str):
        super().__init__(message)
        self.current = current
        self.target = target


class TaskAlreadySucceeded(InvalidStateTransition):
    """对已成功任务执行了非法操作（如重跑）。"""

    reason = "task_already_succeeded"


class TaskCurrentlyRunning(InvalidStateTransition):
    """任务正在被执行者处理中，操作被拒绝。"""

    reason = "task_currently_running"


class TaskBlockedByUpstream(InvalidStateTransition):
    """任务因上游失败被阻塞，需先恢复失败的上游任务。"""

    reason = "task_blocked_by_upstream"


class RetryableTaskError(Exception):
    """任务执行时可重试的失败。"""


class FatalTaskError(Exception):
    """任务执行时不可重试的失败。"""
