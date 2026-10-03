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

    def __init__(
        self,
        message: str,
        *,
        current: str,
        target: str,
        code: str | None = None,
    ):
        super().__init__(message)
        self.current = current
        self.target = target
        # 更细粒度的可区分原因码（在 reason 之下进一步说明具体是哪种非法操作），例如：
        # task_already_succeeded / task_running / task_blocked /
        # task_pending / task_not_runnable 等；老调用方仍可只看 reason/current
        self.code = code


class StaleExecutorWrite(OrchestratorError):
    """失联执行者在任务被接管后产生的迟到写回，必须被忽略。"""

    reason = "stale_executor_write"

    def __init__(self, message: str, *, current: str, claim_epoch: int):
        super().__init__(message)
        self.current = current
        # 携带当前实际纪元，便于排查
        self.claim_epoch = claim_epoch


class RetryableTaskError(Exception):
    """任务执行时可重试的失败。"""


class FatalTaskError(Exception):
    """任务执行时不可重试的失败。"""
