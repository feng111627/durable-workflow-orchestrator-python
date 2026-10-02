"""领域模型：工作流/任务的定义、运行实例与状态枚举。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY_WAIT = "retry_wait"


class WorkflowStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class FailureKind(str, Enum):
    RETRYABLE = "retryable"
    FATAL = "fatal"


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.0


@dataclass(frozen=True)
class TaskDef:
    id: str
    handler: str = "noop"
    dependencies: tuple[str, ...] = ()
    payload: dict = field(default_factory=dict)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    # 内部标记：retry 是否为调用方显式指定（即便值与默认相同也算）
    retry_set: bool = False


@dataclass(frozen=True)
class WorkflowDef:
    tasks: tuple[TaskDef, ...]
    # 工作流级别的默认重试策略；任务自身的 retry 会覆盖它
    retry: RetryPolicy = field(default_factory=RetryPolicy)


@dataclass
class WorkflowRecord:
    run_id: str
    status: WorkflowStatus
    definition: dict


@dataclass
class TaskRecord:
    run_id: str
    task_id: str
    status: TaskStatus
    dependencies: tuple[str, ...]
    payload: dict
    attempt: int = 1
    max_attempts: int = 3
    backoff_seconds: float = 0.0
    not_before: float = 0.0
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    last_error: str | None = None
    failure_kind: FailureKind | None = None
    result: object = None
