"""数据模型与状态枚举定义。"""
from __future__ import annotations

import enum
from typing import Any

from pydantic import BaseModel, Field


class RunStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    # 因（直接或间接）上游任务失败而被阻塞，区别于普通的 PENDING 等待
    BLOCKED = "BLOCKED"


class TaskSpec(BaseModel):
    """工作流中单个任务的定义。"""

    name: str
    action: str
    depends_on: list[str] = Field(default_factory=list)
    max_retries: int = 0
    retry_delay_seconds: float = 0.0
    params: dict[str, Any] = Field(default_factory=dict)


class WorkflowSpec(BaseModel):
    """一个工作流定义：名称 + 一组有依赖关系的任务。"""

    name: str
    tasks: list[TaskSpec]


class TaskRunInfo(BaseModel):
    task_name: str
    status: TaskStatus
    attempts: int = 0
    error: str | None = None
    result: Any = None
    # 当前认领该任务的执行者 ID（未被认领时为 None）
    executor_id: str | None = None
    # 认领纪元（fencing token）：每次被（重新）认领单调递增，
    # 旧执行者的迟到写回会因 epoch 不匹配被忽略
    claim_epoch: int = 0


class RunInfo(BaseModel):
    run_id: str
    workflow_name: str
    idempotency_key: str
    status: RunStatus
    tasks: list[TaskRunInfo] = Field(default_factory=list)
