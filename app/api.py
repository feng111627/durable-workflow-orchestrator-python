"""FastAPI 路由层：工作流提交/查询、任务状态查询与手动执行。

所有非法操作返回 HTTP 409，响应体 {"error": ..., "reason": <可区分原因码>}。
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    ConcurrencyLimitReached,
    CycleDetectedError,
    DuplicateRunError,
    InvalidTaskError,
    OrchestratorError,
    TaskNotReadyError,
)
from .engine import Orchestrator
from .models import (
    FailureKind,
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowDef,
    WorkflowStatus,
)


class RetryPolicyIn(BaseModel):
    max_attempts: int = Field(default=3, ge=1)
    backoff_seconds: float = Field(default=0.0, ge=0.0)


class TaskIn(BaseModel):
    id: str
    handler: str = "noop"
    dependencies: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    retry: RetryPolicyIn | None = None


class WorkflowIn(BaseModel):
    tasks: list[TaskIn] = Field(min_length=1)
    retry: RetryPolicyIn = Field(default_factory=RetryPolicyIn)


class TaskOut(BaseModel):
    task_id: str
    status: TaskStatus
    attempt: int
    max_attempts: int
    dependencies: list[str]
    last_error: str | None = None
    failure_kind: FailureKind | None = None
    result: Any | None = None


class WorkflowOut(BaseModel):
    run_id: str
    status: WorkflowStatus
    tasks: list[TaskOut]


def _to_definition(body: WorkflowIn) -> WorkflowDef:
    return WorkflowDef(
        retry=RetryPolicy(
            max_attempts=body.retry.max_attempts,
            backoff_seconds=body.retry.backoff_seconds,
        ),
        tasks=tuple(
            TaskDef(
                id=t.id,
                handler=t.handler,
                dependencies=tuple(t.dependencies),
                payload=t.payload,
                retry=(
                    RetryPolicy(
                        max_attempts=t.retry.max_attempts,
                        backoff_seconds=t.retry.backoff_seconds,
                    )
                    if t.retry is not None
                    else RetryPolicy()
                ),
                retry_set=t.retry is not None,
            )
            for t in body.tasks
        ),
    )


def create_app(orchestrator: Orchestrator) -> FastAPI:
    app = FastAPI(title="Durable Workflow Orchestrator", version="0.1.0")

    @app.exception_handler(OrchestratorError)
    async def _on_orchestrator_error(_request: Any, exc: OrchestratorError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": str(exc), "reason": exc.reason},
        )

    @app.exception_handler(ConcurrencyLimitReached)
    async def _on_concurrency(_request: Any, exc: ConcurrencyLimitReached) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": str(exc), "reason": exc.reason},
        )

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": "durable-workflow-orchestrator", "node": orchestrator.owner}

    @app.post("/workflows/{run_id}", response_model=WorkflowOut, status_code=201)
    def submit_workflow(run_id: str, body: WorkflowIn) -> WorkflowOut:
        definition = _to_definition(body)
        try:
            orchestrator.submit(run_id, definition)
        except (CycleDetectedError, DuplicateRunError, InvalidTaskError):
            # 让全局异常处理器统一转 409
            raise
        return _load_workflow(run_id, orchestrator)

    @app.get("/workflows/{run_id}", response_model=WorkflowOut)
    def get_workflow(run_id: str) -> WorkflowOut:
        if orchestrator.store.get_workflow(run_id) is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"workflow not found: {run_id}", "reason": "not_found"},
            )  # type: ignore[return-value]
        return _load_workflow(run_id, orchestrator)

    @app.get("/workflows/{run_id}/tasks/{task_id}", response_model=TaskOut)
    def get_task(run_id: str, task_id: str) -> Any:
        task = orchestrator.store.get_task(run_id, task_id)
        if task is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"task not found: {task_id}", "reason": "not_found"},
            )
        return TaskOut(
            task_id=task.task_id,
            status=task.status,
            attempt=task.attempt,
            max_attempts=task.max_attempts,
            dependencies=list(task.dependencies),
            last_error=task.last_error,
            failure_kind=task.failure_kind,
            result=task.result,
        )

    @app.post("/workflows/{run_id}/tasks/{task_id}/run", response_model=TaskOut)
    def run_task(run_id: str, task_id: str) -> Any:
        try:
            orchestrator.run_task(run_id, task_id)
        except (TaskNotReadyError, ConcurrencyLimitReached):
            raise
        task = orchestrator.get_task(run_id, task_id)
        return TaskOut(
            task_id=task.task_id,
            status=task.status,
            attempt=task.attempt,
            max_attempts=task.max_attempts,
            dependencies=list(task.dependencies),
            last_error=task.last_error,
            failure_kind=task.failure_kind,
            result=task.result,
        )

    return app


def _load_workflow(run_id: str, orchestrator: Orchestrator) -> WorkflowOut:
    tasks = orchestrator.store.list_tasks(run_id)
    return WorkflowOut(
        run_id=run_id,
        status=orchestrator.get_workflow_status(run_id),
        tasks=[
            TaskOut(
                task_id=t.task_id,
                status=t.status,
                attempt=t.attempt,
                max_attempts=t.max_attempts,
                dependencies=list(t.dependencies),
                last_error=t.last_error,
                failure_kind=t.failure_kind,
                result=t.result,
            )
            for t in tasks
        ],
    )
