"""FastAPI 路由：工作流定义、运行提交、状态查询、任务重跑。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import handlers
from .engine import Orchestrator
from .errors import (
    CyclicDependencyError,
    DuplicateTaskName,
    InvalidStateTransition,
    OrchestratorError,
    RunNotFound,
    UnknownAction,
    UnknownTaskDependency,
    WorkflowNotFound,
)
from .models import RunInfo, WorkflowSpec
from .store import Store

_STATUS_CODES = {
    "workflow_not_found": 404,
    "run_not_found": 404,
    "invalid_state_transition": 409,
    "stale_executor_write": 409,
    "cyclic_dependency": 422,
    "unknown_task_dependency": 422,
    "duplicate_task_name": 422,
    "unknown_action": 422,
}


def validate_workflow(spec: WorkflowSpec) -> None:
    """定义时校验：任务名唯一、依赖存在、action 已注册、无循环依赖。"""
    names = [t.name for t in spec.tasks]
    if len(names) != len(set(names)):
        raise DuplicateTaskName("duplicate task names in workflow")
    known = set(names)
    for t in spec.tasks:
        for dep in t.depends_on:
            if dep not in known:
                raise UnknownTaskDependency(
                    f"task '{t.name}' depends on unknown task '{dep}'"
                )
    # Kahn 拓扑排序检测循环依赖
    indegree = {n: 0 for n in names}
    for t in spec.tasks:
        for _ in t.depends_on:
            indegree[t.name] += 1
    queue = [n for n, d in indegree.items() if d == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for t in spec.tasks:
            if node in t.depends_on:
                indegree[t.name] -= 1
                if indegree[t.name] == 0:
                    queue.append(t.name)
    if visited != len(names):
        raise CyclicDependencyError("workflow contains a dependency cycle")
    for t in spec.tasks:
        if handlers.get(t.action) is None:
            raise UnknownAction(f"task '{t.name}' uses unregistered action '{t.action}'")


class CreateRunRequest(BaseModel):
    idempotency_key: str


class CreateRunResponse(BaseModel):
    run_id: str
    created: bool


def create_app(
    db_path: str = "orchestrator.db",
    max_concurrent_tasks: int = 4,
    *,
    heartbeat_interval_seconds: float = 1.0,
    heartbeat_timeout_seconds: float = 30.0,
    maintenance_interval_seconds: float = 1.0,
    executor_id: str | None = None,
) -> FastAPI:
    store = Store(db_path)
    orchestrator = Orchestrator(
        store,
        max_concurrent_tasks=max_concurrent_tasks,
        executor_id=executor_id,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        maintenance_interval_seconds=maintenance_interval_seconds,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 启动执行者：登记心跳；只接管“确实失联”的任务（心跳超时的 RUNNING），
        # 仍被其他活着的实例持有的任务保持原样，已成功的任务任何情况下不重跑
        await orchestrator.start()
        recovered = await orchestrator.recover_unfinished()
        if recovered:
            app.state.recovered_runs = recovered
        yield
        await orchestrator.shutdown()
        store.close()

    app = FastAPI(title="Durable Workflow Orchestrator", lifespan=lifespan)
    app.state.store = store
    app.state.orchestrator = orchestrator

    @app.exception_handler(OrchestratorError)
    async def orchestrator_error_handler(request: Request, exc: OrchestratorError):
        body: dict = {"reason": exc.reason, "detail": str(exc)}
        if isinstance(exc, InvalidStateTransition):
            body["current"] = exc.current
            body["target"] = exc.target
            # 细粒度可区分原因码：task_already_succeeded / task_running /
            # task_blocked / task_pending / task_missing / task_not_runnable
            if exc.code is not None:
                body["code"] = exc.code
        return JSONResponse(
            status_code=_STATUS_CODES.get(exc.reason, 500), content=body
        )

    @app.post("/workflows", status_code=201)
    async def define_workflow(spec: WorkflowSpec) -> WorkflowSpec:
        validate_workflow(spec)
        store.save_workflow(spec)
        return spec

    @app.post("/workflows/{name}/runs", status_code=201)
    async def create_run(name: str, req: CreateRunRequest) -> CreateRunResponse:
        if store.get_workflow(name) is None:
            raise WorkflowNotFound(f"workflow '{name}' not defined")
        run_id, created = store.create_run_idempotent(name, req.idempotency_key)
        if created:
            await orchestrator.start_run(run_id)
        # created=False：同一幂等键重复提交，返回已有运行，不产生并行执行
        return CreateRunResponse(run_id=run_id, created=created)

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> RunInfo:
        info = store.get_run(run_id)
        if info is None:
            raise RunNotFound(f"run {run_id} not found")
        return info

    @app.post("/runs/{run_id}/tasks/{task_name}/rerun")
    async def rerun_task(run_id: str, task_name: str) -> RunInfo:
        """手动重跑一个 FAILED 的任务并自动恢复其失败影响面。

        只允许 FAILED -> PENDING；对 SUCCEEDED/RUNNING/BLOCKED/PENDING 等状态
        重跑会被拒绝，返回 409，并在 reason=invalid_state_transition 之下
        额外携带可区分的 code（task_already_succeeded/task_running/
        task_blocked/task_pending）。原先被该失败挡住的 BLOCKED 下游会在依赖
        重新满足后自动解阻并继续跑完，无需重新提交工作流。
        """
        info = await orchestrator.rerun_task(run_id, task_name)
        return info

    return app
