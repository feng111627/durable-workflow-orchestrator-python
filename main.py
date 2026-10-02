"""本地运行入口。

启动：
    uv run uvicorn main:app --reload
或：
    uv run python main.py

环境变量：
    WF_DB_PATH          SQLite 数据库路径（默认 ./.workflow.db）
    WF_MAX_CONCURRENCY  全局并发执行任务上限（默认 4）
    WF_LEASE_SECONDS    任务租约时长（默认 30）
"""

from __future__ import annotations

import os

from fastapi import FastAPI

from app.api import create_app
from app.engine import EngineConfig, Orchestrator
from app.errors import FatalTaskError, RetryableTaskError
from app.handlers import HandlerRegistry
from app.store import Store


def build_handlers() -> HandlerRegistry:
    """内置示例处理器，方便本地直接提交工作流体验。"""
    import time

    registry = HandlerRegistry()
    registry.register("noop", lambda _payload: None)
    registry.register("echo", lambda payload: payload)

    def sleep(payload: dict) -> dict:
        seconds = float(payload.get("seconds", 0.1))
        time.sleep(seconds)
        return {"slept": seconds}

    registry.register("sleep", sleep)

    def flaky(payload: dict) -> str:
        # 靠环境变量模拟：FAIL_FLAKY_N 次可重试失败后成功
        state = flaky  # type: ignore[attr-defined]
        n = getattr(state, "_calls", 0) + 1
        state._calls = n  # type: ignore[attr-defined]
        if n <= int(os.environ.get("FAIL_FLAKY_N", "0")):
            raise RetryableTaskError(f"transient failure #{n}")
        return f"ok after {n} attempt(s)"

    registry.register("flaky", flaky)
    registry.register(
        "boom",
        lambda _payload: (_ for _ in ()).throw(
            FatalTaskError("this failure can never be retried")
        ),
    )
    return registry


def build_app() -> FastAPI:
    db_path = os.environ.get("WF_DB_PATH", ".workflow.db")
    cfg = EngineConfig(
        max_concurrency=int(os.environ.get("WF_MAX_CONCURRENCY", "4")),
        lease_seconds=float(os.environ.get("WF_LEASE_SECONDS", "30")),
    )
    store = Store(db_path)
    orchestrator = Orchestrator(store, build_handlers(), cfg)
    orchestrator.start()
    return create_app(orchestrator)


app = build_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
