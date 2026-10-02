"""内置示例动作：让服务开箱即可运行演示工作流。"""
from __future__ import annotations

import asyncio
from typing import Any

from . import handlers
from .errors import FatalTaskError, RetryableTaskError


def echo(params: dict[str, Any]) -> Any:
    """原样返回 params['message']（默认 'hello'）。"""
    return params.get("message", "hello")


async def sleep(params: dict[str, Any]) -> str:
    """睡眠 params['seconds']（默认 1）秒。"""
    await asyncio.sleep(float(params.get("seconds", 1)))
    return "slept"


def fail(params: dict[str, Any]) -> None:
    """按 params['kind']（retryable/fatal，默认 fatal）抛出失败。"""
    if params.get("kind") == "retryable":
        raise RetryableTaskError(params.get("message", "retryable failure"))
    raise FatalTaskError(params.get("message", "fatal failure"))


def register_builtins() -> None:
    handlers.register("echo", echo)
    handlers.register("sleep", sleep)
    handlers.register("fail", fail)
