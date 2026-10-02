"""任务动作（action）注册表：把任务定义里的 action 名称映射到可执行函数。"""
from __future__ import annotations

from typing import Any, Awaitable, Callable

Handler = Callable[[dict[str, Any]], Any | Awaitable[Any]]

_REGISTRY: dict[str, Handler] = {}


def register(action: str, handler: Handler) -> None:
    _REGISTRY[action] = handler


def get(action: str) -> Handler | None:
    return _REGISTRY.get(action)


def clear() -> None:
    _REGISTRY.clear()
