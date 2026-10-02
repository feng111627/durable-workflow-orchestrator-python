"""任务处理器注册表：任务类型名 -> 可调用逻辑。

处理器是 ``Callable[[dict], object]``，接收任务 payload，返回结果（可 JSON 序列化）。
失败时抛出 :class:`app.errors.RetryableTaskError` 或 :class:`app.errors.FatalTaskError`；
抛出其它异常按可重试处理。
"""

from __future__ import annotations

from collections.abc import Callable

from .errors import InvalidTaskError

TaskHandler = Callable[[dict], object]


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, name: str, fn: TaskHandler) -> None:
        if not name:
            raise ValueError("handler name must not be empty")
        self._handlers[name] = fn

    def get(self, name: str) -> TaskHandler:
        try:
            return self._handlers[name]
        except KeyError:
            raise InvalidTaskError(f"unknown handler: {name}") from None

    def names(self) -> list[str]:
        return sorted(self._handlers)
