"""pytest 公共夹具与分组日志。

运行 ``uv run pytest -s`` 时，每个测试模块作为一个"业务"分组打印：
    ▶ 业务：xxx
      ✓ 用例名
      ✗ 用例名（失败原因）
    汇总：通过 X / 失败 Y / 共 Z
"""

from __future__ import annotations

import os
import tempfile
import time
from collections.abc import Iterator

import pytest

from app.engine import EngineConfig, Orchestrator
from app.handlers import HandlerRegistry
from app.store import Store


class BusinessReporter:
    def __init__(self) -> None:
        self.results: list[tuple[str, str, bool, str]] = []
        self.module_title: str = ""

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item, nextitem):
        outcome = yield
        # 结果在 pytest_runtest_logreport 中记录
        return outcome


_reporter = BusinessReporter()


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(_reporter, "business-reporter")


def _module_title(module: str | None) -> str:
    if not module:
        return ""
    name = module.replace("tests.", "").replace("test_", "")
    titles = {
        "dependency": "依赖顺序与拓扑校验",
        "retry_recovery": "重试策略与失败恢复",
        "restart": "重启后续跑",
        "concurrency_idempotency": "并发控制与幂等",
        "api": "HTTP API 行为",
        "smoke": "冒烟",
    }
    return titles.get(name, name)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        title = _module_title(item.module.__name__)
        case_id = item.name
        ok = report.passed
        reason = ""
        if not ok:
            reason = str(call.excinfo.value) if call.excinfo else "error"
        _reporter.results.append((title, case_id, ok, reason))
        mark = "✓" if ok else "✗"
        line = f"    {mark} {case_id}"
        if not ok:
            line += f"  -> {reason}"
        print(line, flush=True)


def pytest_runtest_logstart(nodeid, location):
    title = _module_title(location[0].replace("/", ".").replace(".py", "").replace("tests.", ""))
    if title and title != _reporter.module_title:
        _reporter.module_title = title
        print(f"\n▶ 业务：{title}", flush=True)


def pytest_sessionfinish(session, exitstatus):
    print("\n" + "=" * 68)
    print("业务覆盖汇总")
    print("=" * 68)
    by_biz: dict[str, list[tuple[str, bool, str]]] = {}
    for title, case, ok, reason in _reporter.results:
        by_biz.setdefault(title, []).append((case, ok, reason))
    total = passed = 0
    for title, cases in by_biz.items():
        p = sum(1 for _, ok, _ in cases if ok)
        total += len(cases)
        passed += p
        print(f"【{title}】用例 {len(cases)} 个，通过 {p}，失败 {len(cases) - p}")
        for case, ok, reason in cases:
            tag = "PASS" if ok else "FAIL"
            extra = f" ({reason})" if not ok else ""
            print(f"    [{tag}] {case}{extra}")
    print("-" * 68)
    print(f"合计：{total} 个用例，通过 {passed}，失败 {total - passed}")
    print("=" * 68)


# --------------------------------------------------------------------- fixtures
@pytest.fixture
def db_path() -> Iterator[str]:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)  # Store 自己建库
    yield path
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(path + suffix)
        except FileNotFoundError:
            pass


@pytest.fixture
def store(db_path: str) -> Store:
    s = Store(db_path)
    yield s
    s.close()


def make_config(**overrides) -> EngineConfig:
    base = dict(
        max_concurrency=4,
        lease_seconds=30.0,
        heartbeat_interval=5.0,
        poll_interval=0.01,
    )
    base.update(overrides)
    return EngineConfig(**base)


@pytest.fixture
def started_engine(store, request) -> Iterator[Orchestrator]:
    """构造带可定制处理器的引擎并启动，用例结束自动停止。"""
    reg = HandlerRegistry()
    reg.register("noop", lambda _p: None)
    reg.register("echo", lambda p: dict(p))
    reg.register("slow", make_slow())
    cfg_kwargs = getattr(request, "param", {}) or {}
    engine = Orchestrator(store, reg, make_config(**cfg_kwargs))
    engine.start()
    yield engine
    engine.stop(timeout=3)


def make_slow():
    import threading

    gate = threading.Event()

    def slow(p):
        gate.wait(3.0)
        return p

    slow.gate = gate  # type: ignore[attr-defined]
    return slow


@pytest.fixture
def wait_for():
    def _wait(predicate, timeout: float = 8.0, interval: float = 0.01) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(interval)
        return predicate()

    return _wait
