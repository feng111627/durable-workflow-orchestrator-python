"""测试公共装置：临时数据库、动作注册表清理、按业务分组的测试报告。"""
from __future__ import annotations

import os
import tempfile

import pytest

from app import handlers
from app.store import Store

# 业务域 -> 展示名，用于测试报告里按业务归类用例
BUSINESS_AREAS = {
    "TestDependencyOrdering": "依赖顺序与定义校验",
    "TestRetryAndFailure": "重试与失败处理",
    "TestRecoveryAfterRestart": "服务重启后的续跑恢复",
    "TestConcurrencyAndIdempotency": "并发控制与幂等",
}


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture()
def store(db_path):
    s = Store(db_path)
    yield s
    s.close()


@pytest.fixture(autouse=True)
def clean_handlers():
    handlers.clear()
    yield
    handlers.clear()


def _business_of(item) -> str:
    for cls in BUSINESS_AREAS:
        if cls in item.nodeid:
            return BUSINESS_AREAS[cls]
    return "其他"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        item.config._business_results.setdefault(_business_of(item), []).append(
            (item.name, "通过" if report.passed else ("失败" if report.failed else "跳过"))
        )


def pytest_configure(config):
    config._business_results = {}


def pytest_terminal_summary(terminalreporter):
    terminalreporter.write_sep("=", "业务覆盖汇总")
    for business, cases in terminalreporter.config._business_results.items():
        terminalreporter.write_line(f"[业务] {business}（{len(cases)} 个用例）")
        for name, result in cases:
            terminalreporter.write_line(f"    - {name}: {result}")
