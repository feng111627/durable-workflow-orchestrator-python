"""测试公共装置：临时数据库、动作注册表清理、按业务分组的测试报告。"""
from __future__ import annotations

import os
import tempfile

import pytest

from app import handlers
from app.store import Store

# 业务域 -> 展示名，用于测试报告里按业务归类用例。
# 顺序即报告中的展示顺序；按文件/类名子串匹配。
BUSINESS_AREAS = [
    ("TestDependencyOrdering", "依赖顺序与定义校验"),
    ("TestRetryAndFailure", "重试与失败处理"),
    ("TestRecoveryAfterRestart", "服务重启后的续跑恢复"),
    ("TestRecoveryDistinguishesAliveFromDead", "重启恢复：区分活执行者与失联"),
    ("TestConcurrencyAndIdempotency", "并发控制与幂等"),
    ("test_multi_executor", "多执行者安全：租约/认领/接管/迟到写回"),
    ("TestClaimAndLease", "多执行者安全：租约/认领/接管/迟到写回"),
    ("TestFencingLateWrites", "多执行者安全：租约/认领/接管/迟到写回"),
    ("TestConcurrentExecutors", "多执行者安全：租约/认领/接管/迟到写回"),
    ("test_engine_takeover", "多执行者安全：保活/接管/迟到结果忽略（引擎级）"),
    ("TestHeartbeatKeepsLongTaskAlive", "多执行者安全：保活/接管/迟到结果忽略（引擎级）"),
    ("TestTakeoverAfterDeath", "多执行者安全：保活/接管/迟到结果忽略（引擎级）"),
    ("TestLateResultIgnoredByEngine", "多执行者安全：保活/接管/迟到结果忽略（引擎级）"),
    ("test_multi_process", "多执行者安全：真实多进程接管"),
    ("TestRealMultiProcessTakeover", "多执行者安全：真实多进程接管"),
    ("test_failure_cascade", "失败影响面：BLOCKED 级联与恢复续跑"),
    ("TestBlockedCascade", "失败影响面：BLOCKED 级联与恢复续跑"),
    ("TestRecoveryRerun", "失败影响面：BLOCKED 级联与恢复续跑"),
    ("TestIllegalOperationReasons", "失败影响面：非法操作原因区分"),
]


def _business_of(item) -> str:
    nodeid = item.nodeid
    for key, title in BUSINESS_AREAS:
        if key in nodeid:
            return title
    return "其他"


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
    results = terminalreporter.config._business_results
    # 按 BUSINESS_AREAS 定义顺序输出，保证每次报告的业务分组顺序稳定
    ordered_titles: list[str] = []
    for _, title in BUSINESS_AREAS:
        if title in results and title not in ordered_titles:
            ordered_titles.append(title)
    for title in ordered_titles:
        cases = results[title]
        terminalreporter.write_line(f"[业务] {title}（{len(cases)} 个用例）")
        for name, result in cases:
            terminalreporter.write_line(f"    - {name}: {result}")
    if "其他" in results:
        cases = results["其他"]
        terminalreporter.write_line(f"[业务] 其他（{len(cases)} 个用例）")
        for name, result in cases:
            terminalreporter.write_line(f"    - {name}: {result}")
