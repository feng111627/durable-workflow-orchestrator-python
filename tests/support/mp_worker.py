"""多进程测试用的独立执行者驱动脚本（由 pytest 以子进程方式启动）。

用法:
    python -m tests.support.mp_worker <db> <role>

role:
  holder  : 认领任务 t 后长时间运行（持续续心跳），用于模拟"还活着的执行者"；
            收到 SIGTERM 模拟进程崩溃（不做停机清理，不释放租约）。
  watcher : 启动一个完整 Orchestrator（短失联超时），周期性试图接管并最终跑完。

通过约定文件做轻量协调：
  <db>.ready   holder 认领成功后创建
  <db>.done    运行到达终态后创建（内容为 run_id）
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

# 允许直接 `python tests/support/mp_worker.py` 运行
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app import handlers  # noqa: E402
from app.engine import Orchestrator  # noqa: E402
from app.models import RunStatus, TaskSpec, WorkflowSpec  # noqa: E402
from app.store import Store  # noqa: E402

WORKFLOW = "mp-wf"
TASK = "t"


def _mark(path: Path, content: str = "") -> None:
    path.write_text(content, encoding="utf-8")


async def run_holder(db: str) -> None:
    ready = Path(db + ".ready")
    started = asyncio.Event()

    async def long_task(params):
        # 认领成功即标记 ready；随后一直挂着，由心跳保活。
        # SIGTERM 直接杀进程（测试用 Popen.kill/terminate），不会有任何清理。
        started.set()
        _mark(ready)
        await asyncio.Event().wait()
        return "should-never-return"

    handlers.register("mp-long", long_task)
    store = Store(db)
    store.save_workflow(
        WorkflowSpec(name=WORKFLOW, tasks=[TaskSpec(name=TASK, action="mp-long")])
    )
    run_id, _ = store.create_run_idempotent(WORKFLOW, "mp-key")
    orch = Orchestrator(
        store,
        executor_id="holder",
        heartbeat_interval_seconds=0.1,
        heartbeat_timeout_seconds=2.0,
        maintenance_interval_seconds=60,
    )
    await orch.start_run(run_id)
    # 等任务真正被认领执行
    await asyncio.wait_for(started.wait(), timeout=10)
    # 保持事件循环存活，等待被信号杀死
    while True:
        await asyncio.sleep(1)


async def run_watcher(db: str) -> None:
    done = Path(db + ".done")

    # 一个普通动作：被接管后真正执行并产出结果
    def finish(params):
        return "finished-by-watcher"

    handlers.register("mp-long", finish)
    store = Store(db)
    orch = Orchestrator(
        store,
        executor_id="watcher",
        heartbeat_interval_seconds=0.1,
        heartbeat_timeout_seconds=1.0,
        maintenance_interval_seconds=0.2,
    )
    await orch.start()
    # 主动接管一次（重启语义），之后维护循环也会持续兜底
    await orch.recover_unfinished()
    # 等待任意一个未完成运行到终态
    run_id = None
    for _ in range(300):
        for run in store.list_unfinished_runs():
            run_id = run.run_id
        if run_id is not None:
            info = store.get_run(run_id)
            if info.status in (RunStatus.SUCCEEDED, RunStatus.FAILED):
                break
        await asyncio.sleep(0.05)
    info = store.get_run(run_id)
    assert info.status == RunStatus.SUCCEEDED, info
    _mark(done, run_id)
    await orch.shutdown()
    store.close()


def main() -> None:
    db = sys.argv[1]
    role = sys.argv[2]
    # holder 收到 SIGTERM 直接死掉（默认行为），这里不注册处理器，
    # 模拟真实崩溃：无 shutdown、无租约释放、无注销。
    if role == "holder":
        asyncio.run(run_holder(db))
    elif role == "watcher":
        asyncio.run(run_watcher(db))
    else:
        raise SystemExit(f"unknown role {role}")


if __name__ == "__main__":
    main()
