"""业务：真实多进程（多个 OS 进程共享同一份 SQLite 数据）接管验证。

这些用例真正拉起独立进程并发访问同一个数据库文件，而不是顺序循环：

* holder 进程认领任务并持续心跳，证明"活着"时 watcher 抢不走；
* 强杀 holder（SIGKILL，无任何清理）模拟崩溃；
* watcher 进程在失联超时后接管并把任务跑完。
反复运行结果确定，不依赖时序巧合（等待基于数据库中的明确状态）。
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app.models import TaskStatus
from app.store import Store

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "tests" / "support" / "mp_worker.py"


def _spawn(db: str, role: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(WORKER), db, role],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_file(path: Path, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {path.name}")
        time.sleep(0.05)


def _wait_task_status(
    db: str, status: TaskStatus, executor: str | None = None,
    timeout: float = 15.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        store = Store(db)
        try:
            runs = store.list_unfinished_runs()
            if runs:
                task = runs[0].tasks[0]
                if task.status == status and (
                    executor is None or task.executor_id == executor
                ):
                    return task.model_dump()
        finally:
            store.close()
        if time.monotonic() > deadline:
            raise AssertionError(f"task never reached {status}/{executor}")
        time.sleep(0.05)


@pytest.fixture()
def mp_db(tmp_path):
    db = str(tmp_path / "mp.db")
    yield db
    for suffix in (".ready", ".done"):
        p = Path(db + suffix)
        if p.exists():
            p.unlink()


class TestRealMultiProcessTakeover:
    def test_live_holder_not_stolen_then_killed_and_taken_over(self, mp_db):
        """完整链路（真实进程）：活持有者不被抢 -> 被强杀 -> watcher 接管完成。"""
        # 1) holder 启动并认领任务、持续心跳
        holder = _spawn(mp_db, "holder")
        try:
            _wait_file(Path(mp_db + ".ready"))
            # 数据库中确认 holder 确实持有任务（RUNNING + executor_id=holder）
            task = _wait_task_status(mp_db, TaskStatus.RUNNING, executor="holder")
            assert task["claim_epoch"] == 1

            # 2) watcher 启动并在场运行一段时间：holder 心跳新鲜，不能被抢走
            watcher = _spawn(mp_db, "watcher")
            try:
                # 给 watcher 足够时间做过若干轮接管扫描（其失联阈值为 1s）
                time.sleep(2.0)
                # holder 仍持有、任务仍 RUNNING、纪元未变 => 没被抢
                store = Store(mp_db)
                running = [
                    r for r in store.list_unfinished_runs()
                ]
                store.close()
                assert running and running[0].tasks[0].executor_id == "holder"
                assert running[0].tasks[0].status == TaskStatus.RUNNING
                assert running[0].tasks[0].claim_epoch == 1

                # 3) 强杀 holder（SIGKILL：无 finally、无心跳、无租约释放）
                holder.kill()
                holder.wait(timeout=10)
                holder = None

                # 4) watcher 在失联超时后接管并完成；等待 done 文件这一明确事实
                _wait_file(Path(mp_db + ".done"))

                # 5) 独立打开数据库核验最终事实
                store = Store(mp_db)
                run_id = Path(mp_db + ".done").read_text().strip()
                info = store.get_run(run_id)
                store.close()
                task = {t.task_name: t for t in info.tasks}["t"]
                assert info.status.value == "SUCCEEDED"
                assert task.status == TaskStatus.SUCCEEDED
                assert task.result == "finished-by-watcher"
                assert task.executor_id == "watcher"
                # holder 第一次执行 + watcher 接管 = 2 次尝试，历史接着算
                assert task.attempts == 2
                assert task.claim_epoch == 2

                stdout, stderr = watcher.communicate(timeout=10)
                assert watcher.returncode == 0, stderr
            finally:
                if watcher.poll() is None:
                    watcher.kill()
                    watcher.wait(timeout=10)
        finally:
            if holder is not None and holder.poll() is None:
                holder.kill()
                holder.wait(timeout=10)
