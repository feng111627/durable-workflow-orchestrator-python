"""业务：失败影响面标记与恢复续跑。"""
from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from app import handlers
from app.api import create_app
from app.engine import Orchestrator
from app.errors import FatalTaskError
from app.models import RunStatus, TaskSpec, TaskStatus, WorkflowSpec


def _run_until_terminal(store, spec: WorkflowSpec, key="k"):
    store.save_workflow(spec)

    async def go():
        orch = Orchestrator(store, lease_seconds=0.3, heartbeat_interval=0.1)
        run_id, _ = store.create_run_idempotent(spec.name, key)
        await orch.start_run(run_id)
        info = await orch.wait_run(run_id, timeout=5)
        await orch.shutdown()
        return run_id, info

    return asyncio.run(go())


class TestFailureBlastRadius:
    def test_direct_and_indirect_downstream_marked_blocked(self, store):
        def fail(params):
            raise FatalTaskError("broken")

        handlers.register("fail", fail)
        handlers.register("ok", lambda p: "fine")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="a", action="fail"),
                TaskSpec(name="b", action="ok", depends_on=["a"]),   # 直接下游
                TaskSpec(name="c", action="ok", depends_on=["b"]),   # 间接下游
                TaskSpec(name="d", action="ok", depends_on=["a", "c"]),  # 多依赖
                TaskSpec(name="e", action="ok"),                     # 无关分支
            ],
        )
        _, info = _run_until_terminal(store, spec)
        assert info.status == RunStatus.FAILED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["a"].status == TaskStatus.FAILED
        # 直接+间接下游都被明确标成 BLOCKED，而不是混在普通 PENDING 里
        for name in ("b", "c", "d"):
            assert by_name[name].status == TaskStatus.BLOCKED, name
            assert "blocked by failed upstream" in by_name[name].error
        # 无关分支不受影响，正常跑完
        assert by_name["e"].status == TaskStatus.SUCCEEDED

    def test_independent_long_running_branch_not_aborted_when_sibling_fails(self, store):
        """真实并发复现：失败分支落定失败时，无关的长任务仍在执行中。

        期望：失败只级联到自己的下游；无关分支不被取消、不被回收，
        一直跑到 SUCCEEDED；运行在无关分支跑完后才整体判 FAILED。
        """
        slow_running = asyncio.Event()
        release = asyncio.Event()

        async def slow(params):
            slow_running.set()
            # 跑得久：在整个失败判定窗口期间都处于执行中
            await release.wait()
            return "slow-done"

        async def fail_after_slow_started(params):
            # 保证失败发生时，无关任务确实已经真正在执行（真实并发）
            await slow_running.wait()
            raise FatalTaskError("branch failure")

        handlers.register("slow", slow)
        handlers.register("fail", fail_after_slow_started)
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="bad", action="fail"),   # 会彻底失败的分支
                TaskSpec(name="slow", action="slow"),  # 与失败分支互不相干
            ],
        )
        store.save_workflow(spec)

        async def go():
            orch = Orchestrator(store, lease_seconds=30, heartbeat_interval=0.1)
            run_id, _ = store.create_run_idempotent(spec.name, "k")
            await orch.start_run(run_id)
            # 等到“失败已落定、无关任务仍在执行”这个关键窗口
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 5
            while True:
                snap = {t.task_name: t for t in store.get_run(run_id).tasks}
                if (snap["bad"].status == TaskStatus.FAILED
                        and snap["slow"].status == TaskStatus.RUNNING):
                    break
                assert loop.time() < deadline
                await asyncio.sleep(0.01)
            mid = {t.task_name: t for t in store.get_run(run_id).tasks}
            assert mid["slow"].status == TaskStatus.RUNNING
            assert mid["slow"].owner is not None  # 租约仍被存活执行者持有
            # 放行无关的长任务：它必须能正常跑完，而不是已被取消而永久挂起
            release.set()
            deadline = loop.time() + 5
            while True:
                info = store.get_run(run_id)
                snap = {t.task_name: t for t in info.tasks}
                if snap["slow"].status == TaskStatus.SUCCEEDED:
                    break
                assert loop.time() < deadline, "independent task stuck, never finished"
                await asyncio.sleep(0.01)
            info = store.get_run(run_id)
            await orch.shutdown()
            return info

        info = asyncio.run(go())
        # 运行整体仍然失败（失败分支 + 其下游语义不变）……
        assert info.status == RunStatus.FAILED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["bad"].status == TaskStatus.FAILED
        # ……但无关分支正常跑完，没有被牵连、没有卡在 RUNNING
        assert by_name["slow"].status == TaskStatus.SUCCEEDED
        assert by_name["slow"].result == "slow-done"
        assert by_name["slow"].owner is None

    def test_poisoned_failed_run_self_heals_after_restart(self, store):
        """旧版本中毒状态恢复：运行已判 FAILED，无关分支任务却还挂着。

        直接构造两种卡住的形态：无关任务停在 RUNNING（执行者失联留下的
        孤儿）、另一条无关分支停在 PENDING（没人再推进）。重启服务后
        它们必须被识别出来重新推进到 SUCCEEDED，而失败分支及其下游
        维持 FAILED / BLOCKED，运行最终仍是 FAILED。
        """
        calls = {"slow": 0, "ind": 0}

        def slow(params):
            calls["slow"] += 1
            return "slow-done"

        def ind(params):
            calls["ind"] += 1
            return "ind-done"

        handlers.register("slow", slow)
        handlers.register("ind", ind)
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="bad", action="ind"),
                TaskSpec(name="down", action="ind", depends_on=["bad"]),
                TaskSpec(name="slow", action="slow"),   # 无关分支：卡在 RUNNING
                TaskSpec(name="ind", action="ind"),  # 无关分支：卡在 PENDING
            ],
        )
        store.save_workflow(spec)
        run_id, _ = store.create_run_idempotent(spec.name, "k")

        # 构造旧版本留下的中毒状态：bad 失败、down 级联阻塞；
        # slow 被一个已经失联的执行者“执行中”，ind 还在 PENDING；
        # 运行整体却已经被判 FAILED（旧行为），无人再推进。
        token = store.claim_task(run_id, "bad", "lost-executor", lease_seconds=30.0)
        assert store.fail_task_and_block_downstream(
            run_id, "bad", "lost-executor", token, "fatal: boom"
        ) is True
        token_slow = store.claim_task(
            run_id, "slow", "lost-executor", lease_seconds=0.0
        )  # 租约立即过期，模拟执行者已失联
        assert token_slow > 0
        store.set_run_status(run_id, RunStatus.FAILED)
        poisoned = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert poisoned["slow"].status == TaskStatus.RUNNING
        assert poisoned["ind"].status == TaskStatus.PENDING

        async def reboot():
            orch = Orchestrator(store, lease_seconds=0.3, heartbeat_interval=0.1)
            recovered = await orch.recover_unfinished()
            assert run_id in recovered  # 带非终态任务的失败运行也会被识别
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(reboot())
        assert info.status == RunStatus.FAILED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["bad"].status == TaskStatus.FAILED
        assert by_name["down"].status == TaskStatus.BLOCKED
        assert by_name["slow"].status == TaskStatus.SUCCEEDED
        assert by_name["slow"].result == "slow-done"
        assert by_name["ind"].status == TaskStatus.SUCCEEDED
        assert by_name["ind"].result == "ind-done"
        assert calls == {"slow": 1, "ind": 1}

    def test_blocked_downstream_resumes_after_upstream_recovery(self, store):
        calls = {"a": 0, "b": 0, "c": 0}

        def flaky(params):
            calls["a"] += 1
            if calls["a"] == 1:
                raise FatalTaskError("first attempt fails")
            return "recovered"

        handlers.register("flaky", flaky)
        handlers.register("b", lambda p: calls.__setitem__("b", calls["b"] + 1) or "B")
        handlers.register("c", lambda p: calls.__setitem__("c", calls["c"] + 1) or "C")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="a", action="flaky"),
                TaskSpec(name="b", action="b", depends_on=["a"]),
                TaskSpec(name="c", action="c", depends_on=["b"]),
            ],
        )
        run_id, info = _run_until_terminal(store, spec)
        assert info.status == RunStatus.FAILED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["b"].status == TaskStatus.BLOCKED
        assert by_name["c"].status == TaskStatus.BLOCKED

        async def recover():
            orch = Orchestrator(store, lease_seconds=0.3, heartbeat_interval=0.1)
            # 恢复失败的上游：重新排队，不用重新提交整个工作流
            store.requeue_failed_task(run_id, "a")
            # 被挡住的下游随依赖自动解除阻塞
            mid = {t.task_name: t for t in store.get_run(run_id).tasks}
            assert mid["b"].status == TaskStatus.PENDING
            assert mid["c"].status == TaskStatus.PENDING
            await orch.start_run(run_id)
            info = await orch.wait_run(run_id, timeout=5)
            await orch.shutdown()
            return info

        info = asyncio.run(recover())
        assert info.status == RunStatus.SUCCEEDED
        by_name = {t.task_name: t for t in info.tasks}
        assert by_name["a"].attempts == 2  # 尝试次数接着算
        assert by_name["b"].result == "B" and by_name["c"].result == "C"
        assert calls == {"a": 2, "b": 1, "c": 1}

    def test_unblock_only_when_all_failed_ancestors_recovered(self, store):
        """菱形依赖：两个上游都失败时，恢复一个不足以解除下游阻塞。"""
        handlers.register("ok", lambda p: "fine")
        spec = WorkflowSpec(
            name="w",
            tasks=[
                TaskSpec(name="a1", action="ok"),
                TaskSpec(name="a2", action="ok"),
                TaskSpec(name="b", action="ok", depends_on=["a1", "a2"]),
            ],
        )
        store.save_workflow(spec)
        run_id, _ = store.create_run_idempotent("w", "k")
        # 直接构造终态：a1、a2 失败，b 被阻塞
        for name in ("a1", "a2"):
            token = store.claim_task(run_id, name, "exec", lease_seconds=30.0)
            assert store.fail_task_and_block_downstream(
                run_id, name, "exec", token, "fatal: boom"
            ) is True
        by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert by_name["b"].status == TaskStatus.BLOCKED
        # 只恢复 a1：b 仍有失败的祖先 a2，保持 BLOCKED
        store.requeue_failed_task(run_id, "a1")
        by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert by_name["b"].status == TaskStatus.BLOCKED
        # 再恢复 a2：所有失败祖先都已恢复，b 自动解除阻塞
        store.requeue_failed_task(run_id, "a2")
        by_name = {t.task_name: t for t in store.get_run(run_id).tasks}
        assert by_name["b"].status == TaskStatus.PENDING

    def test_rerun_blocked_task_rejected_with_distinct_reason(self, db_path):
        def fail(params):
            raise FatalTaskError("no")

        handlers.register("fail", fail)
        handlers.register("ok", lambda p: "fine")
        app = create_app(db_path, lease_seconds=0.3, heartbeat_interval=0.1)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w",
                "tasks": [
                    {"name": "a", "action": "fail"},
                    {"name": "b", "action": "ok", "depends_on": ["a"]},
                ],
            })
            run_id = client.post(
                "/workflows/w/runs", json={"idempotency_key": "k"}
            ).json()["run_id"]
            info = asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            assert info.status == RunStatus.FAILED
            resp = client.post(f"/runs/{run_id}/tasks/b/rerun")
            assert resp.status_code == 409
            body = resp.json()
            # 可区分：是“被上游失败阻塞”，不是笼统的非法迁移
            assert body["reason"] == "task_blocked_by_upstream"
            assert body["current"] == "BLOCKED"

    def test_rerun_running_task_rejected_with_distinct_reason(self, db_path):
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow(params):
            started.set()
            await release.wait()
            return "done"

        handlers.register("slow", slow)
        app = create_app(db_path, lease_seconds=0.3, heartbeat_interval=0.1)
        with TestClient(app) as client:
            client.post("/workflows", json={
                "name": "w", "tasks": [{"name": "t", "action": "slow"}],
            })
            run_id = client.post(
                "/workflows/w/runs", json={"idempotency_key": "k"}
            ).json()["run_id"]

            async def wait_running():
                while True:
                    info = app.state.store.get_run(run_id)
                    if info.tasks[0].status == TaskStatus.RUNNING:
                        return
                    await asyncio.sleep(0.02)

            asyncio.run(asyncio.wait_for(wait_running(), timeout=5))
            resp = client.post(f"/runs/{run_id}/tasks/t/rerun")
            assert resp.status_code == 409
            body = resp.json()
            # 可区分：是“正在执行中”
            assert body["reason"] == "task_currently_running"
            assert body["current"] == "RUNNING"
            release.set()
            info = asyncio.run(app.state.orchestrator.wait_run(run_id, timeout=5))
            assert info.status == RunStatus.SUCCEEDED
