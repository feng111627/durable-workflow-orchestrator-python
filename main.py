"""服务入口：uv run python main.py 或 uvicorn main:app"""
import os

import uvicorn

from app.api import create_app
from app.builtins import register_builtins

register_builtins()

app = create_app(
    db_path=os.environ.get("ORCH_DB_PATH", "orchestrator.db"),
    max_concurrent_tasks=int(os.environ.get("ORCH_MAX_CONCURRENT_TASKS", "4")),
    # 多执行者失联判定：心跳上报间隔与失联超时阈值（阈值须大于间隔）
    heartbeat_interval_seconds=float(
        os.environ.get("ORCH_HEARTBEAT_INTERVAL_SECONDS", "1.0")
    ),
    heartbeat_timeout_seconds=float(
        os.environ.get("ORCH_HEARTBEAT_TIMEOUT_SECONDS", "30.0")
    ),
    maintenance_interval_seconds=float(
        os.environ.get("ORCH_MAINTENANCE_INTERVAL_SECONDS", "1.0")
    ),
    # 不指定则自动生成 executor-<uuid>；多实例部署时也可显式给定稳定身份
    executor_id=os.environ.get("ORCH_EXECUTOR_ID"),
)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
