"""服务入口：uv run python main.py 或 uvicorn main:app"""
import os

import uvicorn

from app.api import create_app
from app.builtins import register_builtins

register_builtins()

app = create_app(
    db_path=os.environ.get("ORCH_DB_PATH", "orchestrator.db"),
    max_concurrent_tasks=int(os.environ.get("ORCH_MAX_CONCURRENT_TASKS", "4")),
    executor_id=os.environ.get("ORCH_EXECUTOR_ID") or None,
    lease_seconds=float(os.environ.get("ORCH_LEASE_SECONDS", "30")),
    heartbeat_interval=float(os.environ.get("ORCH_HEARTBEAT_INTERVAL", "5")),
)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
