from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from io import StringIO
import threading
import uuid

import pandas as pd

from db.market_db import get_setting, set_setting


TASK_REGISTRY_KEY = "async_task_registry_v1"
TASK_RESULT_KEY_PREFIX = "async_task_result_v1_"
MAX_TASK_HISTORY = 30

_executor = ThreadPoolExecutor(max_workers=4)
_futures: dict[str, Future] = {}
_lock = threading.Lock()


def _now_text() -> str:
    return pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_registry() -> list[dict]:
    registry = get_setting(TASK_REGISTRY_KEY, [])
    if not isinstance(registry, list):
        return []
    return registry


def _save_registry(registry: list[dict]) -> None:
    set_setting(TASK_REGISTRY_KEY, registry[:MAX_TASK_HISTORY])


def _normalize_registry() -> list[dict]:
    registry = _load_registry()
    changed = False
    for task in registry:
        if task.get("status") == "running" and task.get("id") not in _futures:
            task["status"] = "interrupted"
            task["message"] = "任务上下文已丢失，通常是服务重启或页面所在进程已重置。"
            task["finished_at"] = task.get("finished_at") or _now_text()
            changed = True
    if changed:
        _save_registry(registry)
    return registry


def _upsert_task(task: dict) -> None:
    registry = _normalize_registry()
    registry = [item for item in registry if item.get("id") != task.get("id")]
    registry.insert(0, task)
    _save_registry(registry)


def _serialize_result(result) -> dict:
    if isinstance(result, pd.DataFrame):
        return {
            "kind": "dataframe",
            "payload": result.to_json(orient="split", date_format="iso", force_ascii=False),
        }
    return {"kind": "json", "payload": _json_ready(result)}


def _json_ready(value):
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # pragma: no cover - defensive branch
            pass
    if pd.isna(value) if not isinstance(value, (str, bytes, dict, list, tuple)) else False:
        return None
    return value


def _deserialize_result(serialized: dict):
    if not serialized:
        return None
    if serialized.get("kind") == "dataframe":
        return pd.read_json(StringIO(serialized.get("payload", "")), orient="split")
    return serialized.get("payload")


def _summarize_result(result) -> dict:
    if isinstance(result, pd.DataFrame):
        return {
            "type": "dataframe",
            "rows": int(len(result)),
            "columns": [str(col) for col in result.columns[:8]],
        }
    if isinstance(result, dict):
        summary = {}
        for key in [
            "saved_symbols",
            "saved_rows",
            "catalog_size",
            "pool_size",
            "resolved",
            "failed",
            "missing",
            "stale",
            "degraded_reason",
            "run_time",
        ]:
            if key in result:
                summary[key] = result[key]
        if "best_config" in result and isinstance(result["best_config"], dict):
            summary["best_config"] = {
                "top_n": result["best_config"].get("top_n"),
                "rebalance_days": result["best_config"].get("rebalance_days"),
                "lookback_years": result["best_config"].get("lookback_years"),
            }
        return {"type": "dict", "highlights": summary or {"keys": list(result.keys())[:8]}}
    return {"type": type(result).__name__, "value": str(result)}


def update_task(task_id: str, **updates) -> dict | None:
    registry = _normalize_registry()
    for task in registry:
        if task.get("id") == task_id:
            task.update(updates)
            _save_registry(registry)
            return task
    return None


def start_async_task(task_type: str, label: str, params: dict, runner) -> str:
    task_id = uuid.uuid4().hex[:12]
    task = {
        "id": task_id,
        "task_type": task_type,
        "label": label,
        "params": params,
        "status": "queued",
        "progress": 0.0,
        "message": "任务已进入队列，等待后台执行。",
        "created_at": _now_text(),
        "started_at": None,
        "finished_at": None,
        "result_key": f"{TASK_RESULT_KEY_PREFIX}{task_id}",
        "result_summary": None,
        "error": None,
    }
    _upsert_task(task)

    def progress_callback(progress: float, message: str) -> None:
        clamped = max(0.0, min(1.0, float(progress)))
        update_task(task_id, progress=clamped, message=message, status="running")

    def job():
        update_task(
            task_id,
            status="running",
            progress=0.05,
            started_at=_now_text(),
            message="后台任务已开始执行。",
        )
        try:
            result = runner(progress_callback)
            set_setting(task["result_key"], _serialize_result(result))
            update_task(
                task_id,
                status="completed",
                progress=1.0,
                finished_at=_now_text(),
                message="任务执行完成，可恢复结果。",
                result_summary=_summarize_result(result),
                error=None,
            )
        except Exception as error:  # pragma: no cover - defensive branch
            update_task(
                task_id,
                status="failed",
                progress=1.0,
                finished_at=_now_text(),
                message="任务执行失败。",
                error=str(error),
            )
            raise
        finally:
            with _lock:
                _futures.pop(task_id, None)

    future = _executor.submit(job)
    with _lock:
        _futures[task_id] = future
    return task_id


def list_async_tasks(task_types: list[str] | None = None, limit: int = 12) -> list[dict]:
    registry = _normalize_registry()
    if task_types:
        registry = [task for task in registry if task.get("task_type") in task_types]
    return registry[:limit]


def get_async_task(task_id: str) -> dict | None:
    for task in _normalize_registry():
        if task.get("id") == task_id:
            return task
    return None


def read_async_task_result(task_id: str):
    task = get_async_task(task_id)
    if not task:
        return None
    serialized = get_setting(task.get("result_key"), None)
    return _deserialize_result(serialized)
