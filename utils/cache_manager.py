from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_ROOT = PROJECT_ROOT / ".cache"
CACHE_ROOT.mkdir(exist_ok=True)

# TTLs in seconds for different cache keys
DEFAULT_TTLS = {
    "catalog": 300,
    "accumulation": 300,
    "growth": 300,
}


def cache_file(name: str) -> Path:
    return CACHE_ROOT / f"{name}.json"


def read_cache(name: str, ttl: int) -> dict | None:
    path = cache_file(name)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        path.unlink(missing_ok=True)
        return None
    updated = payload.get("updated", 0)
    if ttl is not None and time.time() - updated > ttl:
        payload["stale"] = True
    return payload


def write_cache(name: str, data: Any) -> None:
    path = cache_file(name)
    payload = {
        "updated": time.time(),
        "data": data,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def schedule_refresh(
    name: str,
    loader: Callable[..., Any],
    args: tuple[Any, ...] = (),
    kwargs: dict | None = None,
) -> None:
    def worker() -> None:
        try:
            result = loader(*args, **(kwargs or {}))
            write_cache(name, result)
        except Exception:
            pass

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def load_dataframe(cache_key: str, loader: Callable[[], pd.DataFrame], ttl: int) -> pd.DataFrame:
    cached = read_cache(cache_key, ttl)
    if cached and cached.get("data"):
        df = pd.read_json(cached["data"], orient="split")
        if cached.get("stale"):
            schedule_refresh(cache_key, lambda: loader().to_json(orient="split"))
        return df
    df = loader()
    write_cache(cache_key, df.to_json(orient="split"))
    return df
