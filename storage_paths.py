from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STORAGE_ROOT = PROJECT_ROOT
STORAGE_ROOT = Path(os.getenv("APP_STORAGE_DIR", str(DEFAULT_STORAGE_ROOT))).resolve()

DATA_DIR = STORAGE_ROOT / "data"
DB_DIR = STORAGE_ROOT / "db"
CACHE_DIR = DATA_DIR / "cache"
PAPER_DIR = DATA_DIR / "paper_trading"
WATCHLIST_DIR = DATA_DIR / "watchlist"


def ensure_storage_dirs() -> None:
    for path in [DATA_DIR, DB_DIR, CACHE_DIR, PAPER_DIR, WATCHLIST_DIR]:
        path.mkdir(parents=True, exist_ok=True)
