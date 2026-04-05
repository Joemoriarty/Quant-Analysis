from __future__ import annotations

import pandas as pd

import akshare as ak

from data.akshare_loader import DataFetchError
from db.market_db import get_latest_market_sentiment_snapshot, save_market_sentiment_snapshot


def _safe_int(value, default: int = 0) -> int:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return default
    return int(numeric)


def _classify_market_state(score: int) -> str:
    if score >= 70:
        return "偏强"
    if score >= 45:
        return "中性"
    return "偏弱"


def fetch_market_sentiment_snapshot() -> dict:
    try:
        spot_df = ak.stock_zh_a_spot_em()
    except Exception as error:
        raise DataFetchError(f"获取市场情绪快照失败: {error}") from error

    if spot_df is None or spot_df.empty:
        raise DataFetchError("市场情绪快照为空")

    pct_series = pd.to_numeric(spot_df.get("涨跌幅"), errors="coerce").fillna(0.0)
    up_count = int((pct_series > 0).sum())
    down_count = int((pct_series < 0).sum())
    flat_count = int((pct_series == 0).sum())
    limit_up_count = int((pct_series >= 9.8).sum())
    limit_down_count = int((pct_series <= -9.8).sum())

    total = max(len(pct_series), 1)
    breadth = (up_count - down_count) / total
    score = 50.0
    score += breadth * 35
    score += min(limit_up_count, 80) * 0.35
    score -= min(limit_down_count, 60) * 0.6
    score = int(max(0, min(100, round(score))))

    snapshot = {
        "snapshot_time": str(pd.Timestamp.now()),
        "up_count": up_count,
        "down_count": down_count,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "consecutive_board_height": None,
        "margin_balance": None,
        "score": score,
        "market_state": _classify_market_state(score),
        "source": "akshare.stock_zh_a_spot_em",
        "extra": {
            "flat_count": flat_count,
            "breadth": breadth,
            "total_count": total,
        },
    }
    save_market_sentiment_snapshot(snapshot)
    return snapshot


def load_or_fetch_market_sentiment_snapshot() -> dict | None:
    try:
        return fetch_market_sentiment_snapshot()
    except DataFetchError:
        return get_latest_market_sentiment_snapshot()
