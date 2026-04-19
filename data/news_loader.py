from __future__ import annotations

from functools import lru_cache

import akshare as ak
import pandas as pd

from data.akshare_loader import DataFetchError
from db.market_db import get_recent_stock_news, save_stock_news_items


POSITIVE_NEWS_KEYWORDS = [
    "中标",
    "签约",
    "订单",
    "回购",
    "增持",
    "分红",
    "预增",
    "扭亏",
    "增长",
    "创新高",
    "获批",
    "落地",
]

NEGATIVE_NEWS_KEYWORDS = [
    "减持",
    "问询",
    "处罚",
    "风险",
    "诉讼",
    "亏损",
    "预减",
    "首亏",
    "下滑",
    "终止",
    "违约",
    "冻结",
    "暴跌",
]

HIGH_IMPORTANCE_KEYWORDS = [
    "停牌",
    "复牌",
    "并购",
    "重组",
    "回购",
    "增持",
    "减持",
    "业绩",
    "诉讼",
    "处罚",
    "中标",
    "订单",
]


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _parse_news_time(value) -> pd.Timestamp | None:
    if value is None or str(value).strip() == "":
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp


def _summarize_text(content: str, title: str, max_length: int = 90) -> str:
    text = str(content or "").strip()
    if not text:
        return str(title or "").strip()
    compact = " ".join(text.split())
    return compact[:max_length] + ("..." if len(compact) > max_length else "")


def _classify_news_sentiment(title: str, summary: str, content: str) -> tuple[str, float]:
    text = f"{title} {summary} {content}".lower()
    positive_hits = sum(1 for keyword in POSITIVE_NEWS_KEYWORDS if keyword in text)
    negative_hits = sum(1 for keyword in NEGATIVE_NEWS_KEYWORDS if keyword in text)
    if positive_hits > negative_hits:
        score = min(1.0, 0.35 + 0.15 * positive_hits)
        return "positive", round(score, 2)
    if negative_hits > positive_hits:
        score = max(-1.0, -0.35 - 0.15 * negative_hits)
        return "negative", round(score, 2)
    return "neutral", 0.0


def _estimate_importance(title: str, summary: str, content: str, publish_time: pd.Timestamp | None) -> int:
    text = f"{title} {summary} {content}"
    importance = 2
    if any(keyword in text for keyword in HIGH_IMPORTANCE_KEYWORDS):
        importance += 2
    if any(keyword in text for keyword in ["公告", "快报", "预告", "点评"]):
        importance += 1
    if publish_time is not None:
        age_hours = max(0.0, (pd.Timestamp.now() - publish_time).total_seconds() / 3600.0)
        if age_hours <= 6:
            importance += 1
    return int(max(1, min(5, importance)))


def _latest_news_is_fresh(news_items: list[dict], max_age_minutes: int) -> bool:
    if not news_items:
        return False
    latest_time = _parse_news_time(news_items[0].get("publish_time") or news_items[0].get("created_at"))
    if latest_time is None:
        return False
    age_minutes = (pd.Timestamp.now() - latest_time).total_seconds() / 60.0
    return age_minutes <= max(int(max_age_minutes), 1)


@lru_cache(maxsize=256)
def _cached_stock_news(symbol: str) -> pd.DataFrame:
    try:
        return ak.stock_news_em(symbol=str(symbol).zfill(6))
    except Exception as error:
        raise DataFetchError(f"获取实时新闻失败: {symbol} {error}") from error


def fetch_realtime_stock_news(
    symbol: str,
    fallback_name: str | None = None,
    max_news: int = 12,
    lookback_hours: int = 72,
) -> list[dict]:
    symbol = str(symbol).zfill(6)
    df = _cached_stock_news(symbol)
    if df is None or df.empty:
        return []

    title_col = _pick_column(df, ["新闻标题", "标题"])
    content_col = _pick_column(df, ["新闻内容", "内容"])
    time_col = _pick_column(df, ["发布时间", "时间", "日期"])
    source_col = _pick_column(df, ["文章来源", "来源"])
    url_col = _pick_column(df, ["新闻链接", "链接", "网址"])
    if not title_col:
        return []

    cutoff = pd.Timestamp.now() - pd.Timedelta(hours=max(int(lookback_hours), 1))
    results: list[dict] = []
    for _, row in df.iterrows():
        title = str(row.get(title_col) or "").strip()
        if not title:
            continue
        publish_time = _parse_news_time(row.get(time_col))
        if publish_time is not None and publish_time < cutoff:
            continue
        content = str(row.get(content_col) or "").strip()
        summary = _summarize_text(content, title)
        sentiment, sentiment_score = _classify_news_sentiment(title, summary, content)
        results.append(
            {
                "symbol": symbol,
                "name": str(fallback_name or symbol),
                "publish_time": publish_time.isoformat() if publish_time is not None else None,
                "title": title,
                "summary": summary,
                "content": content,
                "source": str(row.get(source_col) or "akshare.stock_news_em"),
                "url": str(row.get(url_col) or "").strip() or None,
                "sentiment": sentiment,
                "sentiment_score": sentiment_score,
                "importance": _estimate_importance(title, summary, content, publish_time),
                "data_source": "akshare.stock_news_em",
            }
        )

    results.sort(
        key=lambda item: (
            str(item.get("publish_time") or ""),
            int(item.get("importance", 0)),
            str(item.get("title") or ""),
        ),
        reverse=True,
    )
    return results[: max(int(max_news), 1)]


def load_or_fetch_realtime_stock_news(
    symbol: str,
    fallback_name: str | None = None,
    prefer_cache: bool = False,
    max_age_minutes: int = 120,
    lookback_hours: int = 72,
    limit: int = 12,
) -> list[dict]:
    symbol = str(symbol).zfill(6)
    cached_items = get_recent_stock_news(symbol, lookback_hours=lookback_hours, limit=limit)
    if prefer_cache and cached_items:
        return cached_items
    if _latest_news_is_fresh(cached_items, max_age_minutes):
        return cached_items

    try:
        fresh_items = fetch_realtime_stock_news(
            symbol,
            fallback_name=fallback_name,
            max_news=limit,
            lookback_hours=lookback_hours,
        )
    except DataFetchError:
        return cached_items

    if fresh_items:
        replace_from_time = (pd.Timestamp.now() - pd.Timedelta(hours=max(int(lookback_hours), 1))).isoformat()
        save_stock_news_items(symbol, fresh_items, replace_from_time=replace_from_time)
        return fresh_items
    return cached_items
