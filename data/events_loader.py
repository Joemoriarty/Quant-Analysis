from __future__ import annotations

from functools import lru_cache

import akshare as ak
import pandas as pd

from data.akshare_loader import DataFetchError


NOTICE_CATEGORIES = ["重大事项", "财务报告", "风险提示", "资产重组", "持股变动"]

POSITIVE_KEYWORDS = [
    "预增",
    "扭亏",
    "增长",
    "增持",
    "回购",
    "分红",
    "分配预案",
    "中标",
    "签订",
    "获批",
    "收购",
    "摘帽",
]

NEGATIVE_KEYWORDS = [
    "预减",
    "首亏",
    "续亏",
    "增亏",
    "减持",
    "风险",
    "处罚",
    "问询",
    "诉讼",
    "退市",
    "终止",
    "失败",
    "下滑",
    "质押",
    "冻结",
    "亏损",
]

POSITIVE_TYPES = {"业绩预增", "回购", "分红", "增持"}
NEGATIVE_TYPES = {"风险提示", "减持", "处罚", "退市风险", "诉讼"}


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _normalize_symbol(value) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    return digits.zfill(6)


def _normalize_date(value) -> str | None:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.date().isoformat()


def _classify_event_bias(title: str, event_type: str) -> str:
    text = f"{event_type} {title}".lower()
    if event_type in POSITIVE_TYPES or any(keyword in text for keyword in POSITIVE_KEYWORDS):
        return "positive"
    if event_type in NEGATIVE_TYPES or any(keyword in text for keyword in NEGATIVE_KEYWORDS):
        return "negative"
    return "neutral"


def _estimate_importance(event_type: str, title: str, source: str) -> int:
    text = f"{event_type} {title}"
    if source == "akshare.stock_yjyg_em":
        if any(keyword in text for keyword in ["预增", "扭亏", "预减", "首亏", "续亏", "增亏"]):
            return 5
        return 4
    if any(keyword in text for keyword in ["退市", "处罚", "重大资产重组", "资产重组", "回购", "增持", "减持"]):
        return 5
    if any(keyword in text for keyword in ["风险提示", "年度报告", "季度报告", "业绩预告", "分红", "分配预案"]):
        return 4
    if any(keyword in text for keyword in ["披露", "调研", "董事会", "股东大会"]):
        return 2
    return 3


def _deduplicate_events(events: list[dict]) -> list[dict]:
    unique: dict[tuple, dict] = {}
    for event in events:
        key = (
            event.get("symbol"),
            event.get("event_date"),
            event.get("event_type"),
            event.get("title"),
        )
        if key not in unique:
            unique[key] = event
            continue
        if int(event.get("importance", 0)) > int(unique[key].get("importance", 0)):
            unique[key] = event
    return sorted(
        unique.values(),
        key=lambda item: (
            str(item.get("event_date") or ""),
            int(item.get("importance", 0)),
            str(item.get("title") or ""),
        ),
        reverse=True,
    )


@lru_cache(maxsize=256)
def _cached_notice_report(category: str, date: str) -> pd.DataFrame:
    try:
        return ak.stock_notice_report(symbol=category, date=date)
    except Exception as error:
        raise DataFetchError(f"获取公告列表失败: {category} {date} {error}") from error


@lru_cache(maxsize=64)
def _cached_yjyg(report_date: str) -> pd.DataFrame:
    try:
        return ak.stock_yjyg_em(date=report_date)
    except Exception as error:
        raise DataFetchError(f"获取业绩预告失败: {report_date} {error}") from error


@lru_cache(maxsize=32)
def _cached_report_disclosure(period: str) -> pd.DataFrame:
    try:
        return ak.stock_report_disclosure(market="沪深京", period=period)
    except Exception as error:
        raise DataFetchError(f"获取财报预约披露失败: {period} {error}") from error


def _recent_report_dates(now: pd.Timestamp | None = None) -> list[str]:
    now = now or pd.Timestamp.now()
    year = now.year
    return [
        f"{year}1231",
        f"{year}0930",
        f"{year}0630",
        f"{year}0331",
        f"{year - 1}1231",
        f"{year - 1}0930",
    ]


def _recent_disclosure_periods(now: pd.Timestamp | None = None) -> list[str]:
    now = now or pd.Timestamp.now()
    year = now.year
    return [
        f"{year - 1}年报",
        f"{year}一季报",
        f"{year}半年报",
        f"{year}三季报",
    ]


def fetch_company_events(symbol: str, fallback_name: str | None = None, lookback_days: int = 30) -> list[dict]:
    symbol = str(symbol).zfill(6)
    now = pd.Timestamp.now().normalize()
    start = now - pd.Timedelta(days=max(int(lookback_days), 1) - 1)
    events: list[dict] = []

    for current_date in pd.date_range(start, now, freq="D"):
        date_key = current_date.strftime("%Y%m%d")
        for category in NOTICE_CATEGORIES:
            try:
                df = _cached_notice_report(category, date_key)
            except DataFetchError:
                continue
            if df is None or df.empty:
                continue
            code_col = _pick_column(df, ["代码", "股票代码"])
            name_col = _pick_column(df, ["名称", "股票简称"])
            title_col = _pick_column(df, ["公告标题", "标题"])
            event_type_col = _pick_column(df, ["公告类型", "类型"])
            date_col = _pick_column(df, ["公告日期", "日期"])
            url_col = _pick_column(df, ["网址", "链接"])
            if not code_col or not title_col:
                continue
            subset = df[df[code_col].astype(str).str.zfill(6) == symbol]
            if subset.empty:
                continue
            for _, row in subset.iterrows():
                title = str(row.get(title_col) or "").strip()
                event_type = str(row.get(event_type_col) or category).strip()
                event_date = _normalize_date(row.get(date_col) or current_date)
                bias = _classify_event_bias(title, event_type)
                events.append(
                    {
                        "symbol": symbol,
                        "name": str(row.get(name_col) or fallback_name or symbol).strip(),
                        "event_date": event_date or current_date.date().isoformat(),
                        "event_type": event_type,
                        "title": title,
                        "summary": f"{category} / {event_type}",
                        "importance": _estimate_importance(event_type, title, "akshare.stock_notice_report"),
                        "source": "akshare.stock_notice_report",
                        "bias": bias,
                        "raw": {
                            "bias": bias,
                            "category": category,
                            "url": row.get(url_col),
                        },
                    }
                )

    for report_date in _recent_report_dates(now):
        try:
            df = _cached_yjyg(report_date)
        except DataFetchError:
            continue
        if df is None or df.empty:
            continue
        code_col = _pick_column(df, ["股票代码"])
        name_col = _pick_column(df, ["股票简称"])
        title_col = _pick_column(df, ["业绩变动"])
        type_col = _pick_column(df, ["预告类型"])
        reason_col = _pick_column(df, ["业绩变动原因"])
        date_col = _pick_column(df, ["公告日期"])
        metric_col = _pick_column(df, ["预测指标"])
        value_col = _pick_column(df, ["预测数值"])
        if not code_col or not title_col:
            continue
        subset = df[df[code_col].astype(str).str.zfill(6) == symbol]
        if subset.empty:
            continue
        for _, row in subset.iterrows():
            event_type = "业绩预告"
            title = str(row.get(title_col) or "").strip()
            forecast_type = str(row.get(type_col) or "").strip()
            metric_name = str(row.get(metric_col) or "").strip()
            bias = _classify_event_bias(title, forecast_type or event_type)
            summary = "；".join([item for item in [forecast_type, metric_name, str(row.get(reason_col) or "").strip()] if item])[:220]
            events.append(
                {
                    "symbol": symbol,
                    "name": str(row.get(name_col) or fallback_name or symbol).strip(),
                    "event_date": _normalize_date(row.get(date_col)) or now.date().isoformat(),
                    "event_type": event_type,
                    "title": f"{forecast_type} - {title}" if forecast_type else title,
                    "summary": summary,
                    "importance": _estimate_importance(forecast_type or event_type, title, "akshare.stock_yjyg_em"),
                    "source": "akshare.stock_yjyg_em",
                    "bias": bias,
                    "raw": {
                        "bias": bias,
                        "forecast_type": forecast_type,
                        "metric_name": metric_name,
                        "forecast_value": row.get(value_col),
                    },
                }
            )

    for period in _recent_disclosure_periods(now):
        try:
            df = _cached_report_disclosure(period)
        except DataFetchError:
            continue
        if df is None or df.empty:
            continue
        code_col = _pick_column(df, ["股票代码"])
        name_col = _pick_column(df, ["股票简称"])
        first_col = _pick_column(df, ["首次预约"])
        actual_col = _pick_column(df, ["实际披露"])
        if not code_col:
            continue
        subset = df[df[code_col].astype(str).str.zfill(6) == symbol]
        if subset.empty:
            continue
        row = subset.iloc[0]
        planned_date = row.get(actual_col) or row.get(first_col)
        normalized = _normalize_date(planned_date)
        if not normalized:
            continue
        events.append(
            {
                "symbol": symbol,
                "name": str(row.get(name_col) or fallback_name or symbol).strip(),
                "event_date": normalized,
                "event_type": "财报预约披露",
                "title": f"{period} 披露日期",
                "summary": f"{period} 的预约/实际披露日期为 {normalized}",
                "importance": 2,
                "source": "akshare.stock_report_disclosure",
                "bias": "neutral",
                "raw": {
                    "bias": "neutral",
                    "period": period,
                    "first_date": _normalize_date(row.get(first_col)),
                    "actual_date": _normalize_date(row.get(actual_col)),
                },
            }
        )

    return _deduplicate_events(events)


def load_or_fetch_company_events(symbol: str, fallback_name: str | None = None, lookback_days: int = 30) -> list[dict]:
    from db.market_db import get_recent_company_events, save_company_events

    cached = get_recent_company_events(symbol, lookback_days=lookback_days, limit=80)
    if cached:
        latest_created = pd.to_datetime(max(item.get("created_at") or "" for item in cached), errors="coerce")
        if not pd.isna(latest_created) and latest_created >= pd.Timestamp.now() - pd.Timedelta(hours=12):
            return cached

    events = fetch_company_events(symbol, fallback_name=fallback_name, lookback_days=lookback_days)
    if events:
        save_company_events(symbol, events, replace_from_date=(pd.Timestamp.now() - pd.Timedelta(days=lookback_days)).date().isoformat())
        return events
    return cached
