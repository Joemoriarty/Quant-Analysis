from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import altair as alt
import pandas as pd

from data.akshare_loader import DataFetchError, get_stock_catalog, get_stock_data, get_stock_lookup
from data.events_loader import load_or_fetch_company_events
from data.fundamental_loader import load_or_fetch_fundamental_snapshot, resolve_industry_membership
from data.news_loader import load_or_fetch_realtime_stock_news
from data.source_registry import build_analysis_data_source_summary
from data.sentiment_loader import load_or_fetch_market_sentiment_snapshot
from portfolio.comparison_plugins import build_comparison_results


def resolve_stock_query(query: str, fallback_names: dict[str, str] | None = None) -> dict:
    query = (query or "").strip()
    if not query:
        raise ValueError("请输入股票代码或名称。")

    fallback_names = fallback_names or {}
    lookup_df = get_stock_lookup(use_cache=True)

    if query.isdigit():
        symbol = query.zfill(6)
        matched = lookup_df[lookup_df["code"] == symbol]
        name = matched["name"].iloc[0] if not matched.empty else fallback_names.get(symbol, symbol)
        return {"symbol": symbol, "name": name}

    exact = lookup_df[lookup_df["name"] == query]
    if not exact.empty:
        row = exact.iloc[0]
        return {"symbol": row["code"], "name": row["name"]}

    fuzzy = lookup_df[lookup_df["name"].str.contains(query, na=False)]
    if not fuzzy.empty:
        row = fuzzy.iloc[0]
        return {"symbol": row["code"], "name": row["name"]}

    raise ValueError(f"没有找到与“{query}”匹配的股票。")


def _compute_macd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd_hist = (dif - dea) * 2
    return dif, dea, macd_hist


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean().replace(0, 1e-9)
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.astype(float).fillna(50.0)


def _support_resistance(df: pd.DataFrame) -> tuple[float, float]:
    recent = df.tail(20)
    return float(recent["low"].min()), float(recent["high"].max())


def _safe_float(value, default: float = 0.0) -> float:
    if pd.isna(value):
        return default
    return float(value)


def _safe_int(value, default: int = 0) -> int:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return default
    return int(numeric)


def _safe_percent_text(value) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):.2f}%"


def _safe_amount_text(value, unit: str = "元") -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):,.2f} {unit}"


def _build_fundamental_summary(fundamental: dict | None, valuation: dict | None) -> dict:
    if not fundamental and not valuation:
        return {
            "available": False,
            "headline": "当前没有可用的基本面快照",
            "items": [],
            "conclusion": "最终结论主要基于技术面，当前未获得足够的基本面校正数据。",
            "positive_flags": [],
            "risk_flags": ["缺少财务和估值快照，无法判断公司质量与定价水平。"],
        }

    fundamental = fundamental or {}
    valuation = valuation or {}
    roe = pd.to_numeric(fundamental.get("roe"), errors="coerce")
    debt_ratio = pd.to_numeric(fundamental.get("debt_ratio"), errors="coerce")
    operating_cash_flow = pd.to_numeric(fundamental.get("operating_cash_flow"), errors="coerce")
    revenue = pd.to_numeric(fundamental.get("revenue"), errors="coerce")
    net_profit = pd.to_numeric(fundamental.get("net_profit"), errors="coerce")
    market_value = pd.to_numeric(valuation.get("market_value"), errors="coerce")
    industry = valuation.get("industry")

    positive_flags: list[str] = []
    risk_flags: list[str] = []

    if pd.notna(roe):
        if roe >= 15:
            positive_flags.append(f"ROE {roe:.2f}%，盈利能力较强")
        elif roe < 8:
            risk_flags.append(f"ROE {roe:.2f}%，盈利能力偏弱")

    if pd.notna(debt_ratio):
        if debt_ratio <= 40:
            positive_flags.append(f"资产负债率 {debt_ratio:.2f}%，财务压力较轻")
        elif debt_ratio >= 65:
            risk_flags.append(f"资产负债率 {debt_ratio:.2f}%，需关注杠杆风险")

    if pd.notna(operating_cash_flow):
        if operating_cash_flow > 0:
            positive_flags.append("经营现金流为正，主营现金回笼正常")
        else:
            risk_flags.append("经营现金流为负，需警惕盈利质量")

    if pd.notna(net_profit) and net_profit <= 0:
        risk_flags.append("归母净利润不为正，基本面保护较弱")

    items = [
        {"项目": "报告期", "内容": str(fundamental.get("report_period") or "-")},
        {"项目": "营业总收入", "内容": _safe_amount_text(revenue)},
        {"项目": "归母净利润", "内容": _safe_amount_text(net_profit)},
        {"项目": "ROE", "内容": _safe_percent_text(roe)},
        {"项目": "资产负债率", "内容": _safe_percent_text(debt_ratio)},
        {"项目": "经营现金流", "内容": _safe_amount_text(operating_cash_flow)},
        {"项目": "行业", "内容": str(industry or "-")},
        {"项目": "总市值", "内容": _safe_amount_text(market_value)},
    ]

    return {
        "available": True,
        "headline": "基本面快照已载入，并参与最终结论校正",
        "items": items,
        "conclusion": "基本面结果已进入最终判断链路，但不会替代技术面买卖时机判断。",
        "positive_flags": positive_flags,
        "risk_flags": risk_flags,
        "source": {
            "fundamental": fundamental.get("source_chain") or fundamental.get("source"),
            "valuation": valuation.get("source_chain") or valuation.get("source"),
        },
    }


def _compute_fundamental_score(fundamental: dict | None, valuation: dict | None) -> tuple[int, list[str], list[str]]:
    if not fundamental and not valuation:
        return 50, ["当前缺少可用基本面快照，本次不做加分"], ["缺少基本面数据，结论仍以技术面为主"]

    score = 50.0
    explanations: list[str] = []
    risks: list[str] = []

    fundamental = fundamental or {}
    valuation = valuation or {}
    roe = pd.to_numeric(fundamental.get("roe"), errors="coerce")
    debt_ratio = pd.to_numeric(fundamental.get("debt_ratio"), errors="coerce")
    operating_cash_flow = pd.to_numeric(fundamental.get("operating_cash_flow"), errors="coerce")
    net_profit = pd.to_numeric(fundamental.get("net_profit"), errors="coerce")
    revenue = pd.to_numeric(fundamental.get("revenue"), errors="coerce")
    market_value = pd.to_numeric(valuation.get("market_value"), errors="coerce")

    if pd.notna(roe):
        if roe >= 20:
            score += 18
            explanations.append(f"ROE {roe:.2f}%，盈利能力很强")
        elif roe >= 12:
            score += 10
            explanations.append(f"ROE {roe:.2f}%，盈利能力较稳健")
        elif roe >= 8:
            score += 4
            explanations.append(f"ROE {roe:.2f}%，盈利能力一般")
        else:
            score -= 14
            risks.append(f"ROE {roe:.2f}%，盈利能力偏弱")
    else:
        risks.append("ROE 数据缺失")

    if pd.notna(debt_ratio):
        if debt_ratio <= 25:
            score += 10
            explanations.append(f"资产负债率 {debt_ratio:.2f}%，财务压力小")
        elif debt_ratio <= 45:
            score += 5
            explanations.append(f"资产负债率 {debt_ratio:.2f}%，财务结构尚可")
        elif debt_ratio >= 70:
            score -= 16
            risks.append(f"资产负债率 {debt_ratio:.2f}%，杠杆偏高")
        elif debt_ratio >= 55:
            score -= 8
            risks.append(f"资产负债率 {debt_ratio:.2f}%，需关注负债压力")
    else:
        risks.append("资产负债率数据缺失")

    if pd.notna(operating_cash_flow):
        if operating_cash_flow > 0:
            score += 8
            explanations.append("经营现金流为正，主营造血正常")
        else:
            score -= 14
            risks.append("经营现金流为负，盈利质量需警惕")
    else:
        risks.append("经营现金流数据缺失")

    if pd.notna(net_profit):
        if net_profit > 0:
            score += 4
            explanations.append("归母净利润为正")
        else:
            score -= 10
            risks.append("归母净利润不为正")

    if pd.notna(revenue) and revenue <= 0:
        score -= 8
        risks.append("营业总收入不为正，数据异常或基本面较弱")

    if pd.notna(market_value) and market_value < 5_000_000_000:
        risks.append("总市值较小，波动可能更大")

    score = max(0, min(100, round(score)))
    return score, explanations, risks


def _build_market_sentiment_view(snapshot: dict | None) -> tuple[dict, list[str], list[str]]:
    if not snapshot:
        return (
            {
                "available": False,
                "headline": "当前没有可用的市场情绪快照",
                "state": "中性",
                "score": 50,
                "items": [],
                "conclusion": "当前未获取到市场情绪快照，本次按中性环境处理，不做额外升降档。",
            },
            ["缺少市场情绪快照，本次按中性环境处理"],
        ["当前无法确认市场是顺风还是逆风环境，因此不做情绪面升降档"],
        )

    score = _safe_int(snapshot.get("score", 50), default=50)
    state = str(snapshot.get("market_state") or "中性")
    extra = snapshot.get("extra") or {}
    up_count = _safe_int(snapshot.get("up_count", 0), default=0)
    down_count = _safe_int(snapshot.get("down_count", 0), default=0)
    limit_up_count = _safe_int(snapshot.get("limit_up_count", 0), default=0)
    limit_down_count = _safe_int(snapshot.get("limit_down_count", 0), default=0)
    breadth = pd.to_numeric(extra.get("breadth"), errors="coerce")

    explanations: list[str] = []
    risks: list[str] = []

    if state == "偏强":
        explanations.append("市场情绪偏强，顺风环境有利于技术面强势信号兑现")
    elif state == "偏弱":
        risks.append("市场情绪偏弱，逆风环境下追涨和放量突破的可靠性下降")
    else:
        explanations.append("市场情绪中性，个股判断仍以技术面和基本面为主")

    if pd.notna(breadth):
        if float(breadth) >= 0.15:
            explanations.append("上涨家数明显多于下跌家数，市场广度较好")
        elif float(breadth) <= -0.15:
            risks.append("下跌家数明显多于上涨家数，市场广度偏弱")

    if limit_up_count >= 40:
        explanations.append(f"涨停家数 {limit_up_count} 家，短线风险偏好较高")
    if limit_down_count >= 15:
        risks.append(f"跌停家数 {limit_down_count} 家，短线情绪承压")

    summary = {
        "available": True,
        "headline": "市场情绪快照已参与最终结论校正",
        "state": state,
        "score": score,
        "items": [
            {"项目": "市场状态", "内容": state},
            {"项目": "情绪得分", "内容": f"{score}/100"},
            {"项目": "上涨家数", "内容": up_count},
            {"项目": "下跌家数", "内容": down_count},
            {"项目": "涨停家数", "内容": limit_up_count},
            {"项目": "跌停家数", "内容": limit_down_count},
        ],
        "conclusion": "市场情绪只做顺风/逆风校正，不直接替代个股技术面和基本面判断。",
    }
    return summary, explanations, risks


def _build_event_summary(events: list[dict] | None) -> tuple[dict, list[str], list[str]]:
    if not events:
        return (
            {
                "available": False,
                "headline": "当前没有可用的事件驱动数据",
                "state": "中性",
                "score": 50,
                "items": [],
                "conclusion": "当前没有检索到足以影响结论的近期事件，本次按中性事件环境处理。",
            },
            ["近期没有显著事件催化，本次按中性事件环境处理"],
            ["缺少近期有效事件，无法确认是否存在公告催化或风险落地"],
        )

    now = pd.Timestamp.now().normalize()
    score = 50.0
    positive_flags: list[str] = []
    risk_flags: list[str] = []
    items: list[dict] = []

    for event in sorted(events, key=lambda item: (str(item.get("event_date") or ""), int(item.get("importance", 0))), reverse=True)[:10]:
        event_date = pd.to_datetime(event.get("event_date"), errors="coerce")
        days_ago = 30 if pd.isna(event_date) else max(0, int((now - event_date.normalize()).days))
        recency_factor = 1.0 if days_ago <= 3 else 0.8 if days_ago <= 7 else 0.6 if days_ago <= 14 else 0.35
        importance = int(pd.to_numeric(event.get("importance"), errors="coerce") or 0)
        bias = str(event.get("bias") or "neutral")
        impact = importance * 4 * recency_factor
        if bias == "positive":
            score += impact
            positive_flags.append(f"{event.get('event_type')}: {event.get('title')}")
        elif bias == "negative":
            score -= impact
            risk_flags.append(f"{event.get('event_type')}: {event.get('title')}")

        items.append(
            {
                "日期": str(event.get("event_date") or "-"),
                "类型": str(event.get("event_type") or "-"),
                "影响": "利多" if bias == "positive" else "利空" if bias == "negative" else "中性",
                "重要性": importance,
                "标题": str(event.get("title") or "-"),
            }
        )

    score = max(0, min(100, round(score)))
    if score >= 70:
        state = "偏利多"
        conclusion = "近期公告和事件整体偏正面，若技术面同步配合，信号更容易兑现。"
    elif score <= 35:
        state = "偏利空"
        conclusion = "近期公告和事件偏负面，需要更重视风险控制和仓位约束。"
    else:
        state = "中性"
        conclusion = "近期事件整体中性，更多作为辅助解释，不单独决定买卖。"

    summary = {
        "available": True,
        "headline": "事件驱动结果已载入，并参与最终结论校正",
        "state": state,
        "score": score,
        "items": items,
        "conclusion": conclusion,
        "positive_flags": positive_flags[:4],
        "risk_flags": risk_flags[:4],
    }

    explanations = []
    risks = []
    if summary["positive_flags"]:
        explanations.extend(summary["positive_flags"][:2])
    if summary["risk_flags"]:
        risks.extend(summary["risk_flags"][:2])
    if not explanations:
        explanations.append("近期未出现足够强的正面事件催化")
    if not risks:
        risks.append("近期未出现显著风险事件，事件层不做额外下调")

    return summary, explanations, risks


def _build_news_summary(news_items: list[dict] | None) -> tuple[dict, list[str], list[str]]:
    if not news_items:
        return (
            {
                "available": False,
                "headline": "当前没有可用的实时新闻数据",
                "state": "中性",
                "score": 50,
                "items": [],
                "conclusion": "当前没有检索到近期实时新闻，本次不对结论做额外新闻校正。",
                "high_importance_count": 0,
                "positive_flags": [],
                "risk_flags": ["缺少近期实时新闻，无法判断短期舆情和催化变化。"],
            },
            ["缺少实时新闻，本次不做新闻层加减分"],
            ["当前无法确认是否存在短期新闻扰动或突发催化"],
        )

    score = 50.0
    items: list[dict] = []
    positive_flags: list[str] = []
    risk_flags: list[str] = []
    high_importance_count = 0

    for item in news_items[:10]:
        sentiment = str(item.get("sentiment") or "neutral")
        importance = _safe_int(item.get("importance", 0), default=0)
        sentiment_score = float(pd.to_numeric(item.get("sentiment_score"), errors="coerce") or 0.0)
        publish_time = pd.to_datetime(item.get("publish_time"), errors="coerce")
        age_hours = 24.0 if pd.isna(publish_time) else max(
            0.0,
            (pd.Timestamp.now() - publish_time).total_seconds() / 3600.0,
        )
        recency_factor = 1.0 if age_hours <= 6 else 0.75 if age_hours <= 24 else 0.45
        impact = sentiment_score * importance * 8 * recency_factor
        score += impact

        title = str(item.get("title") or "-")
        if importance >= 4:
            high_importance_count += 1
        if sentiment == "positive":
            positive_flags.append(title)
        elif sentiment == "negative":
            risk_flags.append(title)

        items.append(
            {
                "时间": str(item.get("publish_time") or "-"),
                "来源": str(item.get("source") or "-"),
                "情绪": "偏正面" if sentiment == "positive" else "偏负面" if sentiment == "negative" else "中性",
                "重要性": importance,
                "标题": title,
            }
        )

    score = max(0, min(100, round(score)))
    if score >= 65:
        state = "偏正面"
        conclusion = "近期新闻整体偏正面，短期催化和市场关注度对股价表现相对有利。"
    elif score <= 35:
        state = "偏负面"
        conclusion = "近期新闻整体偏负面，短期扰动和风险事件需要更高权重对待。"
    else:
        state = "中性"
        conclusion = "近期新闻整体中性，更多用于补充解释短期扰动，不单独改变最终结论。"

    summary = {
        "available": True,
        "headline": "实时新闻层已接入，用于补足事件面之外的短期新闻流。",
        "state": state,
        "score": score,
        "items": items,
        "conclusion": conclusion,
        "high_importance_count": high_importance_count,
        "positive_flags": positive_flags[:4],
        "risk_flags": risk_flags[:4],
    }

    explanations = summary["positive_flags"][:2] or ["近期没有显著正面新闻催化"]
    risks = summary["risk_flags"][:2] or ["近期没有显著负面新闻冲击，新闻层暂不下调"]
    return summary, explanations, risks


def _calculate_metrics(df: pd.DataFrame) -> tuple[pd.DataFrame, dict, pd.Series]:
    if len(df) < 60:
        raise DataFetchError("历史数据不足，至少需要 60 个交易日。")

    df = df.copy()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["rsi14"] = _compute_rsi(df["close"], 14)
    df["dif"], df["dea"], df["macd_hist"] = _compute_macd(df["close"])

    support, resistance = _support_resistance(df)
    latest = df.iloc[-1]
    prev_macd_hist = float(df["macd_hist"].iloc[-2]) if len(df) >= 2 else float(df["macd_hist"].iloc[-1])
    volume_ma10 = df["volume"].rolling(10).mean().iloc[-1]
    atr14 = ((df["high"] - df["low"]) / df["close"].shift(1)).rolling(14).mean().iloc[-1]
    intraday_amplitude = ((df["high"] - df["low"]) / df["open"]).iloc[-1]

    metrics = {
        "close": float(latest["close"]),
        "return_5d": _safe_float(df["close"].pct_change(5).iloc[-1]),
        "return_20d": _safe_float(df["close"].pct_change(20).iloc[-1]),
        "ma20": _safe_float(latest["ma20"]),
        "ma60": _safe_float(latest["ma60"]),
        "volume_ratio_10d": _safe_float(latest["volume"] / volume_ma10, default=1.0) if pd.notna(volume_ma10) and volume_ma10 else 1.0,
        "recent_high_20d": float(df["high"].tail(20).max()),
        "recent_low_20d": float(df["low"].tail(20).min()),
        "support": support,
        "resistance": resistance,
        "rsi14": _safe_float(latest["rsi14"], default=50.0),
        "dif": _safe_float(latest["dif"]),
        "dea": _safe_float(latest["dea"]),
        "macd_hist": _safe_float(latest["macd_hist"]),
        "macd_hist_prev": prev_macd_hist,
        "atr14": _safe_float(atr14),
        "intraday_amplitude": _safe_float(intraday_amplitude),
        "analysis_date": pd.to_datetime(df.index[-1]),
    }
    return df, metrics, latest


def _compute_trend_score(metrics: dict, latest: pd.Series) -> tuple[int, list[str]]:
    score = 50.0
    explanations = []

    if metrics["ma20"] > 0 and metrics["ma60"] > 0:
        if latest["close"] > metrics["ma20"]:
            score += 12
            explanations.append("股价站上 20 日均线")
        else:
            score -= 12
            explanations.append("股价跌破 20 日均线")

        if metrics["ma20"] > metrics["ma60"]:
            score += 14
            explanations.append("20 日均线位于 60 日均线上方")
        else:
            score -= 10
            explanations.append("20 日均线尚未站上 60 日均线")

    if metrics["return_20d"] > 0.12:
        score += 12
        explanations.append(f"近 20 日涨幅 {metrics['return_20d']:.2%}，趋势较强")
    elif metrics["return_20d"] > 0:
        score += 6
        explanations.append(f"近 20 日上涨 {metrics['return_20d']:.2%}，趋势偏强")
    else:
        score -= min(15, abs(metrics["return_20d"]) * 100)
        explanations.append(f"近 20 日回撤 {metrics['return_20d']:.2%}")

    if metrics["dif"] > metrics["dea"] and metrics["macd_hist"] > 0:
        score += 8
        explanations.append("MACD 金叉区间内，动能偏强")
    elif metrics["dif"] < metrics["dea"]:
        score -= 8
        explanations.append("MACD 偏弱，短线动能回落")

    if 45 <= metrics["rsi14"] <= 70:
        score += 6
        explanations.append(f"RSI {metrics['rsi14']:.1f}，趋势与热度较均衡")
    elif metrics["rsi14"] > 75:
        score -= 6
        explanations.append(f"RSI {metrics['rsi14']:.1f}，短线偏热")
    elif metrics["rsi14"] < 35:
        score -= 3
        explanations.append(f"RSI {metrics['rsi14']:.1f}，走势偏弱或仍在修复")

    if metrics["volume_ratio_10d"] > 1.3:
        score += 6
        explanations.append(f"量比 {metrics['volume_ratio_10d']:.2f}，资金活跃")
    elif metrics["volume_ratio_10d"] < 0.8:
        score -= 4
        explanations.append(f"量比 {metrics['volume_ratio_10d']:.2f}，量能不足")

    score = max(0, min(100, round(score)))
    return score, explanations


def _build_recommendation(latest: pd.Series, metrics: dict) -> tuple[str, list[str], list[str]]:
    bullish = 0
    reasons = []
    risks = []

    if latest["close"] > metrics["ma20"]:
        bullish += 1
        reasons.append("股价站上 20 日均线，短线趋势偏强")
    else:
        risks.append("股价跌破 20 日均线，短线走弱")

    if metrics["ma20"] > metrics["ma60"]:
        bullish += 1
        reasons.append("20 日均线高于 60 日均线，中期结构偏多")
    else:
        risks.append("20 日均线未能站上 60 日均线，中期趋势一般")

    if metrics["return_20d"] > 0:
        bullish += 1
        reasons.append(f"近 20 日累计上涨 {metrics['return_20d']:.2%}")
    else:
        risks.append(f"近 20 日累计回撤 {metrics['return_20d']:.2%}")

    if metrics["volume_ratio_10d"] > 1:
        bullish += 1
        reasons.append(f"量比 {metrics['volume_ratio_10d']:.2f}，交投活跃")
    else:
        risks.append(f"量比 {metrics['volume_ratio_10d']:.2f}，放量不明显")

    if metrics["macd_hist"] > 0 and metrics["dif"] > metrics["dea"]:
        bullish += 1
        reasons.append("MACD 位于零轴上方且 DIF 高于 DEA，趋势动能偏强")
    else:
        risks.append("MACD 动能不足，趋势可能转弱")

    if metrics["rsi14"] >= 70:
        risks.append(f"RSI {metrics['rsi14']:.1f}，短线偏热，注意冲高回落")
    elif metrics["rsi14"] <= 30:
        reasons.append(f"RSI {metrics['rsi14']:.1f}，短线超跌后存在修复机会")
    else:
        reasons.append(f"RSI {metrics['rsi14']:.1f}，处于相对中性区间")

    if bullish >= 5:
        recommendation = "推荐关注"
    elif bullish >= 3:
        recommendation = "中性观察"
    else:
        recommendation = "暂不推荐"

    return recommendation, reasons, risks


def _build_final_recommendation(
    technical_recommendation: str,
    trend_score: int,
    fundamental_score: int,
    technical_reasons: list[str],
    technical_risks: list[str],
    fundamental_explanations: list[str],
    fundamental_risks: list[str],
    market_sentiment_score: int,
    market_sentiment_state: str,
    market_sentiment_explanations: list[str],
    market_sentiment_risks: list[str],
    event_score: int,
    event_state: str,
    event_explanations: list[str],
    event_risks: list[str],
    industry_score: int | None = None,
    industry_conclusion: str | None = None,
    industry_positive_flags: list[str] | None = None,
    industry_risk_flags: list[str] | None = None,
) -> tuple[str, list[str], list[str], str]:
    reasons = list(technical_reasons)
    risks = list(technical_risks)
    reasons.extend(fundamental_explanations[:2])
    risks.extend(fundamental_risks[:2])
    reasons.extend(market_sentiment_explanations[:2])
    risks.extend(market_sentiment_risks[:2])
    reasons.extend(event_explanations[:2])
    risks.extend(event_risks[:2])
    reasons.extend((industry_positive_flags or [])[:2])
    risks.extend((industry_risk_flags or [])[:2])

    if technical_recommendation == "推荐关注":
        if trend_score >= 75 and fundamental_score >= 65:
            recommendation = "推荐关注"
            basis = "技术面和基本面同时给出正向信号"
        elif trend_score >= 75 and fundamental_score < 45:
            risks.append("技术面偏强，但基本面保护不足")
            recommendation = "中性观察"
            basis = "技术面强，但基本面不足以支持积极推荐"
        else:
            recommendation = "中性观察"
            basis = "技术面偏好，但基本面暂时不足以进一步确认"
    elif technical_recommendation == "中性观察":
        if trend_score >= 70 and fundamental_score >= 75:
            reasons.append("基本面质地较好，可先列入重点跟踪")
            recommendation = "推荐关注"
            basis = "基本面显著加分，技术面也不弱"
        else:
            recommendation = "中性观察"
            basis = "技术面未给出足够明确的买入信号"
    elif fundamental_score >= 80 and trend_score >= 60:
        reasons.append("基本面质地较强，可作为中长期跟踪对象")
        recommendation = "中性观察"
        basis = "技术面还没有给出买点，但基本面较好"
    else:
        recommendation = "暂不推荐"
        basis = "技术面信号不足，基本面也没有给出充分增强"

    if market_sentiment_state == "偏弱":
        if recommendation == "推荐关注":
            recommendation = "中性观察"
            risks.append("市场情绪偏弱，本次将积极结论下调一档")
            basis = f"{basis}；但市场情绪偏弱，最终下调一档"
        elif recommendation == "中性观察" and trend_score < 80:
            risks.append("市场情绪偏弱，中性结论需更重视仓位控制")
            basis = f"{basis}；当前处于逆风市场环境"
    elif market_sentiment_state == "偏强":
        if (
            recommendation == "中性观察"
            and technical_recommendation == "推荐关注"
            and trend_score >= 75
            and fundamental_score >= 60
            and market_sentiment_score >= 75
        ):
            reasons.append("市场情绪偏强，强势技术信号在顺风环境中更容易兑现")
            recommendation = "推荐关注"
            basis = f"{basis}；叠加市场情绪顺风，上调为推荐关注"
        elif recommendation == "暂不推荐" and trend_score >= 68 and fundamental_score >= 75:
            reasons.append("市场情绪偏强，可保留为观察对象")
            recommendation = "中性观察"
            basis = f"{basis}；但市场情绪偏强，暂上调为中性观察"

    if event_state == "偏利空":
        if recommendation == "推荐关注":
            recommendation = "中性观察"
            risks.append("近期事件偏利空，本次将积极结论下调一档")
            basis = f"{basis}；但近期事件偏利空，最终下调一档"
        elif recommendation == "中性观察" and trend_score < 82:
            recommendation = "暂不推荐"
            risks.append("近期事件偏利空，中性结论进一步下调")
            basis = f"{basis}；近期事件偏利空，下调为暂不推荐"
    elif event_state == "偏利多":
        if recommendation == "中性观察" and trend_score >= 65 and fundamental_score >= 55 and event_score >= 75:
            recommendation = "推荐关注"
            reasons.append("近期事件偏利多，催化与基本面形成共振")
            basis = f"{basis}；叠加事件面偏利多，上调为推荐关注"
        elif recommendation == "暂不推荐" and trend_score >= 60 and fundamental_score >= 70 and event_score >= 80:
            recommendation = "中性观察"
            reasons.append("近期事件催化较强，保留为观察对象")
            basis = f"{basis}；事件面偏利多，暂上调为中性观察"

    if industry_score is not None:
        if industry_conclusion:
            reasons.append(f"行业横向比较: {industry_conclusion}")
        if industry_score >= 75:
            if recommendation == "中性观察" and trend_score >= 68 and fundamental_score >= 60:
                recommendation = "推荐关注"
                basis = f"{basis}；叠加行业横向分 {industry_score}/100，行业位置较强，上调一档"
            elif recommendation == "暂不推荐" and trend_score >= 62 and fundamental_score >= 78:
                recommendation = "中性观察"
                basis = f"{basis}；行业横向分 {industry_score}/100 较强，保留为中性观察"
        elif industry_score <= 30:
            if recommendation == "推荐关注" and not (trend_score >= 85 and fundamental_score >= 80):
                recommendation = "中性观察"
                basis = f"{basis}；但行业横向分仅 {industry_score}/100，行业相对位置偏弱，下调一档"
            elif recommendation == "中性观察" and fundamental_score < 60:
                recommendation = "暂不推荐"
                basis = f"{basis}；行业横向分仅 {industry_score}/100，缺少行业相对优势，下调为暂不推荐"

    return recommendation, reasons, risks, basis


def _extract_industry_comparison_view(comparison_results: list[dict] | None) -> tuple[int | None, str | None, list[str], list[str], bool]:
    if not comparison_results:
        return None, None, [], [], False

    industry_results = [
        item
        for item in comparison_results
        if str(item.get("name") or "").startswith("industry_") and item.get("available")
    ]
    if not industry_results:
        return None, None, [], [], False

    plugin_weights = {
        "industry_peers": 0.35,
        "industry_valuation": 0.20,
        "industry_growth": 0.25,
        "industry_heat": 0.20,
    }
    weighted_scores: list[tuple[float, float]] = []
    conclusions: list[str] = []
    positive_flags: list[str] = []
    risk_flags: list[str] = []

    for result in industry_results:
        raw_score = pd.to_numeric(result.get("score"), errors="coerce")
        if pd.notna(raw_score):
            weighted_scores.append((float(raw_score), plugin_weights.get(str(result.get("name")), 0.0)))
        conclusion = str(result.get("conclusion") or "").strip()
        if conclusion:
            conclusions.append(conclusion)
        positive_flags.extend([str(item) for item in (result.get("positive_flags") or []) if str(item).strip()])
        risk_flags.extend([str(item) for item in (result.get("risk_flags") or []) if str(item).strip()])

    if weighted_scores:
        total_weight = sum(weight for _, weight in weighted_scores if weight > 0)
        if total_weight > 0:
            score = int(round(sum(value * weight for value, weight in weighted_scores) / total_weight))
        else:
            score = int(round(sum(value for value, _ in weighted_scores) / len(weighted_scores)))
    else:
        score = None

    conclusion = "；".join(conclusions[:3]) if conclusions else None
    return score, conclusion, positive_flags, risk_flags, True


def _build_sell_guidance(latest: pd.Series, metrics: dict) -> list[str]:
    guidance = []
    high_volatility = metrics["atr14"] > 0.04 or metrics["intraday_amplitude"] > 0.05

    if latest["close"] < metrics["support"]:
        guidance.append("股价已经跌破近 20 日支撑位，若持仓偏短线可考虑明显减仓或止损。")
    else:
        guidance.append(f"当前支撑位大致在 {metrics['support']:.2f} 附近，跌破后要提高止损警惕。")

    if latest["close"] >= metrics["resistance"] * 0.98:
        guidance.append(f"股价已接近近 20 日压力位 {metrics['resistance']:.2f}，若放量滞涨可考虑分批止盈。")
    else:
        guidance.append(f"上方压力位大致在 {metrics['resistance']:.2f}，接近该区域时要观察是否放量突破。")

    if high_volatility and latest["close"] > metrics["ma20"] and latest["close"] > metrics["support"]:
        guidance.append("当天波动偏大，但股价仍站在 20 日线和支撑位上方，先不要因为单日震荡误判成必须减仓。")
    elif metrics["rsi14"] > 75 and metrics["macd_hist"] < metrics["macd_hist_prev"]:
        guidance.append("RSI 偏热且 MACD 红柱缩短，更适合分批卖出而不是一次性清仓。")
    elif latest["close"] < metrics["ma20"] and metrics["dif"] < metrics["dea"]:
        guidance.append("跌破 20 日均线且 MACD 死叉，若没有明确基本面支撑，更适合先减仓。")
    else:
        guidance.append("只要股价仍在 20 日均线之上且 MACD 未明显转弱，可以继续持有并跟踪。")

    return guidance


def _analyze_accumulation(metrics: dict, latest: pd.Series) -> tuple[str, list[str], int]:
    signals = []
    score = 0

    if metrics["volume_ratio_10d"] > 1.3:
        score += 25
        signals.append(f"量比 {metrics['volume_ratio_10d']:.2f}，近期成交明显放大")

    if metrics["return_20d"] > 0 and latest["close"] > metrics["ma20"] > metrics["ma60"]:
        score += 25
        signals.append("价格沿均线抬升，呈现边整理边吸筹特征")

    if metrics["rsi14"] < 70 and metrics["dif"] > metrics["dea"] and metrics["macd_hist"] > 0:
        score += 25
        signals.append("MACD 偏强且 RSI 未极端过热，资金推动较平稳")

    if latest["close"] > metrics["support"] * 1.03:
        score += 25
        signals.append("股价脱离近期支撑区，说明下方承接相对稳定")

    if score >= 75:
        conclusion = "存在一定量价共振吸筹迹象，但这仍属于量价代理信号"
    elif score >= 50:
        conclusion = "出现部分量价共振信号，但还不足以视为很强的吸筹代理"
    else:
        conclusion = "暂未看到很明显的量价共振吸筹代理迹象"

    return conclusion, signals, score


def _build_sell_plan(metrics: dict, latest: pd.Series) -> list[dict]:
    support_buffer = metrics["support"] * 0.99
    resistance_near = metrics["resistance"] * 0.98

    plan = [
        {
            "触发条件": f"跌破 20 日线 {metrics['ma20']:.2f}",
            "建议动作": "减仓 30%",
            "原因": "短线趋势转弱，先把弹性仓位降下来。若只是单日大振幅但收盘仍站回 20 日线，可先观察一天。",
        },
        {
            "触发条件": f"跌破 60 日线 {metrics['ma60']:.2f}",
            "建议动作": "再减仓 40%",
            "原因": "中期趋势被破坏，继续控制回撤。",
        },
        {
            "触发条件": f"接近压力位 {metrics['resistance']:.2f}",
            "建议动作": "分批止盈 20%-30%",
            "原因": "容易遇到前高抛压，适合先锁定一部分利润。",
        },
        {
            "触发条件": f"跌破支撑位 {support_buffer:.2f}",
            "建议动作": "保守型可清仓或只保留底仓",
            "原因": "近期承接失效，防止下跌进一步扩大。",
        },
    ]

    if latest["close"] > resistance_near and metrics["volume_ratio_10d"] > 1.2:
        plan.append(
            {
                "触发条件": "放量突破压力位后回踩不破",
                "建议动作": "继续持有，暂缓止盈",
                "原因": "若突破真实成立，说明趋势延续概率更高。",
            }
        )
    elif metrics["dif"] < metrics["dea"] and latest["close"] < metrics["ma20"]:
        plan.append(
            {
                "触发条件": "MACD 死叉且股价位于 20 日线下方",
                "建议动作": "优先执行减仓计划",
                "原因": "说明短线趋势与动能同时走弱，不宜硬扛。",
            }
        )

    return plan


def _build_add_position_guidance(metrics: dict, latest: pd.Series) -> list[dict]:
    guidance = []

    if latest["close"] > metrics["ma20"] > metrics["ma60"] and metrics["dif"] > metrics["dea"]:
        guidance.append(
            {
                "触发条件": "回踩 20 日线附近但不破位",
                "建议动作": "可小幅加仓 10%-20%",
                "原因": "趋势仍强，回踩更像正常整理而不是转弱。",
            }
        )
    if latest["close"] > metrics["resistance"] and metrics["volume_ratio_10d"] > 1.2:
        guidance.append(
            {
                "触发条件": "放量突破压力位后站稳",
                "建议动作": "可顺势加仓 10%-15%",
                "原因": "说明资金接力存在，趋势可能继续扩展。",
            }
        )
    if metrics["rsi14"] < 40 and latest["close"] > metrics["support"] and metrics["dif"] >= metrics["dea"]:
        guidance.append(
            {
                "触发条件": "低位企稳并出现 MACD 修复",
                "建议动作": "适合试探性加仓",
                "原因": "更像回调后的再启动，而不是高位追涨。",
            }
        )
    if not guidance:
        guidance.append(
            {
                "触发条件": "当前不建议主动加仓",
                "建议动作": "先观察",
                "原因": "趋势或量能还不够顺，贸然加仓容易把成本抬高。",
            }
        )

    return guidance


def _build_volatility_note(metrics: dict, latest: pd.Series) -> str:
    if (metrics["atr14"] > 0.04 or metrics["intraday_amplitude"] > 0.05) and latest["close"] > metrics["ma20"]:
        return "近期波动偏大，但只要收盘仍在 20 日线和支撑位上方，更像高波动洗盘，不宜因单日大阴大阳就机械减仓。"
    if metrics["atr14"] > 0.05 and latest["close"] < metrics["ma20"]:
        return "近期波动偏大且已经跌破 20 日线，这种情况下要更严格执行减仓计划。"
    return "近期波动处于可接受区间，可以按趋势与支撑压力位来管理仓位。"


def _build_hold_or_sell_view(metrics: dict, trend_score: int, latest: pd.Series) -> str:
    if trend_score >= 75 and latest["close"] > metrics["ma20"] and metrics["dif"] > metrics["dea"]:
        return "更适合继续持有，只有在接近压力位或趋势转弱时再做分批止盈。"
    if trend_score >= 55:
        return "适合边走边看，按照分批卖出计划管理仓位，不建议一次性卖光。"
    if latest["close"] < metrics["ma20"] and metrics["dif"] < metrics["dea"]:
        return "更适合先执行减仓，再观察是否失守 60 日线。"
    return "趋势保护不足，若已有盈利可优先落袋，若被套则更要严格执行止损。"


def _risk_level_from_score(score: float, reverse: bool = False) -> str:
    score = float(score)
    if reverse:
        if score >= 75:
            return "低"
        if score >= 55:
            return "中"
        return "高"
    if score <= 35:
        return "低"
    if score <= 65:
        return "中"
    return "高"


def _build_risk_committee_summary(
    metrics: dict,
    trend_score: int,
    fundamental_score: int,
    event_score: int,
    event_state: str,
    industry_score: int | None,
    market_sentiment_state: str,
    valuation_snapshot: dict | None,
) -> dict:
    close = metrics["close"]
    market_value = pd.to_numeric((valuation_snapshot or {}).get("market_value"), errors="coerce")

    trend_risk_score = 25.0 if trend_score >= 75 else 55.0 if trend_score >= 55 else 78.0
    if close < metrics["ma20"]:
        trend_risk_score += 10
    if metrics["dif"] < metrics["dea"]:
        trend_risk_score += 8
    trend_risk_score = max(0.0, min(100.0, trend_risk_score))

    fundamental_risk_score = 100.0 - float(fundamental_score)
    if pd.notna(market_value) and float(market_value) < 5_000_000_000:
        fundamental_risk_score += 8
    fundamental_risk_score = max(0.0, min(100.0, fundamental_risk_score))

    if event_state == "偏利空":
        event_risk_score = max(60.0, 100.0 - float(event_score))
    elif event_state == "偏利多":
        event_risk_score = max(10.0, 50.0 - float(event_score) * 0.3)
    else:
        event_risk_score = max(20.0, 60.0 - float(event_score) * 0.3)
    event_risk_score = max(0.0, min(100.0, event_risk_score))

    industry_effective_score = 50 if industry_score is None else float(industry_score)
    industry_risk_score = 100.0 - industry_effective_score
    if market_sentiment_state == "偏弱":
        industry_risk_score += 10
    industry_risk_score = max(0.0, min(100.0, industry_risk_score))

    overall_risk_score = (
        trend_risk_score * 0.35
        + fundamental_risk_score * 0.25
        + event_risk_score * 0.20
        + industry_risk_score * 0.20
    )

    return {
        "headline": "这层不是再给结论打分，而是把趋势、基本面、事件和行业/组合暴露拆成风险维度做二次审视。",
        "overall_level": _risk_level_from_score(overall_risk_score),
        "trend_risk": {
            "level": _risk_level_from_score(trend_risk_score),
            "summary": (
                "股价仍在 20 日线和关键支撑位上方，趋势保护尚可。"
                if trend_risk_score <= 35
                else "趋势保护一般，短线需要继续跟踪均线和 MACD 动能。"
                if trend_risk_score <= 65
                else "趋势保护偏弱，均线和动能已出现同步走弱迹象。"
            ),
        },
        "fundamental_risk": {
            "level": _risk_level_from_score(fundamental_risk_score),
            "summary": (
                "基本面提供了较好的质量保护。"
                if fundamental_risk_score <= 35
                else "基本面保护中性，暂时不构成强支撑也不算明显拖累。"
                if fundamental_risk_score <= 65
                else "基本面保护偏弱，若趋势转弱会缺少足够缓冲。"
            ),
        },
        "event_risk": {
            "level": _risk_level_from_score(event_risk_score),
            "summary": (
                "近期事件面对结论没有明显负反馈。"
                if event_risk_score <= 35
                else "近期事件整体中性，更多起到解释作用。"
                if event_risk_score <= 65
                else "近期事件面对结论存在明显扰动，需要提高警惕。"
            ),
        },
        "industry_portfolio_risk": {
            "level": _risk_level_from_score(industry_risk_score),
            "summary": (
                "行业相对位置和市场环境较友好，叠加到组合层时压力不大。"
                if industry_risk_score <= 35
                else "行业位置中性，单票可以跟踪，但放进组合后仍要看集中度。"
                if industry_risk_score <= 65
                else "行业相对位置或当前市场环境偏弱，进入组合时要更重视暴露控制。"
            ),
        },
    }


def _build_research_workflow_summary(
    recommendation: str,
    final_decision_basis: str,
    technical_reasons: list[str],
    reasons: list[str],
    risks: list[str],
    metrics: dict,
    market_sentiment_state: str,
    event_state: str,
    industry_conclusion: str | None,
    hold_or_sell_view: str,
) -> dict:
    bullish_points = list(dict.fromkeys(technical_reasons[:2] + reasons[:3]))
    bearish_points = list(dict.fromkeys(risks[:4]))

    if recommendation == "推荐关注":
        stance = "正向研究"
        action_bias = "更适合顺趋势跟踪，等待回踩确认或突破后的执行点。"
        next_review_window = "1-3 个交易日内复核量能、均线和事件变化。"
    elif recommendation == "中性观察":
        stance = "观察研究"
        action_bias = "先放入重点观察池，等趋势或催化进一步明确。"
        next_review_window = "3-5 个交易日内复核是否出现新的催化或趋势确认。"
    else:
        stance = "保守研究"
        action_bias = "当前不适合积极配置，优先观察风险是否缓释。"
        next_review_window = "等待趋势修复或基本面/事件出现新的改善信号。"

    thesis_parts = [final_decision_basis]
    thesis_parts.append(hold_or_sell_view)
    if industry_conclusion:
        thesis_parts.append(f"行业侧补充：{industry_conclusion}")

    tracking_indicators = [
        {
            "跟踪项": "趋势与位置",
            "当前状态": f"收盘 {metrics['close']:.2f} / MA20 {metrics['ma20']:.2f} / MA60 {metrics['ma60']:.2f}",
            "关注点": "确认股价是否继续站稳 20 日线并维持中期多头结构。",
        },
        {
            "跟踪项": "量能与动能",
            "当前状态": f"量比 {metrics['volume_ratio_10d']:.2f} / RSI {metrics['rsi14']:.1f} / MACD柱 {metrics['macd_hist']:.3f}",
            "关注点": "观察放量是否持续，以及 MACD 动能是否继续扩张。",
        },
        {
            "跟踪项": "市场与事件",
            "当前状态": f"市场情绪 {market_sentiment_state} / 事件状态 {event_state}",
            "关注点": "顺风市场和正向催化更利于信号兑现，逆风环境要降预期。",
        },
    ]

    invalidation_conditions = [
        f"若股价跌破 20 日线 {metrics['ma20']:.2f} 且 MACD 同步转弱，本轮趋势假设需要重审。",
        f"若股价跌破支撑位 {metrics['support']:.2f} 附近，说明下方承接失效，原有交易节奏应失效。",
    ]
    if event_state == "偏利空":
        invalidation_conditions.append("若新的负面公告继续落地，事件面会优先否定当前观察逻辑。")
    if market_sentiment_state == "偏弱":
        invalidation_conditions.append("若市场继续处于逆风环境，即便个股自身不差，也要降低执行强度。")

    return {
        "stance": stance,
        "action_bias": action_bias,
        "next_review_window": next_review_window,
        "investment_thesis": "；".join(part for part in thesis_parts if part),
        "bullish_points": bullish_points,
        "bearish_points": bearish_points,
        "tracking_indicators": tracking_indicators,
        "invalidation_conditions": invalidation_conditions,
    }


def _evaluation_level(score: float, high: float = 75.0, medium: float = 55.0) -> str:
    score = float(score)
    if score >= high:
        return "强支撑"
    if score >= medium:
        return "中性"
    return "拖累项"


def _build_evaluation_framework_summary(
    recommendation: str,
    technical_recommendation: str,
    trend_score: int,
    fundamental_score: int,
    market_sentiment_score: int,
    market_sentiment_state: str,
    event_score: int,
    event_state: str,
    industry_score: int | None,
    risk_committee_summary: dict,
) -> dict:
    risk_level = str(risk_committee_summary.get("overall_level") or "中")
    risk_inverse_score = {"低": 85.0, "中": 60.0, "高": 30.0}.get(risk_level, 60.0)
    effective_industry_score = 50.0 if industry_score is None else float(industry_score)

    items = [
        {
            "维度": "技术面",
            "当前判断": technical_recommendation,
            "评分": int(trend_score),
            "角色": "趋势、均线、动量与量价结构",
            "结论": _evaluation_level(trend_score),
        },
        {
            "维度": "基本面",
            "当前判断": "质量支撑" if fundamental_score >= 60 else "中性" if fundamental_score >= 45 else "偏弱",
            "评分": int(fundamental_score),
            "角色": "盈利质量、现金流、负债与估值保护",
            "结论": _evaluation_level(fundamental_score),
        },
        {
            "维度": "市场情绪",
            "当前判断": market_sentiment_state,
            "评分": int(market_sentiment_score),
            "角色": "市场顺风 / 逆风环境校正",
            "结论": _evaluation_level(market_sentiment_score, high=70.0, medium=45.0),
        },
        {
            "维度": "事件驱动",
            "当前判断": event_state,
            "评分": int(event_score),
            "角色": "公告、预告、披露与事件催化",
            "结论": _evaluation_level(event_score, high=70.0, medium=45.0),
        },
        {
            "维度": "行业比较",
            "当前判断": "行业内偏强" if effective_industry_score >= 65 else "行业内居中" if effective_industry_score >= 45 else "行业内偏弱",
            "评分": int(round(effective_industry_score)),
            "角色": "同行质量与相对位置",
            "结论": _evaluation_level(effective_industry_score, high=65.0, medium=45.0),
        },
        {
            "维度": "风险校正",
            "当前判断": f"风险等级 {risk_level}",
            "评分": int(round(risk_inverse_score)),
            "角色": "把趋势、基本面、事件和行业暴露做二次约束",
            "结论": _evaluation_level(risk_inverse_score, high=75.0, medium=55.0),
        },
    ]

    overall_score = round(
        trend_score * 0.26
        + fundamental_score * 0.22
        + market_sentiment_score * 0.14
        + event_score * 0.14
        + effective_industry_score * 0.12
        + risk_inverse_score * 0.12
    )

    return {
        "headline": "这层借鉴了 TradingAgents-CN 的 analyst 分维思路，但继续使用当前项目自己的评分与推荐内核，不直接照搬多智能体辩论。",
        "overall_score": int(overall_score),
        "overall_stance": recommendation,
        "items": items,
    }


def _build_target_price_scenarios(
    metrics: dict,
    recommendation: str,
    fundamental_score: int,
    market_sentiment_state: str,
    event_state: str,
    risk_committee_summary: dict,
) -> dict:
    close = float(metrics["close"])
    risk_level = str(risk_committee_summary.get("overall_level") or "中")
    if recommendation == "推荐关注":
        base_upside = 0.14
        bull_upside = 0.24
        bear_change = -0.06
    elif recommendation == "中性观察":
        base_upside = 0.07
        bull_upside = 0.14
        bear_change = -0.08
    else:
        base_upside = 0.02
        bull_upside = 0.08
        bear_change = -0.12

    if fundamental_score >= 70:
        base_upside += 0.03
        bull_upside += 0.04
    elif fundamental_score <= 40:
        base_upside -= 0.03
        bull_upside -= 0.04

    if market_sentiment_state == "偏强":
        base_upside += 0.02
        bull_upside += 0.02
    elif market_sentiment_state == "偏弱":
        base_upside -= 0.02
        bull_upside -= 0.03

    if event_state == "偏利多":
        bull_upside += 0.03
    elif event_state == "偏利空":
        base_upside -= 0.03
        bear_change -= 0.02

    if risk_level == "高":
        base_upside -= 0.03
        bull_upside -= 0.05
        bear_change -= 0.03
    elif risk_level == "低":
        base_upside += 0.01

    base_upside = max(0.01, base_upside)
    bull_upside = max(base_upside + 0.04, bull_upside)
    bear_change = min(-0.02, bear_change)

    scenarios = [
        {
            "情景": "保守",
            "目标价": round(close * (1 + bear_change), 2),
            "涨跌幅": f"{bear_change:.1%}",
            "时间范围": "1-4 周",
            "触发条件": "市场转弱、事件扰动或趋势跌破支撑位时参考该情景。",
        },
        {
            "情景": "基准",
            "目标价": round(close * (1 + base_upside), 2),
            "涨跌幅": f"{base_upside:.1%}",
            "时间范围": "1-3 个月",
            "触发条件": "当前研究结论按正常兑现节奏推进时参考该情景。",
        },
        {
            "情景": "乐观",
            "目标价": round(close * (1 + bull_upside), 2),
            "涨跌幅": f"{bull_upside:.1%}",
            "时间范围": "3-6 个月",
            "触发条件": "趋势延续、基本面验证和事件催化同时增强时参考该情景。",
        },
    ]

    return {
        "headline": "这层借鉴了 TradingAgents-CN 的目标价拆解方式，但改成规则化情景推演，避免对单一模型输出过度依赖。",
        "currency": "CNY",
        "current_price": round(close, 2),
        "base_case_price": scenarios[1]["目标价"],
        "bull_case_price": scenarios[2]["目标价"],
        "bear_case_price": scenarios[0]["目标价"],
        "items": scenarios,
    }


def _build_execution_plan_summary(
    recommendation: str,
    technical_recommendation: str,
    metrics: dict,
    target_price_scenarios: dict,
    risk_committee_summary: dict,
    market_sentiment_state: str,
    event_state: str,
    hold_or_sell_view: str,
) -> dict:
    risk_level = str(risk_committee_summary.get("overall_level") or "中")
    close = float(metrics["close"])
    stop_loss = round(min(metrics["support"], metrics["ma20"]) * 0.99, 2)
    first_take_profit = float(target_price_scenarios.get("base_case_price") or close)
    second_take_profit = float(target_price_scenarios.get("bull_case_price") or close)

    if recommendation == "推荐关注":
        trade_action = "试探买入"
        base_position = "20%-30%"
        action_reason = "研究结论偏正向，可以开始观察买点并做小仓位试错。"
    elif recommendation == "中性观察":
        trade_action = "观察等待"
        base_position = "0%-10%"
        action_reason = "研究层还不够统一，更适合先观察而不是立即重仓。"
    else:
        trade_action = "减仓 / 回避"
        base_position = "0%"
        action_reason = "当前结论不支持积极配置，应先回避高风险执行。"

    risk_adjusted_action = trade_action
    adjusted_position = base_position
    risk_adjustment_notes = []

    if risk_level == "高":
        if trade_action == "试探买入":
            risk_adjusted_action = "观察等待"
            adjusted_position = "0%-10%"
        elif trade_action == "观察等待":
            adjusted_position = "0%"
        risk_adjustment_notes.append("整体风险等级偏高，执行动作下调一档。")
    elif risk_level == "中" and trade_action == "试探买入":
        adjusted_position = "10%-20%"
        risk_adjustment_notes.append("整体风险中性，仓位建议从进攻型下调到试错型。")

    if market_sentiment_state == "偏弱":
        risk_adjustment_notes.append("市场处于逆风环境，优先等确认而不是追高。")
    if event_state == "偏利空":
        risk_adjustment_notes.append("事件层存在负反馈，目标价与仓位都应保守处理。")
    if technical_recommendation == "减仓防守" and risk_adjusted_action == "试探买入":
        risk_adjusted_action = "观察等待"
        adjusted_position = "0%-10%"
        risk_adjustment_notes.append("技术面仍偏防守，执行动作不应过于积极。")

    confidence_score = 72.0 if recommendation == "推荐关注" else 56.0 if recommendation == "中性观察" else 38.0
    if risk_level == "高":
        confidence_score -= 10
    elif risk_level == "低":
        confidence_score += 6
    if event_state == "偏利多":
        confidence_score += 4
    elif event_state == "偏利空":
        confidence_score -= 6
    confidence_score = max(5.0, min(95.0, confidence_score))

    execution_risk_score = 70.0 if risk_level == "高" else 48.0 if risk_level == "中" else 26.0
    if market_sentiment_state == "偏弱":
        execution_risk_score += 8
    if event_state == "偏利空":
        execution_risk_score += 8
    execution_risk_score = max(0.0, min(100.0, execution_risk_score))

    trigger_to_upgrade = "放量站稳压力位、事件转正或市场情绪改善后，可把观察升级为试探买入。"
    trigger_to_downgrade = f"跌破 {stop_loss:.2f} 附近，或事件继续转弱时，应把当前计划降级为减仓 / 回避。"

    return {
        "headline": "这层把研究结论转成可执行动作，借鉴 TradingAgents-CN 的 trader / risk manager 思路，但用规则化方式实现。",
        "trade_action": trade_action,
        "risk_adjusted_action": risk_adjusted_action,
        "position_guidance": adjusted_position,
        "base_position": base_position,
        "target_price_range": f"{first_take_profit:.2f} - {second_take_profit:.2f}",
        "stop_loss": stop_loss,
        "first_take_profit": round(first_take_profit, 2),
        "second_take_profit": round(second_take_profit, 2),
        "execution_confidence": int(round(confidence_score)),
        "execution_risk_score": int(round(execution_risk_score)),
        "action_reasoning": action_reason,
        "risk_adjustment_notes": risk_adjustment_notes,
        "trigger_to_upgrade": trigger_to_upgrade,
        "trigger_to_downgrade": trigger_to_downgrade,
        "execution_commentary": hold_or_sell_view,
    }


def _build_candlestick_chart(chart_df: pd.DataFrame):
    base = alt.Chart(chart_df).encode(x=alt.X("date:T", title="日期"))
    rule = base.mark_rule().encode(
        y="low:Q",
        y2="high:Q",
        color=alt.condition("datum.close >= datum.open", alt.value("#e74c3c"), alt.value("#2ecc71")),
    )
    bar = base.mark_bar(size=6).encode(
        y="open:Q",
        y2="close:Q",
        color=alt.condition("datum.close >= datum.open", alt.value("#e74c3c"), alt.value("#2ecc71")),
    )
    ma20 = base.mark_line(color="#1f77b4").encode(y="ma20:Q")
    ma60 = base.mark_line(color="#f39c12").encode(y="ma60:Q")
    support = base.mark_rule(color="#2ecc71", strokeDash=[4, 4]).encode(y="support:Q")
    resistance = base.mark_rule(color="#e67e22", strokeDash=[4, 4]).encode(y="resistance:Q")
    return (rule + bar + ma20 + ma60 + support + resistance).properties(height=360)


def analyze_single_stock(symbol: str, name: str, prefer_cache_only: bool = False) -> dict:
    df = get_stock_data(symbol)
    try:
        df, metrics, latest = _calculate_metrics(df)
    except DataFetchError as error:
        raise DataFetchError(f"{symbol} {error}") from error

    chart_df = df.tail(90).reset_index().copy()
    chart_df["support"] = metrics["support"]
    chart_df["resistance"] = metrics["resistance"]

    technical_recommendation, technical_reasons, technical_risks = _build_recommendation(latest, metrics)
    sell_guidance = _build_sell_guidance(latest, metrics)
    accumulation_conclusion, accumulation_signals, accumulation_score = _analyze_accumulation(metrics, latest)
    trend_score, trend_explanations = _compute_trend_score(metrics, latest)
    sell_plan = _build_sell_plan(metrics, latest)
    add_position_guidance = _build_add_position_guidance(metrics, latest)
    volatility_note = _build_volatility_note(metrics, latest)
    hold_or_sell_view = _build_hold_or_sell_view(metrics, trend_score, latest)
    chart = _build_candlestick_chart(chart_df)

    fundamental_snapshot, valuation_snapshot = load_or_fetch_fundamental_snapshot(
        symbol,
        fallback_name=name,
        prefer_cache=prefer_cache_only,
    )
    industry_membership = resolve_industry_membership(
        symbol,
        valuation_snapshot=valuation_snapshot,
        max_age_days=30,
        allow_live_fetch=not prefer_cache_only,
    )
    if valuation_snapshot is None:
        valuation_snapshot = {}
    if industry_membership and not valuation_snapshot.get("industry"):
        valuation_snapshot["industry"] = industry_membership.get("industry_name")
        valuation_snapshot["industry_source"] = industry_membership.get("source")
        valuation_snapshot["industry_stale"] = industry_membership.get("stale")
    fundamental_summary = _build_fundamental_summary(fundamental_snapshot, valuation_snapshot)
    fundamental_score, fundamental_explanations, fundamental_risks = _compute_fundamental_score(
        fundamental_snapshot,
        valuation_snapshot,
    )

    market_sentiment_snapshot = load_or_fetch_market_sentiment_snapshot(prefer_cache=prefer_cache_only)
    market_sentiment_summary, market_sentiment_explanations, market_sentiment_risks = _build_market_sentiment_view(
        market_sentiment_snapshot
    )
    news_items = load_or_fetch_realtime_stock_news(
        symbol,
        fallback_name=name,
        prefer_cache=prefer_cache_only,
        lookback_hours=72,
        limit=12,
    )
    news_summary, news_explanations, news_risks = _build_news_summary(news_items)
    company_events = load_or_fetch_company_events(symbol, fallback_name=name, lookback_days=30)
    event_summary, event_explanations, event_risks = _build_event_summary(company_events)
    comparison_results, comparison_overview = build_comparison_results(
        {
            "symbol": symbol,
            "name": name,
            "fundamental_snapshot": fundamental_snapshot,
            "valuation_snapshot": valuation_snapshot,
            "market_sentiment_snapshot": market_sentiment_snapshot,
            "news_items": news_items,
            "company_events": company_events,
            "industry_membership": industry_membership,
            "metrics": metrics,
            "prefer_cache_only": prefer_cache_only,
        }
    )
    (
        industry_comparison_score,
        industry_comparison_conclusion,
        industry_positive_flags,
        industry_risk_flags,
        industry_comparison_available,
    ) = _extract_industry_comparison_view(comparison_results)

    recommendation, reasons, risks, final_decision_basis = _build_final_recommendation(
        technical_recommendation,
        trend_score,
        fundamental_score,
        technical_reasons,
        technical_risks,
        fundamental_explanations,
        fundamental_risks,
        _safe_int(market_sentiment_summary.get("score", 50), default=50),
        str(market_sentiment_summary.get("state", "中性")),
        market_sentiment_explanations,
        market_sentiment_risks,
        _safe_int(event_summary.get("score", 50), default=50),
        str(event_summary.get("state", "中性")),
        event_explanations,
        event_risks,
        industry_comparison_score,
        industry_comparison_conclusion,
        industry_positive_flags,
        industry_risk_flags,
    )
    risk_committee_summary = _build_risk_committee_summary(
        metrics,
        trend_score,
        fundamental_score,
        _safe_int(event_summary.get("score", 50), default=50),
        str(event_summary.get("state", "中性")),
        industry_comparison_score,
        str(market_sentiment_summary.get("state", "中性")),
        valuation_snapshot,
    )
    research_workflow_summary = _build_research_workflow_summary(
        recommendation,
        final_decision_basis,
        technical_reasons,
        reasons,
        risks,
        metrics,
        str(market_sentiment_summary.get("state", "中性")),
        str(event_summary.get("state", "中性")),
        industry_comparison_conclusion,
        hold_or_sell_view,
    )
    evaluation_framework_summary = _build_evaluation_framework_summary(
        recommendation,
        technical_recommendation,
        trend_score,
        fundamental_score,
        _safe_int(market_sentiment_summary.get("score", 50), default=50),
        str(market_sentiment_summary.get("state", "中性")),
        _safe_int(event_summary.get("score", 50), default=50),
        str(event_summary.get("state", "中性")),
        industry_comparison_score,
        risk_committee_summary,
    )
    target_price_scenarios = _build_target_price_scenarios(
        metrics,
        recommendation,
        fundamental_score,
        str(market_sentiment_summary.get("state", "中性")),
        str(event_summary.get("state", "中性")),
        risk_committee_summary,
    )
    execution_plan_summary = _build_execution_plan_summary(
        recommendation,
        technical_recommendation,
        metrics,
        target_price_scenarios,
        risk_committee_summary,
        str(market_sentiment_summary.get("state", "中性")),
        str(event_summary.get("state", "中性")),
        hold_or_sell_view,
    )
    result = {
        "symbol": symbol,
        "name": name,
        "metrics": metrics,
        "recommendation": recommendation,
        "technical_recommendation": technical_recommendation,
        "reasons": reasons,
        "risks": risks,
        "sell_guidance": sell_guidance,
        "sell_plan": sell_plan,
        "add_position_guidance": add_position_guidance,
        "volatility_note": volatility_note,
        "hold_or_sell_view": hold_or_sell_view,
        "accumulation_conclusion": accumulation_conclusion,
        "accumulation_signals": accumulation_signals,
        "accumulation_score": accumulation_score,
        "trend_score": trend_score,
        "trend_explanations": trend_explanations,
        "fundamental_score": fundamental_score,
        "fundamental_explanations": fundamental_explanations,
        "fundamental_risks": fundamental_risks,
        "market_sentiment_snapshot": market_sentiment_snapshot,
        "market_sentiment_summary": market_sentiment_summary,
        "market_sentiment_score": _safe_int(market_sentiment_summary.get("score", 50), default=50),
        "market_sentiment_state": str(market_sentiment_summary.get("state", "中性")),
        "market_sentiment_explanations": market_sentiment_explanations,
        "market_sentiment_risks": market_sentiment_risks,
        "news_items": news_items,
        "news_summary": news_summary,
        "news_score": _safe_int(news_summary.get("score", 50), default=50),
        "news_state": str(news_summary.get("state", "中性")),
        "news_explanations": news_explanations,
        "news_risks": news_risks,
        "company_events": company_events,
        "event_summary": event_summary,
        "event_score": _safe_int(event_summary.get("score", 50), default=50),
        "event_state": str(event_summary.get("state", "中性")),
        "event_explanations": event_explanations,
        "event_risks": event_risks,
        "final_decision_basis": final_decision_basis,
        "research_workflow_summary": research_workflow_summary,
        "risk_committee_summary": risk_committee_summary,
        "evaluation_framework_summary": evaluation_framework_summary,
        "target_price_scenarios": target_price_scenarios,
        "execution_plan_summary": execution_plan_summary,
        "chart": chart,
        "data_source": df.attrs.get("api_source", "unknown"),
        "fundamental_snapshot": fundamental_snapshot,
        "valuation_snapshot": valuation_snapshot,
        "industry_membership": industry_membership,
        "fundamental_summary": fundamental_summary,
        "comparison_results": comparison_results,
        "comparison_overview": comparison_overview,
        "industry_comparison_score": industry_comparison_score,
        "industry_comparison_conclusion": industry_comparison_conclusion,
        "industry_comparison_available": industry_comparison_available,
    }
    result["data_source_summary"] = build_analysis_data_source_summary(result)
    return result


def _screen_one_accumulation_candidate(row: pd.Series) -> dict | None:
    symbol = str(row["code"]).zfill(6)
    name = str(row.get("name", symbol))
    try:
        df = get_stock_data(symbol)
        _, metrics, latest = _calculate_metrics(df)
        conclusion, signals, accumulation_score = _analyze_accumulation(metrics, latest)
        trend_score, _ = _compute_trend_score(metrics, latest)
    except DataFetchError:
        return None

    if accumulation_score < 50:
        return None

    return {
        "symbol": symbol,
        "name": name,
        "display_name": f"{symbol} {name}",
        "量价代理评分": accumulation_score,
        "趋势评分": trend_score,
        "20日动量": metrics["return_20d"],
        "量比": metrics["volume_ratio_10d"],
        "RSI": metrics["rsi14"],
        "MACD柱": metrics["macd_hist"],
        "结论": conclusion,
        "信号摘要": "；".join(signals[:3]),
    }


def screen_accumulation_candidates(scan_limit: int = 500, top_k: int = 20) -> pd.DataFrame:
    catalog = get_stock_catalog(limit=scan_limit)
    rows = [row for _, row in catalog.iterrows()]
    results = []

    with ThreadPoolExecutor(max_workers=min(12, max(4, scan_limit // 40))) as executor:
        future_map = {executor.submit(_screen_one_accumulation_candidate, row): row["code"] for row in rows}
        for future in as_completed(future_map):
            item = future.result()
            if item:
                results.append(item)

    if not results:
        return pd.DataFrame(
            columns=["display_name", "量价代理评分", "趋势评分", "20日动量", "量比", "RSI", "MACD柱", "结论", "信号摘要"]
        )

    result_df = pd.DataFrame(results).sort_values(
        by=["量价代理评分", "趋势评分", "20日动量", "量比"],
        ascending=False,
    )
    return result_df.head(top_k).reset_index(drop=True)


def recommend_growth_candidates(scan_limit: int = 300, top_k: int = 10, target_return: float = 0.30) -> pd.DataFrame:
    catalog = get_stock_catalog(limit=scan_limit)
    rows = [row for _, row in catalog.iterrows()]
    results = []

    with ThreadPoolExecutor(max_workers=min(12, max(4, scan_limit // 40))) as executor:
        future_map = {executor.submit(_screen_one_accumulation_candidate, row): row["code"] for row in rows}
        for future in as_completed(future_map):
            item = future.result()
            if not item:
                continue
            potential_score = (
                item["量价代理评分"] * 0.45
                + item["趋势评分"] * 0.35
                + min(item["20日动量"] * 100, 25) * 0.20
            )
            if potential_score < 45:
                continue
            item["一年30%目标"] = f"{target_return:.0%}"
            item["潜力评分"] = round(potential_score, 2)
            item["推荐理由"] = "量价代理评分较高、趋势评分较高，且近期量能与动量配合较好。"
            results.append(item)

    if not results:
        return pd.DataFrame(columns=["display_name", "潜力评分", "一年30%目标", "量价代理评分", "趋势评分", "推荐理由"])

    result_df = pd.DataFrame(results).sort_values(
        by=["潜力评分", "量价代理评分", "趋势评分"],
        ascending=False,
    )
    return result_df.head(top_k).reset_index(drop=True)
