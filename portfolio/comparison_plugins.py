from __future__ import annotations

import pandas as pd

from data.fundamental_loader import load_or_fetch_industry_peer_snapshots


_COMPARISON_REGISTRY: dict[str, dict] = {}


def register_comparison_type(name: str, title: str):
    def decorator(func):
        _COMPARISON_REGISTRY[name] = {"name": name, "title": title, "handler": func}
        return func

    return decorator


def list_comparison_types() -> list[dict]:
    return [{"name": item["name"], "title": item["title"]} for item in _COMPARISON_REGISTRY.values()]


def _format_metric_value(value, as_percent: bool = False) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "-"
    if as_percent:
        return f"{float(numeric):.2f}%"
    return f"{float(numeric):,.2f}"


def _rank_description(current_value, peer_values, higher_better: bool = True) -> tuple[str, float | None]:
    current = pd.to_numeric(current_value, errors="coerce")
    peer_series = pd.to_numeric(pd.Series(peer_values), errors="coerce").dropna()
    if pd.isna(current) or peer_series.empty:
        return "-", None

    total = len(peer_series) + 1
    if higher_better:
        rank = 1 + int((peer_series > current).sum())
    else:
        rank = 1 + int((peer_series < current).sum())
    percentile = (total - rank) / max(total - 1, 1) * 100
    return f"第 {rank}/{total}", percentile


def _build_unavailable_result(name: str, title: str, headline: str, conclusion: str) -> dict:
    return {
        "name": name,
        "title": title,
        "available": False,
        "headline": headline,
        "items": [],
        "conclusion": conclusion,
        "positive_flags": [],
        "risk_flags": [],
        "peer_count": 0,
        "score": None,
    }


def _build_metric_item(
    label: str,
    current_value,
    peer_df: pd.DataFrame,
    column: str,
    higher_better: bool,
    as_percent: bool = False,
) -> tuple[dict, float | None]:
    if column not in peer_df.columns:
        return (
            {
                "比较项": label,
                "当前值": _format_metric_value(current_value, as_percent=as_percent),
                "同行中位数": "-",
                "行业内排名": "-",
                "分位": "-",
            },
            None,
        )

    rank_text, percentile = _rank_description(current_value, peer_df[column], higher_better=higher_better)
    median_value = pd.to_numeric(peer_df[column], errors="coerce").median()
    return (
        {
            "比较项": label,
            "当前值": _format_metric_value(current_value, as_percent=as_percent),
            "同行中位数": _format_metric_value(median_value, as_percent=as_percent),
            "行业内排名": rank_text,
            "分位": "-" if percentile is None else f"{percentile:.0f}",
        },
        percentile,
    )


def _weighted_score(score_items: list[tuple[float | None, float]]) -> float | None:
    used = [(score, weight) for score, weight in score_items if score is not None]
    if not used:
        return None
    total_weight = sum(weight for _, weight in used)
    return sum(score * weight for score, weight in used) / total_weight


def _load_industry_peer_context(context: dict) -> tuple[str, dict, dict, pd.DataFrame | None]:
    valuation = context.get("valuation_snapshot") or {}
    fundamental = context.get("fundamental_snapshot") or {}
    industry_membership = context.get("industry_membership") or {}
    symbol = str(context.get("symbol") or "")
    industry_name = str(valuation.get("industry") or industry_membership.get("industry_name") or "").strip()
    if not industry_name:
        return "", fundamental, valuation, None

    peers = load_or_fetch_industry_peer_snapshots(industry_name, exclude_symbol=symbol, limit=20)
    if not peers:
        return industry_name, fundamental, valuation, None

    peer_df = pd.DataFrame(peers)
    if peer_df.empty or len(peer_df) < 3:
        return industry_name, fundamental, valuation, None
    return industry_name, fundamental, valuation, peer_df


@register_comparison_type("industry_peers", "行业质量比较")
def build_industry_peer_comparison(context: dict) -> dict:
    industry_name, fundamental, valuation, peer_df = _load_industry_peer_context(context)
    if not industry_name:
        return _build_unavailable_result(
            "industry_peers",
            "行业质量比较",
            "当前缺少行业归属，暂时无法做行业质量比较。",
            "没有行业归属时，无法判断个股在行业内的质量位置。",
        )
    if peer_df is None:
        return _build_unavailable_result(
            "industry_peers",
            "行业质量比较",
            f"已识别行业“{industry_name}”，但当前同行样本不足。",
            "同行样本不足时，行业质量比较结论只会失真，因此本次不输出结果。",
        )

    peer_count = int(len(peer_df))
    items: list[dict] = [
        {
            "比较项": "行业",
            "当前值": industry_name,
            "同行中位数": f"{peer_count} 个样本",
            "行业内排名": "-",
            "分位": "-",
        }
    ]
    positive_flags: list[str] = []
    risk_flags: list[str] = []
    score_items: list[tuple[float | None, float]] = []

    metric_specs = [
        ("ROE", fundamental.get("roe"), "roe", True, True, 0.24),
        ("资产负债率", fundamental.get("debt_ratio"), "debt_ratio", False, True, 0.18),
        ("归母净利润", fundamental.get("net_profit"), "net_profit", True, False, 0.18),
        ("营业总收入", fundamental.get("revenue"), "revenue", True, False, 0.18),
        ("总市值", valuation.get("market_value"), "market_value", True, False, 0.12),
        ("经营现金流", fundamental.get("operating_cash_flow"), "operating_cash_flow", True, False, 0.10),
    ]

    percentiles: dict[str, float | None] = {}
    for label, current_value, column, higher_better, as_percent, weight in metric_specs:
        item, percentile = _build_metric_item(label, current_value, peer_df, column, higher_better, as_percent)
        items.append(item)
        percentiles[label] = percentile
        score_items.append((percentile, weight))

    score = _weighted_score(score_items)
    if score is not None:
        items.append(
            {
                "比较项": "行业质量综合分",
                "当前值": f"{score:.1f}/100",
                "同行中位数": "50.0/100",
                "行业内排名": "-",
                "分位": f"{score:.0f}",
            }
        )

    roe_pct = percentiles.get("ROE")
    debt_pct = percentiles.get("资产负债率")
    profit_pct = percentiles.get("归母净利润")
    revenue_pct = percentiles.get("营业总收入")
    if roe_pct is not None and roe_pct >= 70:
        positive_flags.append("ROE 在同行中靠前，盈利质量相对更强。")
    elif roe_pct is not None and roe_pct <= 30:
        risk_flags.append("ROE 在同行中偏弱，盈利优势不明显。")
    if debt_pct is not None and debt_pct >= 70:
        positive_flags.append("资产负债率在同行中更稳健，财务结构相对安全。")
    elif debt_pct is not None and debt_pct <= 30:
        risk_flags.append("资产负债率在同行中偏高，需要警惕杠杆压力。")
    if profit_pct is not None and profit_pct >= 70:
        positive_flags.append("净利润规模在同行中靠前。")
    elif profit_pct is not None and profit_pct <= 30:
        risk_flags.append("净利润规模在同行中偏弱。")
    if revenue_pct is not None and revenue_pct >= 70:
        positive_flags.append("收入体量在同行中靠前。")
    elif revenue_pct is not None and revenue_pct <= 30:
        risk_flags.append("收入体量在同行中不占优。")

    if score is None:
        conclusion = "同行样本里缺少足够的质量指标，暂时无法形成稳定的行业质量比较。"
    elif score >= 70:
        conclusion = "这只股票在行业质量比较中处于较强位置，更像行业内相对优质的观察对象。"
    elif score >= 55:
        conclusion = "这只股票在行业质量比较中处于中上水平，具备一定行业竞争力。"
    elif score >= 40:
        conclusion = "这只股票在行业质量比较中大体居中。"
    else:
        conclusion = "这只股票在行业质量比较中偏弱，行业相对优势不明显。"

    return {
        "name": "industry_peers",
        "title": "行业质量比较",
        "available": True,
        "headline": f"基于“{industry_name}”的 {peer_count} 个同行样本，比较行业质量位置。",
        "items": items,
        "conclusion": conclusion,
        "positive_flags": positive_flags,
        "risk_flags": risk_flags,
        "peer_count": peer_count,
        "score": None if score is None else round(score, 2),
        "industry_name": industry_name,
    }


@register_comparison_type("industry_valuation", "行业估值分位")
def build_industry_valuation_comparison(context: dict) -> dict:
    industry_name, _, valuation, peer_df = _load_industry_peer_context(context)
    if not industry_name:
        return _build_unavailable_result(
            "industry_valuation",
            "行业估值分位",
            "当前缺少行业归属，暂时无法做行业估值分位。",
            "没有行业归属时，无法判断个股估值在同行中的高低。",
        )
    if peer_df is None:
        return _build_unavailable_result(
            "industry_valuation",
            "行业估值分位",
            f"已识别行业“{industry_name}”，但当前同行样本不足。",
            "同行样本不足时，行业估值分位不具备稳定参考价值。",
        )

    items: list[dict] = []
    score_items: list[tuple[float | None, float]] = []
    positive_flags: list[str] = []
    risk_flags: list[str] = []

    metric_specs = [
        ("PE", valuation.get("pe"), "pe", False, False, 0.55),
        ("PB", valuation.get("pb"), "pb", False, False, 0.45),
    ]
    percentiles: dict[str, float | None] = {}
    for label, current_value, column, higher_better, as_percent, weight in metric_specs:
        item, percentile = _build_metric_item(label, current_value, peer_df, column, higher_better, as_percent)
        items.append(item)
        percentiles[label] = percentile
        score_items.append((percentile, weight))

    score = _weighted_score(score_items)
    if score is not None:
        items.append(
            {
                "比较项": "行业估值分位得分",
                "当前值": f"{score:.1f}/100",
                "同行中位数": "50.0/100",
                "行业内排名": "-",
                "分位": f"{score:.0f}",
            }
        )

    pe_pct = percentiles.get("PE")
    pb_pct = percentiles.get("PB")
    if pe_pct is not None and pe_pct >= 70:
        positive_flags.append("PE 位于同行较低分位，估值压力较小。")
    elif pe_pct is not None and pe_pct <= 30:
        risk_flags.append("PE 位于同行较高分位，估值可能已经偏贵。")
    if pb_pct is not None and pb_pct >= 70:
        positive_flags.append("PB 位于同行较低分位，账面估值更克制。")
    elif pb_pct is not None and pb_pct <= 30:
        risk_flags.append("PB 位于同行较高分位，需要确认高估值是否有基本面支撑。")

    if score is None:
        conclusion = "当前同行样本里的估值字段不足，无法形成稳定的行业估值分位。"
    elif score >= 70:
        conclusion = "这只股票当前估值在同行中相对偏低，具备一定估值安全垫。"
    elif score >= 45:
        conclusion = "这只股票当前估值在同行中大体中性。"
    else:
        conclusion = "这只股票当前估值在同行中偏高，需要警惕预期透支。"

    return {
        "name": "industry_valuation",
        "title": "行业估值分位",
        "available": True,
        "headline": f"基于“{industry_name}”同行样本，输出 PE/PB 在行业内的估值分位。",
        "items": items,
        "conclusion": conclusion,
        "positive_flags": positive_flags,
        "risk_flags": risk_flags,
        "peer_count": int(len(peer_df)),
        "score": None if score is None else round(score, 2),
        "industry_name": industry_name,
    }


@register_comparison_type("industry_growth", "行业增长性比较")
def build_industry_growth_comparison(context: dict) -> dict:
    industry_name, fundamental, _, peer_df = _load_industry_peer_context(context)
    if not industry_name:
        return _build_unavailable_result(
            "industry_growth",
            "行业增长性比较",
            "当前缺少行业归属，暂时无法做行业增长性比较。",
            "没有行业归属时，无法判断个股增长性是否领先同行。",
        )
    if peer_df is None:
        return _build_unavailable_result(
            "industry_growth",
            "行业增长性比较",
            f"已识别行业“{industry_name}”，但当前同行样本不足。",
            "同行样本不足时，增长性比较结论不稳定。",
        )

    items: list[dict] = []
    score_items: list[tuple[float | None, float]] = []
    positive_flags: list[str] = []
    risk_flags: list[str] = []

    metric_specs = [
        ("营收同比", fundamental.get("revenue_yoy"), "revenue_yoy", True, True, 0.45),
        ("净利润同比", fundamental.get("net_profit_yoy"), "net_profit_yoy", True, True, 0.55),
    ]
    percentiles: dict[str, float | None] = {}
    for label, current_value, column, higher_better, as_percent, weight in metric_specs:
        item, percentile = _build_metric_item(label, current_value, peer_df, column, higher_better, as_percent)
        items.append(item)
        percentiles[label] = percentile
        score_items.append((percentile, weight))

    score = _weighted_score(score_items)
    if score is not None:
        items.append(
            {
                "比较项": "行业增长分位得分",
                "当前值": f"{score:.1f}/100",
                "同行中位数": "50.0/100",
                "行业内排名": "-",
                "分位": f"{score:.0f}",
            }
        )

    revenue_pct = percentiles.get("营收同比")
    profit_pct = percentiles.get("净利润同比")
    if revenue_pct is not None and revenue_pct >= 70:
        positive_flags.append("营收同比在同行中靠前，收入扩张速度较快。")
    elif revenue_pct is not None and revenue_pct <= 30:
        risk_flags.append("营收同比在同行中偏弱，收入扩张不明显。")
    if profit_pct is not None and profit_pct >= 70:
        positive_flags.append("净利润同比在同行中靠前，利润弹性较强。")
    elif profit_pct is not None and profit_pct <= 30:
        risk_flags.append("净利润同比在同行中偏弱，盈利增长承压。")

    if score is None:
        conclusion = "当前同行样本中的同比增长字段不足，无法形成稳定的增长性比较。"
    elif score >= 70:
        conclusion = "这只股票的增长性在同行中较强，具备较明显的成长分位优势。"
    elif score >= 45:
        conclusion = "这只股票的增长性在同行中大体中性。"
    else:
        conclusion = "这只股票的增长性在同行中偏弱，成长性暂时不占优。"

    return {
        "name": "industry_growth",
        "title": "行业增长性比较",
        "available": True,
        "headline": f"基于“{industry_name}”同行样本，输出营收同比和净利润同比的行业分位。",
        "items": items,
        "conclusion": conclusion,
        "positive_flags": positive_flags,
        "risk_flags": risk_flags,
        "peer_count": int(len(peer_df)),
        "score": None if score is None else round(score, 2),
        "industry_name": industry_name,
    }


def build_comparison_results(context: dict) -> tuple[list[dict], list[dict]]:
    results: list[dict] = []
    overview: list[dict] = []

    for item in _COMPARISON_REGISTRY.values():
        result = item["handler"](context)
        results.append(result)
        overview.append(
            {
                "name": item["name"],
                "title": item["title"],
                "available": bool(result.get("available")),
                "peer_count": int(result.get("peer_count", 0)),
                "score": result.get("score"),
            }
        )

    return results, overview
