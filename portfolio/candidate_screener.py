from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from data.akshare_loader import DataFetchError, get_stock_catalog
from portfolio.scoring_config import (
    RECOMMENDATION_AVOID,
    RECOMMENDATION_FOCUS,
    RECOMMENDATION_WATCH,
    normalize_scoring_config,
)
from portfolio.single_stock_analysis import analyze_single_stock
from utils.cache_manager import DEFAULT_TTLS, load_dataframe


def _recommendation_rank(recommendation: str) -> int:
    if recommendation == RECOMMENDATION_FOCUS:
        return 0
    if recommendation == RECOMMENDATION_WATCH:
        return 1
    return 2


def _build_value_view(fundamental_score: int, industry_score: int | None) -> str:
    if fundamental_score >= 75 and (industry_score is None or industry_score >= 60):
        return "基本面较强，且行业横向位置不弱，价值保护较好"
    if fundamental_score >= 60:
        return "基本面中性偏稳，可结合行业位置和趋势继续跟踪"
    if industry_score is not None and industry_score >= 75:
        return "行业横向位置较强，但公司基本面仍需继续验证"
    return "基本面保护一般，行业相对优势也不突出，价值判断需更谨慎"


def _build_event_view(event_score: int, event_state: str) -> str:
    if event_state == "偏利多" and event_score >= 70:
        return "近期公告和事件偏正面，存在一定催化支撑"
    if event_state == "偏利空" and event_score <= 35:
        return "近期事件偏负面，需警惕风险落地和情绪拖累"
    return "近期事件整体中性，更多作为辅助解释"


def _screen_one_consistent_candidate(row: pd.Series, config: dict) -> dict | None:
    symbol = str(row["code"]).zfill(6)
    name = str(row.get("name", symbol))
    try:
        analysis = analyze_single_stock(symbol, name, prefer_cache_only=True)
    except (DataFetchError, ValueError):
        return None

    thresholds = config["thresholds"]
    if _recommendation_rank(analysis["recommendation"]) > _recommendation_rank(thresholds["min_recommendation"]):
        return None
    if analysis["trend_score"] < thresholds["min_trend_score"]:
        return None
    if analysis["fundamental_score"] < thresholds["min_fundamental_score"]:
        return None

    industry_score = analysis.get("industry_comparison_score")
    if industry_score is not None and industry_score < thresholds["min_industry_score"]:
        return None
    if analysis["event_score"] < thresholds["min_event_score"]:
        return None

    metrics = analysis["metrics"]
    weights = config["weights"]
    recommendation_bonus = config["recommendation_bonus"]
    effective_industry_score = 50 if industry_score is None else industry_score
    final_score = round(
        analysis["trend_score"] * weights["trend"]
        + analysis["fundamental_score"] * weights["fundamental"]
        + analysis["accumulation_score"] * weights["accumulation"]
        + analysis["market_sentiment_score"] * weights["sentiment"]
        + effective_industry_score * weights["industry"]
        + analysis["event_score"] * weights["event"]
        + recommendation_bonus.get(analysis["recommendation"], 0),
        2,
    )

    return {
        "symbol": symbol,
        "name": name,
        "display_name": f"{symbol} {name}",
        "综合评分": final_score,
        "最终结论": analysis["recommendation"],
        "技术结论": analysis["technical_recommendation"],
        "趋势评分": analysis["trend_score"],
        "基本面评分": analysis["fundamental_score"],
        "行业横向分": industry_score,
        "行业比较结论": analysis.get("industry_comparison_conclusion"),
        "市场情绪": analysis["market_sentiment_state"],
        "市场情绪得分": analysis["market_sentiment_score"],
        "事件面": analysis["event_state"],
        "事件驱动分": analysis["event_score"],
        "量价吸筹评分": analysis["accumulation_score"],
        "20日动量": metrics["return_20d"],
        "量比": metrics["volume_ratio_10d"],
        "价值判断": _build_value_view(analysis["fundamental_score"], industry_score),
        "事件判断": _build_event_view(analysis["event_score"], analysis["event_state"]),
        "结论依据": analysis["final_decision_basis"],
    }


def screen_accumulation_candidates(scan_limit: int = 500, top_k: int = 20, config: dict | None = None) -> pd.DataFrame:
    config = normalize_scoring_config(config)
    catalog = load_dataframe(
        f"catalog_{scan_limit}",
        lambda: get_stock_catalog(limit=scan_limit),
        DEFAULT_TTLS.get("catalog", 300),
    )
    rows = [row for _, row in catalog.iterrows()]
    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=min(12, max(4, scan_limit // 40))) as executor:
        future_map = {executor.submit(_screen_one_consistent_candidate, row, config): row["code"] for row in rows}
        for future in as_completed(future_map):
            item = future.result()
            if item and item["量价吸筹评分"] >= config["thresholds"]["min_accumulation_score"]:
                results.append(item)

    if not results:
        return pd.DataFrame(
            columns=[
                "display_name",
                "综合评分",
                "最终结论",
                "技术结论",
                "趋势评分",
                "基本面评分",
                "行业横向分",
                "市场情绪",
                "事件面",
                "事件驱动分",
                "量价吸筹评分",
                "20日动量",
                "量比",
                "价值判断",
                "事件判断",
                "结论依据",
            ]
        )

    result_df = pd.DataFrame(results)
    result_df["recommendation_rank"] = result_df["最终结论"].map(_recommendation_rank)
    result_df = result_df.sort_values(
        by=["recommendation_rank", "综合评分", "量价吸筹评分", "趋势评分", "行业横向分"],
        ascending=[True, False, False, False, False],
    ).drop(columns=["recommendation_rank", "市场情绪得分"])
    return result_df.head(top_k).reset_index(drop=True)


def recommend_growth_candidates(
    scan_limit: int = 300,
    top_k: int = 10,
    target_return: float = 0.30,
    config: dict | None = None,
) -> pd.DataFrame:
    config = normalize_scoring_config(config)
    catalog = load_dataframe(
        f"catalog_{scan_limit}",
        lambda: get_stock_catalog(limit=scan_limit),
        DEFAULT_TTLS.get("catalog", 300),
    )
    rows = [row for _, row in catalog.iterrows()]
    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=min(12, max(4, scan_limit // 40))) as executor:
        future_map = {executor.submit(_screen_one_consistent_candidate, row, config): row["code"] for row in rows}
        for future in as_completed(future_map):
            item = future.result()
            if not item:
                continue

            effective_industry_score = 50 if item["行业横向分"] is None else item["行业横向分"]
            potential_score = (
                item["趋势评分"] * config["weights"]["trend"]
                + item["基本面评分"] * config["weights"]["fundamental"]
                + item["量价吸筹评分"] * config["weights"]["accumulation"]
                + item["市场情绪得分"] * config["weights"]["sentiment"]
                + effective_industry_score * config["weights"]["industry"]
                + item["事件驱动分"] * config["weights"]["event"]
                + min(item["20日动量"] * 100, 20)
                + config["recommendation_bonus"].get(item["最终结论"], 0)
            )
            if potential_score < config["thresholds"]["min_growth_score"]:
                continue

            item["一年30%目标"] = f"{target_return:.0%}"
            item["潜力评分"] = round(potential_score, 2)
            item["推荐理由"] = item["结论依据"]
            results.append(item)

    if not results:
        return pd.DataFrame(
            columns=[
                "display_name",
                "潜力评分",
                "一年30%目标",
                "最终结论",
                "技术结论",
                "趋势评分",
                "基本面评分",
                "行业横向分",
                "市场情绪",
                "事件面",
                "事件驱动分",
                "量价吸筹评分",
                "20日动量",
                "量比",
                "价值判断",
                "事件判断",
                "推荐理由",
            ]
        )

    result_df = pd.DataFrame(results)
    result_df["recommendation_rank"] = result_df["最终结论"].map(_recommendation_rank)
    result_df = result_df.sort_values(
        by=["recommendation_rank", "潜力评分", "基本面评分", "行业横向分", "趋势评分"],
        ascending=[True, False, False, False, False],
    ).drop(columns=["recommendation_rank", "市场情绪得分", "综合评分", "结论依据"])
    return result_df.head(top_k).reset_index(drop=True)
