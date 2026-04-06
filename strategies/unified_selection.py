from __future__ import annotations

from functools import lru_cache

import pandas as pd

from data.events_loader import load_or_fetch_company_events
from data.fundamental_loader import load_or_fetch_fundamental_snapshot, resolve_industry_membership
from data.sentiment_loader import load_or_fetch_market_sentiment_snapshot
from portfolio.comparison_plugins import build_comparison_results
from portfolio.scoring_config import DEFAULT_SCORING_CONFIG, normalize_scoring_config
from portfolio.single_stock_analysis import (
    _analyze_accumulation,
    _build_event_summary,
    _build_final_recommendation,
    _build_market_sentiment_view,
    _build_recommendation,
    _calculate_metrics,
    _compute_fundamental_score,
    _compute_trend_score,
    _extract_industry_comparison_view,
)


DEFAULT_COMPONENT_WEIGHTS = DEFAULT_SCORING_CONFIG["weights"]
DEFAULT_CONFIG = DEFAULT_SCORING_CONFIG


@lru_cache(maxsize=4096)
def _cached_fundamental_snapshot(symbol: str, name: str):
    return load_or_fetch_fundamental_snapshot(symbol, fallback_name=name, prefer_cache=True)


@lru_cache(maxsize=4096)
def _cached_industry_view(symbol: str, name: str):
    fundamental_snapshot, valuation_snapshot = load_or_fetch_fundamental_snapshot(
        symbol,
        fallback_name=name,
        prefer_cache=True,
    )
    valuation_snapshot = valuation_snapshot or {}
    industry_membership = resolve_industry_membership(
        symbol,
        valuation_snapshot=valuation_snapshot,
        max_age_days=30,
        allow_live_fetch=False,
    )
    if industry_membership and not valuation_snapshot.get("industry"):
        valuation_snapshot = dict(valuation_snapshot)
        valuation_snapshot["industry"] = industry_membership.get("industry_name")
        valuation_snapshot["industry_source"] = industry_membership.get("source")
        valuation_snapshot["industry_stale"] = industry_membership.get("stale")

    comparison_results, _ = build_comparison_results(
        {
            "symbol": symbol,
            "name": name,
            "fundamental_snapshot": fundamental_snapshot,
            "valuation_snapshot": valuation_snapshot,
            "industry_membership": industry_membership,
            "prefer_cache_only": True,
        }
    )
    return _extract_industry_comparison_view(comparison_results)


@lru_cache(maxsize=4096)
def _cached_event_view(symbol: str, name: str):
    events = load_or_fetch_company_events(symbol, fallback_name=name, lookback_days=30)
    summary, explanations, risks = _build_event_summary(events)
    return (
        int(summary.get("score", 50)),
        str(summary.get("state", "中性")),
        explanations,
        risks,
    )


def _recommendation_gate_rank(value: str) -> int:
    if value == "推荐关注":
        return 2
    if value == "中性观察":
        return 1
    return 0


def _recommendation_bonus(value: str) -> int:
    if value == "推荐关注":
        return 8
    if value == "中性观察":
        return 0
    return -12


def _build_selection_reason(
    recommendation: str,
    final_decision_basis: str,
    trend_score: int,
    fundamental_score: int,
    accumulation_score: int,
    market_sentiment_state: str,
    event_state: str,
    industry_score: int | None,
) -> str:
    industry_text = "-" if industry_score is None else f"{industry_score}/100"
    return (
        f"{recommendation} | 趋势 {trend_score}/100 | 基本面 {fundamental_score}/100 | "
        f"量价吸筹 {accumulation_score}/100 | 市场情绪 {market_sentiment_state} | 事件面 {event_state} | 行业横向 {industry_text} | {final_decision_basis}"
    )


def run_unified_selection(stock_dict, config=None):
    if not stock_dict:
        return pd.DataFrame(columns=["symbol", "score", "reason"]), DEFAULT_COMPONENT_WEIGHTS.copy()

    config = normalize_scoring_config(config)
    component_weights = config["weights"]
    thresholds = config["thresholds"]
    recommendation_bonus = config["recommendation_bonus"]
    min_recommendation = str(thresholds.get("min_recommendation", "中性观察"))
    min_trend_score = int(thresholds.get("min_trend_score", 55))
    min_fundamental_score = int(thresholds.get("min_fundamental_score", 45))
    min_industry_score = int(thresholds.get("min_industry_score", 40))
    min_event_score = int(thresholds.get("min_event_score", 25))

    market_snapshot = load_or_fetch_market_sentiment_snapshot(prefer_cache=True)
    market_summary, market_explanations, market_risks = _build_market_sentiment_view(market_snapshot)
    market_sentiment_score = int(market_summary.get("score", 50))
    market_sentiment_state = str(market_summary.get("state", "中性"))

    rows: list[dict] = []
    for symbol, df in stock_dict.items():
        if df is None or df.empty:
            continue

        name = str(df.attrs.get("name") or symbol)
        try:
            _, metrics, latest = _calculate_metrics(df)
        except Exception:
            continue

        technical_recommendation, technical_reasons, technical_risks = _build_recommendation(latest, metrics)
        trend_score, _ = _compute_trend_score(metrics, latest)
        _, _, accumulation_score = _analyze_accumulation(metrics, latest)
        fundamental_snapshot, valuation_snapshot = _cached_fundamental_snapshot(symbol, name)
        fundamental_score, fundamental_explanations, fundamental_risks = _compute_fundamental_score(
            fundamental_snapshot,
            valuation_snapshot,
        )
        (
            event_score,
            event_state,
            event_explanations,
            event_risks,
        ) = _cached_event_view(symbol, name)
        (
            industry_score,
            industry_conclusion,
            industry_positive_flags,
            industry_risk_flags,
            _,
        ) = _cached_industry_view(symbol, name)

        recommendation, _, _, final_decision_basis = _build_final_recommendation(
            technical_recommendation,
            trend_score,
            fundamental_score,
            technical_reasons,
            technical_risks,
            fundamental_explanations,
            fundamental_risks,
            market_sentiment_score,
            market_sentiment_state,
            market_explanations,
            market_risks,
            event_score,
            event_state,
            event_explanations,
            event_risks,
            industry_score,
            industry_conclusion,
            industry_positive_flags,
            industry_risk_flags,
        )

        if _recommendation_gate_rank(recommendation) < _recommendation_gate_rank(min_recommendation):
            continue
        if trend_score < min_trend_score:
            continue
        if fundamental_score < min_fundamental_score:
            continue
        if industry_score is not None and industry_score < min_industry_score:
            continue
        if event_score < min_event_score:
            continue

        effective_industry_score = 50 if industry_score is None else industry_score
        composite_score = (
            trend_score * component_weights["trend"]
            + fundamental_score * component_weights["fundamental"]
            + accumulation_score * component_weights["accumulation"]
            + market_sentiment_score * component_weights["sentiment"]
            + effective_industry_score * component_weights["industry"]
            + event_score * component_weights["event"]
            + recommendation_bonus.get(recommendation, _recommendation_bonus(recommendation))
        )

        rows.append(
            {
                "symbol": symbol,
                "score": round(composite_score, 2),
                "reason": _build_selection_reason(
                    recommendation,
                    final_decision_basis,
                    trend_score,
                    fundamental_score,
                    accumulation_score,
                    market_sentiment_state,
                    event_state,
                    industry_score,
                ),
                "recommendation": recommendation,
                "technical_recommendation": technical_recommendation,
                "trend_score": trend_score,
                "fundamental_score": fundamental_score,
                "accumulation_score": accumulation_score,
                "market_sentiment_state": market_sentiment_state,
                "market_sentiment_score": market_sentiment_score,
                "event_state": event_state,
                "event_score": event_score,
                "industry_score": industry_score,
                "industry_conclusion": industry_conclusion,
                "close_price": float(metrics["close"]),
                "return_20d": float(metrics["return_20d"]),
                "volume_ratio_10d": float(metrics["volume_ratio_10d"]),
                "final_decision_basis": final_decision_basis,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["symbol", "score", "reason"]), component_weights

    result_df = pd.DataFrame(rows).sort_values(
        ["score", "trend_score", "fundamental_score", "industry_score"],
        ascending=False,
    )
    return result_df.reset_index(drop=True), component_weights
