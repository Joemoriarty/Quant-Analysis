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
    _build_execution_plan_summary,
    _build_event_summary,
    _build_final_recommendation,
    _build_market_sentiment_view,
    _build_recommendation,
    _build_risk_committee_summary,
    _build_target_price_scenarios,
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
def _cached_industry_membership(symbol: str, name: str):
    fundamental_snapshot, valuation_snapshot = load_or_fetch_fundamental_snapshot(
        symbol,
        fallback_name=name,
        prefer_cache=True,
    )
    valuation_snapshot = valuation_snapshot or {}
    membership = resolve_industry_membership(
        symbol,
        valuation_snapshot=valuation_snapshot,
        max_age_days=30,
        allow_live_fetch=False,
    )
    if membership and membership.get("industry_name"):
        return str(membership.get("industry_name"))
    if valuation_snapshot.get("industry"):
        return str(valuation_snapshot.get("industry"))
    return "未分类"


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
        f"量价代理 {accumulation_score}/100 | 市场情绪 {market_sentiment_state} | 事件面 {event_state} | 行业横向 {industry_text} | {final_decision_basis}"
    )


def _build_position_weights(selected_df: pd.DataFrame, max_position_weight: float) -> dict[str, float]:
    if selected_df.empty:
        return {}
    raw_weights: dict[str, float] = {}
    for _, row in selected_df.iterrows():
        score = max(float(row.get("score", 0.0) or 0.0), 1.0)
        confidence = max(float(row.get("execution_confidence", 50.0) or 50.0), 1.0)
        risk = min(max(float(row.get("execution_risk_score", 50.0) or 50.0), 0.0), 100.0)
        raw_weights[str(row["symbol"])] = score * (0.5 + confidence / 200.0) * (1.15 - risk / 100.0)
    total_raw = sum(raw_weights.values()) or 1.0
    effective_cap = float(max_position_weight)
    weights = {symbol: min(value / total_raw, effective_cap) for symbol, value in raw_weights.items()}
    total = sum(weights.values()) or 1.0
    return {symbol: weight / total for symbol, weight in weights.items()}


def apply_portfolio_constraints(score_df: pd.DataFrame, top_n: int, config: dict | None = None) -> tuple[pd.DataFrame, dict]:
    config = normalize_scoring_config(config)
    constraints = config["portfolio_constraints"]
    max_industry_positions = int(constraints["max_industry_positions"])
    min_turnover_amount = float(constraints["min_turnover_amount"])
    max_position_weight = float(constraints["max_position_weight"])
    min_execution_confidence = int(constraints["min_execution_confidence"])
    max_execution_risk_score = int(constraints["max_execution_risk_score"])

    if score_df.empty:
        return score_df.copy(), {
            "selected_count": 0,
            "skipped_low_liquidity": 0,
            "skipped_industry_cap": 0,
            "industry_exposure": {},
            "max_position_weight": max_position_weight,
            "min_turnover_amount": min_turnover_amount,
            "max_industry_positions": max_industry_positions,
            "min_execution_confidence": min_execution_confidence,
            "max_execution_risk_score": max_execution_risk_score,
        }

    selected_rows: list[dict] = []
    industry_counter: dict[str, int] = {}
    skipped_low_liquidity = 0
    skipped_industry_cap = 0
    skipped_execution_gate = 0

    for _, row in score_df.sort_values("score", ascending=False).iterrows():
        turnover_amount = float(row.get("turnover_amount", 0.0) or 0.0)
        industry_name = str(row.get("industry_name") or "未分类")
        execution_confidence = float(row.get("execution_confidence", 50.0) or 50.0)
        execution_risk_score = float(row.get("execution_risk_score", 50.0) or 50.0)
        if turnover_amount < min_turnover_amount:
            skipped_low_liquidity += 1
            continue
        if execution_confidence < min_execution_confidence or execution_risk_score > max_execution_risk_score:
            skipped_execution_gate += 1
            continue
        if industry_counter.get(industry_name, 0) >= max_industry_positions:
            skipped_industry_cap += 1
            continue
        selected_rows.append(row.to_dict())
        industry_counter[industry_name] = industry_counter.get(industry_name, 0) + 1
        if len(selected_rows) >= top_n:
            break

    selected_df = pd.DataFrame(selected_rows)
    if not selected_df.empty:
        weights = _build_position_weights(selected_df, max_position_weight)
        selected_df["position_weight"] = selected_df["symbol"].map(weights).fillna(0.0)

    summary = {
        "selected_count": int(len(selected_df)),
        "skipped_low_liquidity": int(skipped_low_liquidity),
        "skipped_industry_cap": int(skipped_industry_cap),
        "skipped_execution_gate": int(skipped_execution_gate),
        "industry_exposure": industry_counter,
        "max_position_weight": max_position_weight,
        "min_turnover_amount": min_turnover_amount,
        "max_industry_positions": max_industry_positions,
        "min_execution_confidence": min_execution_confidence,
        "max_execution_risk_score": max_execution_risk_score,
    }
    return selected_df.reset_index(drop=True), summary


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
        industry_name = _cached_industry_membership(symbol, name)

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
        risk_committee_summary = _build_risk_committee_summary(
            metrics,
            trend_score,
            fundamental_score,
            event_score,
            event_state,
            industry_score,
            market_sentiment_state,
            valuation_snapshot,
        )
        target_price_scenarios = _build_target_price_scenarios(
            metrics,
            recommendation,
            fundamental_score,
            market_sentiment_state,
            event_state,
            risk_committee_summary,
        )
        execution_plan_summary = _build_execution_plan_summary(
            recommendation,
            technical_recommendation,
            metrics,
            target_price_scenarios,
            risk_committee_summary,
            market_sentiment_state,
            event_state,
            final_decision_basis,
        )
        execution_confidence = int(execution_plan_summary.get("execution_confidence", 50))
        execution_risk_score = int(execution_plan_summary.get("execution_risk_score", 50))
        execution_score = execution_confidence * 0.7 + (100 - execution_risk_score) * 0.3

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
            + execution_score * component_weights["execution"]
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
                "industry_heat_score": next(
                    (
                        item.get("score")
                        for item in comparison_results
                        if item.get("name") == "industry_heat" and item.get("available")
                    ),
                    None,
                ),
                "industry_name": industry_name,
                "industry_conclusion": industry_conclusion,
                "close_price": float(metrics["close"]),
                "return_20d": float(metrics["return_20d"]),
                "volume_ratio_10d": float(metrics["volume_ratio_10d"]),
                "turnover_amount": float(metrics["close"] * latest["volume"]),
                "execution_confidence": execution_confidence,
                "execution_risk_score": execution_risk_score,
                "execution_score": round(execution_score, 2),
                "risk_adjusted_action": execution_plan_summary.get("risk_adjusted_action"),
                "position_guidance": execution_plan_summary.get("position_guidance"),
                "target_price_range": execution_plan_summary.get("target_price_range"),
                "final_decision_basis": final_decision_basis,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["symbol", "score", "reason"]), component_weights

    result_df = pd.DataFrame(rows).sort_values(
        ["score", "execution_confidence", "execution_risk_score", "trend_score", "fundamental_score", "industry_score"],
        ascending=[False, False, True, False, False, False],
    )
    return result_df.reset_index(drop=True), component_weights
