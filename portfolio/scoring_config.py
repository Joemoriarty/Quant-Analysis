from __future__ import annotations

import json


RECOMMENDATION_FOCUS = "推荐关注"
RECOMMENDATION_WATCH = "中性观察"
RECOMMENDATION_AVOID = "暂不推荐"

RECOMMENDATION_ORDER = [
    RECOMMENDATION_FOCUS,
    RECOMMENDATION_WATCH,
    RECOMMENDATION_AVOID,
]


DEFAULT_SCORING_CONFIG = {
    "weights": {
        "trend": 0.22,
        "fundamental": 0.20,
        "accumulation": 0.14,
        "sentiment": 0.09,
        "industry": 0.11,
        "event": 0.10,
        "execution": 0.14,
    },
    "thresholds": {
        "min_recommendation": RECOMMENDATION_WATCH,
        "min_trend_score": 55,
        "min_fundamental_score": 38,
        "min_accumulation_score": 50,
        "min_growth_score": 48,
        "min_industry_score": 30,
        "min_event_score": 0,
    },
    "recommendation_bonus": {
        RECOMMENDATION_FOCUS: 8,
        RECOMMENDATION_WATCH: 0,
        RECOMMENDATION_AVOID: -12,
    },
    "portfolio_constraints": {
        "max_position_weight": 0.18,
        "max_industry_positions": 2,
        "min_turnover_amount": 20_000_000,
        "min_execution_confidence": 45,
        "max_execution_risk_score": 78,
    },
}


def normalize_scoring_config(config: dict | None = None) -> dict:
    base = json.loads(json.dumps(DEFAULT_SCORING_CONFIG))
    config = config or {}

    for group in ["weights", "thresholds", "recommendation_bonus", "portfolio_constraints"]:
        incoming = config.get(group) or {}
        if isinstance(incoming, dict):
            base[group].update(incoming)

    weights = base["weights"]
    weight_keys = ["trend", "fundamental", "accumulation", "sentiment", "industry", "event", "execution"]
    total = sum(float(weights.get(key, 0.0)) for key in weight_keys)
    if total <= 0:
        total = 1.0
    for key in weight_keys:
        weights[key] = float(weights.get(key, 0.0)) / total

    thresholds = base["thresholds"]
    thresholds["min_trend_score"] = int(thresholds.get("min_trend_score", 55))
    thresholds["min_fundamental_score"] = int(thresholds.get("min_fundamental_score", 38))
    thresholds["min_accumulation_score"] = int(thresholds.get("min_accumulation_score", 50))
    thresholds["min_growth_score"] = int(thresholds.get("min_growth_score", 48))
    thresholds["min_industry_score"] = int(thresholds.get("min_industry_score", 30))
    thresholds["min_event_score"] = int(thresholds.get("min_event_score", 0))

    recommendation = str(thresholds.get("min_recommendation", RECOMMENDATION_WATCH))
    if recommendation not in RECOMMENDATION_ORDER:
        recommendation = RECOMMENDATION_WATCH
    thresholds["min_recommendation"] = recommendation

    bonus = base["recommendation_bonus"]
    for key in RECOMMENDATION_ORDER:
        bonus[key] = int(bonus.get(key, 0))

    constraints = base["portfolio_constraints"]
    constraints["max_position_weight"] = float(constraints.get("max_position_weight", 0.18))
    constraints["max_industry_positions"] = max(1, int(constraints.get("max_industry_positions", 2)))
    constraints["min_turnover_amount"] = max(0.0, float(constraints.get("min_turnover_amount", 20_000_000)))
    constraints["min_execution_confidence"] = max(0, min(100, int(constraints.get("min_execution_confidence", 45))))
    constraints["max_execution_risk_score"] = max(0, min(100, int(constraints.get("max_execution_risk_score", 78))))

    return base


def scoring_config_to_json(config: dict | None = None) -> str:
    return json.dumps(normalize_scoring_config(config), ensure_ascii=False, sort_keys=True)
