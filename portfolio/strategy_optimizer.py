from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from data.akshare_loader import DataFetchError, get_stock_data
from db.market_db import (
    get_recent_best_optimizer_runs,
    get_setting,
    init_db,
    save_market_catalog_snapshot,
    save_optimizer_results,
    save_price_history,
    set_setting,
)
from portfolio.portfolio_backtester import BacktestError, backtest_portfolio_realistic
from portfolio.scoring_config import DEFAULT_SCORING_CONFIG, normalize_scoring_config
from strategies.unified_selection import run_unified_selection


UNIFIED_SEARCH_SPACE = {
    "top_n": [5, 8, 10, 12],
    "rebalance_days": [5, 10, 20],
    "lookback_years": [1, 3, 5],
    "weight_profiles": [
        {"trend": 0.26, "fundamental": 0.22, "accumulation": 0.16, "sentiment": 0.10, "industry": 0.13, "event": 0.13},
        {"trend": 0.24, "fundamental": 0.24, "accumulation": 0.16, "sentiment": 0.10, "industry": 0.13, "event": 0.13},
        {"trend": 0.30, "fundamental": 0.18, "accumulation": 0.16, "sentiment": 0.10, "industry": 0.13, "event": 0.13},
        {"trend": 0.25, "fundamental": 0.20, "accumulation": 0.20, "sentiment": 0.10, "industry": 0.12, "event": 0.13},
    ],
    "threshold_profiles": [
        {"min_recommendation": "中性观察", "min_trend_score": 50, "min_fundamental_score": 40, "min_accumulation_score": 40, "min_growth_score": 50, "min_industry_score": 35, "min_event_score": 20},
        {"min_recommendation": "中性观察", "min_trend_score": 55, "min_fundamental_score": 45, "min_accumulation_score": 50, "min_growth_score": 55, "min_industry_score": 40, "min_event_score": 25},
        {"min_recommendation": "推荐关注", "min_trend_score": 60, "min_fundamental_score": 50, "min_accumulation_score": 60, "min_growth_score": 60, "min_industry_score": 45, "min_event_score": 30},
    ],
    "bonus_profiles": [
        {"推荐关注": 6, "中性观察": 0, "暂不推荐": -8},
        {"推荐关注": 8, "中性观察": 0, "暂不推荐": -12},
        {"推荐关注": 10, "中性观察": 0, "暂不推荐": -15},
    ],
}


def sync_market_data_to_db(symbols: list[str], catalog=None, data_fetch_kwargs: dict | None = None) -> dict:
    init_db()
    saved_symbols = 0
    failed_symbols = 0
    saved_rows = 0
    errors = {}

    if catalog is not None and len(catalog) > 0:
        save_market_catalog_snapshot(catalog)

    if not symbols:
        return {"saved_symbols": 0, "failed_symbols": 0, "saved_rows": 0, "errors": {}}

    max_workers = min(12, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_stock_data, symbol, **(data_fetch_kwargs or {})): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                df = future.result()
                saved_rows += save_price_history(symbol, df)
                saved_symbols += 1
            except DataFetchError as error:
                failed_symbols += 1
                errors[symbol] = str(error)

    return {
        "saved_symbols": saved_symbols,
        "failed_symbols": failed_symbols,
        "saved_rows": saved_rows,
        "errors": errors,
    }


def _objective_score(metrics: dict) -> float:
    total_return = float(metrics.get("total_return", 0.0))
    max_drawdown = abs(float(metrics.get("max_drawdown", 0.0)))
    positive_ratio = float(metrics.get("positive_period_ratio", 0.0))
    latest_value = float(metrics.get("latest_value", 1.0))
    return total_return * 0.55 + positive_ratio * 0.25 + (latest_value - 1.0) * 0.20 - max_drawdown * 0.45


def _parameter_distance(config_a: dict, config_b: dict) -> float:
    weights_a = normalize_scoring_config(config_a).get("weights", {})
    weights_b = normalize_scoring_config(config_b).get("weights", {})
    thresholds_a = normalize_scoring_config(config_a).get("thresholds", {})
    thresholds_b = normalize_scoring_config(config_b).get("thresholds", {})
    bonus_a = normalize_scoring_config(config_a).get("recommendation_bonus", {})
    bonus_b = normalize_scoring_config(config_b).get("recommendation_bonus", {})

    return sum(
        [
            abs(int(config_a.get("top_n", 0)) - int(config_b.get("top_n", 0))) / 5.0,
            abs(int(config_a.get("rebalance_days", 0)) - int(config_b.get("rebalance_days", 0))) / 10.0,
            abs(int(config_a.get("lookback_years", 0)) - int(config_b.get("lookback_years", 0))) / 2.0,
            abs(weights_a.get("trend", 0.0) - weights_b.get("trend", 0.0)) * 4.0,
            abs(weights_a.get("fundamental", 0.0) - weights_b.get("fundamental", 0.0)) * 4.0,
            abs(weights_a.get("accumulation", 0.0) - weights_b.get("accumulation", 0.0)) * 4.0,
            abs(weights_a.get("sentiment", 0.0) - weights_b.get("sentiment", 0.0)) * 4.0,
            abs(weights_a.get("industry", 0.0) - weights_b.get("industry", 0.0)) * 4.0,
            abs(weights_a.get("event", 0.0) - weights_b.get("event", 0.0)) * 4.0,
            abs(int(thresholds_a.get("min_trend_score", 55)) - int(thresholds_b.get("min_trend_score", 55))) / 10.0,
            abs(int(thresholds_a.get("min_fundamental_score", 45)) - int(thresholds_b.get("min_fundamental_score", 45))) / 10.0,
            abs(int(thresholds_a.get("min_accumulation_score", 50)) - int(thresholds_b.get("min_accumulation_score", 50))) / 10.0,
            abs(int(thresholds_a.get("min_industry_score", 40)) - int(thresholds_b.get("min_industry_score", 40))) / 10.0,
            abs(int(thresholds_a.get("min_event_score", 25)) - int(thresholds_b.get("min_event_score", 25))) / 10.0,
            abs(int(bonus_a.get("推荐关注", 0)) - int(bonus_b.get("推荐关注", 0))) / 5.0,
            abs(int(bonus_a.get("暂不推荐", 0)) - int(bonus_b.get("暂不推荐", 0))) / 5.0,
        ]
    )


def _historical_stability_penalty(config: dict, recent_best_runs: list[dict]) -> float:
    if not recent_best_runs:
        return 0.0
    distances = [_parameter_distance(config, prev) for prev in recent_best_runs]
    avg_distance = sum(distances) / len(distances)
    return avg_distance * 0.018


def _is_neighbor(config_a: dict, config_b: dict) -> bool:
    return _parameter_distance(config_a, config_b) <= 2.9


def _apply_stability_constraints(evaluations: list[dict], recent_best_runs: list[dict]) -> list[dict]:
    for item in evaluations:
        config = {
            "top_n": item["top_n"],
            "rebalance_days": item["rebalance_days"],
            "lookback_years": item["lookback_years"],
            **item["meta"].get("strategy_config", {}),
        }
        base_score = item["base_objective_score"]
        stability_penalty = _historical_stability_penalty(config, recent_best_runs)

        neighbor_scores = []
        for peer in evaluations:
            peer_config = {
                "top_n": peer["top_n"],
                "rebalance_days": peer["rebalance_days"],
                "lookback_years": peer["lookback_years"],
                **peer["meta"].get("strategy_config", {}),
            }
            if _is_neighbor(config, peer_config):
                neighbor_scores.append(peer["base_objective_score"])

        if len(neighbor_scores) >= 2:
            neighbor_mean = sum(neighbor_scores) / len(neighbor_scores)
            robustness_bonus = max(0.0, neighbor_mean - abs(base_score - neighbor_mean)) * 0.08
        else:
            robustness_bonus = 0.0

        stability_score = max(0.0, 1.0 - stability_penalty + robustness_bonus)
        item["stability_penalty"] = stability_penalty
        item["robustness_bonus"] = robustness_bonus
        item["stability_score"] = stability_score
        item["objective_score"] = base_score - stability_penalty + robustness_bonus
    return evaluations


def _build_unified_config_candidates(search_space: dict, base_config: dict) -> list[dict]:
    candidates: list[dict] = []
    for weight_profile in search_space["weight_profiles"]:
        for threshold_profile in search_space["threshold_profiles"]:
            for bonus_profile in search_space["bonus_profiles"]:
                candidates.append(
                    normalize_scoring_config(
                        {
                            "weights": weight_profile,
                            "thresholds": threshold_profile,
                            "recommendation_bonus": bonus_profile,
                        }
                    )
                )
    base = normalize_scoring_config(base_config)
    candidates.append(base)
    unique = {}
    for item in candidates:
        key = str(item)
        unique[key] = item
    return list(unique.values())


def run_strategy_parameter_optimization(
    symbols: list[str],
    strategy_func,
    symbol_names: dict[str, str] | None = None,
    search_space: dict | None = None,
    strategy_name: str = "unified_selection",
    data_fetch_kwargs: dict | None = None,
) -> dict:
    if not symbols:
        raise BacktestError("股票池为空，无法执行参数优化。")

    if strategy_func is not run_unified_selection:
        raise BacktestError("当前优化器只支持统一评分策略。")

    symbol_names = symbol_names or {}
    search_space = search_space or UNIFIED_SEARCH_SPACE
    evaluations = []
    best_item = None
    recent_best_runs = get_recent_best_optimizer_runs(strategy_name=strategy_name, limit=8)
    base_strategy_config = normalize_scoring_config(get_setting("unified_scoring_config", DEFAULT_SCORING_CONFIG))

    stage_one_best = None
    for top_n in search_space["top_n"]:
        for rebalance_days in search_space["rebalance_days"]:
            for lookback_years in search_space["lookback_years"]:
                effective_strategy = lambda stock_dict, cfg=base_strategy_config: run_unified_selection(stock_dict, config=cfg)
                try:
                    result = backtest_portfolio_realistic(
                        symbols,
                        effective_strategy,
                        top_n=top_n,
                        rebalance_days=rebalance_days,
                        lookback_years=lookback_years,
                        symbol_names=symbol_names,
                        data_fetch_kwargs=data_fetch_kwargs,
                    )
                    metrics = result["metrics"]
                    objective_score = _objective_score(metrics)
                    item = {
                        "top_n": top_n,
                        "rebalance_days": rebalance_days,
                        "lookback_years": lookback_years,
                        "metrics": metrics,
                        "base_objective_score": objective_score,
                        "objective_score": objective_score,
                        "meta": {
                            "symbols_count": len(symbols),
                            "errors_count": len(result.get("errors", {})),
                            "strategy_config": base_strategy_config,
                            "stage": "portfolio",
                        },
                    }
                    evaluations.append(item)
                    if stage_one_best is None or item["objective_score"] > stage_one_best["objective_score"]:
                        stage_one_best = item
                except (BacktestError, DataFetchError):
                    continue

    if stage_one_best is None:
        raise BacktestError("参数优化第一阶段未产出有效结果，请检查股票池、数据源或回测区间。")

    best_portfolio_params = {
        "top_n": stage_one_best["top_n"],
        "rebalance_days": stage_one_best["rebalance_days"],
        "lookback_years": stage_one_best["lookback_years"],
    }

    config_candidates = _build_unified_config_candidates(search_space, base_strategy_config)
    for strategy_config in config_candidates:
        effective_strategy = lambda stock_dict, cfg=strategy_config: run_unified_selection(stock_dict, config=cfg)
        try:
            result = backtest_portfolio_realistic(
                symbols,
                effective_strategy,
                top_n=best_portfolio_params["top_n"],
                rebalance_days=best_portfolio_params["rebalance_days"],
                lookback_years=best_portfolio_params["lookback_years"],
                symbol_names=symbol_names,
                data_fetch_kwargs=data_fetch_kwargs,
            )
            metrics = result["metrics"]
            objective_score = _objective_score(metrics)
            item = {
                "top_n": best_portfolio_params["top_n"],
                "rebalance_days": best_portfolio_params["rebalance_days"],
                "lookback_years": best_portfolio_params["lookback_years"],
                "metrics": metrics,
                "base_objective_score": objective_score,
                "objective_score": objective_score,
                "meta": {
                    "symbols_count": len(symbols),
                    "errors_count": len(result.get("errors", {})),
                    "strategy_config": strategy_config,
                    "stage": "strategy",
                },
            }
            evaluations.append(item)
            if best_item is None or item["objective_score"] > best_item["objective_score"]:
                best_item = item
        except (BacktestError, DataFetchError):
            continue

    if best_item is None:
        best_item = stage_one_best

    evaluations = _apply_stability_constraints(evaluations, recent_best_runs)
    best_item = max(evaluations, key=lambda item: item["objective_score"])

    best_config = {
        "top_n": best_item["top_n"],
        "rebalance_days": best_item["rebalance_days"],
        "lookback_years": best_item["lookback_years"],
        "strategy_name": strategy_name,
        "objective_score": best_item["objective_score"],
        "base_objective_score": best_item["base_objective_score"],
        "stability_score": best_item["stability_score"],
        "stability_penalty": best_item["stability_penalty"],
        "robustness_bonus": best_item["robustness_bonus"],
        **best_item["meta"].get("strategy_config", {}),
    }
    save_optimizer_results(strategy_name, evaluations, best_config)
    set_setting("auto_optimize_enabled", True)
    set_setting("unified_scoring_config", normalize_scoring_config(best_item["meta"].get("strategy_config", {})))

    ranking = sorted(
        [
            {
                "top_n": item["top_n"],
                "rebalance_days": item["rebalance_days"],
                "lookback_years": item["lookback_years"],
                "total_return": item["metrics"]["total_return"],
                "max_drawdown": item["metrics"]["max_drawdown"],
                "positive_period_ratio": item["metrics"]["positive_period_ratio"],
                "base_objective_score": item["base_objective_score"],
                "objective_score": item["objective_score"],
                "stability_score": item.get("stability_score", 0.0),
                "stability_penalty": item.get("stability_penalty", 0.0),
                "robustness_bonus": item.get("robustness_bonus", 0.0),
                "stage": item["meta"].get("stage"),
                **item["meta"].get("strategy_config", {}),
            }
            for item in evaluations
        ],
        key=lambda row: row["objective_score"],
        reverse=True,
    )

    return {
        "best_config": best_config,
        "best_metrics": best_item["metrics"],
        "evaluations": ranking,
    }
