from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from data.akshare_loader import DataFetchError, get_stock_data
from strategies.unified_selection import apply_portfolio_constraints


class BacktestError(RuntimeError):
    """Raised when the backtest cannot continue with the available data."""


class Market:
    def __init__(self):
        self.commission = 0.0003
        self.stamp_tax = 0.001
        self.slippage = 0.001
        self.limit = 0.10

    def can_buy(self, today, prev):
        return (today - prev) / prev < self.limit

    def can_sell(self, today, prev):
        return (today - prev) / prev > -self.limit

    def cost(self, r, sell=False):
        c = self.commission + self.slippage
        if sell:
            c += self.stamp_tax
        return r - c


def _display_symbol(symbol: str, symbol_names: dict[str, str]) -> str:
    name = symbol_names.get(symbol, "")
    return f"{symbol} {name}".strip()


def _fetch_single_symbol(symbol: str, data_fetch_kwargs=None):
    df = get_stock_data(symbol, **(data_fetch_kwargs or {}))
    return symbol, df


def _collect_data(symbols, data_fetch_kwargs=None):
    data = {}
    errors = {}
    cache_stats = {"cache_fresh": 0, "cache_stale": 0, "network": 0, "network_refresh": 0}
    api_stats = {}

    max_workers = min(12, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_single_symbol, symbol, data_fetch_kwargs): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                _, df = future.result()
                source = df.attrs.get("source", "unknown")
                api_source = df.attrs.get("api_source", "unknown")
                if source in cache_stats:
                    cache_stats[source] += 1
                api_stats[api_source] = api_stats.get(api_source, 0) + 1
                if df.empty:
                    errors[symbol] = "接口返回空数据"
                    continue
                data[symbol] = df
            except DataFetchError as error:
                errors[symbol] = str(error)

    return data, errors, cache_stats, api_stats


def _common_dates(data):
    common = None
    for df in data.values():
        idx = set(df.index)
        common = idx if common is None else common & idx

    if not common:
        return []

    return sorted(common)


def _filter_recent_dates(dates, lookback_years):
    if not dates:
        return []

    if lookback_years is None:
        return dates

    latest_date = dates[-1]
    cutoff = latest_date - pd.DateOffset(years=lookback_years)
    filtered = [date for date in dates if date >= cutoff]
    return filtered or dates


def _trim_data_for_backtest(data, dates):
    if not dates:
        return data

    start_buffer = dates[0] - pd.Timedelta(days=120)
    trimmed = {}
    for symbol, df in data.items():
        trimmed_df = df.loc[df.index >= start_buffer].copy()
        trimmed[symbol] = trimmed_df if not trimmed_df.empty else df.copy()
    return trimmed


def _compute_metrics(curve: pd.Series) -> dict:
    returns = curve.pct_change().fillna(0.0)
    running_max = curve.cummax()
    drawdown = curve / running_max - 1

    total_return = curve.iloc[-1] - 1
    max_drawdown = drawdown.min()
    positive_period_ratio = (returns > 0).mean()

    return {
        "latest_value": float(curve.iloc[-1]),
        "total_return": float(total_return),
        "max_drawdown": float(max_drawdown),
        "rebalance_count": int(len(curve)),
        "positive_period_ratio": float(positive_period_ratio),
        "start_date": curve.index.min(),
        "end_date": curve.index.max(),
    }


def _build_action_suggestion(rank: int, top_n: int, score: float) -> str:
    if rank <= max(2, top_n // 3) and score > 0:
        return "重点关注"
    if rank <= max(4, (top_n * 2) // 3):
        return "次重点"
    return "仅观察"


def _add_display_columns(
    table: pd.DataFrame,
    symbol_names: dict[str, str],
    top_n: int,
    latest_prices: dict[str, float] | None = None,
) -> pd.DataFrame:
    table = table.copy()
    latest_prices = latest_prices or {}
    table["name"] = table["symbol"].map(symbol_names).fillna("")
    table["display_name"] = table["symbol"].map(lambda code: _display_symbol(code, symbol_names))
    table["close_price"] = table["symbol"].map(latest_prices)
    if "reason" not in table.columns:
        table["reason"] = ""
    table["rank"] = range(1, len(table) + 1)
    table["action"] = table.apply(
        lambda row: _build_action_suggestion(int(row["rank"]), top_n, float(row["score"])),
        axis=1,
    )
    if "position_weight" in table.columns:
        table["weight"] = table["position_weight"]
    else:
        table["weight"] = 1 / len(table) if len(table) else 0.0
    return table


def _build_current_recommendation(data, latest_date, strategy_func, top_n, symbol_names):
    stock_dict = {
        symbol: df.loc[:latest_date]
        for symbol, df in data.items()
        if latest_date in df.index and len(df.loc[:latest_date]) > 30
    }
    latest_prices = {
        symbol: float(df.loc[latest_date, "close"])
        for symbol, df in data.items()
        if latest_date in df.index
    }

    empty_table = pd.DataFrame(
        columns=[
            "symbol",
            "name",
            "display_name",
            "score",
            "reason",
            "action",
            "weight",
            "close_price",
        ]
    )
    if not stock_dict:
        return {"as_of_date": latest_date, "weights": {}, "table": empty_table}

    score_df, weights = strategy_func(stock_dict)
    if score_df.empty:
        return {"as_of_date": latest_date, "weights": weights, "table": empty_table}

    candidate_df = score_df.dropna(subset=["symbol", "score"]).sort_values("score", ascending=False).reset_index(drop=True)
    table, risk_summary = apply_portfolio_constraints(candidate_df, top_n=top_n)
    if table.empty:
        table = candidate_df.head(top_n).reset_index(drop=True)
        risk_summary = {
            "selected_count": 0,
            "skipped_low_liquidity": 0,
            "skipped_industry_cap": 0,
            "industry_exposure": {},
            "max_position_weight": 0.0,
            "min_turnover_amount": 0.0,
            "max_industry_positions": 0,
        }
    table = _add_display_columns(table, symbol_names, top_n, latest_prices)

    columns = ["symbol", "name", "display_name", "score", "reason", "action", "weight", "close_price"]
    extra_columns = [col for col in table.columns if col not in columns]
    return {
        "as_of_date": latest_date,
        "weights": weights,
        "risk_summary": risk_summary,
        "table": table[columns + extra_columns],
    }


def backtest_portfolio_realistic(
    symbols,
    strategy_func,
    top_n=10,
    rebalance_days=20,
    lookback_years=3,
    symbol_names=None,
    data_fetch_kwargs=None,
):
    if not symbols:
        raise BacktestError("股票列表为空，无法运行回测。")

    symbol_names = symbol_names or {}
    market = Market()
    data, errors, cache_stats, api_stats = _collect_data(symbols, data_fetch_kwargs=data_fetch_kwargs)

    if not data:
        detail = "; ".join(f"{symbol}: {msg}" for symbol, msg in list(errors.items())[:5])
        raise BacktestError(f"未能加载任何股票行情。{detail}")

    all_dates = _common_dates(data)
    dates = _filter_recent_dates(all_dates, lookback_years)
    if len(dates) <= 60:
        raise BacktestError("最近区间的共同交易日不足，无法完成回测。请尝试增大回测年数。")

    data = _trim_data_for_backtest(data, dates)

    value = 1.0
    curve_values = []
    holdings = {}
    curve_dates = []
    rebalance_records = []
    diagnostics = []

    for i in range(60, len(dates), rebalance_days):
        date = dates[i]
        latest_prices = {
            symbol: float(df.loc[date, "close"])
            for symbol, df in data.items()
            if date in df.index
        }
        stock_dict = {
            symbol: df.loc[:date]
            for symbol, df in data.items()
            if date in df.index and len(df.loc[:date]) > 30
        }

        if not stock_dict:
            diagnostics.append({"date": date, "message": "当期可用股票不足，已跳过。"})
            continue

        score_df, weights = strategy_func(stock_dict)
        if score_df.empty or "symbol" not in score_df.columns or "score" not in score_df.columns:
            diagnostics.append({"date": date, "message": "策略未返回有效打分结果，已跳过。"})
            continue

        score_df = score_df.dropna(subset=["symbol", "score"])
        if score_df.empty:
            diagnostics.append({"date": date, "message": "策略打分均为空，已跳过。"})
            continue

        candidate_df = score_df.sort_values("score", ascending=False).reset_index(drop=True)
        selected_df, risk_summary = apply_portfolio_constraints(candidate_df, top_n=top_n)
        if selected_df.empty:
            diagnostics.append({"date": date, "message": "风险约束后没有可用股票，已跳过。"})
            continue
        selected = selected_df["symbol"].tolist()
        if not selected:
            diagnostics.append({"date": date, "message": "本次调仓未选出股票，已跳过。"})
            continue

        selected_df = _add_display_columns(selected_df, symbol_names, top_n, latest_prices)

        new_hold = {
            symbol: float(weight)
            for symbol, weight in zip(selected_df["symbol"], selected_df["weight"])
        }
        returns = []

        future_idx = i + rebalance_days
        if future_idx >= len(dates):
            break
        future_date = dates[future_idx]

        period_return = 0.0
        realized_positions = []

        for symbol, weight in holdings.items():
            df = data.get(symbol)
            if df is None or date not in df.index or future_date not in df.index:
                continue

            today = df.loc[date, "close"]
            prev_dates = df.index[df.index < date]
            if len(prev_dates) == 0:
                continue
            prev = df.loc[prev_dates[-1], "close"]

            if not market.can_sell(today, prev):
                continue

            future = df.loc[future_date, "close"]
            r = future / today - 1
            net_r = market.cost(r, sell=True)
            weighted_return = net_r * weight
            returns.append(weighted_return)
            realized_positions.append(
                {
                    "symbol": symbol,
                    "name": symbol_names.get(symbol, ""),
                    "display_name": _display_symbol(symbol, symbol_names),
                    "weight": weight,
                    "period_return": net_r,
                    "weighted_return": weighted_return,
                }
            )

        if returns:
            period_return = sum(returns)
            value *= (1 + period_return)

        curve_values.append(value)
        curve_dates.append(date)
        holdings = new_hold

        rebalance_records.append(
            {
                "rebalance_date": date,
                "next_rebalance_date": future_date,
                "portfolio_value": value,
                "period_return": period_return,
                "selected_symbols": ", ".join(
                    _display_symbol(symbol, symbol_names) for symbol in selected
                ),
                "selected_count": len(selected),
                "strategy_weights": weights,
                "risk_summary": risk_summary,
                "selected_detail": selected_df.to_dict("records"),
                "realized_positions": realized_positions,
            }
        )

    if not curve_values:
        raise BacktestError("回测未生成结果，请检查股票池、回测区间或数据源是否可用。")

    curve = pd.Series(curve_values, index=pd.to_datetime(curve_dates), name="portfolio_value")
    metrics = _compute_metrics(curve)

    holdings_df = pd.DataFrame(
        [
            {
                "调仓日": record["rebalance_date"],
                "下次调仓日": record["next_rebalance_date"],
                "组合净值": record["portfolio_value"],
                "区间收益": record["period_return"],
                "入选数量": record["selected_count"],
                "入选股票": record["selected_symbols"],
            }
            for record in rebalance_records
        ]
    )

    weights_df = pd.DataFrame(
        [
            {
                "调仓日": record["rebalance_date"],
                **record["strategy_weights"],
            }
            for record in rebalance_records
        ]
    ).fillna(0.0)

    diagnostics_df = pd.DataFrame(
        [{"日期": item["date"], "说明": item["message"]} for item in diagnostics]
    )

    current_pick = _build_current_recommendation(
        data, dates[-1], strategy_func, top_n, symbol_names
    )
    latest_prices = {
        symbol: float(df.loc[dates[-1], "close"])
        for symbol, df in data.items()
        if dates[-1] in df.index
    }

    return {
        "curve": curve,
        "metrics": metrics,
        "holdings": holdings_df,
        "weights": weights_df,
        "errors": errors,
        "diagnostics": diagnostics_df,
        "rebalance_records": rebalance_records,
        "current_pick": current_pick,
        "cache_stats": cache_stats,
        "api_stats": api_stats,
        "latest_prices": latest_prices,
    }
