from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import re

import pandas as pd

from data.akshare_loader import DataFetchError
from portfolio.single_stock_analysis import analyze_single_stock, resolve_stock_query


DATA_DIR = Path(__file__).resolve().parent.parent / "data"
WATCHLIST_DIR = DATA_DIR / "watchlist"
WATCHLIST_FILE = WATCHLIST_DIR / "watchlist.csv"


def _ensure_watchlist_dir() -> None:
    WATCHLIST_DIR.mkdir(parents=True, exist_ok=True)


def _safe_int(value, default: int = 0) -> int:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return default
    return int(numeric)


def _safe_float(value, default: float = 0.0) -> float:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return default
    return float(numeric)


def load_watchlist() -> pd.DataFrame:
    if not WATCHLIST_FILE.exists():
        return pd.DataFrame(
            columns=[
                "symbol",
                "name",
                "display_name",
                "note",
                "cost_price",
                "shares",
                "target_weight",
                "added_at",
            ]
        )

    watchlist_df = pd.read_csv(
        WATCHLIST_FILE,
        dtype={"symbol": str, "name": str, "display_name": str, "note": str},
        parse_dates=["added_at"],
    )
    for column in ["name", "display_name", "note", "cost_price", "shares", "target_weight", "added_at"]:
        if column not in watchlist_df.columns:
            watchlist_df[column] = None
    watchlist_df["symbol"] = watchlist_df["symbol"].astype(str).str.zfill(6)
    watchlist_df["display_name"] = watchlist_df.apply(
        lambda row: row["display_name"] if pd.notna(row["display_name"]) and row["display_name"] else f"{row['symbol']} {row.get('name', '')}".strip(),
        axis=1,
    )
    return watchlist_df.sort_values(["added_at", "symbol"], ascending=[False, True]).reset_index(drop=True)


def save_watchlist(watchlist_df: pd.DataFrame) -> None:
    _ensure_watchlist_dir()
    watchlist_df.to_csv(WATCHLIST_FILE, index=False)


def add_watchlist_stock(
    query: str,
    fallback_names: dict[str, str] | None = None,
    note: str = "",
    cost_price: float | None = None,
    shares: int | None = None,
    target_weight: float | None = None,
) -> dict:
    resolved = resolve_stock_query(query, fallback_names=fallback_names)
    watchlist_df = load_watchlist()
    symbol = resolved["symbol"]
    if symbol in watchlist_df["symbol"].tolist():
        return {"symbol": symbol, "name": resolved["name"], "display_name": f"{symbol} {resolved['name']}".strip(), "duplicate": True}

    new_row = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": resolved["name"],
                "display_name": f"{symbol} {resolved['name']}".strip(),
                "note": note.strip(),
                "cost_price": cost_price,
                "shares": shares,
                "target_weight": target_weight,
                "added_at": pd.Timestamp.now(),
            }
        ]
    )
    watchlist_df = pd.concat([watchlist_df, new_row], ignore_index=True)
    save_watchlist(watchlist_df)
    return {"symbol": symbol, "name": resolved["name"], "display_name": f"{symbol} {resolved['name']}".strip(), "duplicate": False}


def remove_watchlist_stocks(symbols: list[str]) -> int:
    if not symbols:
        return 0
    watchlist_df = load_watchlist()
    before = len(watchlist_df)
    watchlist_df = watchlist_df[~watchlist_df["symbol"].isin([str(symbol).zfill(6) for symbol in symbols])].copy()
    save_watchlist(watchlist_df)
    return before - len(watchlist_df)


def update_watchlist_positions(updates: pd.DataFrame) -> int:
    watchlist_df = load_watchlist()
    if watchlist_df.empty or updates.empty:
        return 0

    updates = updates.copy()
    updates["symbol"] = updates["symbol"].astype(str).str.zfill(6)
    updates = updates.set_index("symbol")
    watchlist_df = watchlist_df.set_index("symbol")

    updated = 0
    for symbol in watchlist_df.index.intersection(updates.index):
        for column in ["note", "cost_price", "shares", "target_weight"]:
            if column in updates.columns:
                watchlist_df.at[symbol, column] = updates.at[symbol, column]
        updated += 1

    save_watchlist(watchlist_df.reset_index())
    return updated


def _build_position_advice(row: pd.Series, analysis: dict) -> dict:
    metrics = analysis["metrics"]
    close = float(metrics["close"])
    trend_score = int(analysis["trend_score"])
    recommendation = analysis["recommendation"]

    shares = _safe_int(row.get("shares", 0), default=0)
    cost_price = _safe_float(row.get("cost_price", 0), default=0.0)
    target_weight = _safe_float(row.get("target_weight", 0), default=0.0)
    market_value = close * shares
    pnl_amount = (close - cost_price) * shares if shares > 0 and cost_price > 0 else 0.0
    pnl_ratio = (close / cost_price - 1) if shares > 0 and cost_price > 0 else None

    if shares <= 0:
        action = "未持仓"
        advice = "当前没有持仓，可以按交易决策卡决定是否开仓。"
    elif recommendation == "暂不推荐" and trend_score < 45:
        action = "建议减仓"
        advice = "当前趋势较弱，若你已经持有，优先把仓位降下来。"
    elif close < metrics["ma20"] and analysis["metrics"]["dif"] < analysis["metrics"]["dea"]:
        action = "建议减仓"
        advice = "价格跌破20日线且 MACD 偏弱，更适合先减仓控制风险。"
    elif recommendation == "推荐关注" and trend_score >= 75:
        action = "继续持有"
        advice = "趋势仍然健康，更适合继续持有，接近压力位再分批止盈。"
    elif recommendation == "中性观察":
        action = "持有观察"
        advice = "先不要大幅动作，观察是否重新站稳20日线。"
    else:
        action = "谨慎持有"
        advice = "当前不算强趋势，控制仓位比加仓更重要。"

    if shares > 0 and recommendation == "推荐关注" and trend_score >= 75 and close > metrics["ma20"] > metrics["ma60"]:
        add_action = "可考虑小幅加仓"
    else:
        add_action = "暂不建议加仓"

    if target_weight > 0:
        suggested_weight_range = f"{target_weight * 100:.0f}%"
    else:
        if shares <= 0:
            if recommendation == "推荐关注" and trend_score >= 75:
                suggested_weight_range = "首仓 5%-10%"
            elif recommendation == "推荐关注":
                suggested_weight_range = "试仓 3%-5%"
            else:
                suggested_weight_range = "先不建仓"
        elif trend_score >= 80:
            suggested_weight_range = "8%-12%"
        elif trend_score >= 65:
            suggested_weight_range = "5%-8%"
        elif trend_score >= 50:
            suggested_weight_range = "3%-5%"
        else:
            suggested_weight_range = "控制在 0%-3%"

    return {
        "position_action": action,
        "position_advice": advice,
        "add_action": add_action,
        "current_value": market_value,
        "pnl_amount": pnl_amount,
        "pnl_ratio": pnl_ratio,
        "cost_price": cost_price if shares > 0 else None,
        "shares": shares,
        "target_weight": target_weight if target_weight > 0 else None,
        "suggested_weight_range": suggested_weight_range,
    }


def _analyze_one_watchlist_stock(row: pd.Series) -> dict:
    analysis = analyze_single_stock(str(row["symbol"]).zfill(6), str(row.get("name", row["symbol"])))
    metrics = analysis["metrics"]
    position = _build_position_advice(row, analysis)
    return {
        "symbol": analysis["symbol"],
        "name": analysis["name"],
        "display_name": f"{analysis['symbol']} {analysis['name']}".strip(),
        "note": row.get("note", ""),
        "recommendation": analysis["recommendation"],
        "trend_score": analysis["trend_score"],
        "accumulation_score": analysis["accumulation_score"],
        "close": metrics["close"],
        "support": metrics["support"],
        "resistance": metrics["resistance"],
        "return_20d": metrics["return_20d"],
        "volume_ratio_10d": metrics["volume_ratio_10d"],
        "cost_price": position["cost_price"],
        "shares": position["shares"],
        "target_weight": position["target_weight"],
        "suggested_weight_range": position["suggested_weight_range"],
        "current_value": position["current_value"],
        "pnl_amount": position["pnl_amount"],
        "pnl_ratio": position["pnl_ratio"],
        "position_action": position["position_action"],
        "position_advice": position["position_advice"],
        "add_action": position["add_action"],
        "analysis": analysis,
    }


def analyze_watchlist(max_workers: int = 4) -> tuple[pd.DataFrame, dict[str, str]]:
    watchlist_df = load_watchlist()
    if watchlist_df.empty:
        return pd.DataFrame(), {}

    results: list[dict] = []
    errors: dict[str, str] = {}
    workers = min(max_workers, max(1, len(watchlist_df)))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(_analyze_one_watchlist_stock, row): str(row["symbol"]).zfill(6)
            for _, row in watchlist_df.iterrows()
        }
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                results.append(future.result())
            except (DataFetchError, ValueError) as error:
                errors[symbol] = str(error)

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values(["trend_score", "accumulation_score"], ascending=False).reset_index(drop=True)
    return result_df, errors


def _suggested_weight_midpoint(value) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or "先不建仓" in text:
        return 0.0
    matched = re.findall(r"(\d+(?:\.\d+)?)%", text)
    if not matched:
        return None
    values = [float(item) / 100 for item in matched]
    return sum(values) / len(values)


def build_watchlist_rebalance_plan(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df is None or result_df.empty:
        return pd.DataFrame()

    plan_df = result_df.copy()
    plan_df["current_value"] = plan_df["current_value"].fillna(0.0).astype(float)
    plan_df["close"] = plan_df["close"].fillna(0.0).astype(float)
    total_value = float(plan_df["current_value"].sum())
    if total_value <= 0:
        return pd.DataFrame()

    desired_weights = []
    for _, row in plan_df.iterrows():
        explicit_weight = row.get("target_weight")
        if pd.notna(explicit_weight) and float(explicit_weight) > 0:
            desired_weights.append(float(explicit_weight))
        else:
            desired_weights.append(_suggested_weight_midpoint(row.get("suggested_weight_range")))
    plan_df["desired_weight"] = desired_weights
    plan_df["current_weight"] = plan_df["current_value"] / total_value
    plan_df["desired_value"] = plan_df["desired_weight"].fillna(0.0) * total_value
    plan_df["rebalance_delta_value"] = plan_df["desired_value"] - plan_df["current_value"]

    def _delta_shares(row: pd.Series) -> int | None:
        price = float(row.get("close", 0) or 0)
        if price <= 0:
            return None
        raw_shares = int(row["rebalance_delta_value"] / price)
        if raw_shares > 0:
            return (raw_shares // 100) * 100
        return -((abs(raw_shares) // 100) * 100)

    def _rebalance_action(row: pd.Series) -> str:
        delta = float(row["rebalance_delta_value"])
        if abs(delta) < max(1000.0, float(row["current_value"]) * 0.1):
            return "基本匹配"
        if delta > 0:
            return "建议补仓"
        return "建议减仓"

    plan_df["delta_shares"] = plan_df.apply(_delta_shares, axis=1)
    plan_df["rebalance_action"] = plan_df.apply(_rebalance_action, axis=1)
    return plan_df[
        [
            "display_name",
            "current_value",
            "current_weight",
            "desired_weight",
            "desired_value",
            "rebalance_delta_value",
            "delta_shares",
            "rebalance_action",
            "close",
        ]
    ].reset_index(drop=True)


def build_watchlist_execution_list(result_df: pd.DataFrame) -> pd.DataFrame:
    plan_df = build_watchlist_rebalance_plan(result_df)
    if plan_df.empty:
        return pd.DataFrame()

    execution_df = plan_df.copy()
    execution_df = execution_df[execution_df["rebalance_action"] != "基本匹配"].copy()
    if execution_df.empty:
        return pd.DataFrame()

    def _priority(row: pd.Series) -> int:
        action = str(row["rebalance_action"])
        delta_value = abs(float(row["rebalance_delta_value"]))
        if action == "建议减仓":
            return 0
        if action == "建议补仓":
            return 1
        return 2

    def _instruction(row: pd.Series) -> str:
        shares = int(row["delta_shares"]) if pd.notna(row["delta_shares"]) else 0
        amount = abs(float(row["rebalance_delta_value"]))
        if row["rebalance_action"] == "建议减仓":
            return f"先卖出约 {abs(shares)} 股，回收约 {amount:,.0f} 元"
        return f"再买入约 {abs(shares)} 股，投入约 {amount:,.0f} 元"

    execution_df["priority"] = execution_df.apply(_priority, axis=1)
    execution_df["执行说明"] = execution_df.apply(_instruction, axis=1)
    execution_df["执行顺序"] = range(1, len(execution_df) + 1)
    execution_df = execution_df.sort_values(
        ["priority", "rebalance_delta_value"],
        ascending=[True, True],
    ).reset_index(drop=True)
    execution_df["执行顺序"] = range(1, len(execution_df) + 1)
    return execution_df[
        [
            "执行顺序",
            "display_name",
            "rebalance_action",
            "delta_shares",
            "rebalance_delta_value",
            "close",
            "执行说明",
        ]
    ]
