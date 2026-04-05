from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from data.akshare_loader import DataFetchError, get_realtime_quotes, get_stock_data


DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PAPER_DIR = DATA_DIR / "paper_trading"
SETTINGS_FILE = PAPER_DIR / "settings.json"
LOG_FILE = PAPER_DIR / "paper_trade_log.csv"
MARK_LOG_FILE = PAPER_DIR / "paper_mark_log.csv"
DEFAULT_PAPER_CAPITAL = 100000.0
BOARD_LOT_SIZE = 100


def _ensure_paper_dir() -> None:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)


def is_paper_trading_enabled() -> bool:
    if not SETTINGS_FILE.exists():
        return False
    try:
        return bool(json.loads(SETTINGS_FILE.read_text(encoding="utf-8")).get("enabled", False))
    except Exception:
        return False


def set_paper_trading_enabled(enabled: bool) -> None:
    _ensure_paper_dir()
    settings = _load_settings()
    settings["enabled"] = enabled
    SETTINGS_FILE.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_settings() -> dict:
    if not SETTINGS_FILE.exists():
        return {"enabled": False, "paper_capital": DEFAULT_PAPER_CAPITAL}
    try:
        settings = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        settings = {}
    settings.setdefault("enabled", False)
    settings.setdefault("paper_capital", DEFAULT_PAPER_CAPITAL)
    return settings


def get_paper_trading_capital() -> float:
    settings = _load_settings()
    try:
        capital = float(settings.get("paper_capital", DEFAULT_PAPER_CAPITAL))
    except Exception:
        capital = DEFAULT_PAPER_CAPITAL
    return capital if capital > 0 else DEFAULT_PAPER_CAPITAL


def set_paper_trading_capital(capital: float) -> None:
    _ensure_paper_dir()
    settings = _load_settings()
    settings["paper_capital"] = float(capital)
    SETTINGS_FILE.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")


def _empty_trade_log() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "symbol",
            "display_name",
            "action",
            "score",
            "weight",
            "close_price",
            "reason",
            "entry_time",
            "entry_price_source",
        ]
    )


def load_trade_log() -> pd.DataFrame:
    if not LOG_FILE.exists():
        return _empty_trade_log()

    log_df = pd.read_csv(
        LOG_FILE,
        parse_dates=["trade_date"],
        dtype={"symbol": str, "display_name": str, "action": str},
    )

    for column in ["display_name", "action", "score", "weight", "close_price", "reason", "entry_time", "entry_price_source"]:
        if column not in log_df.columns:
            log_df[column] = None
    return log_df


def load_mark_log() -> pd.DataFrame:
    if not MARK_LOG_FILE.exists():
        return pd.DataFrame(
            columns=[
                "mark_time",
                "trade_date",
                "symbol",
                "display_name",
                "mark_price",
                "mark_price_source",
                "shares",
                "market_value",
                "pnl_amount",
                "pnl_ratio",
            ]
        )

    mark_df = pd.read_csv(
        MARK_LOG_FILE,
        parse_dates=["mark_time", "trade_date"],
        dtype={"symbol": str, "display_name": str, "mark_price_source": str},
    )
    for column in ["display_name", "mark_price", "mark_price_source", "shares", "market_value", "pnl_amount", "pnl_ratio"]:
        if column not in mark_df.columns:
            mark_df[column] = None
    return mark_df


def _get_price_on_or_before(symbol: str, eval_date: pd.Timestamp) -> tuple[float | None, str]:
    try:
        hist_df = get_stock_data(symbol, use_cache=True, refresh_stale_cache=False)
    except DataFetchError:
        return None, "缺失"

    matched = hist_df.loc[hist_df.index <= eval_date]
    if matched.empty:
        return None, "缺失"

    price = matched["close"].iloc[-1]
    if pd.isna(price):
        return None, "缺失"
    return float(price), "历史缓存补全"


def _resolve_eval_price(
    symbol: str,
    next_prices: dict[str, float],
    eval_date: pd.Timestamp,
) -> tuple[float | None, str]:
    current_price = next_prices.get(symbol)
    if current_price is not None and not pd.isna(current_price):
        return float(current_price), "下次调仓记录"
    return _get_price_on_or_before(symbol, eval_date)


def save_trade_log(log_df: pd.DataFrame) -> None:
    _ensure_paper_dir()
    log_df.to_csv(LOG_FILE, index=False)


def save_mark_log(mark_df: pd.DataFrame) -> None:
    _ensure_paper_dir()
    mark_df.to_csv(MARK_LOG_FILE, index=False)


def refresh_trade_mark_prices(
    symbols: list[str] | None = None,
    refresh_stale_cache: bool = True,
) -> tuple[dict[str, float], pd.Timestamp | None, dict]:
    trade_log = load_trade_log()
    if symbols is None:
        symbols = sorted(trade_log["symbol"].dropna().astype(str).unique().tolist()) if not trade_log.empty else []

    latest_prices: dict[str, float] = {}
    latest_dates: list[pd.Timestamp] = []
    failed_symbols: dict[str, str] = {}
    source_stats = {"cache_fresh": 0, "cache_stale": 0, "network": 0, "network_refresh": 0}

    for symbol in symbols:
        try:
            hist_df = get_stock_data(symbol, use_cache=True, refresh_stale_cache=refresh_stale_cache)
        except DataFetchError as error:
            failed_symbols[symbol] = str(error)
            continue
        if hist_df.empty:
            failed_symbols[symbol] = "行情为空"
            continue
        latest_row = hist_df.iloc[-1]
        latest_prices[symbol] = float(latest_row["close"])
        latest_dates.append(pd.to_datetime(hist_df.index[-1]).normalize())
        source = hist_df.attrs.get("source", "unknown")
        if source in source_stats:
            source_stats[source] += 1

    latest_date = max(latest_dates) if latest_dates else None
    diagnostics = {
        "symbol_count": len(symbols),
        "updated_count": len(latest_prices),
        "failed_count": len(failed_symbols),
        "failed_symbols": failed_symbols,
        "source_stats": source_stats,
    }
    return latest_prices, latest_date, diagnostics


def upsert_daily_trade(signal_date: pd.Timestamp, recommendation_df: pd.DataFrame) -> None:
    if recommendation_df.empty:
        return

    trade_date = pd.to_datetime(signal_date).normalize()
    log_df = load_trade_log()
    log_df = log_df[log_df["trade_date"] != trade_date].copy()

    record_df = recommendation_df.copy()
    quote_df = get_realtime_quotes(record_df["symbol"].astype(str).tolist())
    quote_map = quote_df.set_index("symbol").to_dict("index") if not quote_df.empty else {}
    record_df["trade_date"] = trade_date
    record_df["close_price"] = record_df.apply(
        lambda row: quote_map.get(str(row["symbol"]), {}).get("last_price", row.get("close_price")),
        axis=1,
    )
    record_df["entry_time"] = record_df["symbol"].map(
        lambda symbol: quote_map.get(str(symbol), {}).get("quote_time", pd.Timestamp.now())
    )
    record_df["entry_price_source"] = record_df["symbol"].map(
        lambda symbol: quote_map.get(str(symbol), {}).get("price_source", "strategy_snapshot")
    )
    keep_cols = [
        "trade_date",
        "symbol",
        "display_name",
        "action",
        "score",
        "weight",
        "close_price",
        "reason",
        "entry_time",
        "entry_price_source",
    ]
    for column in keep_cols:
        if column not in record_df.columns:
            record_df[column] = None
    if log_df.empty:
        log_df = record_df[keep_cols].copy()
    else:
        log_df = pd.concat([log_df, record_df[keep_cols]], ignore_index=True)
    log_df.sort_values(["trade_date", "score"], ascending=[True, False], inplace=True)
    save_trade_log(log_df)


def compute_trade_performance(latest_prices: dict[str, float], latest_date: pd.Timestamp) -> pd.DataFrame:
    position_df = build_trade_position_review(latest_prices, latest_date)
    if position_df.empty:
        return pd.DataFrame(columns=["持仓日期", "评估日期", "期初资金", "期末资金", "组合收益", "持仓数量", "说明"])

    summary_df = (
        position_df.groupby(["买入日期", "评估日期"], dropna=False)
        .agg(
            期初资金=("期初资金", "first"),
            期末资金=("期末资金", "first"),
            组合收益=("组合收益率", "first"),
            持仓数量=("股票", "count"),
            说明=("价格来源", lambda s: "；".join(sorted(set(str(v) for v in s if pd.notna(v))))),
        )
        .reset_index()
        .rename(columns={"买入日期": "持仓日期"})
    )
    return summary_df


def build_trade_review(latest_prices: dict[str, float], latest_date: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    perf_df = compute_trade_performance(latest_prices, latest_date)
    if perf_df.empty:
        return perf_df, {
            "record_count": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "cumulative_return": 0.0,
            "latest_nav": 1.0,
            "max_period_gain": 0.0,
            "max_period_loss": 0.0,
        }

    review_df = perf_df.copy()
    review_df["组合收益数值"] = review_df["组合收益"].astype(float)
    nav = (1 + review_df["组合收益数值"]).cumprod()
    review_df["累计净值"] = nav
    review_df["是否盈利"] = review_df["组合收益数值"] > 0

    summary = {
        "record_count": int(len(review_df)),
        "win_rate": float(review_df["是否盈利"].mean()),
        "avg_return": float(review_df["组合收益数值"].mean()),
        "cumulative_return": float(nav.iloc[-1] - 1),
        "latest_nav": float(nav.iloc[-1]),
        "max_period_gain": float(review_df["组合收益数值"].max()),
        "max_period_loss": float(review_df["组合收益数值"].min()),
    }
    return review_df, summary


def _calc_board_lot_shares(target_amount: float, price: float) -> tuple[int, int, float]:
    if pd.isna(price) or price <= 0 or target_amount <= 0:
        return 0, 0, 0.0
    lots = int(target_amount // (price * BOARD_LOT_SIZE))
    shares = lots * BOARD_LOT_SIZE
    buy_amount = float(shares * price)
    return lots, shares, buy_amount


def build_trade_position_review(
    latest_prices: dict[str, float],
    latest_date: pd.Timestamp,
    initial_capital: float | None = None,
) -> pd.DataFrame:
    log_df = load_trade_log()
    if log_df.empty:
        return pd.DataFrame(
            columns=[
                "买入日期",
                "评估日期",
                "股票",
                "买入理由",
                "操作建议",
                "权重",
                "期初资金",
                "目标分配金额",
                "买入手数",
                "买入股数",
                "建仓价",
                "建仓金额",
                "评估价",
                "评估市值",
                "盈亏金额",
                "个股收益",
                "组合收益率",
                "评估类型",
                "价格来源",
                "建仓时间",
                "建仓价来源",
                "期末资金",
            ]
        )

    period_capital = float(initial_capital or get_paper_trading_capital())
    latest_date = pd.to_datetime(latest_date).normalize()
    grouped = list(log_df.groupby("trade_date"))
    grouped.sort(key=lambda item: item[0])
    records = []

    for idx, (trade_date, trade_df) in enumerate(grouped):
        if idx + 1 < len(grouped):
            next_trade_date, next_trade_df = grouped[idx + 1]
            next_prices = dict(zip(next_trade_df["symbol"], next_trade_df["close_price"]))
            eval_date = pd.to_datetime(next_trade_date).normalize()
            evaluation_type = "下一调仓日估值"
        else:
            next_prices = latest_prices
            eval_date = latest_date
            evaluation_type = "当前估值"

        period_rows = []
        recovered = []
        missing = []
        for _, row in trade_df.iterrows():
            base_price = row.get("close_price")
            current_price, price_source = _resolve_eval_price(row["symbol"], next_prices, eval_date)
            weight = float(row.get("weight", 0.0) or 0.0)
            target_amount = float(period_capital * weight)
            lots, shares, buy_amount = _calc_board_lot_shares(target_amount, float(base_price) if pd.notna(base_price) else 0.0)

            if pd.isna(base_price) or current_price is None or shares == 0:
                eval_price = None
                market_value = 0.0
                pnl_amount = 0.0
                stock_return = None
            else:
                eval_price = float(current_price)
                market_value = float(shares * eval_price)
                pnl_amount = float(market_value - buy_amount)
                stock_return = float(eval_price / base_price - 1)

            if current_price is None:
                missing.append(row["symbol"])
            elif price_source == "历史缓存补全":
                recovered.append(row["symbol"])

            period_rows.append(
                {
                    "买入日期": pd.to_datetime(trade_date).date(),
                    "评估日期": pd.to_datetime(eval_date).date(),
                    "股票": row.get("display_name", row.get("symbol")),
                    "买入理由": row.get("reason", "") or "当期综合得分靠前",
                    "操作建议": row.get("action", ""),
                    "权重": weight,
                    "期初资金": period_capital,
                    "目标分配金额": target_amount,
                    "买入手数": lots,
                    "买入股数": shares,
                    "建仓价": row.get("close_price"),
                    "建仓金额": buy_amount,
                    "评估价": eval_price,
                    "评估市值": market_value if eval_price is not None else None,
                    "盈亏金额": pnl_amount if eval_price is not None else None,
                    "个股收益": stock_return,
                    "组合收益率": None,
                    "评估类型": evaluation_type,
                    "价格来源": price_source,
                    "建仓时间": row.get("entry_time"),
                    "建仓价来源": row.get("entry_price_source", "strategy_snapshot"),
                    "期末资金": None,
                }
            )

        period_df = pd.DataFrame(period_rows)
        total_buy = float(period_df["建仓金额"].fillna(0).sum())
        total_value = float(period_df["评估市值"].fillna(0).sum())
        leftover_cash = float(period_capital - total_buy)
        period_end_capital = total_value + leftover_cash
        period_return = float(period_end_capital / period_capital - 1) if period_capital > 0 else 0.0
        period_df["组合收益率"] = period_return
        period_df["期末资金"] = period_end_capital
        period_df["价格来源"] = period_df["价格来源"].replace(
            {
                "下次调仓记录": "价格完整-下次调仓记录",
                "历史缓存补全": "价格完整-历史缓存补全",
                "缺失": "价格缺失",
            }
        )
        records.extend(period_df.to_dict(orient="records"))
        period_capital = period_end_capital

    return pd.DataFrame(records)


def build_trade_detail_review(latest_prices: dict[str, float], latest_date: pd.Timestamp) -> pd.DataFrame:
    detail_df = build_trade_position_review(latest_prices, latest_date)
    if detail_df.empty:
        return detail_df
    detail_df = detail_df.copy()
    detail_df["加权贡献"] = (
        detail_df["盈亏金额"].fillna(0) / detail_df["期初资金"].replace(0, pd.NA)
    )
    return detail_df


def build_trade_ledger(latest_prices: dict[str, float], latest_date: pd.Timestamp) -> pd.DataFrame:
    detail_df = build_trade_position_review(latest_prices, latest_date)
    if detail_df.empty:
        return pd.DataFrame(
            columns=["交易日期", "股票", "交易类型", "价格", "手数", "股数", "金额", "收益金额", "说明"]
        )

    records = []
    for _, row in detail_df.iterrows():
        records.append(
            {
                "交易日期": row["买入日期"],
                "股票": row["股票"],
                "交易类型": "买入",
                "价格": row["建仓价"],
                "手数": row["买入手数"],
                "股数": row["买入股数"],
                "金额": row["建仓金额"],
                "收益金额": None,
                "说明": f"{row['买入理由']} | 建仓源: {row.get('建仓价来源', 'strategy_snapshot')}",
            }
        )
        records.append(
            {
                "交易日期": row["评估日期"],
                "股票": row["股票"],
                "交易类型": row.get("评估类型", "当前估值"),
                "价格": row["评估价"],
                "手数": row["买入手数"],
                "股数": row["买入股数"],
                "金额": row["评估市值"],
                "收益金额": row["盈亏金额"],
                "说明": row["价格来源"],
            }
        )
    ledger_df = pd.DataFrame(records)
    return ledger_df.sort_values(["交易日期", "股票", "交易类型"]).reset_index(drop=True)


def refresh_live_position_marks() -> tuple[pd.DataFrame, dict]:
    trade_log = load_trade_log()
    if trade_log.empty:
        return pd.DataFrame(), {"updated_count": 0, "failed_count": 0}

    latest_trade_date = pd.to_datetime(trade_log["trade_date"]).max().normalize()
    latest_holdings = trade_log[trade_log["trade_date"] == latest_trade_date].copy()
    quote_df = get_realtime_quotes(latest_holdings["symbol"].astype(str).tolist())
    quote_map = quote_df.set_index("symbol").to_dict("index") if not quote_df.empty else {}

    capital = get_paper_trading_capital()
    rows = []
    failed = 0
    for _, row in latest_holdings.iterrows():
        symbol = str(row["symbol"])
        weight = float(row.get("weight", 0.0) or 0.0)
        target_amount = capital * weight
        lots, shares, buy_amount = _calc_board_lot_shares(
            target_amount,
            float(row["close_price"]) if pd.notna(row["close_price"]) else 0.0,
        )
        quote = quote_map.get(symbol)
        if not quote or pd.isna(quote.get("last_price")):
            failed += 1
            continue
        mark_price = float(quote["last_price"])
        market_value = float(mark_price * shares)
        pnl_amount = float(market_value - buy_amount)
        pnl_ratio = float(mark_price / row["close_price"] - 1) if pd.notna(row["close_price"]) and row["close_price"] else None
        rows.append(
            {
                "mark_time": pd.to_datetime(quote.get("quote_time", pd.Timestamp.now())),
                "trade_date": latest_trade_date,
                "symbol": symbol,
                "display_name": row.get("display_name", symbol),
                "mark_price": mark_price,
                "mark_price_source": quote.get("price_source", "realtime_spot"),
                "shares": shares,
                "market_value": market_value,
                "pnl_amount": pnl_amount,
                "pnl_ratio": pnl_ratio,
            }
        )

    mark_df = load_mark_log()
    new_df = pd.DataFrame(rows)
    if not new_df.empty:
        mark_df = pd.concat([mark_df, new_df], ignore_index=True)
        mark_df.sort_values(["mark_time", "symbol"], inplace=True)
        save_mark_log(mark_df)
    return new_df, {"updated_count": len(rows), "failed_count": failed}


def build_live_position_review() -> pd.DataFrame:
    trade_log = load_trade_log()
    mark_log = load_mark_log()
    if trade_log.empty or mark_log.empty:
        return pd.DataFrame()

    latest_trade_date = pd.to_datetime(trade_log["trade_date"]).max().normalize()
    latest_holdings = trade_log[trade_log["trade_date"] == latest_trade_date].copy()
    latest_marks = (
        mark_log.sort_values("mark_time")
        .groupby("symbol", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    merged = latest_holdings.merge(
        latest_marks[["symbol", "mark_time", "mark_price", "mark_price_source", "shares", "market_value", "pnl_amount", "pnl_ratio"]],
        on="symbol",
        how="left",
    )
    return merged
