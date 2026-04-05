from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, Iterable

import akshare as ak
import pandas as pd
import requests


CACHE_DIR = Path(__file__).resolve().parent / "cache"
SYMBOL_CACHE_FILE = CACHE_DIR / "symbols.csv"
LOOKUP_CACHE_FILE = CACHE_DIR / "lookup_symbols.csv"
HIST_REQUIRED_COLUMNS = {"date", "close", "open", "high", "low", "volume"}
REALTIME_REQUIRED_COLUMNS = {"代码", "名称", "最新价"}
DEFAULT_SYMBOL_LIMIT = 100
CATALOG_CACHE_MAX_AGE_SECONDS = 6 * 60 * 60
HIST_CACHE_MAX_AGE_SECONDS = 12 * 60 * 60
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1.0


class DataFetchError(RuntimeError):
    """Raised when market data cannot be fetched or recovered from cache."""


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _hist_cache_file(symbol: str) -> Path:
    return CACHE_DIR / f"{symbol}.csv"


def _classify_error(error: Exception) -> str:
    message = str(error).lower()
    if any(
        keyword in message
        for keyword in ["nameresolutionerror", "nodename nor servname", "failed to resolve", "dns"]
    ):
        return "网络/DNS 解析失败"
    if any(
        keyword in message
        for keyword in ["connectionerror", "max retries exceeded", "httpsconnectionpool", "proxy"]
    ):
        return "网络连接失败"
    if "timeout" in message:
        return "请求超时"
    if "缺少必要字段" in str(error):
        return "接口字段异常"
    if "为空" in str(error):
        return "接口返回空数据"
    return "未知错误"


def _format_error_message(action: str, error: Exception) -> str:
    category = _classify_error(error)
    return (
        f"{action}失败（{category}）：{error}。请检查网络、DNS、代理设置，"
        "或稍后重试。"
    )


def _validate_columns(df: pd.DataFrame, required_columns: Iterable[str], label: str) -> None:
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise DataFetchError(f"{label}缺少必要字段: {', '.join(missing)}")


def _cache_age_seconds(path: Path) -> float:
    if not path.exists():
        return float("inf")
    return time.time() - path.stat().st_mtime


def _attach_source_metadata(df: pd.DataFrame, **metadata) -> pd.DataFrame:
    df = df.copy()
    for key, value in metadata.items():
        df.attrs[key] = value
    return df


def _run_with_retry(action: str, func: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func()
        except Exception as error:  # pragma: no cover - depends on network state
            last_error = error
            if attempt == MAX_RETRIES:
                break
            time.sleep(RETRY_DELAY_SECONDS * attempt)
    raise DataFetchError(_format_error_message(action, last_error)) from last_error


def _read_symbol_cache(limit: int) -> pd.DataFrame:
    if not SYMBOL_CACHE_FILE.exists():
        return pd.DataFrame(
            columns=[
                "code",
                "name",
                "latest_price",
                "pct_change",
                "turnover",
                "volume_ratio",
                "sixty_day_return",
                "ytd_return",
                "pool_score",
                "pool_reason",
            ]
        )

    cached = pd.read_csv(SYMBOL_CACHE_FILE, dtype={"code": str, "name": str})
    _validate_columns(cached, {"code", "name"}, "股票列表缓存")
    for column in [
        "latest_price",
        "pct_change",
        "turnover",
        "volume_ratio",
        "sixty_day_return",
        "ytd_return",
        "pool_score",
        "pool_reason",
    ]:
        if column not in cached.columns:
            cached[column] = None
    return cached.head(limit).reset_index(drop=True)


def _read_lookup_cache() -> pd.DataFrame:
    if not LOOKUP_CACHE_FILE.exists():
        return pd.DataFrame(columns=["code", "name"])

    cached = pd.read_csv(LOOKUP_CACHE_FILE, dtype={"code": str, "name": str})
    _validate_columns(cached, {"code", "name"}, "股票检索缓存")
    return cached[["code", "name"]].dropna(subset=["code"]).reset_index(drop=True)


def _write_symbol_cache(symbol_df: pd.DataFrame) -> None:
    _ensure_cache_dir()
    symbol_df.to_csv(SYMBOL_CACHE_FILE, index=False)


def _write_lookup_cache(symbol_df: pd.DataFrame) -> None:
    _ensure_cache_dir()
    symbol_df[["code", "name"]].to_csv(LOOKUP_CACHE_FILE, index=False)


def _read_hist_cache(symbol: str) -> pd.DataFrame:
    cache_file = _hist_cache_file(symbol)
    if not cache_file.exists():
        raise DataFetchError(f"未找到 {symbol} 的本地缓存")

    cached = pd.read_csv(cache_file, parse_dates=["date"])
    _validate_columns(cached, HIST_REQUIRED_COLUMNS, f"{symbol} 行情缓存")
    cached = cached.sort_values("date")
    cached.set_index("date", inplace=True)
    return cached


def _write_hist_cache(symbol: str, df: pd.DataFrame) -> None:
    _ensure_cache_dir()
    df.reset_index().to_csv(_hist_cache_file(symbol), index=False)


def _build_pool_reason(row: pd.Series) -> str:
    reasons = []
    if pd.notna(row.get("sixty_day_return")):
        reasons.append(f"近60日涨幅 {row['sixty_day_return']:.1f}%")
    if pd.notna(row.get("ytd_return")):
        reasons.append(f"年内涨幅 {row['ytd_return']:.1f}%")
    if pd.notna(row.get("turnover")):
        reasons.append(f"换手率 {row['turnover']:.1f}%")
    return "，".join(reasons[:3])


def _fetch_stock_catalog_from_spot(limit: int) -> pd.DataFrame:
    spot_df = ak.stock_zh_a_spot_em()
    _validate_columns(
        spot_df,
        {"代码", "名称", "最新价", "涨跌幅", "换手率", "量比", "60日涨跌幅", "年初至今涨跌幅"},
        "A股实时行情接口返回",
    )

    catalog = pd.DataFrame(
        {
            "code": spot_df["代码"].astype(str),
            "name": spot_df["名称"].astype(str),
            "latest_price": pd.to_numeric(spot_df["最新价"], errors="coerce"),
            "pct_change": pd.to_numeric(spot_df["涨跌幅"], errors="coerce"),
            "turnover": pd.to_numeric(spot_df["换手率"], errors="coerce"),
            "volume_ratio": pd.to_numeric(spot_df["量比"], errors="coerce"),
            "sixty_day_return": pd.to_numeric(spot_df["60日涨跌幅"], errors="coerce"),
            "ytd_return": pd.to_numeric(spot_df["年初至今涨跌幅"], errors="coerce"),
        }
    )

    catalog = catalog[~catalog["name"].str.contains("ST", na=False)].copy()
    catalog["pool_score"] = (
        catalog["sixty_day_return"].fillna(0) * 0.5
        + catalog["ytd_return"].fillna(0) * 0.3
        + catalog["turnover"].fillna(0) * 0.15
        + catalog["pct_change"].fillna(0) * 0.05
    )
    catalog["pool_reason"] = catalog.apply(_build_pool_reason, axis=1)
    catalog = catalog.sort_values(
        by=["pool_score", "sixty_day_return", "ytd_return", "turnover"],
        ascending=False,
    ).head(limit)
    return catalog.reset_index(drop=True)


def _fetch_stock_catalog_from_hot_rank(limit: int) -> pd.DataFrame:
    hot_df = ak.stock_hot_rank_em()
    _validate_columns(hot_df, {"代码", "股票名称", "最新价", "涨跌幅", "当前排名"}, "人气榜接口返回")
    catalog = pd.DataFrame(
        {
            "code": hot_df["代码"].astype(str).str.replace(r"^(SZ|SH)", "", regex=True),
            "name": hot_df["股票名称"].astype(str),
            "latest_price": pd.to_numeric(hot_df["最新价"], errors="coerce"),
            "pct_change": pd.to_numeric(hot_df["涨跌幅"], errors="coerce"),
            "turnover": None,
            "volume_ratio": None,
            "sixty_day_return": None,
            "ytd_return": None,
            "pool_score": 101 - pd.to_numeric(hot_df["当前排名"], errors="coerce"),
            "pool_reason": "来自东方财富人气榜靠前股票",
        }
    )
    return catalog.head(limit).reset_index(drop=True)


def _fetch_stock_catalog(limit: int) -> pd.DataFrame:
    try:
        return _fetch_stock_catalog_from_spot(limit)
    except Exception:
        return _fetch_stock_catalog_from_hot_rank(limit)


def _fallback_quote_from_hist(symbol: str) -> dict | None:
    try:
        hist_df = get_stock_data(symbol, use_cache=True, refresh_stale_cache=False)
    except DataFetchError:
        return None
    if hist_df.empty:
        return None
    latest_idx = pd.to_datetime(hist_df.index[-1]).normalize()
    latest_row = hist_df.iloc[-1]
    return {
        "symbol": symbol,
        "name": "",
        "last_price": float(latest_row["close"]),
        "open": float(latest_row["open"]) if pd.notna(latest_row.get("open")) else None,
        "high": float(latest_row["high"]) if pd.notna(latest_row.get("high")) else None,
        "low": float(latest_row["low"]) if pd.notna(latest_row.get("low")) else None,
        "volume": float(latest_row["volume"]) if pd.notna(latest_row.get("volume")) else None,
        "pct_change": None,
        "quote_time": latest_idx,
        "price_source": "daily_cache_close",
    }


def _market_prefixed_symbol(symbol: str) -> str:
    return f"sh{symbol}" if str(symbol).startswith(("5", "6", "9")) else f"sz{symbol}"


def _fetch_realtime_quotes_from_sina(symbols: list[str]) -> pd.DataFrame:
    secids = ",".join(_market_prefixed_symbol(symbol) for symbol in symbols)
    response = requests.get(
        "https://hq.sinajs.cn/list=" + secids,
        headers={"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"},
        timeout=5,
    )
    response.encoding = "gbk"
    rows = []
    for line in response.text.splitlines():
        if '="' not in line:
            continue
        left, payload = line.split('="', 1)
        payload = payload.rstrip('";')
        if not payload:
            continue
        secid = left.split("hq_str_")[-1]
        symbol = secid[-6:]
        parts = payload.split(",")
        if len(parts) < 32:
            continue
        last_price = pd.to_numeric(parts[3], errors="coerce")
        if pd.isna(last_price):
            continue
        rows.append(
            {
                "symbol": symbol,
                "name": parts[0],
                "last_price": float(last_price),
                "open": float(pd.to_numeric(parts[1], errors="coerce")) if pd.notna(pd.to_numeric(parts[1], errors="coerce")) else None,
                "high": float(pd.to_numeric(parts[4], errors="coerce")) if pd.notna(pd.to_numeric(parts[4], errors="coerce")) else None,
                "low": float(pd.to_numeric(parts[5], errors="coerce")) if pd.notna(pd.to_numeric(parts[5], errors="coerce")) else None,
                "volume": float(pd.to_numeric(parts[8], errors="coerce")) if pd.notna(pd.to_numeric(parts[8], errors="coerce")) else None,
                "pct_change": None,
                "quote_time": pd.Timestamp.now(),
                "price_source": "realtime_sina",
            }
        )
    return pd.DataFrame(rows)


def _fetch_realtime_quotes_from_tencent(symbols: list[str]) -> pd.DataFrame:
    secids = ",".join(_market_prefixed_symbol(symbol) for symbol in symbols)
    response = requests.get(
        "https://qt.gtimg.cn/q=" + secids,
        headers={"Referer": "https://gu.qq.com", "User-Agent": "Mozilla/5.0"},
        timeout=5,
    )
    response.encoding = "gbk"
    rows = []
    for line in response.text.split(";"):
        if "~" not in line or "=" not in line:
            continue
        left, payload = line.split("=", 1)
        payload = payload.strip().strip('"')
        if not payload:
            continue
        parts = payload.split("~")
        if len(parts) < 34:
            continue
        secid = left.split("v_")[-1]
        symbol = secid[-6:]
        last_price = pd.to_numeric(parts[3], errors="coerce")
        if pd.isna(last_price):
            continue
        prev_close = pd.to_numeric(parts[4], errors="coerce")
        pct_change = None
        if pd.notna(prev_close) and prev_close:
            pct_change = float(last_price / prev_close - 1)
        rows.append(
            {
                "symbol": symbol,
                "name": parts[1],
                "last_price": float(last_price),
                "open": float(pd.to_numeric(parts[5], errors="coerce")) if pd.notna(pd.to_numeric(parts[5], errors="coerce")) else None,
                "high": float(pd.to_numeric(parts[33], errors="coerce")) if len(parts) > 33 and pd.notna(pd.to_numeric(parts[33], errors="coerce")) else None,
                "low": float(pd.to_numeric(parts[34], errors="coerce")) if len(parts) > 34 and pd.notna(pd.to_numeric(parts[34], errors="coerce")) else None,
                "volume": float(pd.to_numeric(parts[36], errors="coerce")) if len(parts) > 36 and pd.notna(pd.to_numeric(parts[36], errors="coerce")) else None,
                "pct_change": pct_change,
                "quote_time": pd.Timestamp.now(),
                "price_source": "realtime_tencent",
            }
        )
    return pd.DataFrame(rows)


def get_realtime_quotes(symbols: list[str]) -> pd.DataFrame:
    symbols = [str(symbol) for symbol in symbols if symbol]
    if not symbols:
        return pd.DataFrame(
            columns=["symbol", "name", "last_price", "open", "high", "low", "volume", "pct_change", "quote_time", "price_source"]
        )

    results: list[dict] = []
    missing = set(symbols)
    fetchers = [
        (
            "akshare_spot",
            lambda target_symbols: _run_with_retry(
                "获取实时行情(AKShare)",
                lambda: ak.stock_zh_a_spot_em(),
            ),
        ),
        ("sina", lambda target_symbols: _fetch_realtime_quotes_from_sina(target_symbols)),
        ("tencent", lambda target_symbols: _fetch_realtime_quotes_from_tencent(target_symbols)),
    ]

    for source_name, fetcher in fetchers:
        if not missing:
            break
        target_symbols = sorted(missing)
        try:
            if source_name == "akshare_spot":
                spot_df = fetcher(target_symbols)
                _validate_columns(spot_df, REALTIME_REQUIRED_COLUMNS, "A股实时行情接口返回")
                filtered = spot_df[spot_df["代码"].astype(str).isin(target_symbols)].copy()
                if filtered.empty:
                    continue
                results.extend(
                    [
                        {
                            "symbol": str(row["代码"]),
                            "name": str(row.get("名称", "")),
                            "last_price": float(pd.to_numeric(row.get("最新价"), errors="coerce")),
                            "open": float(pd.to_numeric(row.get("今开"), errors="coerce")) if pd.notna(pd.to_numeric(row.get("今开"), errors="coerce")) else None,
                            "high": float(pd.to_numeric(row.get("最高"), errors="coerce")) if pd.notna(pd.to_numeric(row.get("最高"), errors="coerce")) else None,
                            "low": float(pd.to_numeric(row.get("最低"), errors="coerce")) if pd.notna(pd.to_numeric(row.get("最低"), errors="coerce")) else None,
                            "volume": float(pd.to_numeric(row.get("成交量"), errors="coerce")) if pd.notna(pd.to_numeric(row.get("成交量"), errors="coerce")) else None,
                            "pct_change": float(pd.to_numeric(row.get("涨跌幅"), errors="coerce")) / 100 if pd.notna(pd.to_numeric(row.get("涨跌幅"), errors="coerce")) else None,
                            "quote_time": pd.Timestamp.now(),
                            "price_source": "realtime_akshare_spot",
                        }
                        for _, row in filtered.iterrows()
                        if pd.notna(pd.to_numeric(row.get("最新价"), errors="coerce"))
                    ]
                )
                missing -= set(filtered["代码"].astype(str).tolist())
            else:
                fetched_df = fetcher(target_symbols)
                if fetched_df.empty:
                    continue
                results.extend(fetched_df.to_dict(orient="records"))
                missing -= set(fetched_df["symbol"].astype(str).tolist())
        except Exception:
            continue

    for symbol in sorted(missing):
        fallback = _fallback_quote_from_hist(symbol)
        if fallback:
            results.append(fallback)

    quote_df = pd.DataFrame(results)
    if quote_df.empty:
        raise DataFetchError("未能获取任何实时行情，实时接口和本地日线缓存都不可用。")
    quote_df = quote_df.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    return quote_df


def get_stock_catalog(limit: int = DEFAULT_SYMBOL_LIMIT, use_cache: bool = True) -> pd.DataFrame:
    cached = _read_symbol_cache(limit)
    cache_age = _cache_age_seconds(SYMBOL_CACHE_FILE)

    if use_cache and not cached.empty and cache_age <= CATALOG_CACHE_MAX_AGE_SECONDS:
        return _attach_source_metadata(
            cached,
            source="cache_fresh",
            cache_age_seconds=cache_age,
            pool_description=f"近期强势股票池 Top {len(cached)}",
        )

    refresh_error = None
    try:
        refreshed = _run_with_retry("获取股票列表", lambda: _fetch_stock_catalog(limit))
        _write_symbol_cache(refreshed)
        source = "network_refresh" if use_cache and not cached.empty else "network"
        return _attach_source_metadata(
            refreshed,
            source=source,
            cache_age_seconds=0.0,
            pool_description=f"近期强势股票池 Top {len(refreshed)}",
        )
    except DataFetchError as error:
        refresh_error = str(error)

    if use_cache and not cached.empty:
        return _attach_source_metadata(
            cached,
            source="cache_stale",
            cache_age_seconds=cache_age,
            refresh_error=refresh_error,
            pool_description=f"近期强势股票池 Top {len(cached)}（旧缓存）",
        )

    raise DataFetchError(refresh_error or "获取股票列表失败")


def get_stock_symbols(limit: int = DEFAULT_SYMBOL_LIMIT, use_cache: bool = True) -> list[str]:
    catalog = get_stock_catalog(limit=limit, use_cache=use_cache)
    return catalog["code"].tolist()


def get_stock_name_map(limit: int = DEFAULT_SYMBOL_LIMIT, use_cache: bool = True) -> dict[str, str]:
    catalog = get_stock_catalog(limit=limit, use_cache=use_cache)
    return dict(zip(catalog["code"], catalog["name"]))


def get_stock_lookup(use_cache: bool = True) -> pd.DataFrame:
    cached = _read_lookup_cache()

    try:
        lookup_df = _run_with_retry("获取股票检索列表", ak.stock_info_a_code_name)
        _validate_columns(lookup_df, {"code", "name"}, "股票检索接口返回")
        lookup_df = (
            lookup_df[["code", "name"]]
            .dropna(subset=["code"])
            .astype({"code": str, "name": str})
            .reset_index(drop=True)
        )
        _write_lookup_cache(lookup_df)
        return _attach_source_metadata(lookup_df, source="network")
    except DataFetchError as error:
        if use_cache and not cached.empty:
            return _attach_source_metadata(cached, source="cache_stale", refresh_error=str(error))
        raise


def _normalize_em_hist(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        raise DataFetchError("东方财富接口返回为空")

    df = df.rename(
        columns={
            "日期": "date",
            "收盘": "close",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        }
    )
    _validate_columns(df, HIST_REQUIRED_COLUMNS, "东方财富行情接口返回")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df.set_index("date", inplace=True)
    return df


def _tx_symbol(symbol: str) -> str:
    return f"sh{symbol}" if symbol.startswith(("5", "6", "9")) else f"sz{symbol}"


def _normalize_tx_hist(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        raise DataFetchError("腾讯接口返回为空")

    normalized = df.rename(columns={"amount": "volume"}).copy()
    _validate_columns(normalized, HIST_REQUIRED_COLUMNS, "腾讯行情接口返回")
    normalized["date"] = pd.to_datetime(normalized["date"])
    normalized = normalized.sort_values("date")
    normalized.set_index("date", inplace=True)
    return normalized


def _fetch_hist_data_from_em(symbol: str) -> pd.DataFrame:
    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        adjust="qfq",
    )
    return _normalize_em_hist(df)


def _fetch_hist_data_from_tx(symbol: str) -> pd.DataFrame:
    df = ak.stock_zh_a_hist_tx(
        symbol=_tx_symbol(symbol),
        start_date="20180101",
        end_date="20500101",
        adjust="qfq",
        timeout=10,
    )
    return _normalize_tx_hist(df)


def _fetch_hist_data(symbol: str) -> pd.DataFrame:
    errors = []
    try:
        df = _fetch_hist_data_from_em(symbol)
        df.attrs["api_source"] = "eastmoney"
        return df
    except Exception as error:
        errors.append(f"东方财富: {error}")

    try:
        df = _fetch_hist_data_from_tx(symbol)
        df.attrs["api_source"] = "tencent"
        return df
    except Exception as error:
        errors.append(f"腾讯证券: {error}")

    raise DataFetchError("；".join(errors))


def get_stock_data(
    symbol: str,
    use_cache: bool = True,
    refresh_stale_cache: bool = True,
) -> pd.DataFrame:
    cache_file = _hist_cache_file(symbol)
    cached = None
    cache_age = _cache_age_seconds(cache_file)

    if use_cache and cache_file.exists():
        try:
            cached = _read_hist_cache(symbol)
            if cache_age <= HIST_CACHE_MAX_AGE_SECONDS:
                return _attach_source_metadata(
                    cached,
                    source="cache_fresh",
                    cache_age_seconds=cache_age,
                    symbol=symbol,
                    api_source="local_cache",
                )
            if not refresh_stale_cache:
                return _attach_source_metadata(
                    cached,
                    source="cache_stale",
                    cache_age_seconds=cache_age,
                    symbol=symbol,
                    api_source="local_cache",
                )
        except Exception:
            cached = None

    refresh_error = None
    try:
        refreshed = _run_with_retry(f"获取 {symbol} 行情", lambda: _fetch_hist_data(symbol))
        _write_hist_cache(symbol, refreshed)
        source = "network_refresh" if cached is not None else "network"
        return _attach_source_metadata(
            refreshed,
            source=source,
            cache_age_seconds=0.0,
            symbol=symbol,
            api_source=refreshed.attrs.get("api_source", "network"),
        )
    except DataFetchError as error:
        refresh_error = str(error)

    if cached is not None:
        return _attach_source_metadata(
            cached,
            source="cache_stale",
            cache_age_seconds=cache_age,
            refresh_error=refresh_error,
            symbol=symbol,
            api_source="local_cache",
        )

    if use_cache and cache_file.exists():
        cache_error = f"本地缓存不可读: {cache_file.name}"
        raise DataFetchError(f"{refresh_error}；{cache_error}")

    raise DataFetchError(f"{refresh_error}；本地缓存也不可用：未找到 {symbol} 的本地缓存")
