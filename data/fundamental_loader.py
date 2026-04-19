from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from data.akshare_loader import DataFetchError


def _safe_numeric(value):
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _is_membership_stale(updated_at, max_age_days: int) -> bool:
    timestamp = pd.to_datetime(updated_at, errors="coerce")
    if pd.isna(timestamp):
        return True
    age = pd.Timestamp.now() - timestamp
    return age > pd.Timedelta(days=max_age_days)


def _extract_latest_period_columns(df: pd.DataFrame) -> list[str]:
    period_cols = [col for col in df.columns if str(col).isdigit()]
    return sorted(period_cols, reverse=True)


def _extract_metric_value(df: pd.DataFrame, metric_name: str, period: str | None):
    if not period:
        return None
    matched = df[df["指标"] == metric_name]
    if matched.empty or period not in matched.columns:
        return None
    return _safe_numeric(matched.iloc[0][period])


def _find_yoy_period(period_cols: list[str], latest_period: str) -> str | None:
    latest_text = str(latest_period)
    if len(latest_text) != 8:
        return None
    yoy_period = f"{int(latest_text[:4]) - 1}{latest_text[4:]}"
    return yoy_period if yoy_period in period_cols else None


def _safe_growth_rate(current_value, previous_value):
    current = pd.to_numeric(current_value, errors="coerce")
    previous = pd.to_numeric(previous_value, errors="coerce")
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return None
    return float((current - previous) / abs(previous) * 100)


def _snapshot_is_recent(snapshot: dict | None, max_age_hours: float, time_keys: list[str]) -> bool:
    if not snapshot:
        return False
    for key in time_keys:
        timestamp = pd.to_datetime(snapshot.get(key), errors="coerce")
        if not pd.isna(timestamp):
            return timestamp >= pd.Timestamp.now() - pd.Timedelta(hours=max_age_hours)
    return False


def _normalize_tushare_code(symbol: str) -> str:
    normalized = str(symbol).zfill(6)
    if normalized.startswith(("4", "8")):
        suffix = ".BJ"
    elif normalized.startswith(("5", "6", "9")):
        suffix = ".SH"
    else:
        suffix = ".SZ"
    return f"{normalized}{suffix}"


def _needs_backup(snapshot: dict | None, required_fields: list[str]) -> bool:
    if not snapshot:
        return True
    for field in required_fields:
        value = snapshot.get(field)
        if value is None:
            return True
        if isinstance(value, str) and value.strip().lower() in {"", "nan", "none"}:
            return True
    return False


def _merge_snapshot(primary: dict | None, backup: dict | None, *, required_fields: list[str]) -> dict | None:
    if not primary and not backup:
        return None
    if not primary:
        merged = dict(backup or {})
        merged["source"] = merged.get("source", "tushare")
        merged["source_chain"] = merged["source"]
        return merged
    merged = dict(primary)
    if not backup:
        merged["source_chain"] = merged.get("source")
        return merged

    used_backup_fields: list[str] = []
    for field in required_fields:
        primary_value = merged.get(field)
        if primary_value is None or (
            isinstance(primary_value, str) and primary_value.strip().lower() in {"", "nan", "none"}
        ):
            backup_value = backup.get(field)
            if backup_value is not None and not (isinstance(backup_value, str) and not backup_value.strip()):
                merged[field] = backup_value
                used_backup_fields.append(field)

    backup_source = backup.get("source")
    primary_source = merged.get("source")
    if used_backup_fields and backup_source:
        merged["backup_source"] = backup_source
        merged["backup_fields"] = used_backup_fields
        if primary_source and primary_source != backup_source:
            merged["source_chain"] = f"{primary_source} -> {backup_source}"
        else:
            merged["source_chain"] = backup_source
    else:
        merged["source_chain"] = primary_source
    return merged


def _get_tushare_client():
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if not token:
        return None
    try:
        import tushare as ts
    except Exception:
        return None
    try:
        return ts.pro_api(token)
    except Exception:
        return None


def _build_period_lookup(df: pd.DataFrame, value_column: str) -> dict[str, float]:
    if df is None or df.empty or "end_date" not in df.columns or value_column not in df.columns:
        return {}
    subset = df[["end_date", value_column]].copy()
    subset["end_date"] = subset["end_date"].astype(str)
    subset[value_column] = pd.to_numeric(subset[value_column], errors="coerce")
    subset = subset.dropna(subset=["end_date", value_column])
    return {row["end_date"]: float(row[value_column]) for _, row in subset.iterrows()}


def _fetch_tushare_fundamental_snapshot(symbol: str) -> tuple[dict | None, dict | None]:
    pro = _get_tushare_client()
    if pro is None:
        return None, None

    ts_code = _normalize_tushare_code(symbol)
    try:
        basic_df = pro.stock_basic(
            ts_code=ts_code,
            fields="ts_code,symbol,name,industry",
        )
    except Exception:
        basic_df = pd.DataFrame()

    try:
        daily_basic_df = pro.daily_basic(
            ts_code=ts_code,
            fields="ts_code,trade_date,pe,pb,ps_ttm,dv_ttm,total_mv",
            limit=1,
        )
    except Exception:
        daily_basic_df = pd.DataFrame()

    try:
        income_df = pro.income(
            ts_code=ts_code,
            fields="ts_code,end_date,total_revenue,n_income_attr_p",
            limit=8,
        )
    except Exception:
        income_df = pd.DataFrame()

    try:
        indicator_df = pro.fina_indicator(
            ts_code=ts_code,
            fields="ts_code,end_date,roe,debt_to_assets",
            limit=8,
        )
    except Exception:
        indicator_df = pd.DataFrame()

    try:
        cashflow_df = pro.cashflow(
            ts_code=ts_code,
            fields="ts_code,end_date,n_cashflow_act",
            limit=8,
        )
    except Exception:
        cashflow_df = pd.DataFrame()

    if income_df.empty and indicator_df.empty and daily_basic_df.empty and basic_df.empty:
        return None, None

    latest_period = None
    if not income_df.empty and "end_date" in income_df.columns:
        income_df = income_df.sort_values("end_date", ascending=False)
        latest_period = str(income_df.iloc[0]["end_date"])
    elif not indicator_df.empty and "end_date" in indicator_df.columns:
        indicator_df = indicator_df.sort_values("end_date", ascending=False)
        latest_period = str(indicator_df.iloc[0]["end_date"])

    revenue_lookup = _build_period_lookup(income_df, "total_revenue")
    net_profit_lookup = _build_period_lookup(income_df, "n_income_attr_p")
    cashflow_lookup = _build_period_lookup(cashflow_df, "n_cashflow_act")

    yoy_period = _find_yoy_period(sorted(revenue_lookup.keys(), reverse=True), latest_period) if latest_period else None
    name = None
    industry = None
    if not basic_df.empty:
        name = basic_df.iloc[0].get("name")
        industry = basic_df.iloc[0].get("industry")

    roe = None
    debt_ratio = None
    if not indicator_df.empty:
        indicator_df = indicator_df.sort_values("end_date", ascending=False)
        latest_indicator = indicator_df.iloc[0]
        roe = _safe_numeric(latest_indicator.get("roe"))
        debt_ratio = _safe_numeric(latest_indicator.get("debt_to_assets"))

    revenue = revenue_lookup.get(latest_period) if latest_period else None
    net_profit = net_profit_lookup.get(latest_period) if latest_period else None
    operating_cash_flow = cashflow_lookup.get(latest_period) if latest_period else None

    fundamental = {
        "symbol": str(symbol).zfill(6),
        "name": name,
        "report_period": latest_period,
        "revenue": revenue,
        "revenue_yoy": _safe_growth_rate(revenue, revenue_lookup.get(yoy_period)) if latest_period and yoy_period else None,
        "net_profit": net_profit,
        "net_profit_yoy": _safe_growth_rate(net_profit, net_profit_lookup.get(yoy_period)) if latest_period and yoy_period else None,
        "roe": roe,
        "debt_ratio": debt_ratio,
        "operating_cash_flow": operating_cash_flow,
        "source": "tushare.pro",
    }

    latest_daily = daily_basic_df.iloc[0] if not daily_basic_df.empty else {}
    valuation = {
        "symbol": str(symbol).zfill(6),
        "name": name,
        "pe": _safe_numeric(latest_daily.get("pe")) if latest_daily is not None else None,
        "pb": _safe_numeric(latest_daily.get("pb")) if latest_daily is not None else None,
        "ps": _safe_numeric(latest_daily.get("ps_ttm")) if latest_daily is not None else None,
        "dividend_yield": _safe_numeric(latest_daily.get("dv_ttm")) if latest_daily is not None else None,
        "market_value": _safe_numeric(latest_daily.get("total_mv")) if latest_daily is not None else None,
        "industry": industry,
        "source": "tushare.pro",
    }
    return fundamental, valuation


def _fetch_cninfo_industry_membership(symbol: str) -> dict | None:
    date_candidates = ["变更日期", "日期"]
    level_candidates = [
        ("行业次类", "tertiary"),
        ("行业中类", "secondary"),
        ("行业大类", "primary"),
        ("行业门类", "sector"),
    ]
    name_candidates = ["新证券简称", "证券简称", "股票简称"]

    try:
        df = ak.stock_industry_change_cninfo(
            symbol=str(symbol).zfill(6),
            start_date="20000101",
            end_date="20300101",
        )
    except Exception:
        return None

    if df is None or df.empty:
        return None

    date_col = _pick_column(df, date_candidates)
    if date_col:
        df = df.sort_values(date_col)
    latest = df.iloc[-1]

    industry_name = None
    industry_level = "primary"
    for candidate, mapped_level in level_candidates:
        if candidate not in df.columns:
            continue
        raw = str(latest.get(candidate) or "").strip()
        if raw and raw.lower() != "nan":
            industry_name = raw
            industry_level = mapped_level
            break

    if not industry_name:
        return None

    updated_at = latest.get(date_col) if date_col else pd.Timestamp.now()
    parsed_updated_at = pd.to_datetime(updated_at, errors="coerce")
    name = None
    for candidate in name_candidates:
        if candidate not in df.columns:
            continue
        raw = str(latest.get(candidate) or "").strip()
        if raw and raw.lower() != "nan":
            name = raw
            break

    return {
        "symbol": str(symbol).zfill(6),
        "industry_name": industry_name,
        "industry_level": industry_level,
        "source": "akshare.stock_industry_change_cninfo",
        "updated_at": parsed_updated_at.isoformat() if not pd.isna(parsed_updated_at) else pd.Timestamp.now().isoformat(),
        "name": name,
    }


def fetch_fundamental_snapshot(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    revenue_metric = "营业总收入"
    net_profit_metric = "归母净利润"
    roe_metric = "净资产收益率(ROE)"
    debt_metric = "资产负债率"
    cash_metric = "经营现金流量净额"
    eps_metric = "基本每股收益"
    bps_metric = "每股净资产"

    fundamentals: dict | None = None
    valuation: dict | None = None
    latest_eps = None
    latest_bps = None
    akshare_error: Exception | None = None

    try:
        abstract_df = ak.stock_financial_abstract(symbol=symbol)
        if abstract_df is None or abstract_df.empty or "指标" not in abstract_df.columns:
            raise DataFetchError(f"{symbol} 基本面摘要为空或字段异常")

        period_cols = _extract_latest_period_columns(abstract_df)
        if not period_cols:
            raise DataFetchError(f"{symbol} 基本面摘要缺少报告期列")

        latest_period = period_cols[0]
        yoy_period = _find_yoy_period(period_cols, latest_period)
        revenue_latest = _extract_metric_value(abstract_df, revenue_metric, latest_period)
        revenue_previous = _extract_metric_value(abstract_df, revenue_metric, yoy_period)
        net_profit_latest = _extract_metric_value(abstract_df, net_profit_metric, latest_period)
        net_profit_previous = _extract_metric_value(abstract_df, net_profit_metric, yoy_period)

        fundamentals = {
            "symbol": symbol,
            "name": None,
            "report_period": latest_period,
            "revenue": revenue_latest,
            "revenue_yoy": _safe_growth_rate(revenue_latest, revenue_previous),
            "net_profit": net_profit_latest,
            "net_profit_yoy": _safe_growth_rate(net_profit_latest, net_profit_previous),
            "roe": _extract_metric_value(abstract_df, roe_metric, latest_period),
            "debt_ratio": _extract_metric_value(abstract_df, debt_metric, latest_period),
            "operating_cash_flow": _extract_metric_value(abstract_df, cash_metric, latest_period),
            "source": "akshare.stock_financial_abstract",
        }

        latest_eps = _extract_metric_value(abstract_df, eps_metric, latest_period)
        latest_bps = _extract_metric_value(abstract_df, bps_metric, latest_period)

        valuation = {
            "symbol": symbol,
            "name": None,
            "pe": None,
            "pb": None,
            "ps": None,
            "dividend_yield": None,
            "market_value": None,
            "industry": None,
            "source": "akshare.stock_individual_info_em",
        }

        try:
            info_df = ak.stock_individual_info_em(symbol=symbol)
        except Exception:
            info_df = pd.DataFrame(columns=["item", "value"])

        if not info_df.empty and {"item", "value"}.issubset(info_df.columns):
            info_map = dict(zip(info_df["item"], info_df["value"]))
            valuation["market_value"] = _safe_numeric(info_map.get("总市值"))
            valuation["industry"] = info_map.get("行业")
            fundamentals["name"] = info_map.get("股票简称")
            valuation["name"] = info_map.get("股票简称")
    except Exception as error:
        akshare_error = error

    if valuation and not valuation["industry"]:
        cninfo_membership = _fetch_cninfo_industry_membership(symbol)
        if cninfo_membership:
            valuation["industry"] = cninfo_membership.get("industry_name")
            valuation["source"] = cninfo_membership.get("source", valuation["source"])
            if not valuation.get("name") and cninfo_membership.get("name"):
                valuation["name"] = cninfo_membership["name"]
            if fundamentals is not None and not fundamentals.get("name") and cninfo_membership.get("name"):
                fundamentals["name"] = cninfo_membership["name"]

    if fundamentals and valuation:
        if fundamentals["name"] is None and valuation["name"] is not None:
            fundamentals["name"] = valuation["name"]
        if valuation["name"] is None and fundamentals["name"] is not None:
            valuation["name"] = fundamentals["name"]

    if valuation and (valuation["pe"] is None or valuation["pb"] is None):
        try:
            from data.akshare_loader import get_stock_data

            price_df = get_stock_data(symbol)
            latest_close = _safe_numeric(price_df["close"].iloc[-1]) if price_df is not None and not price_df.empty else None
        except Exception:
            latest_close = None

        if latest_close is not None:
            if valuation["pe"] is None and latest_eps not in (None, 0):
                valuation["pe"] = latest_close / latest_eps
                valuation["source"] = "derived_from_close_and_eps"
            if valuation["pb"] is None and latest_bps not in (None, 0):
                valuation["pb"] = latest_close / latest_bps
                if valuation["source"] == "akshare.stock_individual_info_em":
                    valuation["source"] = "derived_from_close_and_bps"

    tushare_fundamental, tushare_valuation = (None, None)
    if _needs_backup(
        fundamentals,
        ["report_period", "revenue", "net_profit", "roe", "debt_ratio", "operating_cash_flow"],
    ) or _needs_backup(valuation, ["market_value", "industry"]):
        tushare_fundamental, tushare_valuation = _fetch_tushare_fundamental_snapshot(symbol)

    fundamentals = _merge_snapshot(
        fundamentals,
        tushare_fundamental,
        required_fields=["name", "report_period", "revenue", "revenue_yoy", "net_profit", "net_profit_yoy", "roe", "debt_ratio", "operating_cash_flow"],
    )
    valuation = _merge_snapshot(
        valuation,
        tushare_valuation,
        required_fields=["name", "pe", "pb", "ps", "dividend_yield", "market_value", "industry"],
    )

    if not fundamentals and not valuation:
        if akshare_error:
            raise DataFetchError(f"获取 {symbol} 基本面摘要失败: {akshare_error}") from akshare_error
        raise DataFetchError(f"{symbol} 基本面与估值快照均不可用")

    fundamental_df = pd.DataFrame([fundamentals]) if fundamentals else pd.DataFrame()
    valuation_df = pd.DataFrame([valuation]) if valuation else pd.DataFrame()
    if not fundamental_df.empty:
        fundamental_df.attrs["source"] = fundamentals.get("source", "unknown")
    if not valuation_df.empty:
        valuation_df.attrs["source"] = valuation.get("source", "unknown")
    return fundamental_df, valuation_df


def load_or_fetch_fundamental_snapshot(
    symbol: str,
    fallback_name: str | None = None,
    max_age_hours: float = 24.0,
    prefer_cache: bool = False,
) -> tuple[dict | None, dict | None]:
    from db.market_db import (
        get_latest_fundamental_snapshot,
        get_latest_industry_membership,
        get_latest_valuation_snapshot,
        save_fundamental_snapshot,
        save_industry_membership,
        save_valuation_snapshot,
    )

    fundamental = get_latest_fundamental_snapshot(symbol)
    valuation = get_latest_valuation_snapshot(symbol)
    membership = get_latest_industry_membership(symbol)
    if fundamental and fallback_name and not fundamental.get("name"):
        fundamental["name"] = fallback_name
    if valuation and fallback_name and not valuation.get("name"):
        valuation["name"] = fallback_name
    if valuation is not None and not valuation.get("industry") and membership:
        valuation["industry"] = membership.get("industry_name")
        valuation["industry_source"] = membership.get("source")

    if prefer_cache and (fundamental or valuation):
        return fundamental, valuation

    if (
        _snapshot_is_recent(fundamental, max_age_hours, ["snapshot_date", "updated_at"])
        or _snapshot_is_recent(valuation, max_age_hours, ["snapshot_date", "updated_at"])
    ):
        return fundamental, valuation

    try:
        fundamental_df, valuation_df = fetch_fundamental_snapshot(symbol)
        if fallback_name:
            if "name" in fundamental_df.columns and pd.isna(fundamental_df.at[0, "name"]):
                fundamental_df.at[0, "name"] = fallback_name
            if "name" in valuation_df.columns and pd.isna(valuation_df.at[0, "name"]):
                valuation_df.at[0, "name"] = fallback_name
        save_fundamental_snapshot(fundamental_df)
        save_valuation_snapshot(valuation_df)
        industry_value = valuation_df.at[0, "industry"] if "industry" in valuation_df.columns else None
        if pd.notna(industry_value) and str(industry_value).strip():
            membership_df = pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "name": valuation_df.at[0, "name"] if "name" in valuation_df.columns else fallback_name,
                        "industry_name": str(industry_value).strip(),
                        "industry_level": "primary",
                        "source": "valuation_snapshot",
                    }
                ]
            )
            save_industry_membership(membership_df, updated_at=pd.Timestamp.now())
        fundamental = fundamental_df.to_dict(orient="records")[0] if not fundamental_df.empty else None
        valuation = valuation_df.to_dict(orient="records")[0] if not valuation_df.empty else None
        return fundamental, valuation
    except DataFetchError:
        return fundamental, valuation


def resolve_industry_membership(
    symbol: str,
    valuation_snapshot: dict | None = None,
    max_age_days: int = 30,
    allow_live_fetch: bool = True,
) -> dict | None:
    from db.market_db import get_latest_industry_membership, get_latest_valuation_snapshot, save_industry_membership

    membership = get_latest_industry_membership(symbol)
    if membership:
        membership = dict(membership)
        membership["stale"] = _is_membership_stale(membership.get("updated_at"), max_age_days)
        membership["resolution_path"] = "industry_membership"
        if not membership["stale"]:
            return membership

    valuation = valuation_snapshot or get_latest_valuation_snapshot(symbol) or {}
    valuation_industry = str(valuation.get("industry") or "").strip()
    if valuation_industry:
        resolved = {
            "symbol": str(symbol).zfill(6),
            "industry_name": valuation_industry,
            "industry_level": "primary",
            "source": valuation.get("source", "valuation_snapshot"),
            "updated_at": valuation.get("snapshot_date") or pd.Timestamp.now().isoformat(),
            "stale": False,
            "resolution_path": "valuation_snapshot",
        }
        save_industry_membership(
            pd.DataFrame(
                [
                    {
                        "symbol": resolved["symbol"],
                        "name": valuation.get("name"),
                        "industry_name": resolved["industry_name"],
                        "industry_level": resolved["industry_level"],
                        "source": resolved["source"],
                    }
                ]
            ),
            updated_at=resolved["updated_at"],
        )
        return resolved

    cninfo_membership = _fetch_cninfo_industry_membership(symbol)
    if cninfo_membership:
        resolved = {
            "symbol": str(symbol).zfill(6),
            "industry_name": cninfo_membership["industry_name"],
            "industry_level": cninfo_membership.get("industry_level", "primary"),
            "source": cninfo_membership.get("source", "akshare.stock_industry_change_cninfo"),
            "updated_at": cninfo_membership.get("updated_at", pd.Timestamp.now().isoformat()),
            "stale": False,
            "resolution_path": "cninfo_change",
        }
        save_industry_membership(pd.DataFrame([cninfo_membership]), updated_at=resolved["updated_at"])
        return resolved

    if allow_live_fetch:
        try:
            info_df = ak.stock_individual_info_em(symbol=str(symbol).zfill(6))
        except Exception:
            info_df = pd.DataFrame(columns=["item", "value"])

        if not info_df.empty and {"item", "value"}.issubset(info_df.columns):
            info_map = dict(zip(info_df["item"], info_df["value"]))
            live_industry = str(info_map.get("行业") or "").strip()
            if live_industry:
                resolved = {
                    "symbol": str(symbol).zfill(6),
                    "industry_name": live_industry,
                    "industry_level": "primary",
                    "source": "akshare.stock_individual_info_em",
                    "updated_at": pd.Timestamp.now().isoformat(),
                    "stale": False,
                    "resolution_path": "live_info",
                }
                save_industry_membership(
                    pd.DataFrame(
                        [
                            {
                                "symbol": resolved["symbol"],
                                "name": info_map.get("股票简称"),
                                "industry_name": resolved["industry_name"],
                                "industry_level": resolved["industry_level"],
                                "source": resolved["source"],
                            }
                        ]
                    ),
                    updated_at=resolved["updated_at"],
                )
                return resolved

    if membership:
        return membership
    return None


def fetch_industry_peer_symbols(industry_name: str, exclude_symbol: str | None = None, limit: int = 6) -> list[dict]:
    from db.market_db import list_industry_members, list_industry_members_from_history, save_industry_membership

    industry_name = (industry_name or "").strip()
    if not industry_name:
        return []

    cached_members = list_industry_members(industry_name, exclude_symbol=exclude_symbol, limit=limit)
    if cached_members:
        return [{"symbol": item["symbol"], "name": item["symbol"]} for item in cached_members]

    history_members = list_industry_members_from_history(industry_name, exclude_symbol=exclude_symbol, limit=limit)
    if history_members:
        return [{"symbol": item["symbol"], "name": item.get("name") or item["symbol"]} for item in history_members]

    try:
        industry_df = ak.stock_board_industry_name_em()
    except Exception:
        return []

    if industry_df is None or industry_df.empty:
        return []

    name_col = _pick_column(industry_df, ["板块名称", "名称", "name"])
    if not name_col:
        return []

    matched = industry_df[industry_df[name_col].astype(str) == industry_name]
    if matched.empty:
        matched = industry_df[industry_df[name_col].astype(str).str.contains(industry_name, na=False)]
    if matched.empty:
        return []

    board_name = str(matched.iloc[0][name_col])
    try:
        constituents_df = ak.stock_board_industry_cons_em(symbol=board_name)
    except Exception:
        return []

    if constituents_df is None or constituents_df.empty:
        return []

    code_col = _pick_column(constituents_df, ["代码", "code"])
    stock_name_col = _pick_column(constituents_df, ["名称", "name"])
    if not code_col:
        return []

    peers: list[dict] = []
    for _, row in constituents_df.iterrows():
        peer_symbol = str(row[code_col]).zfill(6)
        if exclude_symbol and peer_symbol == str(exclude_symbol).zfill(6):
            continue
        peers.append({"symbol": peer_symbol, "name": str(row[stock_name_col]) if stock_name_col else peer_symbol})
        if len(peers) >= limit:
            break

    if peers:
        save_industry_membership(
            pd.DataFrame(
                [
                    {
                        "symbol": item["symbol"],
                        "name": item["name"],
                        "industry_name": industry_name,
                        "industry_level": "primary",
                        "source": "akshare.stock_board_industry_cons_em",
                    }
                    for item in peers
                ]
            ),
            updated_at=pd.Timestamp.now(),
        )

    return peers


def load_or_fetch_industry_peer_snapshots(
    industry_name: str,
    exclude_symbol: str | None = None,
    limit: int = 6,
    prefer_cache: bool = False,
) -> list[dict]:
    from db.market_db import (
        get_industry_peer_snapshots,
        list_industry_members,
        list_industry_members_from_history,
    )

    def _has_quant_snapshot(row: dict) -> bool:
        for key in [
            "roe",
            "debt_ratio",
            "net_profit",
            "net_profit_yoy",
            "revenue",
            "revenue_yoy",
            "market_value",
            "pe",
            "pb",
        ]:
            numeric = pd.to_numeric(row.get(key), errors="coerce")
            if pd.notna(numeric):
                return True
        return False

    def _needs_enrichment(row: dict) -> bool:
        check_keys = ["pe", "pb", "revenue_yoy", "net_profit_yoy"]
        for key in check_keys:
            numeric = pd.to_numeric(row.get(key), errors="coerce")
            if pd.notna(numeric):
                return False
        return True

    local_rows = get_industry_peer_snapshots(industry_name, exclude_symbol=exclude_symbol, limit=max(limit * 5, 20))
    usable_local_rows = [row for row in local_rows if _has_quant_snapshot(row)]
    if prefer_cache and usable_local_rows:
        return usable_local_rows[:limit]

    cached_members = list_industry_members(industry_name, exclude_symbol=exclude_symbol, limit=max(limit * 6, 30))
    history_members = list_industry_members_from_history(industry_name, exclude_symbol=exclude_symbol, limit=max(limit * 6, 30))
    if cached_members:
        peer_symbols = [{"symbol": item["symbol"], "name": item.get("name") or item["symbol"]} for item in cached_members]
    elif history_members:
        peer_symbols = [{"symbol": item["symbol"], "name": item.get("name") or item["symbol"]} for item in history_members]
    else:
        peer_symbols = fetch_industry_peer_symbols(industry_name, exclude_symbol=exclude_symbol, limit=max(limit * 6, 30))

    if prefer_cache:
        return usable_local_rows[:limit]

    if not peer_symbols:
        try:
            from data.akshare_loader import get_stock_lookup

            lookup_df = get_stock_lookup(use_cache=True)
        except Exception:
            lookup_df = pd.DataFrame(columns=["code", "name"])

        if not lookup_df.empty:
            for _, row in lookup_df.head(500).iterrows():
                peer_symbol = str(row.get("code") or "").zfill(6)
                if not peer_symbol or peer_symbol == str(exclude_symbol or "").zfill(6):
                    continue
                membership = resolve_industry_membership(peer_symbol, max_age_days=30, allow_live_fetch=True)
                if membership and str(membership.get("industry_name") or "").strip() == industry_name:
                    peer_symbols.append({"symbol": peer_symbol, "name": str(row.get("name") or peer_symbol)})
                if len(peer_symbols) >= max(limit * 6, 30):
                    break

    if not peer_symbols:
        return local_rows[:limit]

    def _load_one(peer: dict) -> dict | None:
        fundamental, valuation = load_or_fetch_fundamental_snapshot(peer["symbol"], fallback_name=peer.get("name"))
        if not fundamental and not valuation:
            return None
        fundamental = fundamental or {}
        valuation = valuation or {}
        return {
            "symbol": peer["symbol"],
            "name": peer.get("name") or valuation.get("name") or fundamental.get("name") or peer["symbol"],
            "industry": valuation.get("industry") or industry_name,
            "roe": fundamental.get("roe"),
            "debt_ratio": fundamental.get("debt_ratio"),
            "operating_cash_flow": fundamental.get("operating_cash_flow"),
            "net_profit": fundamental.get("net_profit"),
            "net_profit_yoy": fundamental.get("net_profit_yoy"),
            "revenue": fundamental.get("revenue"),
            "revenue_yoy": fundamental.get("revenue_yoy"),
            "market_value": valuation.get("market_value"),
            "pe": valuation.get("pe"),
            "pb": valuation.get("pb"),
        }

    peer_rows = list(usable_local_rows)
    seen_symbols = {row["symbol"] for row in peer_rows}
    fetch_targets = [peer for peer in peer_symbols if peer["symbol"] not in seen_symbols]
    fetch_targets.extend(
        [
            {"symbol": row["symbol"], "name": row.get("name") or row["symbol"]}
            for row in peer_rows
            if _needs_enrichment(row)
        ]
    )
    deduped_targets = []
    target_seen = set()
    for peer in fetch_targets:
        peer_symbol = str(peer["symbol"]).zfill(6)
        if peer_symbol in target_seen:
            continue
        target_seen.add(peer_symbol)
        deduped_targets.append({"symbol": peer_symbol, "name": peer.get("name")})
    fetch_targets = deduped_targets

    if fetch_targets:
        max_workers = min(6, max(1, len(fetch_targets)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_load_one, peer) for peer in fetch_targets]
            for future in as_completed(futures):
                try:
                    row = future.result()
                except Exception:
                    row = None
                if row is not None:
                    existing = next((item for item in peer_rows if item["symbol"] == row["symbol"]), None)
                    if existing is None:
                        peer_rows.append(row)
                    else:
                        existing.update({key: value for key, value in row.items() if value is not None})

    if not peer_rows:
        return []

    peer_rows.sort(
        key=lambda item: pd.to_numeric(item.get("market_value"), errors="coerce")
        if pd.notna(pd.to_numeric(item.get("market_value"), errors="coerce"))
        else -1,
        reverse=True,
    )
    return peer_rows[:limit]
