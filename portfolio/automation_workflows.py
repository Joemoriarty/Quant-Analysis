from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from data.akshare_loader import DataFetchError, get_stock_catalog, get_stock_lookup
from data.fundamental_loader import resolve_industry_membership
from db.market_db import (
    get_setting,
    log_automation_run,
    save_market_catalog_snapshot,
    set_setting,
)
from portfolio.strategy_optimizer import run_strategy_parameter_optimization, sync_market_data_to_db
from strategies.unified_selection import run_unified_selection


DEFAULT_AUTOMATION_POOL_SIZE = 300


def _load_catalog_for_automation(pool_size: int) -> tuple[pd.DataFrame, str | None]:
    degraded_reason = None
    try:
        catalog = get_stock_catalog(limit=pool_size, use_cache=False)
    except DataFetchError as error:
        degraded_reason = str(error)
        catalog = get_stock_catalog(limit=pool_size, use_cache=True)
    return catalog, degraded_reason


def get_automation_pool_size() -> int:
    return int(get_setting("automation_pool_size", DEFAULT_AUTOMATION_POOL_SIZE))


def set_automation_pool_size(pool_size: int) -> None:
    set_setting("automation_pool_size", int(pool_size))


def run_daily_update(pool_size: int | None = None) -> dict:
    pool_size = int(pool_size or get_automation_pool_size())

    try:
        catalog, degraded_reason = _load_catalog_for_automation(pool_size)
        save_market_catalog_snapshot(catalog)
        result = sync_market_data_to_db(
            catalog["code"].tolist(),
            catalog=catalog,
            data_fetch_kwargs={"use_cache": True, "refresh_stale_cache": True},
        )
        payload = {
            "run_time": str(pd.Timestamp.now()),
            "pool_size": pool_size,
            "catalog_size": len(catalog),
            "catalog_source": catalog.attrs.get("source", "unknown"),
            "degraded_reason": degraded_reason,
            **result,
        }
        set_setting("last_daily_update", payload)
        log_automation_run(
            "daily-update",
            status="degraded" if degraded_reason else "success",
            pool_size=pool_size,
            catalog_size=len(catalog),
            degraded=bool(degraded_reason),
            summary=f"写入 {result['saved_symbols']} 只股票，共 {result['saved_rows']} 条行情",
            meta=payload,
        )
        return payload
    except Exception as error:
        log_automation_run(
            "daily-update",
            status="failed",
            pool_size=pool_size,
            degraded=False,
            error_message=str(error),
            meta={},
        )
        raise


def run_weekly_optimization(pool_size: int | None = None) -> dict:
    pool_size = int(pool_size or get_automation_pool_size())

    try:
        catalog, degraded_reason = _load_catalog_for_automation(pool_size)
        symbol_names = dict(zip(catalog["code"], catalog["name"]))
        optimization = run_strategy_parameter_optimization(
            catalog["code"].tolist(),
            run_unified_selection,
            symbol_names=symbol_names,
            strategy_name="unified_selection",
            data_fetch_kwargs={"use_cache": True, "refresh_stale_cache": False},
        )
        payload = {
            "run_time": str(pd.Timestamp.now()),
            "pool_size": pool_size,
            "catalog_size": len(catalog),
            "catalog_source": catalog.attrs.get("source", "unknown"),
            "degraded_reason": degraded_reason,
            **optimization,
        }
        set_setting("last_weekly_optimization", payload)
        log_automation_run(
            "weekly-optimize",
            status="degraded" if degraded_reason else "success",
            pool_size=pool_size,
            catalog_size=len(catalog),
            degraded=bool(degraded_reason),
            summary=(
                f"最优参数 {optimization['best_config']['top_n']}/"
                f"{optimization['best_config']['rebalance_days']}/"
                f"{optimization['best_config']['lookback_years']}"
            ),
            meta=payload,
        )
        return payload
    except Exception as error:
        log_automation_run(
            "weekly-optimize",
            status="failed",
            pool_size=pool_size,
            degraded=False,
            error_message=str(error),
            meta={},
        )
        raise


def run_industry_membership_refresh(pool_size: int | None = None, max_age_days: int = 30) -> dict:
    pool_size = int(pool_size or get_automation_pool_size())
    max_age_days = int(max_age_days)

    try:
        catalog, degraded_reason = _load_catalog_for_automation(pool_size)
        save_market_catalog_snapshot(catalog)
        try:
            lookup_df = get_stock_lookup(use_cache=True)
        except Exception:
            lookup_df = pd.DataFrame(columns=["code", "name"])

        if not lookup_df.empty:
            universe_df = lookup_df[["code", "name"]].drop_duplicates().head(pool_size).copy()
            universe_source = "stock_lookup_cache"
        else:
            universe_df = catalog[["code", "name"]].drop_duplicates().copy()
            universe_source = catalog.attrs.get("source", "unknown")

        records = universe_df.to_dict("records")
        stats = {
            "resolved": 0,
            "fresh": 0,
            "stale": 0,
            "missing": 0,
            "failed": 0,
            "from_membership": 0,
            "from_valuation": 0,
            "from_cninfo": 0,
            "from_live": 0,
        }
        failed_symbols: list[dict] = []
        sample_resolutions: list[dict] = []

        # CNInfo industry classification depends on a JS runtime and is not stable under multithreaded fetches.
        worker_count = 1
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(
                    resolve_industry_membership,
                    record["code"],
                    None,
                    max_age_days,
                    True,
                ): record
                for record in records
            }
            for future in as_completed(future_map):
                record = future_map[future]
                symbol = str(record["code"]).zfill(6)
                name = record.get("name") or symbol
                try:
                    membership = future.result()
                except Exception as error:
                    stats["failed"] += 1
                    failed_symbols.append({"symbol": symbol, "name": name, "error": str(error)})
                    continue

                if not membership:
                    stats["missing"] += 1
                    continue

                stats["resolved"] += 1
                if membership.get("stale"):
                    stats["stale"] += 1
                else:
                    stats["fresh"] += 1

                path = membership.get("resolution_path")
                if path == "industry_membership":
                    stats["from_membership"] += 1
                elif path == "valuation_snapshot":
                    stats["from_valuation"] += 1
                elif path == "cninfo_change":
                    stats["from_cninfo"] += 1
                elif path == "live_info":
                    stats["from_live"] += 1

                if len(sample_resolutions) < 12:
                    sample_resolutions.append(
                        {
                            "symbol": symbol,
                            "name": name,
                            "industry_name": membership.get("industry_name"),
                            "source": membership.get("source"),
                            "resolution_path": path,
                            "stale": bool(membership.get("stale")),
                            "updated_at": membership.get("updated_at"),
                        }
                    )

        degraded = bool(degraded_reason) or stats["missing"] > 0 or stats["failed"] > 0
        payload = {
            "run_time": str(pd.Timestamp.now()),
            "pool_size": pool_size,
            "catalog_size": len(universe_df),
            "catalog_source": universe_source,
            "max_age_days": max_age_days,
            "degraded_reason": degraded_reason,
            **stats,
            "sample_resolutions": sample_resolutions,
            "failed_symbols": failed_symbols[:20],
        }
        set_setting("last_industry_membership_refresh", payload)
        log_automation_run(
            "industry-membership-refresh",
            status="degraded" if degraded else "success",
            pool_size=pool_size,
            catalog_size=len(universe_df),
            degraded=degraded,
            summary=(
                f"解析 {stats['resolved']}/{len(universe_df)}，"
                f"主表 {stats['from_membership']}，估值 {stats['from_valuation']}，"
                f"巨潮 {stats['from_cninfo']}，实时 {stats['from_live']}"
            ),
            meta=payload,
        )
        return payload
    except Exception as error:
        log_automation_run(
            "industry-membership-refresh",
            status="failed",
            pool_size=pool_size,
            degraded=False,
            error_message=str(error),
            meta={"max_age_days": max_age_days},
        )
        raise


def run_named_workflow(task_name: str, pool_size: int | None = None) -> dict:
    if task_name == "daily-update":
        return run_daily_update(pool_size=pool_size)
    if task_name == "weekly-optimize":
        return run_weekly_optimization(pool_size=pool_size)
    if task_name == "industry-membership-refresh":
        return run_industry_membership_refresh(pool_size=pool_size)
    raise ValueError(f"未知任务: {task_name}")
