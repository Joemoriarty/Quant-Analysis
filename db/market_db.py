from __future__ import annotations

import json
import sqlite3

import pandas as pd

from storage_paths import DB_DIR, ensure_storage_dirs

DB_PATH = DB_DIR / "market_data.db"


def _connect() -> sqlite3.Connection:
    ensure_storage_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> Path:
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS market_catalog_snapshots (
                snapshot_date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                latest_price REAL,
                pct_change REAL,
                turnover REAL,
                volume_ratio REAL,
                sixty_day_return REAL,
                ytd_return REAL,
                pool_score REAL,
                pool_reason TEXT,
                source TEXT,
                PRIMARY KEY (snapshot_date, code)
            );

            CREATE TABLE IF NOT EXISTS price_history (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                volume REAL,
                data_source TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trade_date)
            );

            CREATE TABLE IF NOT EXISTS strategy_recommendations (
                snapshot_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                score REAL,
                action TEXT,
                reason TEXT,
                weight REAL,
                close_price REAL,
                extra_json TEXT,
                PRIMARY KEY (snapshot_date, symbol)
            );

            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_time TEXT DEFAULT CURRENT_TIMESTAMP,
                strategy_name TEXT,
                top_n INTEGER,
                rebalance_days INTEGER,
                lookback_years INTEGER,
                latest_value REAL,
                total_return REAL,
                max_drawdown REAL,
                positive_period_ratio REAL,
                rebalance_count INTEGER,
                symbols_count INTEGER,
                meta_json TEXT
            );

            CREATE TABLE IF NOT EXISTS optimizer_runs (
                optimizer_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_time TEXT DEFAULT CURRENT_TIMESTAMP,
                strategy_name TEXT,
                top_n INTEGER,
                rebalance_days INTEGER,
                lookback_years INTEGER,
                total_return REAL,
                max_drawdown REAL,
                positive_period_ratio REAL,
                latest_value REAL,
                objective_score REAL,
                is_best INTEGER DEFAULT 0,
                meta_json TEXT
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS automation_runs (
                automation_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_time TEXT DEFAULT CURRENT_TIMESTAMP,
                task_name TEXT,
                status TEXT,
                pool_size INTEGER,
                catalog_size INTEGER,
                degraded INTEGER DEFAULT 0,
                summary TEXT,
                error_message TEXT,
                meta_json TEXT
            );

            CREATE TABLE IF NOT EXISTS fundamental_snapshots (
                snapshot_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                report_period TEXT,
                revenue REAL,
                revenue_yoy REAL,
                net_profit REAL,
                net_profit_yoy REAL,
                roe REAL,
                debt_ratio REAL,
                operating_cash_flow REAL,
                source TEXT,
                extra_json TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_date, symbol)
            );

            CREATE TABLE IF NOT EXISTS valuation_snapshots (
                snapshot_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                pe REAL,
                pb REAL,
                ps REAL,
                dividend_yield REAL,
                market_value REAL,
                source TEXT,
                extra_json TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_date, symbol)
            );

            CREATE TABLE IF NOT EXISTS company_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                name TEXT,
                event_date TEXT NOT NULL,
                event_type TEXT NOT NULL,
                title TEXT,
                summary TEXT,
                importance INTEGER DEFAULT 0,
                source TEXT,
                raw_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS stock_news_items (
                news_id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                name TEXT,
                publish_time TEXT,
                title TEXT NOT NULL,
                summary TEXT,
                content TEXT,
                source TEXT,
                url TEXT,
                sentiment TEXT,
                sentiment_score REAL,
                importance INTEGER DEFAULT 0,
                raw_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS market_sentiment_snapshots (
                snapshot_time TEXT PRIMARY KEY,
                up_count INTEGER,
                down_count INTEGER,
                limit_up_count INTEGER,
                limit_down_count INTEGER,
                consecutive_board_height INTEGER,
                margin_balance REAL,
                score REAL,
                market_state TEXT,
                source TEXT,
                extra_json TEXT
            );

            CREATE TABLE IF NOT EXISTS industry_membership (
                symbol TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_level TEXT DEFAULT 'primary',
                source TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, industry_name, industry_level)
            );

            CREATE TABLE IF NOT EXISTS industry_membership_history (
                snapshot_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                industry_name TEXT NOT NULL,
                industry_level TEXT DEFAULT 'primary',
                source TEXT,
                PRIMARY KEY (snapshot_date, symbol, industry_name, industry_level)
            );

            CREATE TABLE IF NOT EXISTS macro_indicator_snapshots (
                indicator_name TEXT NOT NULL,
                indicator_date TEXT NOT NULL,
                indicator_value REAL,
                unit TEXT,
                frequency TEXT,
                source TEXT,
                extra_json TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (indicator_name, indicator_date)
            );

            CREATE INDEX IF NOT EXISTS idx_price_history_symbol_date
            ON price_history(symbol, trade_date);

            CREATE INDEX IF NOT EXISTS idx_optimizer_best
            ON optimizer_runs(is_best, run_time DESC);

            CREATE INDEX IF NOT EXISTS idx_automation_runs_task_time
            ON automation_runs(task_name, run_time DESC);

            CREATE INDEX IF NOT EXISTS idx_fundamental_symbol_date
            ON fundamental_snapshots(symbol, snapshot_date DESC);

            CREATE INDEX IF NOT EXISTS idx_valuation_symbol_date
            ON valuation_snapshots(symbol, snapshot_date DESC);

            CREATE INDEX IF NOT EXISTS idx_company_events_symbol_date
            ON company_events(symbol, event_date DESC);

            CREATE INDEX IF NOT EXISTS idx_stock_news_symbol_time
            ON stock_news_items(symbol, publish_time DESC, news_id DESC);

            CREATE INDEX IF NOT EXISTS idx_sentiment_state_time
            ON market_sentiment_snapshots(market_state, snapshot_time DESC);

            CREATE INDEX IF NOT EXISTS idx_industry_membership_industry
            ON industry_membership(industry_name, symbol);

            CREATE INDEX IF NOT EXISTS idx_industry_membership_history_industry_date
            ON industry_membership_history(industry_name, snapshot_date DESC, symbol);

            CREATE INDEX IF NOT EXISTS idx_macro_indicator_date
            ON macro_indicator_snapshots(indicator_name, indicator_date DESC);
            """
        )
    return DB_PATH


def _to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
    return out.where(pd.notna(out), None).to_dict("records")


def save_market_catalog_snapshot(catalog: pd.DataFrame, snapshot_date=None) -> int:
    if catalog is None or catalog.empty:
        return 0

    init_db()
    snapshot_date = pd.Timestamp(snapshot_date or pd.Timestamp.now().normalize()).date().isoformat()
    rows = []
    for row in _to_records(catalog):
        rows.append(
            (
                snapshot_date,
                row.get("code"),
                row.get("name"),
                row.get("latest_price"),
                row.get("pct_change"),
                row.get("turnover"),
                row.get("volume_ratio"),
                row.get("sixty_day_return"),
                row.get("ytd_return"),
                row.get("pool_score"),
                row.get("pool_reason"),
                catalog.attrs.get("source", "unknown"),
            )
        )

    with _connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO market_catalog_snapshots (
                snapshot_date, code, name, latest_price, pct_change, turnover,
                volume_ratio, sixty_day_return, ytd_return, pool_score, pool_reason, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def save_fundamental_snapshot(snapshot_df: pd.DataFrame, snapshot_date=None) -> int:
    if snapshot_df is None or snapshot_df.empty:
        return 0

    init_db()
    snapshot_date = pd.Timestamp(snapshot_date or pd.Timestamp.now().normalize()).date().isoformat()
    rows = []
    for row in _to_records(snapshot_df):
        rows.append(
            (
                snapshot_date,
                row.get("symbol"),
                row.get("name"),
                row.get("report_period"),
                row.get("revenue"),
                row.get("revenue_yoy"),
                row.get("net_profit"),
                row.get("net_profit_yoy"),
                row.get("roe"),
                row.get("debt_ratio"),
                row.get("operating_cash_flow"),
                row.get("source", snapshot_df.attrs.get("source", "unknown")),
                json.dumps(
                    {
                        key: value
                        for key, value in row.items()
                        if key
                        not in {
                            "symbol",
                            "name",
                            "report_period",
                            "revenue",
                            "revenue_yoy",
                            "net_profit",
                            "net_profit_yoy",
                            "roe",
                            "debt_ratio",
                            "operating_cash_flow",
                            "source",
                        }
                    },
                    ensure_ascii=False,
                ),
            )
        )

    with _connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO fundamental_snapshots (
                snapshot_date, symbol, name, report_period, revenue, revenue_yoy,
                net_profit, net_profit_yoy, roe, debt_ratio, operating_cash_flow,
                source, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def save_valuation_snapshot(snapshot_df: pd.DataFrame, snapshot_date=None) -> int:
    if snapshot_df is None or snapshot_df.empty:
        return 0

    init_db()
    snapshot_date = pd.Timestamp(snapshot_date or pd.Timestamp.now().normalize()).date().isoformat()
    rows = []
    for row in _to_records(snapshot_df):
        rows.append(
            (
                snapshot_date,
                row.get("symbol"),
                row.get("name"),
                row.get("pe"),
                row.get("pb"),
                row.get("ps"),
                row.get("dividend_yield"),
                row.get("market_value"),
                row.get("source", snapshot_df.attrs.get("source", "unknown")),
                json.dumps(
                    {
                        key: value
                        for key, value in row.items()
                        if key
                        not in {
                            "symbol",
                            "name",
                            "pe",
                            "pb",
                            "ps",
                            "dividend_yield",
                            "market_value",
                            "source",
                        }
                    },
                    ensure_ascii=False,
                ),
            )
        )

    with _connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO valuation_snapshots (
                snapshot_date, symbol, name, pe, pb, ps, dividend_yield, market_value,
                source, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def save_industry_membership(membership_df: pd.DataFrame, updated_at=None) -> int:
    if membership_df is None or membership_df.empty:
        return 0

    init_db()
    updated_at = str(updated_at or pd.Timestamp.now())
    snapshot_date = pd.Timestamp(updated_at).date().isoformat()
    rows = []
    history_rows = []
    for row in _to_records(membership_df):
        industry_name = row.get("industry_name") or row.get("industry")
        symbol = row.get("symbol")
        if not symbol or not industry_name:
            continue
        normalized_symbol = str(symbol).zfill(6)
        normalized_name = row.get("name")
        normalized_level = row.get("industry_level", "primary")
        normalized_source = row.get("source", membership_df.attrs.get("source", "unknown"))
        rows.append(
            (
                normalized_symbol,
                str(industry_name).strip(),
                normalized_level,
                normalized_source,
                updated_at,
            )
        )
        history_rows.append(
            (
                snapshot_date,
                normalized_symbol,
                normalized_name,
                str(industry_name).strip(),
                normalized_level,
                normalized_source,
            )
        )

    if not rows:
        return 0

    with _connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO industry_membership (
                symbol, industry_name, industry_level, source, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.executemany(
            """
            INSERT OR REPLACE INTO industry_membership_history (
                snapshot_date, symbol, name, industry_name, industry_level, source
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            history_rows,
        )
    return len(rows)


def save_market_sentiment_snapshot(snapshot: dict) -> None:
    if not snapshot:
        return

    init_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO market_sentiment_snapshots (
                snapshot_time, up_count, down_count, limit_up_count, limit_down_count,
                consecutive_board_height, margin_balance, score, market_state, source, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.get("snapshot_time"),
                snapshot.get("up_count"),
                snapshot.get("down_count"),
                snapshot.get("limit_up_count"),
                snapshot.get("limit_down_count"),
                snapshot.get("consecutive_board_height"),
                snapshot.get("margin_balance"),
                snapshot.get("score"),
                snapshot.get("market_state"),
                snapshot.get("source", "unknown"),
                json.dumps(snapshot.get("extra", {}), ensure_ascii=False),
            ),
        )


def save_company_events(symbol: str, events: list[dict], replace_from_date: str | None = None) -> int:
    if not events:
        return 0

    init_db()
    normalized_symbol = str(symbol).zfill(6)
    with _connect() as conn:
        if replace_from_date:
            conn.execute(
                """
                DELETE FROM company_events
                WHERE symbol = ? AND event_date >= ?
                """,
                (normalized_symbol, replace_from_date),
            )

        for item in events:
            conn.execute(
                """
                INSERT INTO company_events (
                    symbol, name, event_date, event_type, title, summary,
                    importance, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_symbol,
                    item.get("name"),
                    item.get("event_date"),
                    item.get("event_type"),
                    item.get("title"),
                    item.get("summary"),
                    int(item.get("importance", 0) or 0),
                    item.get("source"),
                    json.dumps(item.get("raw", {}), ensure_ascii=False),
                ),
            )
    return len(events)


def save_price_history(symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    init_db()
    normalized = df.reset_index().copy()
    if "date" not in normalized.columns:
        normalized.rename(columns={normalized.columns[0]: "date"}, inplace=True)
    rows = []
    for row in _to_records(normalized):
        rows.append(
            (
                symbol,
                pd.Timestamp(row["date"]).date().isoformat(),
                row.get("open"),
                row.get("close"),
                row.get("high"),
                row.get("low"),
                row.get("volume"),
                df.attrs.get("api_source", "unknown"),
            )
        )

    with _connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO price_history (
                symbol, trade_date, open, close, high, low, volume, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def save_stock_news_items(symbol: str, news_items: list[dict], replace_from_time: str | None = None) -> int:
    init_db()
    symbol = str(symbol).zfill(6)
    rows = []
    for item in news_items or []:
        rows.append(
            (
                symbol,
                item.get("name"),
                item.get("publish_time"),
                item.get("title"),
                item.get("summary"),
                item.get("content"),
                item.get("source"),
                item.get("url"),
                item.get("sentiment"),
                item.get("sentiment_score"),
                int(pd.to_numeric(item.get("importance"), errors="coerce") or 0),
                json.dumps(
                    {
                        key: value
                        for key, value in item.items()
                        if key
                        not in {
                            "name",
                            "publish_time",
                            "title",
                            "summary",
                            "content",
                            "source",
                            "url",
                            "sentiment",
                            "sentiment_score",
                            "importance",
                        }
                    },
                    ensure_ascii=False,
                ),
            )
        )

    with _connect() as conn:
        if replace_from_time:
            conn.execute(
                "DELETE FROM stock_news_items WHERE symbol = ? AND publish_time >= ?",
                (symbol, str(replace_from_time)),
            )
        if rows:
            conn.executemany(
                """
                INSERT INTO stock_news_items (
                    symbol, name, publish_time, title, summary, content, source, url,
                    sentiment, sentiment_score, importance, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    return len(rows)


def save_recommendations(snapshot_date, recommendation_df: pd.DataFrame) -> int:
    if recommendation_df is None or recommendation_df.empty:
        return 0

    init_db()
    snapshot_date = pd.Timestamp(snapshot_date).date().isoformat()
    rows = []
    for row in _to_records(recommendation_df):
        extra = {
            key: value
            for key, value in row.items()
            if key
            not in {"symbol", "name", "score", "action", "reason", "weight", "close_price", "display_name"}
        }
        rows.append(
            (
                snapshot_date,
                row.get("symbol"),
                row.get("name"),
                row.get("score"),
                row.get("action"),
                row.get("reason"),
                row.get("weight"),
                row.get("close_price"),
                json.dumps(extra, ensure_ascii=False),
            )
        )

    with _connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO strategy_recommendations (
                snapshot_date, symbol, name, score, action, reason, weight, close_price, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def save_backtest_run(
    strategy_name: str,
    top_n: int,
    rebalance_days: int,
    lookback_years: int,
    metrics: dict,
    symbols_count: int,
    meta: dict | None = None,
) -> int:
    init_db()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO backtest_runs (
                strategy_name, top_n, rebalance_days, lookback_years, latest_value,
                total_return, max_drawdown, positive_period_ratio, rebalance_count,
                symbols_count, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strategy_name,
                top_n,
                rebalance_days,
                lookback_years,
                metrics.get("latest_value"),
                metrics.get("total_return"),
                metrics.get("max_drawdown"),
                metrics.get("positive_period_ratio"),
                metrics.get("rebalance_count"),
                symbols_count,
                json.dumps(meta or {}, ensure_ascii=False),
            ),
        )
        return int(cursor.lastrowid)


def save_optimizer_results(strategy_name: str, evaluations: list[dict], best_config: dict) -> int:
    init_db()
    with _connect() as conn:
        conn.execute("UPDATE optimizer_runs SET is_best = 0 WHERE strategy_name = ?", (strategy_name,))
        count = 0
        for item in evaluations:
            item_cfg = item.get("meta", {}).get("strategy_config", {})
            is_best = int(
                item["top_n"] == best_config["top_n"]
                and item["rebalance_days"] == best_config["rebalance_days"]
                and item["lookback_years"] == best_config["lookback_years"]
                and item_cfg.get("momentum_window") == best_config.get("momentum_window")
                and item_cfg.get("reversal_window") == best_config.get("reversal_window")
                and item_cfg.get("volume_window") == best_config.get("volume_window")
                and item_cfg.get("accumulation_threshold") == best_config.get("accumulation_threshold")
                and item_cfg.get("sell_rule_mode") == best_config.get("sell_rule_mode")
            )
            conn.execute(
                """
                INSERT INTO optimizer_runs (
                    strategy_name, top_n, rebalance_days, lookback_years, total_return,
                    max_drawdown, positive_period_ratio, latest_value, objective_score,
                    is_best, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy_name,
                    item["top_n"],
                    item["rebalance_days"],
                    item["lookback_years"],
                    item["metrics"].get("total_return"),
                    item["metrics"].get("max_drawdown"),
                    item["metrics"].get("positive_period_ratio"),
                    item["metrics"].get("latest_value"),
                    item["objective_score"],
                    is_best,
                    json.dumps(item.get("meta", {}), ensure_ascii=False),
                ),
            )
            count += 1

        conn.execute(
            """
            INSERT OR REPLACE INTO app_settings (setting_key, setting_value, updated_at)
            VALUES ('best_strategy_config', ?, CURRENT_TIMESTAMP)
            """,
            (json.dumps(best_config, ensure_ascii=False),),
        )
    return count


def set_setting(key: str, value) -> None:
    init_db()
    payload = json.dumps(value, ensure_ascii=False)
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO app_settings (setting_key, setting_value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (key, payload),
        )


def get_setting(key: str, default=None):
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT setting_value FROM app_settings WHERE setting_key = ?",
            (key,),
        ).fetchone()
    if not row:
        return default
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return default


def get_best_strategy_config(default=None):
    return get_setting("best_strategy_config", default)


def get_latest_fundamental_snapshot(symbol: str) -> dict | None:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT snapshot_date, symbol, name, report_period, revenue, revenue_yoy,
                   net_profit, net_profit_yoy, roe, debt_ratio, operating_cash_flow,
                   source, extra_json
            FROM fundamental_snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
    if not row:
        return None
    extra = {}
    if row[12]:
        try:
            extra = json.loads(row[12])
        except json.JSONDecodeError:
            extra = {}
    return {
        "snapshot_date": row[0],
        "symbol": row[1],
        "name": row[2],
        "report_period": row[3],
        "revenue": row[4],
        "revenue_yoy": row[5],
        "net_profit": row[6],
        "net_profit_yoy": row[7],
        "roe": row[8],
        "debt_ratio": row[9],
        "operating_cash_flow": row[10],
        "source": row[11],
        **extra,
    }


def get_latest_valuation_snapshot(symbol: str) -> dict | None:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT snapshot_date, symbol, name, pe, pb, ps, dividend_yield, market_value,
                   source, extra_json
            FROM valuation_snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
    if not row:
        return None
    extra = {}
    if row[9]:
        try:
            extra = json.loads(row[9])
        except json.JSONDecodeError:
            extra = {}
    return {
        "snapshot_date": row[0],
        "symbol": row[1],
        "name": row[2],
        "pe": row[3],
        "pb": row[4],
        "ps": row[5],
        "dividend_yield": row[6],
        "market_value": row[7],
        "source": row[8],
        **extra,
    }


def get_latest_industry_membership(symbol: str, industry_level: str = "primary") -> dict | None:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT symbol, industry_name, industry_level, source, updated_at
            FROM industry_membership
            WHERE symbol = ? AND industry_level = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (str(symbol).zfill(6), industry_level),
        ).fetchone()
    if not row:
        return None
    return {
        "symbol": row[0],
        "industry_name": row[1],
        "industry_level": row[2],
        "source": row[3],
        "updated_at": row[4],
    }


def list_industry_members(industry_name: str, exclude_symbol: str | None = None, limit: int = 20) -> list[dict]:
    init_db()
    params: list = [industry_name]
    query = """
        SELECT symbol, industry_name, industry_level, source, updated_at
        FROM industry_membership
        WHERE industry_name = ?
    """
    if exclude_symbol:
        query += " AND symbol <> ?"
        params.append(str(exclude_symbol).zfill(6))
    query += " ORDER BY updated_at DESC, symbol ASC LIMIT ?"
    params.append(int(limit))

    with _connect() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [
        {
            "symbol": row[0],
            "industry_name": row[1],
            "industry_level": row[2],
            "source": row[3],
            "updated_at": row[4],
        }
        for row in rows
    ]


def list_industry_members_from_history(
    industry_name: str,
    exclude_symbol: str | None = None,
    limit: int = 20,
    snapshot_date: str | None = None,
) -> list[dict]:
    init_db()
    params: list = [industry_name]
    snapshot_clause = ""
    if snapshot_date:
        snapshot_clause = "AND snapshot_date = ?"
        params.append(snapshot_date)
    else:
        snapshot_clause = """
        AND snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM industry_membership_history
            WHERE industry_name = ?
        )
        """
        params.append(industry_name)

    symbol_clause = ""
    if exclude_symbol:
        symbol_clause = "AND symbol <> ?"
        params.append(str(exclude_symbol).zfill(6))
    params.append(int(limit))

    query = f"""
        SELECT snapshot_date, symbol, name, industry_name, industry_level, source
        FROM industry_membership_history
        WHERE industry_name = ?
        {snapshot_clause}
        {symbol_clause}
        ORDER BY snapshot_date DESC, symbol ASC
        LIMIT ?
    """

    with _connect() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [
        {
            "snapshot_date": row[0],
            "symbol": row[1],
            "name": row[2],
            "industry_name": row[3],
            "industry_level": row[4],
            "source": row[5],
        }
        for row in rows
    ]


def get_industry_peer_snapshots(
    industry_name: str,
    exclude_symbol: str | None = None,
    limit: int = 30,
) -> list[dict]:
    init_db()
    params: list = [industry_name]
    symbol_filter = ""
    if exclude_symbol:
        symbol_filter = "AND symbol <> ?"
        params.append(str(exclude_symbol).zfill(6))
    params.append(int(limit))

    query = f"""
        WITH latest_membership AS (
            SELECT symbol, industry_name, industry_level, source, updated_at
            FROM industry_membership
            WHERE industry_name = ? AND industry_level = 'primary' {symbol_filter}
        ),
        latest_fundamental AS (
            SELECT f1.*
            FROM fundamental_snapshots f1
            JOIN (
                SELECT symbol, MAX(snapshot_date) AS snapshot_date
                FROM fundamental_snapshots
                GROUP BY symbol
            ) f2
            ON f1.symbol = f2.symbol AND f1.snapshot_date = f2.snapshot_date
        ),
        latest_valuation AS (
            SELECT v1.*
            FROM valuation_snapshots v1
            JOIN (
                SELECT symbol, MAX(snapshot_date) AS snapshot_date
                FROM valuation_snapshots
                GROUP BY symbol
            ) v2
            ON v1.symbol = v2.symbol AND v1.snapshot_date = v2.snapshot_date
        )
        SELECT
            im.symbol,
            COALESCE(lv.name, lf.name, im.symbol) AS name,
            im.industry_name,
            lf.roe,
            lf.debt_ratio,
            lf.operating_cash_flow,
            lf.net_profit,
            lf.net_profit_yoy,
            lf.revenue,
            lf.revenue_yoy,
            lv.market_value,
            lv.pe,
            lv.pb,
            lf.snapshot_date AS fundamental_snapshot_date,
            lv.snapshot_date AS valuation_snapshot_date,
            im.source AS membership_source,
            im.updated_at AS membership_updated_at
        FROM latest_membership im
        LEFT JOIN latest_fundamental lf
        ON im.symbol = lf.symbol
        LEFT JOIN latest_valuation lv
        ON im.symbol = lv.symbol
        ORDER BY
            CASE WHEN lv.market_value IS NULL THEN 1 ELSE 0 END,
            lv.market_value DESC,
            im.updated_at DESC
        LIMIT ?
    """

    with _connect() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return [
        {
            "symbol": row[0],
            "name": row[1],
            "industry": row[2],
            "roe": row[3],
            "debt_ratio": row[4],
            "operating_cash_flow": row[5],
            "net_profit": row[6],
            "net_profit_yoy": row[7],
            "revenue": row[8],
            "revenue_yoy": row[9],
            "market_value": row[10],
            "pe": row[11],
            "pb": row[12],
            "fundamental_snapshot_date": row[13],
            "valuation_snapshot_date": row[14],
            "membership_source": row[15],
            "membership_updated_at": row[16],
        }
        for row in rows
    ]


def get_latest_market_sentiment_snapshot() -> dict | None:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT snapshot_time, up_count, down_count, limit_up_count, limit_down_count,
                   consecutive_board_height, margin_balance, score, market_state, source, extra_json
            FROM market_sentiment_snapshots
            ORDER BY snapshot_time DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    extra = {}
    if row[10]:
        try:
            extra = json.loads(row[10])
        except json.JSONDecodeError:
            extra = {}
    return {
        "snapshot_time": row[0],
        "up_count": row[1],
        "down_count": row[2],
        "limit_up_count": row[3],
        "limit_down_count": row[4],
        "consecutive_board_height": row[5],
        "margin_balance": row[6],
        "score": row[7],
        "market_state": row[8],
        "source": row[9],
        **extra,
    }


def get_recent_company_events(symbol: str, lookback_days: int = 30, limit: int = 50) -> list[dict]:
    init_db()
    since_date = (pd.Timestamp.now() - pd.Timedelta(days=max(int(lookback_days), 1))).date().isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, symbol, name, event_date, event_type, title, summary,
                   importance, source, raw_json, created_at
            FROM company_events
            WHERE symbol = ? AND event_date >= ?
            ORDER BY event_date DESC, importance DESC, event_id DESC
            LIMIT ?
            """,
            (str(symbol).zfill(6), since_date, int(limit)),
        ).fetchall()

    results: list[dict] = []
    for row in rows:
        raw = {}
        if row[9]:
            try:
                raw = json.loads(row[9])
            except json.JSONDecodeError:
                raw = {}
        results.append(
            {
                "event_id": row[0],
                "symbol": row[1],
                "name": row[2],
                "event_date": row[3],
                "event_type": row[4],
                "title": row[5],
                "summary": row[6],
                "importance": row[7],
                "source": row[8],
                "created_at": row[10],
                **raw,
            }
        )
    return results


def get_recent_stock_news(symbol: str, lookback_hours: int = 72, limit: int = 20) -> list[dict]:
    init_db()
    since_time = (pd.Timestamp.now() - pd.Timedelta(hours=max(int(lookback_hours), 1))).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT news_id, symbol, name, publish_time, title, summary, content, source, url,
                   sentiment, sentiment_score, importance, raw_json, created_at
            FROM stock_news_items
            WHERE symbol = ? AND publish_time >= ?
            ORDER BY publish_time DESC, importance DESC, news_id DESC
            LIMIT ?
            """,
            (str(symbol).zfill(6), since_time, int(limit)),
        ).fetchall()

    results: list[dict] = []
    for row in rows:
        raw = {}
        if row[12]:
            try:
                raw = json.loads(row[12])
            except json.JSONDecodeError:
                raw = {}
        results.append(
            {
                "news_id": row[0],
                "symbol": row[1],
                "name": row[2],
                "publish_time": row[3],
                "title": row[4],
                "summary": row[5],
                "content": row[6],
                "source": row[7],
                "url": row[8],
                "sentiment": row[9],
                "sentiment_score": row[10],
                "importance": row[11],
                "created_at": row[13],
                **raw,
            }
        )
    return results


def get_db_status() -> dict:
    init_db()
    with _connect() as conn:
        price_rows = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
        symbol_rows = conn.execute("SELECT COUNT(DISTINCT symbol) FROM price_history").fetchone()[0]
        catalog_rows = conn.execute("SELECT COUNT(*) FROM market_catalog_snapshots").fetchone()[0]
        reco_rows = conn.execute("SELECT COUNT(*) FROM strategy_recommendations").fetchone()[0]
        optimizer_rows = conn.execute("SELECT COUNT(*) FROM optimizer_runs").fetchone()[0]
        automation_rows = conn.execute("SELECT COUNT(*) FROM automation_runs").fetchone()[0]
        fundamental_rows = conn.execute("SELECT COUNT(*) FROM fundamental_snapshots").fetchone()[0]
        valuation_rows = conn.execute("SELECT COUNT(*) FROM valuation_snapshots").fetchone()[0]
        event_rows = conn.execute("SELECT COUNT(*) FROM company_events").fetchone()[0]
        sentiment_rows = conn.execute("SELECT COUNT(*) FROM market_sentiment_snapshots").fetchone()[0]
        industry_rows = conn.execute("SELECT COUNT(*) FROM industry_membership").fetchone()[0]
        industry_history_rows = conn.execute("SELECT COUNT(*) FROM industry_membership_history").fetchone()[0]
        macro_rows = conn.execute("SELECT COUNT(*) FROM macro_indicator_snapshots").fetchone()[0]
        last_optimizer = conn.execute(
            """
            SELECT run_time, top_n, rebalance_days, lookback_years, objective_score
            FROM optimizer_runs
            WHERE is_best = 1
            ORDER BY optimizer_run_id DESC
            LIMIT 1
            """
        ).fetchone()

    return {
        "db_path": str(DB_PATH),
        "price_rows": int(price_rows),
        "tracked_symbols": int(symbol_rows),
        "catalog_rows": int(catalog_rows),
        "recommendation_rows": int(reco_rows),
        "optimizer_rows": int(optimizer_rows),
        "automation_rows": int(automation_rows),
        "fundamental_rows": int(fundamental_rows),
        "valuation_rows": int(valuation_rows),
        "event_rows": int(event_rows),
        "sentiment_rows": int(sentiment_rows),
        "industry_rows": int(industry_rows),
        "industry_history_rows": int(industry_history_rows),
        "macro_rows": int(macro_rows),
        "last_best": {
            "run_time": last_optimizer[0],
            "top_n": last_optimizer[1],
            "rebalance_days": last_optimizer[2],
            "lookback_years": last_optimizer[3],
            "objective_score": last_optimizer[4],
        } if last_optimizer else None,
    }


def log_automation_run(
    task_name: str,
    status: str,
    pool_size: int | None = None,
    catalog_size: int | None = None,
    degraded: bool = False,
    summary: str | None = None,
    error_message: str | None = None,
    meta: dict | None = None,
) -> int:
    init_db()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO automation_runs (
                task_name, status, pool_size, catalog_size, degraded, summary, error_message, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_name,
                status,
                pool_size,
                catalog_size,
                int(bool(degraded)),
                summary,
                error_message,
                json.dumps(meta or {}, ensure_ascii=False),
            ),
        )
        return int(cursor.lastrowid)


def get_recent_automation_runs(limit: int = 20) -> pd.DataFrame:
    init_db()
    with _connect() as conn:
        df = pd.read_sql_query(
            """
            SELECT automation_run_id, run_time, task_name, status, pool_size, catalog_size,
                   degraded, summary, error_message
            FROM automation_runs
            ORDER BY automation_run_id DESC
            LIMIT ?
            """,
            conn,
            params=(limit,),
        )
    return df


def get_recent_best_optimizer_runs(strategy_name: str = "alpha_ensemble", limit: int = 8) -> list[dict]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT run_time, top_n, rebalance_days, lookback_years, objective_score, meta_json
            FROM optimizer_runs
            WHERE strategy_name = ? AND is_best = 1
            ORDER BY optimizer_run_id DESC
            LIMIT ?
            """,
            (strategy_name, limit),
        ).fetchall()

    results = []
    for row in rows:
        meta = {}
        if row[5]:
            try:
                meta = json.loads(row[5])
            except json.JSONDecodeError:
                meta = {}
        strategy_config = meta.get("strategy_config", {})
        results.append(
            {
                "run_time": row[0],
                "top_n": row[1],
                "rebalance_days": row[2],
                "lookback_years": row[3],
                "objective_score": row[4],
                **strategy_config,
            }
        )
    return results


def get_latest_strategy_recommendations(strategy_name: str | None = None) -> dict | None:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT snapshot_date
            FROM strategy_recommendations
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        snapshot_date = row[0]
        table = pd.read_sql_query(
            """
            SELECT snapshot_date, symbol, name, score, action, reason, weight, close_price, extra_json
            FROM strategy_recommendations
            WHERE snapshot_date = ?
            ORDER BY score DESC, symbol ASC
            """,
            conn,
            params=(snapshot_date,),
        )

    if table.empty:
        return None

    extra_series = table["extra_json"].apply(
        lambda value: json.loads(value) if isinstance(value, str) and value.strip() else {}
    )
    extra_df = pd.DataFrame(extra_series.tolist()) if not extra_series.empty else pd.DataFrame()
    table = pd.concat([table.drop(columns=["extra_json", "snapshot_date"]), extra_df], axis=1)
    table["symbol"] = table["symbol"].astype(str).str.zfill(6)
    table["display_name"] = table.apply(
        lambda row: f"{row['symbol']} {row['name']}".strip() if pd.notna(row.get("name")) else row["symbol"],
        axis=1,
    )
    return {
        "as_of_date": pd.Timestamp(snapshot_date),
        "table": table.reset_index(drop=True),
    }


def get_latest_backtest_run(strategy_name: str | None = None) -> dict | None:
    init_db()
    query = """
        SELECT run_time, strategy_name, top_n, rebalance_days, lookback_years,
               latest_value, total_return, max_drawdown, positive_period_ratio,
               rebalance_count, symbols_count, meta_json
        FROM backtest_runs
    """
    params: tuple = ()
    if strategy_name:
        query += " WHERE strategy_name = ?"
        params = (strategy_name,)
    query += " ORDER BY run_id DESC LIMIT 1"

    with _connect() as conn:
        row = conn.execute(query, params).fetchone()

    if not row:
        return None

    meta = {}
    if row[11]:
        try:
            meta = json.loads(row[11])
        except json.JSONDecodeError:
            meta = {}

    return {
        "run_time": pd.Timestamp(row[0]),
        "strategy_name": row[1],
        "top_n": int(row[2]),
        "rebalance_days": int(row[3]),
        "lookback_years": int(row[4]),
        "metrics": {
            "latest_value": float(row[5]) if row[5] is not None else 1.0,
            "total_return": float(row[6]) if row[6] is not None else 0.0,
            "max_drawdown": float(row[7]) if row[7] is not None else 0.0,
            "positive_period_ratio": float(row[8]) if row[8] is not None else 0.0,
            "rebalance_count": int(row[9]) if row[9] is not None else 0,
            "start_date": pd.NaT,
            "end_date": pd.NaT,
        },
        "symbols_count": int(row[10]) if row[10] is not None else 0,
        "meta": meta,
    }
