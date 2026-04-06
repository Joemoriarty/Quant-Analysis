import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from data.akshare_loader import DataFetchError, get_stock_catalog
from db.market_db import (
    get_best_strategy_config,
    get_db_status,
    get_recent_automation_runs,
    get_setting,
    init_db,
    save_backtest_run,
    save_recommendations,
    set_setting,
)
from portfolio.automation_workflows import (
    get_automation_pool_size,
    run_daily_update,
    run_industry_membership_refresh,
    run_weekly_optimization,
    set_automation_pool_size,
)
from portfolio.portfolio_backtester import BacktestError, backtest_portfolio_realistic
from portfolio.paper_trading import (
    build_trade_detail_review,
    build_trade_ledger,
    build_live_position_review,
    build_trade_review,
    compute_trade_performance,
    get_paper_trading_capital,
    is_paper_trading_enabled,
    load_mark_log,
    load_trade_log,
    refresh_live_position_marks,
    refresh_trade_mark_prices,
    set_paper_trading_capital,
    set_paper_trading_enabled,
    upsert_daily_trade,
)
from portfolio.candidate_screener import recommend_growth_candidates, screen_accumulation_candidates
from portfolio.comparison_plugins import list_comparison_types
from portfolio.scoring_config import DEFAULT_SCORING_CONFIG, normalize_scoring_config, scoring_config_to_json
from portfolio.single_stock_analysis import (
    analyze_single_stock,
    resolve_stock_query,
)
from portfolio.strategy_optimizer import run_strategy_parameter_optimization, sync_market_data_to_db
from portfolio.watchlist import (
    add_watchlist_stock,
    analyze_watchlist,
    build_watchlist_execution_list,
    build_watchlist_rebalance_plan,
    load_watchlist,
    remove_watchlist_stocks,
    update_watchlist_positions,
)
from strategies.unified_selection import run_unified_selection


st.set_page_config(page_title="统一评分选股系统", layout="wide")
st.title("统一评分选股系统（A股）")
st.caption("默认加载近期强势股票池，并优先使用本地缓存；在线历史行情会优先走东方财富，失败后再尝试腾讯证券。")


@st.cache_data(show_spinner=False)
def load_catalog(limit: int):
    return get_stock_catalog(limit=limit)


@st.cache_data(show_spinner=False)
def load_accumulation_scan(scan_limit: int, top_k: int, config_json: str):
    return screen_accumulation_candidates(
        scan_limit=scan_limit,
        top_k=top_k,
        config=normalize_scoring_config(json.loads(config_json)),
    )


@st.cache_data(show_spinner=False)
def load_growth_candidates(scan_limit: int, top_k: int, target_return: float, config_json: str):
    return recommend_growth_candidates(
        scan_limit=scan_limit,
        top_k=top_k,
        target_return=target_return,
        config=normalize_scoring_config(json.loads(config_json)),
    )


@st.cache_data(show_spinner=False)
def load_db_status():
    init_db()
    return get_db_status()


def render_data_error(message: str) -> None:
    st.error(message)
    st.info("如果这是云端部署，请确认启动命令是 `streamlit run web/app.py --server.port $PORT --server.address 0.0.0.0`。如果是本地运行，可使用 `python run.py`。")


def render_metrics(metrics: dict) -> None:
    cols = st.columns(4)
    cols[0].metric("最新净值", f"{metrics['latest_value']:.3f}")
    cols[1].metric("累计收益", f"{metrics['total_return']:.2%}")
    cols[2].metric("最大回撤", f"{metrics['max_drawdown']:.2%}")
    cols[3].metric("调仓次数", str(metrics["rebalance_count"]))
    st.caption(
        f"回测区间：{metrics['start_date'].date()} 到 {metrics['end_date'].date()}，"
        f"正收益调仓占比：{metrics['positive_period_ratio']:.2%}"
    )


def render_usage_guide() -> None:
    with st.expander("网页端使用说明", expanded=True):
        st.write("推荐按下面顺序使用，第一次上手会更顺。")
        guide_df = pd.DataFrame(
            [
                {"步骤": "1. 加载股票池", "说明": "先点“加载股票池”，系统会加载当前强势活跃股票，作为回测和推荐候选池。"},
                {"步骤": "2. 运行策略", "说明": "设置持仓数、调仓周期、回测年数后点“运行策略”，查看当前建议和历史回测。"},
                {"步骤": "3. 单只股票分析", "说明": "输入代码或名称后分析单只股票，查看趋势评分、推荐理由、K线和分批卖出计划。"},
                {"步骤": "4. 候选筛选", "说明": "设置扫描范围后筛选更大范围的吸筹候选股票，这个结果会和单股分析同时保留。"},
                {"步骤": "5. 策略进化", "说明": "在“策略进化与数据库”里同步数据、执行优化、查看自动任务健康和当前最优参数。"},
            ]
        )
        st.dataframe(guide_df, use_container_width=True)
        st.caption("单只股票分析和候选筛选现在是两个独立面板，互不覆盖，可以同时查看。")


def render_reason_guide() -> None:
    with st.expander("这些推荐数值是什么意思"):
        guide_df = pd.DataFrame(
            [
                {"指标": "20日动量", "含义": "最近20个交易日的涨跌幅", "怎么看": "越大说明中期趋势越强。"},
                {"指标": "5日反转", "含义": "最近5个交易日跌得越多，这个值越大", "怎么看": "值越大代表短线回撤更明显，模型把它当作潜在反弹信号。"},
                {"指标": "量比", "含义": "最新成交量 / 过去10日平均成交量", "怎么看": "大于1表示近期成交放大。"},
                {"指标": "RSI", "含义": "相对强弱指标，常用14日周期", "怎么看": "70以上偏热，30以下偏冷，配合趋势看更有效。"},
                {"指标": "MACD", "含义": "趋势动能指标，观察 DIF、DEA 和红绿柱", "怎么看": "DIF上穿DEA偏强，红柱放大说明动能增强。"},
                {"指标": "支撑位 / 压力位", "含义": "近20日低点和高点形成的价格区", "怎么看": "接近支撑位看承接，接近压力位看是否放量突破或受阻。"},
                {"指标": "综合得分", "含义": "三个子策略标准化后按动态权重加总", "怎么看": "只适合在当前股票池内横向比较。"},
                {"指标": "操作建议", "含义": "按当前候选排名和综合得分做的分层标签", "怎么看": "重点关注代表本轮信号最强；次重点适合继续跟踪；仅观察表示先看不急着动。"},
            ]
        )
        st.dataframe(guide_df, use_container_width=True)


def _format_pick_table(table: pd.DataFrame) -> pd.DataFrame:
    display_df = table.copy()
    rename_map = {
        "display_name": "股票",
        "score": "综合得分",
        "reason": "推荐理由",
        "action": "操作建议",
        "momentum_raw": "20日动量",
        "reversal_raw": "5日反转",
        "volume_raw": "量比",
        "close_price": "最新收盘价",
    }
    visible_cols = [
        col
        for col in [
            "display_name",
            "score",
            "action",
            "momentum_raw",
            "reversal_raw",
            "volume_raw",
            "close_price",
            "reason",
        ]
        if col in display_df.columns
    ]
    display_df = display_df[visible_cols].rename(columns=rename_map)
    if "20日动量" in display_df.columns:
        display_df["20日动量"] = display_df["20日动量"].map(lambda x: f"{x:.2%}" if pd.notna(x) else "")
    if "5日反转" in display_df.columns:
        display_df["5日反转"] = display_df["5日反转"].map(lambda x: f"{x:.2%}" if pd.notna(x) else "")
    if "量比" in display_df.columns:
        display_df["量比"] = display_df["量比"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
    if "最新收盘价" in display_df.columns:
        display_df["最新收盘价"] = display_df["最新收盘价"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
    return display_df


def _safe_float(value, default=0.0):
    return default if pd.isna(value) else float(value)


def _current_pick_decision(row: pd.Series) -> dict:
    score = _safe_float(row.get("score"))
    price = _safe_float(row.get("close_price"))
    momentum = _safe_float(row.get("momentum_raw"))
    reversal = _safe_float(row.get("reversal_raw"))
    volume_ratio = _safe_float(row.get("volume_raw"), 1.0)
    action = row.get("action", "仅观察")

    if action == "重点关注" and score > 1:
        decision = "建议买入"
        position = "建议仓位 8%-12%"
    elif action in {"重点关注", "次重点"} and score > 0:
        decision = "建议小仓试错"
        position = "建议仓位 3%-8%"
    else:
        decision = "暂不追买"
        position = "先观察，不急着开仓"

    buy_low = price * 0.985 if price > 0 else None
    buy_high = price * 1.01 if price > 0 else None
    stop_loss = price * 0.95 if price > 0 else None
    take_profit = price * 1.12 if price > 0 else None

    reasons = []
    if momentum > 0:
        reasons.append(f"20日动量 {momentum:.2%}，趋势偏强")
    if reversal < 0:
        reasons.append(f"近5日回撤 {abs(reversal):.2%}，更适合等企稳而不是追高")
    if volume_ratio > 1:
        reasons.append(f"量比 {volume_ratio:.2f}，近期交易活跃")
    if not reasons:
        reasons.append(row.get("reason", "综合信号暂时不够集中"))

    invalidation = "跌破计划止损位且量能没有同步改善，就说明这次买入逻辑失效。"
    risk_level = "中高" if reversal < -0.08 or volume_ratio > 2.5 else "中等"
    fit = "更适合愿意分批试错的人" if decision != "暂不追买" else "更适合继续观察，不适合新手追涨"

    return {
        "结论": decision,
        "建议仓位": position,
        "买入区间": f"{buy_low:.2f} - {buy_high:.2f}" if buy_low and buy_high else "-",
        "止损位": f"{stop_loss:.2f}" if stop_loss else "-",
        "止盈位": f"{take_profit:.2f}" if take_profit else "-",
        "触发原因": "；".join(reasons[:3]),
        "失效条件": invalidation,
        "风险等级": risk_level,
        "适合人群": fit,
    }


def _single_stock_decision_card(analysis: dict) -> dict:
    metrics = analysis["metrics"]
    close = _safe_float(metrics["close"])
    support = _safe_float(metrics["support"])
    resistance = _safe_float(metrics["resistance"])
    trend_score = int(analysis["trend_score"])
    recommendation = analysis["recommendation"]

    if recommendation == "推荐关注" and trend_score >= 75:
        decision = "建议买入"
        position = "首仓 10%，确认走强后再加到 20%"
    elif recommendation == "推荐关注":
        decision = "建议分批买入"
        position = "首仓 5%-10%"
    elif recommendation == "中性观察":
        decision = "建议继续观察"
        position = "先不开新仓或只做 3%-5% 试仓"
    else:
        decision = "暂不买入"
        position = "不建议开仓"

    buy_low = max(support * 1.01, close * 0.985) if close > 0 else None
    buy_high = min(close * 1.01, resistance * 0.97) if close > 0 and resistance > 0 else None
    if buy_high is not None and buy_low is not None and buy_high < buy_low:
        buy_low, buy_high = close * 0.99, close * 1.01

    if trend_score >= 75:
        fit = "适合稳健型与趋势型投资者"
    elif trend_score >= 55:
        fit = "适合能接受波动、愿意分批操作的人"
    else:
        fit = "不太适合刚入市的人现在直接买"

    return {
        "结论": decision,
        "建议仓位": position,
        "买入区间": f"{buy_low:.2f} - {buy_high:.2f}" if buy_low and buy_high else "-",
        "止损位": f"{support * 0.99:.2f}" if support > 0 else "-",
        "第一止盈位": f"{resistance * 0.98:.2f}" if resistance > 0 else "-",
        "第二止盈位": f"{resistance * 1.05:.2f}" if resistance > 0 else "-",
        "触发原因": "；".join(analysis["reasons"][:3]) or "当前推荐理由不够集中",
        "失效条件": "跌破支撑位并且 MACD 继续走弱，说明本次趋势判断失效。",
        "风险等级": "中高" if metrics["rsi14"] > 75 or trend_score < 55 else "中等",
        "适合人群": fit,
    }


def _render_decision_card(card: dict, title: str) -> None:
    st.write(title)
    card_df = pd.DataFrame([card]).T.reset_index()
    card_df.columns = ["项目", "内容"]
    st.dataframe(card_df, use_container_width=True, hide_index=True)


def _render_beginner_summary(card: dict) -> None:
    st.info(
        f"结论：{card['结论']}。{card['建议仓位']}。买入区间参考 {card['买入区间']}，"
        f"跌破 {card['止损位']} 附近要认错，接近 {card.get('止盈位', card.get('第一止盈位', '-'))} 可先考虑止盈。"
    )


def _decision_filter_label(card: dict) -> str:
    decision = card.get("结论", "")
    if "买入" in decision or "试错" in decision:
        return "只看可买"
    if "持有" in decision:
        return "只看继续持有"
    if "卖" in decision or "减仓" in decision:
        return "只看该卖"
    return "全部"


def _render_visual_decision_card(name: str, card: dict, rank: int | None = None) -> None:
    decision = card.get("结论", "")
    if "买入" in decision or "试错" in decision:
        banner = "#e8f7ee"
        border = "#2e8b57"
    elif "持有" in decision:
        banner = "#eef6ff"
        border = "#1f77b4"
    else:
        banner = "#fff4e8"
        border = "#d97706"

    rank_text = f"<div style='font-size:12px;color:#666;'>排名 {rank}</div>" if rank is not None else ""
    html = f"""
    <div style="border:1px solid {border}; border-radius:14px; padding:16px; background:{banner}; margin:8px 0;">
      {rank_text}
      <div style="font-size:20px; font-weight:700; margin-bottom:8px;">{name}</div>
      <div style="font-size:18px; font-weight:700; color:{border}; margin-bottom:10px;">{card['结论']}</div>
      <div style="margin-bottom:6px;"><strong>建议仓位：</strong>{card['建议仓位']}</div>
      <div style="margin-bottom:6px;"><strong>买入区间：</strong>{card['买入区间']}</div>
      <div style="margin-bottom:6px;"><strong>止损位：</strong>{card['止损位']}</div>
      <div style="margin-bottom:6px;"><strong>止盈位：</strong>{card.get('止盈位', card.get('第一止盈位', '-'))}</div>
      <div style="margin-bottom:6px;"><strong>风险等级：</strong>{card['风险等级']}</div>
      <div style="margin-bottom:6px;"><strong>触发原因：</strong>{card['触发原因']}</div>
      <div style="margin-bottom:6px;"><strong>失效条件：</strong>{card['失效条件']}</div>
      <div><strong>适合人群：</strong>{card['适合人群']}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render_current_pick(result: dict) -> None:
    current_pick = result["current_pick"]
    table = current_pick["table"]
    beginner_mode = st.toggle("新手模式", value=True, key="current_pick_beginner_mode")
    decision_filter = st.selectbox(
        "交易卡筛选",
        ["全部", "只看可买", "只看继续持有", "只看该卖"],
        index=0,
        key="current_pick_decision_filter",
    )

    st.subheader("当前建议关注股票")
    st.caption(
        f"信号日期：{current_pick['as_of_date'].date()}。这份名单是按最新可用行情在当前股票池内重新计算得到的。"
    )

    if table.empty:
        st.info("当前没有生成可用的候选股票。")
    else:
        display_df = _format_pick_table(table)
        display_df.index = display_df.index + 1
        display_df.index.name = "排名"
        st.dataframe(display_df, use_container_width=True)

        st.write("推荐股票交易决策卡")
        shown = 0
        for idx, (_, row) in enumerate(table.head(8).iterrows(), start=1):
            card = _current_pick_decision(row)
            filter_label = _decision_filter_label(card)
            if decision_filter != "全部" and filter_label != decision_filter:
                continue
            shown += 1
            with st.expander(f"{idx}. {row.get('display_name', row.get('symbol', '股票'))}", expanded=shown == 1):
                if beginner_mode:
                    _render_beginner_summary(card)
                    _render_visual_decision_card(
                        row.get("display_name", row.get("symbol", "股票")),
                        card,
                        rank=idx,
                    )
                else:
                    _render_decision_card(card, "交易决策卡")
        if shown == 0:
            st.info("当前筛选条件下没有匹配的交易卡。")

    if current_pick["weights"]:
        weights_df = pd.DataFrame(
            [{"策略": name, "权重": value} for name, value in current_pick["weights"].items()]
        ).sort_values("权重", ascending=False)
        st.write("当前信号使用的策略权重")
        st.dataframe(weights_df, use_container_width=True)

    render_reason_guide()


def summarize_error(message: str) -> str:
    if "网络/DNS 解析失败" in message:
        return "网络或 DNS 不通"
    if "网络连接失败" in message:
        return "网络连接失败"
    if "请求超时" in message:
        return "请求超时"
    if "东方财富" in message and "腾讯证券" in message:
        return "主备历史行情接口都失败了"
    if "本地缓存也不可用" in message:
        return "线上请求失败，同时本地缓存缺失"
    if "接口字段异常" in message:
        return "数据源返回格式变了"
    if "接口返回空数据" in message:
        return "该股票接口返回空数据"
    return "需要查看原始错误信息"


def render_debug_panel(result: dict, symbol_names: dict[str, str]) -> None:
    with st.expander("数据源与调试信息"):
        cache_stats = result.get("cache_stats", {})
        api_stats = result.get("api_stats", {})
        st.write("这里的缓存不是浏览器或网络代理缓存，而是项目本地磁盘里的行情 CSV 缓存。")
        st.write(
            f"缓存命中：新鲜缓存 {cache_stats.get('cache_fresh', 0)} 只，"
            f"旧缓存回退 {cache_stats.get('cache_stale', 0)} 只，"
            f"直接网络加载 {cache_stats.get('network', 0)} 只，"
            f"缓存基础上刷新成功 {cache_stats.get('network_refresh', 0)} 只。"
        )
        st.write(
            f"行情源使用：本地缓存 {api_stats.get('local_cache', 0)} 只，"
            f"东方财富 {api_stats.get('eastmoney', 0)} 只，"
            f"腾讯证券 {api_stats.get('tencent', 0)} 只。"
        )
        st.caption("如果本地缓存少，系统会先试东方财富历史接口，失败后再试腾讯证券，再不行才会记为失败。")

        errors = result["errors"]
        if errors:
            st.warning(f"有 {len(errors)} 只股票加载失败，回测已自动跳过。")
            error_df = pd.DataFrame(
                [
                    {
                        "股票": f"{symbol} {symbol_names.get(symbol, '')}".strip(),
                        "失败概述": summarize_error(message),
                        "原始原因": message,
                    }
                    for symbol, message in errors.items()
                ]
            )
            st.dataframe(error_df, use_container_width=True)
        else:
            st.success("股票行情加载成功，没有发现数据源错误。")

        diagnostics = result["diagnostics"]
        if not diagnostics.empty:
            st.dataframe(diagnostics, use_container_width=True)


def render_weights(weights_df: pd.DataFrame) -> None:
    if weights_df.empty:
        st.info("暂无策略权重数据。")
        return
    numeric_cols = [col for col in weights_df.columns if col != "调仓日"]
    st.area_chart(weights_df.set_index("调仓日")[numeric_cols])
    st.dataframe(weights_df, use_container_width=True)


def render_holdings(result: dict) -> None:
    holdings = result["holdings"]
    if holdings.empty:
        st.info("暂无调仓明细。")
        return

    st.dataframe(holdings, use_container_width=True)

    latest = result["rebalance_records"][-1]
    st.subheader("最近一次历史调仓详情")
    st.write(
        f"调仓日：`{latest['rebalance_date'].date()}`，"
        f"下次调仓日：`{latest['next_rebalance_date'].date()}`"
    )

    selected_detail = pd.DataFrame(latest["selected_detail"])
    if not selected_detail.empty:
        st.write("最近一次历史调仓的入选股票与推荐理由")
        st.dataframe(_format_pick_table(selected_detail), use_container_width=True)

    realized_positions = pd.DataFrame(latest["realized_positions"])
    if not realized_positions.empty:
        st.write("上一期持仓在本次调仓窗口内的收益贡献")
        st.dataframe(
            realized_positions[["display_name", "weight", "period_return", "weighted_return"]].rename(
                columns={
                    "display_name": "股票",
                    "weight": "权重",
                    "period_return": "区间收益",
                    "weighted_return": "加权贡献",
                }
            ),
            use_container_width=True,
        )


def render_pool_preview(catalog: pd.DataFrame) -> None:
    with st.expander("当前股票池预览"):
        preview_cols = ["code", "name", "sixty_day_return", "ytd_return", "turnover", "pool_reason"]
        available = [col for col in preview_cols if col in catalog.columns]
        preview = catalog[available].copy()
        preview.columns = ["代码", "名称", "60日涨幅", "年内涨幅", "换手率", "入池原因"][: len(available)]
        st.dataframe(preview.head(20), use_container_width=True)


def render_single_stock_panel(symbol_names: dict[str, str]) -> None:
    st.subheader("单只股票分析")
    beginner_mode = st.toggle("新手模式", value=True, key="single_stock_beginner_mode")
    with st.form("single_stock_analysis_form", clear_on_submit=False):
        query = st.text_input(
            "输入股票代码或名称",
            value=st.session_state.single_stock_query,
            placeholder="例如：600519 或 贵州茅台",
        )
        submitted = st.form_submit_button("分析这只股票")

    if submitted:
        st.session_state.single_stock_query = query
        try:
            resolved = resolve_stock_query(query, fallback_names=symbol_names)
            st.session_state.single_stock_analysis = analyze_single_stock(resolved["symbol"], resolved["name"])
            st.session_state.single_stock_error = None
        except (ValueError, DataFetchError) as error:
            st.session_state.single_stock_analysis = None
            st.session_state.single_stock_error = str(error)

    if st.session_state.single_stock_error:
        render_data_error(st.session_state.single_stock_error)
        return

    analysis = st.session_state.single_stock_analysis
    if not analysis:
        st.info("输入股票代码或名称后点击“分析这只股票”，结果会保留在页面上。")
        return

    metrics = analysis["metrics"]
    st.success(
        f"{analysis['symbol']} {analysis['name']} | 当前结论：{analysis['recommendation']} | "
        f"数据源：{analysis['data_source']}"
    )

    decision_card = _single_stock_decision_card(analysis)
    if beginner_mode:
        _render_beginner_summary(decision_card)
        _render_visual_decision_card(f"{analysis['symbol']} {analysis['name']}", decision_card)
        st.write("新手先看这几件事")
        quick_df = pd.DataFrame(
            [
                {"问题": "现在能不能买", "答案": decision_card["结论"]},
                {"问题": "买多少", "答案": decision_card["建议仓位"]},
                {"问题": "在哪里买", "答案": decision_card["买入区间"]},
                {"问题": "错了怎么办", "答案": f"跌破 {decision_card['止损位']} 附近就要控制风险"},
                {"问题": "赚了怎么办", "答案": f"先看 {decision_card['第一止盈位']}，再看 {decision_card['第二止盈位']}"},
            ]
        )
        st.dataframe(quick_df, use_container_width=True, hide_index=True)
    _render_decision_card(decision_card, "单股交易决策卡")

    cols = st.columns(5)
    cols[0].metric("最新收盘价", f"{metrics['close']:.2f}")
    cols[1].metric("20日动量", f"{metrics['return_20d']:.2%}")
    cols[2].metric("5日动量", f"{metrics['return_5d']:.2%}")
    cols[3].metric("量比", f"{metrics['volume_ratio_10d']:.2f}")
    cols[4].metric("趋势评分", f"{analysis['trend_score']}/100")

    extra_cols = st.columns(5)
    extra_cols[0].metric("RSI(14)", f"{metrics['rsi14']:.1f}")
    extra_cols[1].metric("MACD DIF", f"{metrics['dif']:.3f}")
    extra_cols[2].metric("MACD DEA", f"{metrics['dea']:.3f}")
    extra_cols[3].metric("MACD柱", f"{metrics['macd_hist']:.3f}")
    extra_cols[4].metric("量价吸筹评分", f"{analysis['accumulation_score']}/100")

    st.caption(
        f"分析日期：{metrics['analysis_date'].date()} | 20日均线 {metrics['ma20']:.2f} | "
        f"60日均线 {metrics['ma60']:.2f} | 近20日高点 {metrics['recent_high_20d']:.2f} | "
        f"近20日低点 {metrics['recent_low_20d']:.2f} | 支撑位 {metrics['support']:.2f} | "
        f"压力位 {metrics['resistance']:.2f}"
    )

    final_cols = st.columns(5)
    final_cols[0].metric("技术结论", str(analysis.get("technical_recommendation", analysis["recommendation"])))
    final_cols[1].metric("基本面评分", f"{analysis.get('fundamental_score', 50)}/100")
    final_cols[2].metric("市场情绪", str(analysis.get("market_sentiment_state", "未知")))
    final_cols[3].metric("事件驱动", str(analysis.get("event_state", "中性")))
    final_cols[4].metric("最终结论", str(analysis["recommendation"]))

    industry_membership = analysis.get("industry_membership") or {}
    industry_overview = analysis.get("comparison_overview") or []
    industry_score = analysis.get("industry_comparison_score")
    if industry_overview or industry_membership:
        st.write("行业横向评分摘要")
        summary_cols = st.columns(4)
        summary_cols[0].metric("行业横向总分", "-" if industry_score is None else f"{industry_score}/100")
        summary_cols[1].metric(
            "行业归属",
            str(industry_membership.get("industry_name") or analysis.get("valuation_snapshot", {}).get("industry") or "-"),
        )
        summary_cols[2].metric(
            "归属来源",
            str(industry_membership.get("source") or analysis.get("valuation_snapshot", {}).get("industry_source") or "-"),
        )
        summary_cols[3].metric("更新时间", str(industry_membership.get("updated_at") or "-"))

        overview_df = pd.DataFrame(industry_overview)
        if not overview_df.empty:
            plugin_name_map = {
                "industry_peers": "行业质量比较",
                "industry_valuation": "行业估值分位",
                "industry_growth": "行业增长性比较",
            }
            plugin_weight_map = {
                "industry_peers": "45%",
                "industry_valuation": "25%",
                "industry_growth": "30%",
            }
            overview_df["title"] = overview_df["name"].map(lambda x: plugin_name_map.get(x, x))
            overview_df["weight"] = overview_df["name"].map(lambda x: plugin_weight_map.get(x, "-"))
            overview_df = overview_df.rename(
                columns={
                    "title": "子模块",
                    "score": "子评分",
                    "peer_count": "样本数",
                    "available": "可用",
                    "weight": "汇总权重",
                }
            )
            st.dataframe(
                overview_df[["子模块", "子评分", "样本数", "汇总权重", "可用"]].astype(str),
                use_container_width=True,
                hide_index=True,
            )
            st.code(
                "industry_comparison_score = 行业质量比较*0.45 + 行业估值分位*0.25 + 行业增长性比较*0.30",
                language="text",
            )

    fundamental_summary = analysis.get("fundamental_summary", {})
    st.write("基本面摘要")
    if fundamental_summary.get("available"):
        st.caption(fundamental_summary.get("headline", ""))
        summary_df = pd.DataFrame(fundamental_summary.get("items", []))
        if not summary_df.empty:
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
        st.write(f"- {fundamental_summary.get('conclusion', '')}")
        if analysis.get("final_decision_basis"):
            st.write(f"- 当前结论依据：{analysis['final_decision_basis']}")
        positive_flags = fundamental_summary.get("positive_flags", [])
        if positive_flags:
            st.write("基本面亮点")
            for item in positive_flags:
                st.write(f"- {item}")
        risk_flags = fundamental_summary.get("risk_flags", [])
        if risk_flags:
            st.write("基本面风险")
            for item in risk_flags:
                st.write(f"- {item}")
    else:
        st.info(fundamental_summary.get("headline", "当前没有可用的基本面快照"))
        for item in fundamental_summary.get("risk_flags", []):
            st.write(f"- {item}")

    market_sentiment_summary = analysis.get("market_sentiment_summary", {})
    st.write("市场情绪摘要")
    if market_sentiment_summary.get("available"):
        st.caption(market_sentiment_summary.get("headline", ""))
        sentiment_df = pd.DataFrame(market_sentiment_summary.get("items", []))
        if not sentiment_df.empty:
            st.dataframe(sentiment_df, use_container_width=True, hide_index=True)
        st.write(f"- {market_sentiment_summary.get('conclusion', '')}")
    else:
        st.info(market_sentiment_summary.get("headline", "当前没有可用的市场情绪快照"))

    event_summary = analysis.get("event_summary", {})
    st.write("事件驱动摘要")
    if event_summary.get("available"):
        st.caption(event_summary.get("headline", ""))
        event_cols = st.columns(3)
        event_cols[0].metric("事件驱动分", f"{analysis.get('event_score', 50)}/100")
        event_cols[1].metric("事件状态", str(analysis.get("event_state", "中性")))
        event_cols[2].metric("事件数量", str(len(analysis.get("company_events", []))))
        event_df = pd.DataFrame(event_summary.get("items", []))
        if not event_df.empty:
            st.dataframe(event_df.astype(str), use_container_width=True, hide_index=True)
        st.write(f"- {event_summary.get('conclusion', '')}")
        for item in event_summary.get("positive_flags", []):
            st.write(f"- {item}")
        for item in event_summary.get("risk_flags", []):
            st.write(f"- {item}")
    else:
        st.info(event_summary.get("headline", "当前没有可用的事件驱动数据"))

    comparison_results = analysis.get("comparison_results", [])
    st.write("??????")
    if comparison_results:
        for result in comparison_results:
            if result.get("available"):
                with st.expander(str(result.get("title", "??????")), expanded=False):
                    st.caption(str(result.get("headline", "")))
                    if result.get("score") is not None:
                        st.write(f"- ????????{result['score']}/100")
                    items_df = pd.DataFrame(result.get("items", []))
                    if not items_df.empty:
                        st.dataframe(items_df.astype(str), use_container_width=True, hide_index=True)
                    if result.get("conclusion"):
                        st.write(f"- {result['conclusion']}")
                    if result.get("sample_warning"):
                        st.write(f"- {result['sample_warning']}")
                    for item in result.get("positive_flags", []):
                        st.write(f"- {item}")
                    for item in result.get("risk_flags", []):
                        st.write(f"- {item}")
            else:
                st.info(str(result.get("headline", "???????????????")))
    else:
        st.info("????????????????")

    left_col, right_col = st.columns(2)
    with left_col:
        st.write("趋势评分解读")
        st.write(f"- 当前趋势评分：`{analysis['trend_score']}/100`")
        for item in analysis["trend_explanations"]:
            st.write(f"- {item}")
        st.write("推荐理由")
        for item in analysis["reasons"]:
            st.write(f"- {item}")
        st.write("风险提示")
        for item in analysis["risks"]:
            st.write(f"- {item}")
        if analysis.get("fundamental_explanations"):
            st.write("基本面加分项")
            for item in analysis["fundamental_explanations"]:
                st.write(f"- {item}")
        if analysis.get("fundamental_risks"):
            st.write("基本面风险补充")
            for item in analysis["fundamental_risks"]:
                st.write(f"- {item}")
        if analysis.get("market_sentiment_explanations"):
            st.write("市场情绪加减分")
            for item in analysis["market_sentiment_explanations"]:
                st.write(f"- {item}")
        if analysis.get("market_sentiment_risks"):
            st.write("市场情绪风险")
            for item in analysis["market_sentiment_risks"]:
                st.write(f"- {item}")
    with right_col:
        st.write("持有还是分批卖出")
        st.write(f"- {analysis['hold_or_sell_view']}")
        st.write("高波动提醒")
        st.write(f"- {analysis['volatility_note']}")
        st.write("卖出参考")
        for item in analysis["sell_guidance"]:
            st.write(f"- {item}")

    st.write("量价吸筹迹象判断")
    st.write(f"- {analysis['accumulation_conclusion']}")
    for item in analysis["accumulation_signals"]:
        st.write(f"- {item}")

    st.write("分批卖出计划表")
    sell_plan_df = pd.DataFrame(analysis["sell_plan"])
    st.dataframe(sell_plan_df, use_container_width=True)

    st.write("加仓参考")
    add_position_df = pd.DataFrame(analysis["add_position_guidance"])
    st.dataframe(add_position_df, use_container_width=True)

    st.write("近期 K 线与均线")
    st.altair_chart(analysis["chart"], use_container_width=True)


def render_watchlist_panel(symbol_names: dict[str, str]) -> None:
    st.subheader("我的自选股")
    with st.form("watchlist_add_form", clear_on_submit=True):
        col1, col2, col3, col4, col5 = st.columns([2, 1.2, 1, 1, 1])
        with col1:
            watchlist_query = st.text_input("添加自选股", placeholder="输入股票代码或名称，例如：000001 或 平安银行")
        with col2:
            watchlist_note = st.text_input("备注", placeholder="例如：想观察回踩机会")
        with col3:
            watchlist_cost = st.number_input("成本价", min_value=0.0, value=0.0, step=0.01)
        with col4:
            watchlist_shares = st.number_input("持仓股数", min_value=0, value=0, step=100)
        with col5:
            watchlist_target_weight = st.number_input("目标仓位%", min_value=0.0, max_value=100.0, value=0.0, step=1.0)
        add_submitted = st.form_submit_button("加入自选股")

    if add_submitted:
        try:
            added = add_watchlist_stock(
                watchlist_query,
                fallback_names=symbol_names,
                note=watchlist_note,
                cost_price=watchlist_cost if watchlist_cost > 0 else None,
                shares=int(watchlist_shares) if watchlist_shares > 0 else None,
                target_weight=(watchlist_target_weight / 100) if watchlist_target_weight > 0 else None,
            )
            if added["duplicate"]:
                st.info(f"{added['display_name']} 已经在自选股里了。")
            else:
                st.success(f"已加入自选股：{added['display_name']}")
            st.session_state.watchlist_error = None
        except (ValueError, DataFetchError) as error:
            st.session_state.watchlist_error = str(error)

    if st.session_state.watchlist_error:
        render_data_error(st.session_state.watchlist_error)

    watchlist_df = load_watchlist()
    if watchlist_df.empty:
        st.info("还没有自选股。你可以输入股票代码或名称，把想长期跟踪的股票放进来。")
        return

    edit_df = watchlist_df[["symbol", "display_name", "note", "cost_price", "shares", "target_weight", "added_at"]].copy()
    edit_df["target_weight"] = edit_df["target_weight"].map(lambda x: x * 100 if pd.notna(x) else 0.0)
    edited_df = st.data_editor(
        edit_df.rename(
            columns={
                "symbol": "代码",
                "display_name": "股票",
                "note": "备注",
                "cost_price": "成本价",
                "shares": "持仓股数",
                "target_weight": "目标仓位%",
                "added_at": "加入时间",
            }
        ),
        use_container_width=True,
        hide_index=True,
        disabled=["代码", "股票", "加入时间"],
        key="watchlist_editor",
    )
    if st.button("保存自选股持仓配置"):
        updates = edited_df.rename(
            columns={
                "代码": "symbol",
                "备注": "note",
                "成本价": "cost_price",
                "持仓股数": "shares",
                "目标仓位%": "target_weight",
            }
        )[["symbol", "note", "cost_price", "shares", "target_weight"]].copy()
        updates["target_weight"] = updates["target_weight"].fillna(0).astype(float) / 100
        updated_count = update_watchlist_positions(updates)
        st.success(f"已更新 {updated_count} 只自选股的持仓配置。")
        st.session_state.watchlist_analysis_result = None
        st.session_state.watchlist_analysis_errors = {}

    remove_symbols = st.multiselect(
        "删除自选股",
        options=watchlist_df["symbol"].tolist(),
        format_func=lambda symbol: watchlist_df.loc[watchlist_df["symbol"] == symbol, "display_name"].iloc[0],
        key="watchlist_remove_symbols",
    )
    if st.button("删除选中的自选股", disabled=not remove_symbols):
        removed_count = remove_watchlist_stocks(remove_symbols)
        st.success(f"已删除 {removed_count} 只自选股。")
        st.session_state.watchlist_analysis_result = None
        st.session_state.watchlist_analysis_errors = {}

    if st.button("批量分析自选股"):
        with st.spinner("正在分析自选股..."):
            result_df, errors = analyze_watchlist()
        st.session_state.watchlist_analysis_result = result_df
        st.session_state.watchlist_analysis_errors = errors

    result_df = st.session_state.watchlist_analysis_result
    errors = st.session_state.watchlist_analysis_errors
    if errors:
        error_df = pd.DataFrame(
            [{"股票代码": symbol, "原因": message} for symbol, message in errors.items()]
        )
        st.warning("部分自选股分析失败，已自动跳过。")
        st.dataframe(error_df, use_container_width=True, hide_index=True)

    if result_df is None or result_df.empty:
        st.caption("点击“批量分析自选股”后，这里会给出每只自选股的结论、交易决策卡和风险提示。")
        return

    summary_df = result_df[
        [
            "display_name",
            "recommendation",
            "position_action",
            "suggested_weight_range",
            "trend_score",
            "accumulation_score",
            "cost_price",
            "shares",
            "close",
            "current_value",
            "pnl_amount",
            "pnl_ratio",
            "return_20d",
            "volume_ratio_10d",
        ]
    ].copy()
    summary_df = summary_df.rename(
        columns={
            "display_name": "股票",
            "recommendation": "当前结论",
            "position_action": "调仓建议",
            "suggested_weight_range": "建议目标仓位",
            "trend_score": "趋势评分",
            "accumulation_score": "量价吸筹评分",
            "cost_price": "成本价",
            "shares": "持仓股数",
            "close": "最新价",
            "current_value": "当前市值",
            "pnl_amount": "浮盈亏金额",
            "pnl_ratio": "浮盈亏比例",
            "return_20d": "20日动量",
            "volume_ratio_10d": "量比",
        }
    )
    for col in ["成本价", "最新价"]:
        summary_df[col] = summary_df[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
    for col in ["当前市值", "浮盈亏金额"]:
        summary_df[col] = summary_df[col].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
    summary_df["浮盈亏比例"] = summary_df["浮盈亏比例"].map(lambda x: f"{x:.2%}" if pd.notna(x) else "")
    summary_df["20日动量"] = summary_df["20日动量"].map(lambda x: f"{x:.2%}")
    summary_df["量比"] = summary_df["量比"].map(lambda x: f"{x:.2f}")
    st.write("自选股分析总览")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    rebalance_df = build_watchlist_rebalance_plan(result_df)
    if not rebalance_df.empty:
        display_rebalance = rebalance_df.rename(
            columns={
                "display_name": "股票",
                "current_value": "当前市值",
                "current_weight": "当前仓位",
                "desired_weight": "目标仓位",
                "desired_value": "目标市值",
                "rebalance_delta_value": "调仓差额",
                "delta_shares": "建议股数变化",
                "rebalance_action": "建议动作",
                "close": "参考价",
            }
        ).copy()
        for col in ["当前市值", "目标市值", "调仓差额"]:
            display_rebalance[col] = display_rebalance[col].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
        for col in ["当前仓位", "目标仓位"]:
            display_rebalance[col] = display_rebalance[col].map(lambda x: f"{x:.2%}" if pd.notna(x) else "")
        display_rebalance["参考价"] = display_rebalance["参考价"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        st.write("自选股调仓差额表")
        st.dataframe(display_rebalance, use_container_width=True, hide_index=True)
        st.caption("说明：如果你没有手动填写目标仓位，系统会用“建议目标仓位”的中位值来估算目标市值，再对比你当前市值，给出大致该补多少或减多少。")

        execution_df = build_watchlist_execution_list(result_df)
        if not execution_df.empty:
            display_execution = execution_df.rename(
                columns={
                    "display_name": "股票",
                    "rebalance_action": "动作",
                    "delta_shares": "建议股数变化",
                    "rebalance_delta_value": "建议金额变化",
                    "close": "参考价",
                }
            ).copy()
            display_execution["建议金额变化"] = display_execution["建议金额变化"].map(
                lambda x: f"{x:,.2f}" if pd.notna(x) else ""
            )
            display_execution["参考价"] = display_execution["参考价"].map(
                lambda x: f"{x:.2f}" if pd.notna(x) else ""
            )
            st.write("调仓执行清单")
            st.dataframe(display_execution, use_container_width=True, hide_index=True)
            st.caption("执行顺序默认先减仓再补仓，避免在总资金有限时先买后卖。股数按100股一手粗略换算，适合做操作参考。")

    st.write("自选股交易决策卡")
    for idx, row in result_df.iterrows():
        analysis = row["analysis"]
        card = _single_stock_decision_card(analysis)
        with st.expander(row["display_name"], expanded=idx == 0):
            _render_visual_decision_card(row["display_name"], card)
            st.write("当前持仓调仓建议")
            advice_df = pd.DataFrame(
                [
                    {"项目": "当前持仓", "内容": f"{int(row.get('shares', 0) or 0)} 股"},
                    {"项目": "成本价", "内容": f"{float(row['cost_price']):.2f}" if pd.notna(row.get("cost_price")) else "-"},
                    {"项目": "当前市值", "内容": f"{float(row['current_value']):,.2f}" if pd.notna(row.get("current_value")) else "-"},
                    {"项目": "浮盈亏", "内容": f"{float(row['pnl_amount']):,.2f} ({float(row['pnl_ratio']):.2%})" if pd.notna(row.get("pnl_ratio")) else "-"},
                    {"项目": "调仓动作", "内容": row.get("position_action", "-")},
                    {"项目": "建议目标仓位", "内容": row.get("suggested_weight_range", "-")},
                    {"项目": "具体建议", "内容": row.get("position_advice", "-")},
                    {"项目": "加仓判断", "内容": row.get("add_action", "-")},
                ]
            )
            st.dataframe(advice_df, use_container_width=True, hide_index=True)
            if row.get("note"):
                st.caption(f"我的备注：{row['note']}")


def render_accumulation_screener() -> None:
    st.subheader("A股吸筹候选筛选")
    st.caption("当前已改为复用单股分析的最终判断链路，再保留量价吸筹作为候选发现条件。")

    with st.form("accumulation_screener_form", clear_on_submit=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            scan_limit = st.selectbox(
                "扫描范围",
                [200, 300, 500, 800],
                index=[200, 300, 500, 800].index(st.session_state.accumulation_scan_limit),
            )
        with col2:
            top_k = st.selectbox(
                "输出数量",
                [10, 20, 30],
                index=[10, 20, 30].index(st.session_state.accumulation_top_k),
            )
        with col3:
            st.caption("扫描范围越大越全面，但第一次运行会更慢；已有缓存时会明显更快。")
        submitted = st.form_submit_button("筛选吸筹候选股票")

    if submitted:
        st.session_state.accumulation_scan_limit = scan_limit
        st.session_state.accumulation_top_k = top_k
        with st.spinner("正在扫描更大范围的候选股票..."):
            try:
                st.session_state.accumulation_scan_result = load_accumulation_scan(
                    scan_limit,
                    top_k,
                    current_scoring_config_json,
                )
                st.session_state.accumulation_scan_error = None
            except DataFetchError as error:
                st.session_state.accumulation_scan_result = None
                st.session_state.accumulation_scan_error = str(error)

    if st.session_state.accumulation_scan_error:
        render_data_error(st.session_state.accumulation_scan_error)
        return

    result_df = st.session_state.accumulation_scan_result
    if result_df is None:
        st.info("设置扫描范围后点击“筛选吸筹候选股票”，筛选结果会保留在页面上。")
        return
    if result_df.empty:
        st.info("这次扫描没有找到量价吸筹信号足够强的股票，可以扩大扫描范围或稍后再试。")
        return

    display_df = result_df.copy()
    if "20日动量" in display_df.columns:
        display_df["20日动量"] = display_df["20日动量"].map(lambda x: f"{x:.2%}")
    if "量比" in display_df.columns:
        display_df["量比"] = display_df["量比"].map(lambda x: f"{x:.2f}")
    if "综合评分" in display_df.columns:
        display_df["综合评分"] = display_df["综合评分"].map(lambda x: f"{x:.2f}")
    st.dataframe(display_df, use_container_width=True)
    st.caption("这里的候选先满足单股最终结论不为“暂不推荐”，再要求量价吸筹评分达标，因此和单股页判断口径保持一致。")


def render_growth_candidate_panel() -> None:
    st.subheader("中线候选池")
    st.caption("当前已改为基于单股最终结论、基本面评分、市场情绪、事件驱动和量价吸筹评分的统一筛选。")

    with st.form("growth_candidate_form", clear_on_submit=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            scan_limit = st.selectbox("候选扫描范围", [100, 200, 300, 500], index=2)
        with col2:
            top_k = st.selectbox("输出候选数", [5, 10, 15], index=1)
        with col3:
            target_return = st.selectbox("目标收益", [0.20, 0.30, 0.50], index=1, format_func=lambda x: f"{x:.0%}")
        submitted = st.form_submit_button("生成潜力候选")

    if submitted:
        with st.spinner("正在筛选中线候选股票..."):
            try:
                st.session_state.growth_candidate_result = load_growth_candidates(
                    scan_limit,
                    top_k,
                    target_return,
                    current_scoring_config_json,
                )
                st.session_state.growth_candidate_error = None
            except DataFetchError as error:
                st.session_state.growth_candidate_result = None
                st.session_state.growth_candidate_error = str(error)

    if st.session_state.growth_candidate_error:
        render_data_error(st.session_state.growth_candidate_error)
        return

    result_df = st.session_state.growth_candidate_result
    if result_df is None:
        st.info("点击“生成潜力候选”后，这里会展示更适合做中线跟踪的股票名单。")
        return
    if result_df.empty:
        st.info("当前没有筛到足够强的候选，可以扩大扫描范围或降低目标收益。")
        return

    display_df = result_df.copy()
    if "潜力评分" in display_df.columns:
        display_df["潜力评分"] = display_df["潜力评分"].map(lambda x: f"{x:.2f}")
    if "20日动量" in display_df.columns:
        display_df["20日动量"] = display_df["20日动量"].map(lambda x: f"{x:.2%}")
    if "量比" in display_df.columns:
        display_df["量比"] = display_df["量比"].map(lambda x: f"{x:.2f}")
    st.dataframe(display_df, use_container_width=True)
    st.caption("入选逻辑现在和单股判断一致，额外用潜力评分做排序。30% 是一年目标收益假设，不代表承诺收益。")


def render_analysis_logic_panel() -> None:
    st.subheader("分析方案")
    st.caption("这里展示统一评分配置、横向比较插件，以及当前用于单股、中线候选、组合选股的同一套算法。")

    config = normalize_scoring_config(st.session_state.unified_scoring_config)
    weights = {**DEFAULT_SCORING_CONFIG["weights"], **(config.get("weights") or {})}
    thresholds = {**DEFAULT_SCORING_CONFIG["thresholds"], **(config.get("thresholds") or {})}
    bonus = {**DEFAULT_SCORING_CONFIG["recommendation_bonus"], **(config.get("recommendation_bonus") or {})}
    recommendation_keys = list(DEFAULT_SCORING_CONFIG["recommendation_bonus"].keys())

    with st.form("scoring_config_form", clear_on_submit=False):
        st.write("统一评分参数")
        weight_cols = st.columns(6)
        trend_weight = weight_cols[0].slider("趋势权重", 0.0, 1.0, float(weights["trend"]), 0.05)
        fundamental_weight = weight_cols[1].slider("基本面权重", 0.0, 1.0, float(weights["fundamental"]), 0.05)
        accumulation_weight = weight_cols[2].slider("吸筹权重", 0.0, 1.0, float(weights["accumulation"]), 0.05)
        sentiment_weight = weight_cols[3].slider("情绪权重", 0.0, 1.0, float(weights["sentiment"]), 0.05)
        industry_weight = weight_cols[4].slider("行业横向权重", 0.0, 1.0, float(weights["industry"]), 0.05)
        event_weight = weight_cols[5].slider("事件权重", 0.0, 1.0, float(weights["event"]), 0.05)

        threshold_cols = st.columns(7)
        current_min_recommendation = thresholds["min_recommendation"]
        if current_min_recommendation not in recommendation_keys:
            current_min_recommendation = recommendation_keys[1]
        min_recommendation = threshold_cols[0].selectbox(
            "最低推荐级别",
            recommendation_keys,
            index=recommendation_keys.index(current_min_recommendation),
        )
        min_trend_score = threshold_cols[1].slider("最低趋势分", 0, 100, int(thresholds["min_trend_score"]), 1)
        min_fundamental_score = threshold_cols[2].slider("最低基本面分", 0, 100, int(thresholds["min_fundamental_score"]), 1)
        min_accumulation_score = threshold_cols[3].slider("最低吸筹分", 0, 100, int(thresholds["min_accumulation_score"]), 1)
        min_growth_score = threshold_cols[4].slider("最低潜力分", 0, 120, int(thresholds["min_growth_score"]), 1)
        min_industry_score = threshold_cols[5].slider("最低行业横向分", 0, 100, int(thresholds["min_industry_score"]), 1)
        min_event_score = threshold_cols[6].slider("最低事件分", 0, 100, int(thresholds["min_event_score"]), 1)

        bonus_cols = st.columns(3)
        recommend_bonus = bonus_cols[0].slider(f"结论加减项: {recommendation_keys[0]}", -20, 20, int(bonus[recommendation_keys[0]]), 1)
        neutral_bonus = bonus_cols[1].slider(f"结论加减项: {recommendation_keys[1]}", -20, 20, int(bonus[recommendation_keys[1]]), 1)
        reject_bonus = bonus_cols[2].slider(f"结论加减项: {recommendation_keys[2]}", -20, 20, int(bonus[recommendation_keys[2]]), 1)

        submitted = st.form_submit_button("应用统一评分参数")

    if submitted:
        st.session_state.unified_scoring_config = normalize_scoring_config(
            {
                "weights": {
                    "trend": trend_weight,
                    "fundamental": fundamental_weight,
                    "accumulation": accumulation_weight,
                    "sentiment": sentiment_weight,
                    "industry": industry_weight,
                    "event": event_weight,
                },
                "thresholds": {
                    "min_recommendation": min_recommendation,
                    "min_trend_score": min_trend_score,
                    "min_fundamental_score": min_fundamental_score,
                    "min_accumulation_score": min_accumulation_score,
                    "min_growth_score": min_growth_score,
                    "min_industry_score": min_industry_score,
                    "min_event_score": min_event_score,
                },
                "recommendation_bonus": {
                    recommendation_keys[0]: recommend_bonus,
                    recommendation_keys[1]: neutral_bonus,
                    recommendation_keys[2]: reject_bonus,
                },
            }
        )
        set_setting("unified_scoring_config", st.session_state.unified_scoring_config)
        load_accumulation_scan.clear()
        load_growth_candidates.clear()
        st.session_state.accumulation_scan_result = None
        st.session_state.growth_candidate_result = None
        st.success("统一评分参数已更新。")
        config = normalize_scoring_config(st.session_state.unified_scoring_config)
        weights = {**DEFAULT_SCORING_CONFIG["weights"], **(config.get("weights") or {})}
        thresholds = {**DEFAULT_SCORING_CONFIG["thresholds"], **(config.get("thresholds") or {})}
        bonus = {**DEFAULT_SCORING_CONFIG["recommendation_bonus"], **(config.get("recommendation_bonus") or {})}

    overview_df = pd.DataFrame(
        [
            {"模块": "单股分析", "使用逻辑": "技术面 + 基本面 + 市场情绪 + 事件驱动 + 行业横向", "作用": "生成最终结论"},
            {"模块": "自选股分析", "使用逻辑": "复用单股分析", "作用": "给出仓位建议"},
            {"模块": "吸筹候选", "使用逻辑": "复用统一逻辑 + 吸筹阈值", "作用": "筛选量价吸筹候选"},
            {"模块": "中线候选", "使用逻辑": "复用统一逻辑 + 潜力评分", "作用": "筛选中线跟踪池"},
            {"模块": "组合选股", "使用逻辑": "复用统一逻辑 + 组合总分", "作用": "选股与回测"},
            {"模块": "策略进化", "使用逻辑": "围绕同一套权重与阈值做搜索", "作用": "优化统一参数"},
        ]
    )
    st.write("统一覆盖范围")
    st.dataframe(overview_df.astype(str), use_container_width=True, hide_index=True)

    comparison_type_df = pd.DataFrame(
        [
            {
                "对比类型": item["title"],
                "插件名": item["name"],
                "状态": "已启用",
                "是否进入最终评分": "是" if str(item["name"]).startswith("industry_") else "否",
            }
            for item in list_comparison_types()
        ]
    )
    st.write("当前对比插件")
    if not comparison_type_df.empty:
        st.dataframe(comparison_type_df.astype(str), use_container_width=True, hide_index=True)
        industry_plugin_df = pd.DataFrame(
            [
                {"行业子评分": "行业质量比较", "插件": "industry_peers", "汇总权重": "45%", "主要依据": "ROE、负债率、净利润、营收、现金流、体量"},
                {"行业子评分": "行业估值分位", "插件": "industry_valuation", "汇总权重": "25%", "主要依据": "PE、PB"},
                {"行业子评分": "行业增长性比较", "插件": "industry_growth", "汇总权重": "30%", "主要依据": "营收同比、净利润同比"},
            ]
        )
        st.write("行业横向总分拆解")
        st.dataframe(industry_plugin_df.astype(str), use_container_width=True, hide_index=True)
        st.code(
            "industry_comparison_score = 行业质量比较*0.45 + 行业估值分位*0.25 + 行业增长性比较*0.30",
            language="text",
        )
    else:
        st.info("当前还没有启用对比插件。")

    value_df = pd.DataFrame(
        [
            {"维度": "盈利能力", "依据": "ROE、净利润", "判断方式": "ROE 越高、利润为正，通常代表公司质量更稳"},
            {"维度": "财务结构", "依据": "资产负债率", "判断方式": "负债率越低，财务压力通常越小"},
            {"维度": "现金质量", "依据": "经营现金流", "判断方式": "经营现金流为正，说明主营业务在造血"},
            {"维度": "估值和体量", "依据": "总市值、PE、PB", "判断方式": "体量太小波动更大，PE/PB 过高要警惕预期透支"},
            {"维度": "事件驱动", "依据": "公告、业绩预告、财报预约披露", "判断方式": "利多事件加分，重大风险、减持、退市风险等事件减分"},
            {"维度": "行业位置", "依据": "行业质量、估值分位、增长性比较", "判断方式": "行业横向分会综合同行质量位置、估值分位和增长分位"},
            {"维度": "市场环境", "依据": "市场广度、情绪分", "判断方式": "只做顺风逆风校正，不替代个股质量判断"},
        ]
    )
    st.write("如何判断股票价值")
    st.dataframe(value_df.astype(str), use_container_width=True, hide_index=True)

    rule_df = pd.DataFrame(
        [
            {"步骤": "1", "规则": "技术层先给出时机判断", "输出": "technical_recommendation"},
            {"步骤": "2", "规则": "基本面分修正技术层结论", "输出": "fundamental_score"},
            {"步骤": "3", "规则": "市场情绪只做顺风逆风校正", "输出": "market_sentiment_state"},
            {"步骤": "4", "规则": "事件驱动对最终结论做催化/风险校正", "输出": "event_score / event_state"},
            {"步骤": "5", "规则": "行业横向比较对最终结论做谨慎升降档", "输出": "industry_comparison_score"},
            {"步骤": "6", "规则": "统一生成最终推荐与结论依据", "输出": "recommendation / final_decision_basis"},
        ]
    )
    st.write("最终判断路径")
    st.dataframe(rule_df.astype(str), use_container_width=True, hide_index=True)

    scoring_df = pd.DataFrame(
        [
            {"组件": "trend_score", "当前权重": f"{weights['trend']:.0%}", "说明": "均线、MACD、RSI、动量、量比"},
            {"组件": "fundamental_score", "当前权重": f"{weights['fundamental']:.0%}", "说明": "ROE、负债率、现金流、净利润"},
            {"组件": "accumulation_score", "当前权重": f"{weights['accumulation']:.0%}", "说明": "量比、均线结构、MACD、支撑位"},
            {"组件": "sentiment_score", "当前权重": f"{weights['sentiment']:.0%}", "说明": "市场广度与情绪快照"},
            {"组件": "event_score", "当前权重": f"{weights['event']:.0%}", "说明": "公告、业绩预告、财报预约等事件驱动"},
            {"组件": "industry_score", "当前权重": f"{weights['industry']:.0%}", "说明": "同行分位、行业横向综合分"},
            {"组件": recommendation_keys[0], "当前权重": str(bonus[recommendation_keys[0]]), "说明": "组合总分加分项"},
            {"组件": recommendation_keys[1], "当前权重": str(bonus[recommendation_keys[1]]), "说明": "组合总分中性项"},
            {"组件": recommendation_keys[2], "当前权重": str(bonus[recommendation_keys[2]]), "说明": "组合总分减分项"},
        ]
    )
    st.write("统一评分算法")
    st.dataframe(scoring_df.astype(str), use_container_width=True, hide_index=True)
    st.code(
        f"portfolio_score = trend_score*{weights['trend']:.2f} + fundamental_score*{weights['fundamental']:.2f} + accumulation_score*{weights['accumulation']:.2f} + sentiment_score*{weights['sentiment']:.2f} + event_score*{weights['event']:.2f} + industry_score*{weights['industry']:.2f} + recommendation_bonus",
        language="text",
    )
    st.code(
        f"growth_score = trend_score*{weights['trend']:.2f} + fundamental_score*{weights['fundamental']:.2f} + accumulation_score*{weights['accumulation']:.2f} + sentiment_score*{weights['sentiment']:.2f} + event_score*{weights['event']:.2f} + industry_score*{weights['industry']:.2f} + min(return_20d*100, 20) + recommendation_bonus",
        language="text",
    )

    threshold_df = pd.DataFrame(
        [
            {"条件": "min_recommendation", "当前值": thresholds["min_recommendation"], "用途": "控制能进入候选池的最低推荐级别"},
            {"条件": "min_trend_score", "当前值": thresholds["min_trend_score"], "用途": "过滤趋势偏弱个股"},
            {"条件": "min_fundamental_score", "当前值": thresholds["min_fundamental_score"], "用途": "过滤基本面保护不足个股"},
            {"条件": "min_accumulation_score", "当前值": thresholds["min_accumulation_score"], "用途": "吸筹候选的最低门槛"},
            {"条件": "min_growth_score", "当前值": thresholds["min_growth_score"], "用途": "中线候选的最低门槛"},
            {"条件": "min_industry_score", "当前值": thresholds["min_industry_score"], "用途": "过滤行业横向位置过弱个股"},
            {"条件": "min_event_score", "当前值": thresholds["min_event_score"], "用途": "过滤近期事件明显偏负面的个股"},
        ]
    )
    st.write("统一阈值")
    st.dataframe(threshold_df.astype(str), use_container_width=True, hide_index=True)

    logic_doc = Path(__file__).resolve().parent.parent / "docs" / "STOCK_ANALYSIS_LOGIC.md"
    if logic_doc.exists():
        with st.expander("打开股票分析逻辑文档"):
            st.markdown(logic_doc.read_text(encoding="utf-8"))

def render_live_paper_snapshot(auto_refresh: bool, interval_seconds: int) -> None:
    def _render_snapshot_content() -> None:
        live_position_df = build_live_position_review()
        if not live_position_df.empty:
            st.write("当前实时模拟持仓")
            live_display = live_position_df.rename(
                columns={
                    "display_name": "股票",
                    "close_price": "建仓价",
                    "entry_time": "建仓时间",
                    "entry_price_source": "建仓价来源",
                    "mark_time": "最新估值时间",
                    "mark_price": "最新估值价",
                    "mark_price_source": "估值来源",
                    "shares": "持仓股数",
                    "market_value": "最新市值",
                    "pnl_amount": "浮盈亏金额",
                    "pnl_ratio": "浮盈亏比例",
                }
            ).copy()
            for col in ["建仓价", "最新估值价"]:
                if col in live_display.columns:
                    live_display[col] = live_display[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
            for col in ["最新市值", "浮盈亏金额"]:
                if col in live_display.columns:
                    live_display[col] = live_display[col].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
            if "浮盈亏比例" in live_display.columns:
                live_display["浮盈亏比例"] = live_display["浮盈亏比例"].map(
                    lambda x: f"{x:.2%}" if pd.notna(x) else ""
                )
            st.dataframe(
                live_display[
                    [
                        "股票",
                        "建仓时间",
                        "建仓价来源",
                        "建仓价",
                        "持仓股数",
                        "最新估值时间",
                        "估值来源",
                        "最新估值价",
                        "最新市值",
                        "浮盈亏金额",
                        "浮盈亏比例",
                    ]
                ],
                use_container_width=True,
            )

        mark_log = load_mark_log()
        if not mark_log.empty:
            st.write("实时模拟估值流水")
            mark_display = mark_log.rename(
                columns={
                    "mark_time": "估值时间",
                    "trade_date": "建仓日期",
                    "display_name": "股票",
                    "mark_price": "估值价",
                    "mark_price_source": "估值来源",
                    "shares": "股数",
                    "market_value": "市值",
                    "pnl_amount": "盈亏金额",
                    "pnl_ratio": "盈亏比例",
                }
            ).copy()
            for col in ["估值价"]:
                mark_display[col] = mark_display[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
            for col in ["市值", "盈亏金额"]:
                mark_display[col] = mark_display[col].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
            mark_display["盈亏比例"] = mark_display["盈亏比例"].map(
                lambda x: f"{x:.2%}" if pd.notna(x) else ""
            )
            st.dataframe(mark_display, use_container_width=True)

    if auto_refresh and hasattr(st, "fragment"):
        @st.fragment(run_every=f"{interval_seconds}s")
        def _live_fragment():
            try:
                refresh_live_position_marks()
            except DataFetchError as error:
                st.caption(f"自动刷新失败，继续保留上一份快照：{error}")
            else:
                st.caption(f"自动刷新已开启：每 {interval_seconds} 秒更新一次实时快照。")
            _render_snapshot_content()

        _live_fragment()
    else:
        _render_snapshot_content()


def render_paper_trading_panel(result: dict) -> None:
    st.subheader("每日模拟交易")
    enabled = st.toggle("开启每日模拟交易", value=is_paper_trading_enabled())
    if enabled != is_paper_trading_enabled():
        set_paper_trading_enabled(enabled)

    if not enabled:
        st.info("开启后可把当前推荐记录为每日模拟持仓，并持续计算收益。")
        return

    current_capital = get_paper_trading_capital()
    capital = st.number_input(
        "每期模拟资金",
        min_value=10000.0,
        max_value=10000000.0,
        value=float(current_capital),
        step=10000.0,
        help="系统会按这笔资金、结合股票权重和 A 股一手 100 股规则，计算每只股票实际买入多少手。",
    )
    if float(capital) != float(current_capital):
        set_paper_trading_capital(capital)
    st.caption("模拟交易默认按 A 股一手 = 100 股计算，无法凑整的零股不会买入，会保留为现金。")

    current_pick = result["current_pick"]
    signal_date = pd.to_datetime(current_pick["as_of_date"]).normalize()
    recommendation_df = current_pick["table"]

    if st.button("记录今日模拟持仓"):
        upsert_daily_trade(signal_date, recommendation_df)
        st.success(f"已记录 {signal_date.date()} 的模拟持仓。")

    refresh_cols = st.columns([1, 3])
    with refresh_cols[0]:
        if st.button("刷新实时模拟估值"):
            try:
                _, live_stats = refresh_live_position_marks()
            except DataFetchError as error:
                render_data_error(str(error))
            else:
                if live_stats["updated_count"]:
                    st.success(f"已刷新 {live_stats['updated_count']} 条实时估值记录。")
                if live_stats["failed_count"]:
                    st.warning(f"仍有 {live_stats['failed_count']} 只股票实时估值未刷新成功。")
    with refresh_cols[1]:
        auto_refresh = st.toggle("自动刷新实时估值", value=False, key="paper_auto_refresh")
        interval_seconds = st.selectbox(
            "自动刷新间隔",
            [10, 20, 30, 60],
            index=2,
            disabled=not auto_refresh,
            key="paper_auto_refresh_interval",
        )
        st.caption("这里刷新的是盘中最新快照价；如果实时接口失败，会退回本地日线缓存。")

    trade_log = load_trade_log()
    if not trade_log.empty:
        st.write("模拟持仓记录")
        st.dataframe(
            trade_log.rename(
                columns={
                    "trade_date": "持仓日期",
                    "display_name": "股票",
                    "action": "操作建议",
                    "score": "综合得分",
                    "weight": "权重",
                    "close_price": "建仓价",
                    "entry_time": "建仓时间",
                    "entry_price_source": "建仓价来源",
                }
            ),
            use_container_width=True,
        )

        render_live_paper_snapshot(auto_refresh=auto_refresh, interval_seconds=interval_seconds)

        perf_df = compute_trade_performance(result["latest_prices"], signal_date)
        if not perf_df.empty:
            perf_df["组合收益"] = perf_df["组合收益"].map(lambda x: f"{x:.2%}")
            st.write("模拟收益历史")
            st.dataframe(perf_df, use_container_width=True)


def render_quant_review_panel(result: dict) -> None:
    st.subheader("量化交易记录与收益核验")
    refresh_col1, refresh_col2 = st.columns([1, 3])
    with refresh_col1:
        if st.button("刷新核验价格"):
            latest_prices, latest_date, diagnostics = refresh_trade_mark_prices(refresh_stale_cache=True)
            if latest_prices and latest_date is not None:
                merged_prices = {**result.get("latest_prices", {}), **latest_prices}
                result["latest_prices"] = merged_prices
                current_as_of = pd.to_datetime(result["current_pick"].get("as_of_date")).normalize()
                result["current_pick"]["as_of_date"] = max(current_as_of, latest_date)
                st.success(
                    f"已刷新 {diagnostics['updated_count']} 只持仓股票的最新日线收盘价，最新估值日期为 {latest_date.date()}。"
                )
                if diagnostics["failed_count"]:
                    st.warning(f"仍有 {diagnostics['failed_count']} 只股票刷新失败，将继续使用旧估值。")
            else:
                st.warning("这次没有刷新到新的估值价格，可能是网络不可用或本地没有对应缓存。")
    with refresh_col2:
        st.caption("这里刷新的是最新日线收盘价估值，不是盘中实时成交价。若当天还没有新的日线数据，买入价和当前估值价看起来可能相同。")

    review_df, summary = build_trade_review(result["latest_prices"], result["current_pick"]["as_of_date"])
    if review_df.empty:
        st.info("当前还没有模拟量化记录。先在“每日模拟交易”里记录每日持仓后，这里会展示专供核验的面板。")
        return

    cols = st.columns(6)
    cols[0].metric("记录期数", str(summary["record_count"]))
    cols[1].metric("胜率", f"{summary['win_rate']:.2%}")
    cols[2].metric("平均单期收益", f"{summary['avg_return']:.2%}")
    cols[3].metric("累计收益", f"{summary['cumulative_return']:.2%}")
    cols[4].metric("最新净值", f"{summary['latest_nav']:.3f}")
    cols[5].metric("最大单期亏损", f"{summary['max_period_loss']:.2%}")
    st.caption(f"当前按每期模拟资金 {get_paper_trading_capital():,.0f} 元计算，A 股一手按 100 股取整。")

    nav_df = review_df[["持仓日期", "累计净值"]].copy().set_index("持仓日期")
    st.line_chart(nav_df)

    display_df = review_df.copy()
    for money_col in ["期初资金", "期末资金"]:
        if money_col in display_df.columns:
            display_df[money_col] = display_df[money_col].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
    display_df["组合收益"] = display_df["组合收益数值"].map(lambda x: f"{x:.2%}")
    display_df["累计净值"] = display_df["累计净值"].map(lambda x: f"{x:.3f}")
    display_df["是否盈利"] = display_df["是否盈利"].map(lambda x: "是" if x else "否")
    st.dataframe(
        display_df[["持仓日期", "评估日期", "期初资金", "期末资金", "组合收益", "累计净值", "持仓数量", "是否盈利", "说明"]],
        use_container_width=True,
    )
    st.caption("这块面板专门用于回看每期模拟持仓的收益、累计净值和胜率。现在表里会直接展示期初资金和期末资金，方便你手工复核。")

    detail_df = build_trade_detail_review(result["latest_prices"], result["current_pick"]["as_of_date"])
    if not detail_df.empty:
        st.write("每次买入了哪些股票、为什么买、后来赚了多少")
        detail_display = detail_df.copy()
        for price_col in ["建仓价", "评估价"]:
            if price_col in detail_display.columns:
                detail_display[price_col] = detail_display[price_col].map(
                    lambda x: f"{x:.2f}" if pd.notna(x) else ""
                )
        for money_col in ["期初资金", "目标分配金额", "建仓金额", "评估市值", "盈亏金额", "期末资金"]:
            if money_col in detail_display.columns:
                detail_display[money_col] = detail_display[money_col].map(
                    lambda x: f"{x:,.2f}" if pd.notna(x) else ""
                )
        for ratio_col in ["个股收益", "加权贡献", "权重", "组合收益率"]:
            if ratio_col in detail_display.columns:
                detail_display[ratio_col] = detail_display[ratio_col].map(
                    lambda x: f"{x:.2%}" if pd.notna(x) else "价格缺失"
                )
        st.dataframe(detail_display, use_container_width=True)
        st.caption("核验方法：建仓金额 = 建仓价 × 买入股数；评估市值 = 评估价 × 买入股数；盈亏金额 = 评估市值 - 建仓金额；组合收益率 = (期末资金 - 期初资金) / 期初资金。`评估类型` 为“当前估值”时，表示这还不是实际卖出，只是按最新日线价格做浮盈浮亏估算。")

    ledger_df = build_trade_ledger(result["latest_prices"], result["current_pick"]["as_of_date"])
    if not ledger_df.empty:
        st.write("逐笔交易流水")
        ledger_display = ledger_df.copy()
        for col in ["价格"]:
            ledger_display[col] = ledger_display[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        for col in ["金额", "收益金额"]:
            ledger_display[col] = ledger_display[col].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
        st.dataframe(ledger_display, use_container_width=True)


def render_evolution_panel(symbols: list[str], symbol_names: dict[str, str], catalog: pd.DataFrame) -> None:
    st.subheader("策略进化与自动任务")
    status = load_db_status()
    best_config = get_best_strategy_config(default={})
    last_daily_update = get_setting("last_daily_update", {})
    last_weekly_optimization = get_setting("last_weekly_optimization", {})
    last_industry_refresh = get_setting("last_industry_membership_refresh", {})
    auto_optimize_enabled = st.toggle(
        "启用每周自动优化并复用当前最佳参数",
        value=bool(get_setting("auto_optimize_enabled", False) and best_config),
        disabled=not bool(best_config),
    )
    set_setting("auto_optimize_enabled", auto_optimize_enabled)
    current_pool_size = get_automation_pool_size()

    cols = st.columns(5)
    cols[0].metric("行情记录", f"{status['price_rows']}")
    cols[1].metric("跟踪股票数", f"{status['tracked_symbols']}")
    cols[2].metric("股票池快照", f"{status['catalog_rows']}")
    cols[3].metric("推荐记录", f"{status['recommendation_rows']}")
    cols[4].metric("优化记录", f"{status['optimizer_rows']}")
    st.caption(f"当前数据库路径：{status['db_path']}")

    auto_runs = get_recent_automation_runs(limit=10)
    if not auto_runs.empty:
        success_count = int((auto_runs["status"] == "success").sum())
        degraded_count = int((auto_runs["status"] == "degraded").sum())
        failed_count = int((auto_runs["status"] == "failed").sum())
        health_cols = st.columns(4)
        health_cols[0].metric("自动任务总数", str(status.get("automation_rows", 0)))
        health_cols[1].metric("成功次数", str(success_count))
        health_cols[2].metric("降级次数", str(degraded_count))
        health_cols[3].metric("失败次数", str(failed_count))
        with st.expander("查看最近自动任务记录"):
            display_runs = auto_runs.copy()
            display_runs["degraded"] = display_runs["degraded"].map(lambda x: "是" if x else "否")
            display_runs = display_runs.rename(
                columns={
                    "automation_run_id": "运行ID",
                    "run_time": "运行时间",
                    "task_name": "任务名",
                    "status": "状态",
                    "pool_size": "样本池大小",
                    "catalog_size": "股票池大小",
                    "degraded": "是否降级",
                    "summary": "摘要",
                    "error_message": "错误信息",
                }
            )
            st.dataframe(display_runs, use_container_width=True)

    if status["last_best"]:
        last_best = status["last_best"]
        st.info(
            f"最近一次最佳参数：持仓 {last_best['top_n']} 只，调仓周期 {last_best['rebalance_days']} 天，"
            f"回看 {last_best['lookback_years']} 年，目标分 {last_best['objective_score']:.4f}"
        )
    else:
        st.caption("当前还没有历史最佳参数记录，先运行一次策略优化后这里会显示结果。")

    if best_config:
        weights_cfg = (best_config.get("weights") or {})
        thresholds_cfg = (best_config.get("thresholds") or {})
        bonus_cfg = (best_config.get("recommendation_bonus") or {})
        st.caption(
            "当前生效的最佳统一评分参数："
            f"趋势 {weights_cfg.get('trend', '-')}, "
            f"基本面 {weights_cfg.get('fundamental', '-')}, "
            f"吸筹 {weights_cfg.get('accumulation', '-')}, "
            f"情绪 {weights_cfg.get('sentiment', '-')}, "
            f"行业 {weights_cfg.get('industry', '-')}, "
            f"事件 {weights_cfg.get('event', '-')}, "
            f"最低推荐 {thresholds_cfg.get('min_recommendation', '-')}, "
            f"最低行业分 {thresholds_cfg.get('min_industry_score', '-')}, "
            f"推荐关注加分 {bonus_cfg.get('推荐关注', '-')}"
        )
        st.caption(
            f"基础目标分 {best_config.get('base_objective_score', 0.0):.4f} | "
            f"稳定性 {best_config.get('stability_score', 0.0):.4f} | "
            f"稳定性惩罚 {best_config.get('stability_penalty', 0.0):.4f} | "
            f"稳健性奖励 {best_config.get('robustness_bonus', 0.0):.4f}"
        )

    settings_col1, settings_col2 = st.columns(2)
    with settings_col1:
        automation_pool_size = st.selectbox(
            "自动任务股票池规模",
            [100, 200, 300, 500],
            index=[100, 200, 300, 500].index(current_pool_size) if current_pool_size in [100, 200, 300, 500] else 2,
        )
        if automation_pool_size != current_pool_size:
            set_automation_pool_size(automation_pool_size)
    with settings_col2:
        st.caption("自动任务默认会围绕这批股票做日常更新、行业刷新和参数优化，建议先从 300 只开始。")
        if last_daily_update:
            st.write(
                f"最近日更：{last_daily_update.get('run_time', '--')} | "
                f"保存 {last_daily_update.get('saved_symbols', 0)} 只 | "
                f"来源 {last_daily_update.get('catalog_source', '--')}"
            )
            if last_daily_update.get("degraded_reason"):
                st.caption(f"日更曾降级运行：{last_daily_update['degraded_reason']}")
        if last_weekly_optimization:
            best = last_weekly_optimization.get("best_config", {})
            st.write(
                f"最近优化：样本池 {last_weekly_optimization.get('pool_size', '-')} | "
                f"来源 {last_weekly_optimization.get('catalog_source', '--')} | "
                f"最佳参数 {best.get('top_n', '-')}/{best.get('rebalance_days', '-')}/{best.get('lookback_years', '-')}"
            )
            if last_weekly_optimization.get("degraded_reason"):
                st.caption(f"优化曾降级运行：{last_weekly_optimization['degraded_reason']}")
        if last_industry_refresh:
            st.write(
                f"最近行业刷新：{last_industry_refresh.get('run_time', '--')} | "
                f"解析 {last_industry_refresh.get('resolved', 0)}/{last_industry_refresh.get('catalog_size', 0)} | "
                f"巨潮来源 {last_industry_refresh.get('from_cninfo', 0)}"
            )
            st.caption(
                f"主表 {last_industry_refresh.get('from_membership', 0)} | "
                f"估值回退 {last_industry_refresh.get('from_valuation', 0)} | "
                f"巨潮接口 {last_industry_refresh.get('from_cninfo', 0)} | "
                f"过期 {last_industry_refresh.get('stale', 0)} | "
                f"缺失 {last_industry_refresh.get('missing', 0)} | "
                f"失败 {last_industry_refresh.get('failed', 0)}"
            )
            if last_industry_refresh.get("degraded_reason"):
                st.caption(f"行业刷新曾降级运行：{last_industry_refresh['degraded_reason']}")

    left_col, right_col = st.columns(2)
    with left_col:
        if st.button("同步当前股票池行情到数据库", key="sync_market_data_button", disabled=not symbols):
            with st.spinner("正在同步当前股票池行情..."):
                sync_result = sync_market_data_to_db(symbols, catalog=catalog)
            load_db_status.clear()
            if sync_result["failed_symbols"]:
                st.warning(
                    f"已保存 {sync_result['saved_symbols']} 只、{sync_result['saved_rows']} 行数据，"
                    f"但仍有 {sync_result['failed_symbols']} 只失败。"
                )
            else:
                st.success(f"已保存 {sync_result['saved_symbols']} 只、{sync_result['saved_rows']} 行数据。")
        if st.button("执行一次日常数据更新", key="run_daily_update_button"):
            with st.spinner("正在执行日常数据更新..."):
                try:
                    update_result = run_daily_update(pool_size=automation_pool_size)
                except DataFetchError as error:
                    render_data_error(str(error))
                else:
                    load_db_status.clear()
                    message = (
                        f"日常更新完成：股票池 {update_result['catalog_size']} 只，"
                        f"保存 {update_result['saved_symbols']} 只、{update_result['saved_rows']} 行数据。"
                    )
                    if update_result.get("degraded_reason"):
                        st.warning(f"{message} 本次为降级运行。")
                        st.caption(update_result["degraded_reason"])
                    else:
                        st.success(message)
        if st.button("刷新行业归属主表", key="refresh_industry_membership_button"):
            with st.spinner("正在刷新行业归属..."):
                try:
                    refresh_result = run_industry_membership_refresh(pool_size=automation_pool_size)
                except DataFetchError as error:
                    render_data_error(str(error))
                except Exception as error:
                    render_data_error(str(error))
                else:
                    load_db_status.clear()
                    message = (
                        f"行业归属刷新完成：{refresh_result['resolved']}/{refresh_result['catalog_size']} 已解析；"
                        f"主表 {refresh_result['from_membership']}、估值回退 {refresh_result['from_valuation']}、"
                        f"巨潮 {refresh_result['from_cninfo']}、实时接口 {refresh_result['from_live']}。"
                    )
                    if refresh_result.get("degraded_reason") or refresh_result.get("failed") or refresh_result.get("missing"):
                        st.warning(message)
                    else:
                        st.success(message)
                    st.caption(
                        f"过期 {refresh_result.get('stale', 0)} | "
                        f"缺失 {refresh_result.get('missing', 0)} | "
                        f"失败 {refresh_result.get('failed', 0)}"
                    )
                    sample_resolutions = refresh_result.get("sample_resolutions") or []
                    if sample_resolutions:
                        st.dataframe(pd.DataFrame(sample_resolutions), use_container_width=True)

    with right_col:
        if st.button("运行一次统一参数优化", key="run_weekly_optimization_button", disabled=not symbols):
            with st.spinner("正在运行统一参数优化..."):
                try:
                    optimization = run_strategy_parameter_optimization(
                        symbols,
                        run_unified_selection,
                        symbol_names=symbol_names,
                        strategy_name="unified_selection",
                    )
                except (BacktestError, DataFetchError) as error:
                    render_data_error(str(error))
                else:
                    load_db_status.clear()
                    best = optimization["best_config"]
                    st.success(
                        f"优化完成：最佳持仓 {best['top_n']} 只，调仓 {best['rebalance_days']} 天，"
                        f"回看 {best['lookback_years']} 年。"
                    )
                    eval_df = pd.DataFrame(optimization["evaluations"][:10]).rename(
                        columns={
                            "stage": "阶段",
                            "top_n": "持仓数",
                            "rebalance_days": "调仓天数",
                            "lookback_years": "回看年数",
                            "total_return": "总收益",
                            "max_drawdown": "最大回撤",
                            "positive_period_ratio": "正收益占比",
                            "base_objective_score": "基础目标分",
                            "objective_score": "综合目标分",
                            "stability_score": "稳定性",
                            "stability_penalty": "稳定性惩罚",
                            "robustness_bonus": "稳健性奖励",
                        }
                    )
                    if "weights" in optimization["best_config"]:
                        eval_df["趋势权重"] = eval_df["weights"].map(lambda x: (x or {}).get("trend"))
                        eval_df["基本面权重"] = eval_df["weights"].map(lambda x: (x or {}).get("fundamental"))
                        eval_df["吸筹权重"] = eval_df["weights"].map(lambda x: (x or {}).get("accumulation"))
                        eval_df["情绪权重"] = eval_df["weights"].map(lambda x: (x or {}).get("sentiment"))
                        eval_df["行业权重"] = eval_df["weights"].map(lambda x: (x or {}).get("industry"))
                        eval_df["事件权重"] = eval_df["weights"].map(lambda x: (x or {}).get("event"))
                    if "thresholds" in optimization["best_config"]:
                        eval_df["最低推荐级别"] = eval_df["thresholds"].map(lambda x: (x or {}).get("min_recommendation"))
                        eval_df["最低趋势分"] = eval_df["thresholds"].map(lambda x: (x or {}).get("min_trend_score"))
                        eval_df["最低基本面分"] = eval_df["thresholds"].map(lambda x: (x or {}).get("min_fundamental_score"))
                        eval_df["最低行业分"] = eval_df["thresholds"].map(lambda x: (x or {}).get("min_industry_score"))
                        eval_df["最低事件分"] = eval_df["thresholds"].map(lambda x: (x or {}).get("min_event_score"))
                    if "recommendation_bonus" in optimization["best_config"]:
                        eval_df["推荐关注加分"] = eval_df["recommendation_bonus"].map(lambda x: (x or {}).get("推荐关注"))
                        eval_df["暂不推荐扣分"] = eval_df["recommendation_bonus"].map(lambda x: (x or {}).get("暂不推荐"))
                    for weight_col in ["趋势权重", "基本面权重", "吸筹权重", "情绪权重", "行业权重", "事件权重"]:
                        if weight_col in eval_df.columns:
                            eval_df[weight_col] = eval_df[weight_col].map(lambda x: f"{x:.2%}")
                    for metric_col in ["总收益", "最大回撤", "正收益占比", "基础目标分", "综合目标分", "稳定性", "稳定性惩罚", "稳健性奖励"]:
                        if metric_col in eval_df.columns:
                            eval_df[metric_col] = eval_df[metric_col].map(lambda x: f"{x:.4f}")
                    st.dataframe(eval_df, use_container_width=True)
                    st.caption("这里展示的是统一评分口径下的参数搜索结果，不再是旧版多因子独立策略。")
        if st.button("执行每周自动优化流程", key="reload_database_status_button"):
            with st.spinner("正在执行每周自动优化流程..."):
                try:
                    optimization = run_weekly_optimization(pool_size=automation_pool_size)
                except (BacktestError, DataFetchError) as error:
                    render_data_error(str(error))
                else:
                    load_db_status.clear()
                    best = optimization["best_config"]
                    message = (
                        f"每周优化完成：持仓 {best['top_n']} / "
                        f"调仓 {best['rebalance_days']} / 回看 {best['lookback_years']} 年。"
                    )
                    if optimization.get("degraded_reason"):
                        st.warning(f"{message} 本次为降级运行。")
                        st.caption(optimization["degraded_reason"])
                    else:
                        st.success(message)


if "symbols" not in st.session_state:
    st.session_state.symbols = []
if "symbol_names" not in st.session_state:
    st.session_state.symbol_names = {}
if "catalog" not in st.session_state:
    st.session_state.catalog = pd.DataFrame()
if "symbols_error" not in st.session_state:
    st.session_state.symbols_error = None
if "last_result" not in st.session_state:
    st.session_state.last_result = None
if "single_stock_analysis" not in st.session_state:
    st.session_state.single_stock_analysis = None
if "single_stock_error" not in st.session_state:
    st.session_state.single_stock_error = None
if "single_stock_query" not in st.session_state:
    st.session_state.single_stock_query = ""
if "accumulation_scan_result" not in st.session_state:
    st.session_state.accumulation_scan_result = None
if "accumulation_scan_error" not in st.session_state:
    st.session_state.accumulation_scan_error = None
if "accumulation_scan_limit" not in st.session_state:
    st.session_state.accumulation_scan_limit = 500
if "accumulation_top_k" not in st.session_state:
    st.session_state.accumulation_top_k = 20
if "growth_candidate_result" not in st.session_state:
    st.session_state.growth_candidate_result = None
if "growth_candidate_error" not in st.session_state:
    st.session_state.growth_candidate_error = None
if "watchlist_error" not in st.session_state:
    st.session_state.watchlist_error = None
if "watchlist_analysis_result" not in st.session_state:
    st.session_state.watchlist_analysis_result = None
if "watchlist_analysis_errors" not in st.session_state:
    st.session_state.watchlist_analysis_errors = {}
if "unified_scoring_config" not in st.session_state:
    st.session_state.unified_scoring_config = normalize_scoring_config(
        get_setting("unified_scoring_config", DEFAULT_SCORING_CONFIG)
    )

render_usage_guide()
current_scoring_config = normalize_scoring_config(st.session_state.unified_scoring_config)
current_scoring_config_json = scoring_config_to_json(current_scoring_config)
tab_strategy, tab_single, tab_accumulation, tab_growth, tab_logic, tab_evolution = st.tabs(
    ["策略总览", "单股分析", "吸筹候选", "中线候选", "分析方案", "策略进化"]
)

with tab_strategy:
    pool_col1, pool_col2, pool_col3 = st.columns(3)
    with pool_col1:
        pool_size = st.selectbox("股票池数量", [50, 100, 150, 200, 300, 500], index=1)
    with pool_col2:
        top_n = st.slider("持仓数", 5, 20, 10)
    with pool_col3:
        rebalance = st.slider("调仓周期", 5, 60, 20)

    extra_col1, extra_col2 = st.columns(2)
    with extra_col1:
        lookback_years = st.selectbox("回测年数", [1, 3, 5, 10], index=1)
    with extra_col2:
        st.caption("股票池默认按近60日涨幅、年内涨幅和换手率综合排序。")

    col_a, col_b, col_c = st.columns([1, 1, 3])
    with col_a:
        if st.button("加载股票池", type="primary"):
            try:
                catalog = load_catalog(pool_size)
                st.session_state.catalog = catalog
                st.session_state.symbols = catalog["code"].tolist()
                st.session_state.symbol_names = dict(zip(catalog["code"], catalog["name"]))
                st.session_state.symbols_error = None
            except DataFetchError as error:
                st.session_state.symbols = []
                st.session_state.symbol_names = {}
                st.session_state.catalog = pd.DataFrame()
                st.session_state.symbols_error = str(error)
    with col_b:
        run_strategy_clicked = st.button("运行策略", disabled=not st.session_state.symbols)
    with col_c:
        symbol_count = len(st.session_state.symbols)
        if symbol_count:
            catalog = st.session_state.catalog
            pool_description = catalog.attrs.get("pool_description", f"股票池 Top {symbol_count}")
            source = catalog.attrs.get("source", "unknown")
            st.success(f"已加载 {symbol_count} 只股票。来源：{pool_description}。")
            if source == "cache_stale":
                st.warning("当前股票池来自旧缓存，系统已尝试刷新但本次未成功。")
            elif source == "cache_fresh":
                st.info("当前股票池直接来自本地缓存。")
            elif source == "network_refresh":
                st.info("当前股票池基于缓存自动刷新成功。")
        else:
            st.warning("尚未加载股票池。点击左侧按钮开始。")

    if st.session_state.symbols_error:
        render_data_error(st.session_state.symbols_error)

    if not st.session_state.catalog.empty:
        render_pool_preview(st.session_state.catalog)

    symbols = st.session_state.symbols
    symbol_names = st.session_state.symbol_names

    if run_strategy_clicked:
        with st.spinner("正在执行回测..."):
            try:
                best_config = get_best_strategy_config(default={})
                auto_optimize_enabled = bool(
                    get_setting("auto_optimize_enabled", False)
                    and best_config
                    and best_config.get("strategy_name") == "unified_selection"
                )
                effective_top_n = top_n
                effective_rebalance = rebalance
                effective_lookback_years = lookback_years
                if auto_optimize_enabled:
                    effective_top_n = int(best_config.get("top_n", top_n))
                    effective_rebalance = int(best_config.get("rebalance_days", rebalance))
                    effective_lookback_years = int(best_config.get("lookback_years", lookback_years))

                strategy_config = current_scoring_config
                effective_strategy = (
                    lambda stock_dict, cfg=strategy_config: run_unified_selection(stock_dict, config=cfg)
                )

                result = backtest_portfolio_realistic(
                    symbols,
                    effective_strategy,
                    effective_top_n,
                    effective_rebalance,
                    lookback_years=effective_lookback_years,
                    symbol_names=symbol_names,
                )
            except (BacktestError, DataFetchError) as error:
                st.session_state.last_result = None
                render_data_error(str(error))
            else:
                save_recommendations(result["current_pick"]["as_of_date"], result["current_pick"]["table"])
                save_backtest_run(
                    strategy_name="unified_selection",
                    top_n=effective_top_n,
                    rebalance_days=effective_rebalance,
                    lookback_years=effective_lookback_years,
                    metrics=result["metrics"],
                    symbols_count=len(symbols),
                    meta={
                        "errors_count": len(result.get("errors", {})),
                        "cache_stats": result.get("cache_stats", {}),
                        "api_stats": result.get("api_stats", {}),
                        "strategy_config": strategy_config,
                    },
                )
                load_db_status.clear()
                st.session_state.last_result = result

    result = st.session_state.last_result
    if result:
        render_current_pick(result)
        st.divider()
        render_paper_trading_panel(result)
        st.divider()
        render_quant_review_panel(result)
        st.divider()

        curve = result["curve"]
        render_metrics(result["metrics"])

        st.subheader("组合净值走势")
        st.line_chart(curve)
        st.dataframe(curve.rename("净值").to_frame(), use_container_width=True)

        left_col, right_col = st.columns(2)
        with left_col:
            st.subheader("策略权重变化")
            render_weights(result["weights"])
        with right_col:
            st.subheader("历史调仓记录")
            render_holdings(result)

        render_debug_panel(result, symbol_names)
    else:
        st.info("先加载股票池并运行策略，这里会展示当前推荐、回测结果和量化交易核验面板。")

with tab_single:
    render_single_stock_panel(st.session_state.symbol_names)
    st.divider()
    render_watchlist_panel(st.session_state.symbol_names)

with tab_accumulation:
    render_accumulation_screener()

with tab_growth:
    render_growth_candidate_panel()

with tab_logic:
    render_analysis_logic_panel()

with tab_evolution:
    render_evolution_panel(
        st.session_state.symbols,
        st.session_state.symbol_names,
        st.session_state.catalog,
    )
