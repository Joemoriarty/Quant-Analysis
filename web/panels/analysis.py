from __future__ import annotations

import pandas as pd
import streamlit as st


def render_execution_plan_panel(analysis: dict) -> None:
    summary = analysis.get("execution_plan_summary") or {}
    if not summary:
        return

    st.write("执行计划摘要")
    st.caption(str(summary.get("headline") or ""))
    top_cols = st.columns(4)
    top_cols[0].metric("原始动作", str(summary.get("trade_action") or "-"))
    top_cols[1].metric("风险修正后动作", str(summary.get("risk_adjusted_action") or "-"))
    top_cols[2].metric("执行置信度", f"{int(summary.get('execution_confidence', 0))}/100")
    top_cols[3].metric("执行风险分", f"{int(summary.get('execution_risk_score', 0))}/100")

    detail_df = pd.DataFrame(
        [
            {"项目": "建议仓位", "内容": str(summary.get("position_guidance") or "-")},
            {"项目": "目标价区间", "内容": str(summary.get("target_price_range") or "-")},
            {"项目": "止损位", "内容": str(summary.get("stop_loss") or "-")},
            {"项目": "第一止盈位", "内容": str(summary.get("first_take_profit") or "-")},
            {"项目": "第二止盈位", "内容": str(summary.get("second_take_profit") or "-")},
        ]
    )
    st.dataframe(detail_df, use_container_width=True, hide_index=True)
    st.write(f"- 执行动作解释：{summary.get('action_reasoning', '-')}")
    st.write(f"- 当前执行备注：{summary.get('execution_commentary', '-')}")
    st.write(f"- 升级条件：{summary.get('trigger_to_upgrade', '-')}")
    st.write(f"- 降级条件：{summary.get('trigger_to_downgrade', '-')}")
    notes = summary.get("risk_adjustment_notes") or []
    if notes:
        st.write("风险修正说明")
        for item in notes:
            st.write(f"- {item}")


def render_target_price_panel(analysis: dict) -> None:
    scenarios = analysis.get("target_price_scenarios") or {}
    items = scenarios.get("items") or []
    if not items:
        return

    st.write("目标价情景")
    st.caption(str(scenarios.get("headline") or ""))
    overview_cols = st.columns(4)
    overview_cols[0].metric("当前价格", f"{float(scenarios.get('current_price', 0.0)):.2f}")
    overview_cols[1].metric("保守情景", f"{float(scenarios.get('bear_case_price', 0.0)):.2f}")
    overview_cols[2].metric("基准情景", f"{float(scenarios.get('base_case_price', 0.0)):.2f}")
    overview_cols[3].metric("乐观情景", f"{float(scenarios.get('bull_case_price', 0.0)):.2f}")
    st.dataframe(pd.DataFrame(items).astype(str), use_container_width=True, hide_index=True)


def render_evaluation_framework_panel(analysis: dict) -> None:
    summary = analysis.get("evaluation_framework_summary") or {}
    items = summary.get("items") or []
    if not items:
        return

    st.write("股票评估框架摘要")
    top_cols = st.columns(2)
    with top_cols[0]:
        st.info(str(summary.get("headline") or ""))
    with top_cols[1]:
        overview_df = pd.DataFrame(
            [
                {"项目": "综合评估分", "内容": f"{int(summary.get('overall_score', 0))}/100"},
                {"项目": "当前立场", "内容": str(summary.get("overall_stance") or "-")},
            ]
        )
        st.dataframe(overview_df, use_container_width=True, hide_index=True)

    framework_df = pd.DataFrame(items)
    st.dataframe(framework_df.astype(str), use_container_width=True, hide_index=True)


def render_news_panel(analysis: dict) -> None:
    summary = analysis.get("news_summary") or {}
    st.write("实时新闻摘要")
    if not summary.get("available"):
        st.info(str(summary.get("headline") or "当前没有可用的实时新闻数据"))
        return

    st.caption(str(summary.get("headline") or ""))
    overview_cols = st.columns(4)
    overview_cols[0].metric("新闻倾向", str(summary.get("state") or "-"))
    overview_cols[1].metric("新闻评分", f"{int(summary.get('score', 50))}/100")
    overview_cols[2].metric("新闻数量", str(len(analysis.get("news_items") or [])))
    overview_cols[3].metric("重点新闻", str(summary.get("high_importance_count") or 0))

    items = summary.get("items") or []
    if items:
        st.dataframe(pd.DataFrame(items).astype(str), use_container_width=True, hide_index=True)
    st.write(f"- {summary.get('conclusion', '-')}")

    positive_flags = summary.get("positive_flags") or []
    if positive_flags:
        st.write("新闻亮点")
        for item in positive_flags:
            st.write(f"- {item}")

    risk_flags = summary.get("risk_flags") or []
    if risk_flags:
        st.write("新闻风险")
        for item in risk_flags:
            st.write(f"- {item}")


def render_data_source_panel(analysis: dict) -> None:
    summary = analysis.get("data_source_summary") or {}
    items = summary.get("items") or []
    if not items:
        return

    st.write("数据来源与降级路径")
    st.caption(str(summary.get("headline") or ""))
    st.dataframe(pd.DataFrame(items).astype(str), use_container_width=True, hide_index=True)

    platform_matrix = summary.get("platform_matrix") or []
    if platform_matrix:
        with st.expander("统一数据来源矩阵"):
            st.dataframe(pd.DataFrame(platform_matrix).astype(str), use_container_width=True, hide_index=True)


def render_research_workflow_panel(analysis: dict) -> None:
    research = analysis.get("research_workflow_summary") or {}
    if not research:
        return

    st.write("研究流程摘要")
    headline_cols = st.columns(2)
    with headline_cols[0]:
        st.info(str(research.get("investment_thesis") or "当前还没有生成结构化投资逻辑。"))
    with headline_cols[1]:
        review_df = pd.DataFrame(
            [
                {"项目": "研究立场", "内容": str(research.get("stance") or "-")},
                {"项目": "优先动作", "内容": str(research.get("action_bias") or "-")},
                {"项目": "下次复核", "内容": str(research.get("next_review_window") or "-")},
            ]
        )
        st.dataframe(review_df, use_container_width=True, hide_index=True)

    detail_cols = st.columns(3)
    with detail_cols[0]:
        st.write("看多依据")
        bullish_points = research.get("bullish_points") or []
        if bullish_points:
            for item in bullish_points:
                st.write(f"- {item}")
        else:
            st.caption("当前没有足够强的正向催化。")

    with detail_cols[1]:
        st.write("看空 / 反方依据")
        bearish_points = research.get("bearish_points") or []
        if bearish_points:
            for item in bearish_points:
                st.write(f"- {item}")
        else:
            st.caption("当前没有显著反方证据。")

    with detail_cols[2]:
        st.write("失效条件")
        invalidation_conditions = research.get("invalidation_conditions") or []
        if invalidation_conditions:
            for item in invalidation_conditions:
                st.write(f"- {item}")
        else:
            st.caption("当前没有结构化失效条件。")

    tracking_items = research.get("tracking_indicators") or []
    if tracking_items:
        st.write("跟踪指标")
        tracking_df = pd.DataFrame(tracking_items)
        st.dataframe(tracking_df.astype(str), use_container_width=True, hide_index=True)


def render_risk_committee_panel(analysis: dict) -> None:
    summary = analysis.get("risk_committee_summary") or {}
    if not summary:
        return

    st.write("风险委员会摘要")
    overview_cols = st.columns(4)
    overview_cols[0].metric("最终风险等级", str(summary.get("overall_level", "-")))
    overview_cols[1].metric("趋势风险", str((summary.get("trend_risk") or {}).get("level", "-")))
    overview_cols[2].metric("基本面风险", str((summary.get("fundamental_risk") or {}).get("level", "-")))
    overview_cols[3].metric("事件风险", str((summary.get("event_risk") or {}).get("level", "-")))
    st.caption(str(summary.get("headline") or ""))

    detail_rows = []
    for label, key in [
        ("趋势风险", "trend_risk"),
        ("基本面风险", "fundamental_risk"),
        ("事件风险", "event_risk"),
        ("行业/组合风险", "industry_portfolio_risk"),
    ]:
        item = summary.get(key) or {}
        detail_rows.append(
            {
                "维度": label,
                "等级": str(item.get("level") or "-"),
                "说明": str(item.get("summary") or "-"),
            }
        )
    st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)
