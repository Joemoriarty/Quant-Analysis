from __future__ import annotations

import pandas as pd
import streamlit as st


def render_async_task_center(title: str, tasks_source, key_prefix: str, auto_refresh_seconds: int = 4) -> str | None:
    initial_tasks = tasks_source() if callable(tasks_source) else tasks_source
    if not initial_tasks:
        st.caption("当前还没有后台任务记录。")
        return None

    restored_task_id = None

    def _render_body() -> str | None:
        tasks = tasks_source() if callable(tasks_source) else tasks_source
        local_restore_id = None
        running_count = sum(1 for task in tasks if task.get("status") in {"queued", "running"})
        if running_count:
            st.caption(f"当前有 {running_count} 个后台任务正在执行，任务面板会自动刷新。")

        summary_rows = [
            {
                "任务": task.get("label"),
                "状态": task.get("status"),
                "进度": f"{int(float(task.get('progress', 0.0)) * 100)}%",
                "最近消息": task.get("message"),
                "开始时间": task.get("started_at") or task.get("created_at"),
                "完成时间": task.get("finished_at") or "-",
            }
            for task in tasks
        ]
        st.write(title)
        st.dataframe(pd.DataFrame(summary_rows).astype(str), width="stretch", hide_index=True)

        for task in tasks[:5]:
            task_label = f"{task.get('label')} | {task.get('status')}"
            with st.expander(task_label, expanded=task.get("status") in {"queued", "running"}):
                st.write(f"- 任务 ID：`{task.get('id')}`")
                st.write(f"- 参数摘要：{task.get('params')}")
                st.write(f"- 最近消息：{task.get('message')}")
                if task.get("result_summary"):
                    st.write(f"- 结果摘要：{task.get('result_summary')}")
                if task.get("error"):
                    st.write(f"- 错误信息：{task.get('error')}")
                if task.get("status") == "completed":
                    if st.button("恢复结果", key=f"{key_prefix}_restore_{task.get('id')}"):
                        local_restore_id = str(task.get("id"))
        return local_restore_id

    if hasattr(st, "fragment") and any(task.get("status") in {"queued", "running"} for task in initial_tasks):
        @st.fragment(run_every=f"{auto_refresh_seconds}s")
        def _task_fragment():
            return _render_body()

        restored_task_id = _task_fragment()
    else:
        restored_task_id = _render_body()

    return restored_task_id


def render_research_workbench_home(
    latest_result: dict | None,
    tasks_source,
    latest_daily_update: dict | None,
    latest_weekly_optimization: dict | None,
) -> None:
    st.subheader("投研工作台首页")
    st.caption("这里优先看结论变化、风险暴露和待处理任务，而不是先跳进某个功能页。")

    tasks = tasks_source() if callable(tasks_source) else tasks_source
    running_count = sum(1 for task in tasks if task.get("status") in {"queued", "running"})
    completed_count = sum(1 for task in tasks if task.get("status") == "completed")

    overview_cols = st.columns(4)
    overview_cols[0].metric("后台任务", str(len(tasks)))
    overview_cols[1].metric("运行中任务", str(running_count))
    overview_cols[2].metric("已完成任务", str(completed_count))
    overview_cols[3].metric(
        "最近日更",
        str(latest_daily_update.get("run_time", "-"))[:16] if latest_daily_update else "-",
    )

    if latest_result and latest_result.get("current_pick"):
        current_pick = latest_result["current_pick"]
        table = current_pick.get("table", pd.DataFrame())
        risk_summary = current_pick.get("risk_summary") or {}

        home_cols = st.columns(3)
        home_cols[0].metric("当前候选数", str(len(table)))
        home_cols[1].metric(
            "最大单票权重",
            f"{float(table['weight'].max()):.2%}" if not table.empty and "weight" in table.columns else "-",
        )
        top_industry = "-"
        if risk_summary.get("industry_exposure"):
            top_industry = max(risk_summary["industry_exposure"].items(), key=lambda item: item[1])[0]
        home_cols[2].metric("当前最集中行业", top_industry)

        summary_rows = [
            {
                "看板项": "信号日期",
                "当前状态": str(pd.to_datetime(current_pick["as_of_date"]).date()),
            },
            {
                "看板项": "低流动性过滤",
                "当前状态": f"跳过 {risk_summary.get('skipped_low_liquidity', 0)} 只",
            },
            {
                "看板项": "行业持仓上限",
                "当前状态": f"单行业最多 {risk_summary.get('max_industry_positions', '-') } 只，实际 {risk_summary.get('industry_exposure', {})}",
            },
            {
                "看板项": "最近优化",
                "当前状态": (
                    f"{latest_weekly_optimization.get('run_time', '-')[:16]} | "
                    f"{(latest_weekly_optimization.get('best_config') or {}).get('top_n', '-')}/"
                    f"{(latest_weekly_optimization.get('best_config') or {}).get('rebalance_days', '-')}"
                    if latest_weekly_optimization
                    else "-"
                ),
            },
        ]
        st.dataframe(pd.DataFrame(summary_rows).astype(str), width="stretch", hide_index=True)

        if not table.empty:
            watch_df = table.copy()
            for col in ["score", "weight", "industry_name", "industry_score", "turnover_amount"]:
                if col not in watch_df.columns:
                    watch_df[col] = None
            watch_df = watch_df.rename(
                columns={
                    "display_name": "股票",
                    "score": "综合评分",
                    "weight": "组合权重",
                    "industry_name": "行业",
                    "industry_score": "行业横向分",
                    "turnover_amount": "成交额估算",
                    "recommendation": "最终结论",
                }
            )
            if "组合权重" in watch_df.columns:
                watch_df["组合权重"] = watch_df["组合权重"].map(lambda x: f"{float(x):.2%}" if pd.notna(x) else "-")
            if "成交额估算" in watch_df.columns:
                watch_df["成交额估算"] = watch_df["成交额估算"].map(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "-")
            st.write("当前重点候选与暴露摘要")
            st.dataframe(
                watch_df[["股票", "最终结论", "综合评分", "组合权重", "行业", "行业横向分", "成交额估算"]],
                width="stretch",
                hide_index=True,
            )
    else:
        st.info("当前还没有最近一次策略结果。先运行策略后，这里会自动汇总候选、暴露和任务状态。")
