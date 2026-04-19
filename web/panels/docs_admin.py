from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import streamlit as st


@st.cache_data(show_spinner=False)
def read_doc_text(path_str: str) -> str:
    path = Path(path_str)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def extract_markdown_section(text: str, heading: str) -> str:
    lines = text.splitlines()
    section_lines: list[str] = []
    collecting = False
    heading_level = 0
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            heading_level = len(stripped) - len(stripped.lstrip("#"))
            continue
        if collecting and stripped.startswith("#"):
            current_level = len(stripped) - len(stripped.lstrip("#"))
            if current_level <= heading_level:
                break
        if collecting:
            section_lines.append(line)
    return "\n".join(section_lines).strip()


def parse_release_note_summaries(text: str, limit: int = 3) -> list[dict[str, object]]:
    sections = re.split(r"^##\s+", text, flags=re.MULTILINE)
    parsed: list[dict[str, object]] = []
    for block in sections:
        block = block.strip()
        if not block or not re.match(r"\d{4}-\d{2}-\d{2}", block):
            continue
        lines = block.splitlines()
        title = lines[0].strip()
        goal_match = re.search(r"- 本次目标：(.+)", block)
        additions = re.findall(r"- 本次新增：\s*\n((?:  - .+\n?)*)", block)
        addition_items: list[str] = []
        if additions:
            addition_items = [line.strip()[2:].strip() for line in additions[0].splitlines() if line.strip().startswith("-")]
        risks = re.findall(r"- 风险或遗留问题：\s*\n((?:  - .+\n?)*)", block)
        risk_items: list[str] = []
        if risks:
            risk_items = [line.strip()[2:].strip() for line in risks[0].splitlines() if line.strip().startswith("-")]
        parsed.append(
            {
                "日期": title,
                "本次目标": goal_match.group(1).strip() if goal_match else "-",
                "本次新增": addition_items,
                "风险或遗留问题": risk_items,
            }
        )
    return parsed[:limit]


def parse_tracker_progress(text: str) -> pd.DataFrame:
    rows = []
    for match in re.finditer(
        r"^\d+\.\s+(.+?)：系统接入状态 `(.+?)` \| 机构成熟度 `(.+?)`$",
        text,
        flags=re.MULTILINE,
    ):
        rows.append(
            {
                "能力项": match.group(1).strip(),
                "系统接入状态": match.group(2).strip(),
                "机构成熟度": match.group(3).strip(),
            }
        )
    return pd.DataFrame(rows)


def parse_backlog_items(text: str, limit: int = 8) -> pd.DataFrame:
    rows = []
    for match in re.finditer(r"^###\s+(.+?)\n(.*?)- 当前状态：`(.+?)`", text, flags=re.MULTILINE | re.DOTALL):
        rows.append(
            {
                "缺陷项": match.group(1).strip(),
                "当前状态": match.group(3).strip(),
            }
        )
        if len(rows) >= limit:
            break
    return pd.DataFrame(rows)


def render_docs_hub_panel(project_root: Path, docs_root: Path, doc_library: list[dict]) -> None:
    st.subheader("Docs 看板")
    st.caption("这里把 docs 目录里的当前逻辑、专业化方向、缺陷 backlog 和历史修补记录集中到一个入口。")

    docs_df = pd.DataFrame(
        [
            {
                "文档": item["label"],
                "位置": str(item["path"].relative_to(project_root)),
                "作用": item["role"],
            }
            for item in doc_library
        ]
    )
    st.write("文档入口总览")
    st.dataframe(docs_df.astype(str), use_container_width=True, hide_index=True)

    release_text = read_doc_text(str(docs_root / "history" / "RELEASE_NOTES.md"))
    tracker_text = read_doc_text(str(docs_root / "current" / "PROFESSIONALIZATION_TRACKER.md"))
    backlog_text = read_doc_text(str(docs_root / "current" / "PRIVATE_FUND_GAP_BACKLOG.md"))
    readme_text = read_doc_text(str(docs_root / "README.md"))

    overview_tab, release_tab, direction_tab, issue_tab, raw_tab = st.tabs(
        ["概览", "最近改动", "专业化方向", "问题清单", "文档原文"]
    )

    with overview_tab:
        summary_cols = st.columns(3)
        release_summaries = parse_release_note_summaries(release_text, limit=1)
        tracker_df = parse_tracker_progress(tracker_text)
        backlog_df = parse_backlog_items(backlog_text, limit=6)

        with summary_cols[0]:
            st.write("最近改了什么")
            if release_summaries:
                latest = release_summaries[0]
                st.write(f"- 时间：{latest['日期']}")
                st.write(f"- 目标：{latest['本次目标']}")
                for item in latest["本次新增"]:
                    st.write(f"- {item}")
            else:
                st.info("当前没有读到修补记录。")

        with summary_cols[1]:
            st.write("朝什么方向改")
            if not tracker_df.empty:
                st.dataframe(tracker_df, use_container_width=True, hide_index=True)
            else:
                st.info("当前没有读到专业化追踪摘要。")

        with summary_cols[2]:
            st.write("现在还有哪些问题")
            if not backlog_df.empty:
                st.dataframe(backlog_df[["缺陷项", "当前状态"]], use_container_width=True, hide_index=True)
            else:
                st.info("当前没有读到缺陷清单。")

        readme_section = extract_markdown_section(readme_text, "### 快速定位规则")
        if readme_section:
            with st.expander("怎么读这些文档", expanded=False):
                st.markdown(readme_section)

    with release_tab:
        st.write("最近几次修补记录")
        release_rows = []
        for item in parse_release_note_summaries(release_text, limit=5):
            release_rows.append(
                {
                    "日期": item["日期"],
                    "本次目标": item["本次目标"],
                    "新增要点": "；".join(item["本次新增"]) if item["本次新增"] else "-",
                    "遗留问题": "；".join(item["风险或遗留问题"]) if item["风险或遗留问题"] else "-",
                }
            )
        if release_rows:
            st.dataframe(pd.DataFrame(release_rows).astype(str), use_container_width=True, hide_index=True)
        else:
            st.info("当前没有可展示的修补记录。")

    with direction_tab:
        st.write("专业化推进顺序")
        tracker_df = parse_tracker_progress(tracker_text)
        if not tracker_df.empty:
            st.dataframe(tracker_df.astype(str), use_container_width=True, hide_index=True)
        else:
            st.info("当前没有可展示的专业化推进摘要。")

    with issue_tab:
        st.write("私募视角问题清单")
        backlog_df = parse_backlog_items(backlog_text, limit=12)
        if not backlog_df.empty:
            st.dataframe(backlog_df.astype(str), use_container_width=True, hide_index=True)
        else:
            st.info("当前没有可展示的缺陷清单。")

    with raw_tab:
        selected_label = st.selectbox("选择要查看的文档", [item["label"] for item in doc_library], index=0)
        selected_doc = next(item for item in doc_library if item["label"] == selected_label)
        st.caption(f"当前查看：{selected_doc['path'].relative_to(project_root)}")
        doc_text = read_doc_text(str(selected_doc["path"]))
        if doc_text:
            st.markdown(doc_text)
        else:
            st.warning("当前文档不存在或尚未生成。")
