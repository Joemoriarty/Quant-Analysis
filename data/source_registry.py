from __future__ import annotations


PLATFORM_DATA_SOURCE_MATRIX = [
    {
        "分析环节": "股票池 / 检索列表",
        "主数据源": "akshare.stock_zh_a_spot_em",
        "缓存与恢复": "磁盘 CSV 缓存（symbols / lookup）",
        "降级策略": "东方财富人气榜回退",
        "当前定位": "统一股票池入口",
        "借鉴自 TradingAgents-CN": "统一数据源入口 + 主备降级",
    },
    {
        "分析环节": "历史行情 / 实时价格",
        "主数据源": "AKShare 行情 + 本地行情缓存",
        "缓存与恢复": "个股 CSV 缓存 + stale cache 回退",
        "降级策略": "优先本地缓存，网络失败时回退历史缓存",
        "当前定位": "技术面与量价代理底座",
        "借鉴自 TradingAgents-CN": "行情服务抽象 + 缓存优先",
    },
    {
        "分析环节": "基本面 / 估值",
        "主数据源": "AKShare 主源 + Tushare 备源",
        "缓存与恢复": "SQLite 快照缓存",
        "降级策略": "优先快照，必要时实时刷新；AKShare 缺字段或失败时回退 Tushare，行业信息再回退到 cninfo 行业变更",
        "当前定位": "基本面评分与行业归属",
        "借鉴自 TradingAgents-CN": "统一基本面入口 + 主备降级 + 快照优先",
    },
    {
        "分析环节": "事件驱动",
        "主数据源": "AKShare 公告 / 业绩预告 / 披露预约",
        "缓存与恢复": "函数缓存 + SQLite 事件快照",
        "降级策略": "单接口失败时跳过该分支，保留已有事件集",
        "当前定位": "事件评分与催化识别",
        "借鉴自 TradingAgents-CN": "新闻 / 事件作为独立 analyst 维度",
    },
    {
        "分析环节": "实时新闻",
        "主数据源": "akshare.stock_news_em",
        "缓存与恢复": "SQLite 新闻快照",
        "降级策略": "优先最近新闻快照，实时抓取失败时回退缓存",
        "当前定位": "新闻扰动与短期催化识别",
        "借鉴自 TradingAgents-CN": "独立新闻层 + 数据源显式治理",
    },
    {
        "分析环节": "市场情绪",
        "主数据源": "akshare.stock_zh_a_spot_em",
        "缓存与恢复": "SQLite 情绪快照",
        "降级策略": "优先最近快照，实时抓取失败时回退缓存",
        "当前定位": "市场顺风 / 逆风校正",
        "借鉴自 TradingAgents-CN": "情绪维度单独建模",
    },
    {
        "分析环节": "行业横向比较",
        "主数据源": "行业归属 + 行业同行快照插件",
        "缓存与恢复": "SQLite 同行快照 + 缓存优先比较",
        "降级策略": "缺同行样本时不强行输出行业结论",
        "当前定位": "行业质量比较",
        "借鉴自 TradingAgents-CN": "多 analyst / 多插件汇总为统一判断",
    },
]


def build_platform_data_source_matrix() -> list[dict]:
    return [dict(item) for item in PLATFORM_DATA_SOURCE_MATRIX]


def build_analysis_data_source_summary(analysis: dict) -> dict:
    fundamental_summary = analysis.get("fundamental_summary") or {}
    market_sentiment_snapshot = analysis.get("market_sentiment_snapshot") or {}
    company_events = analysis.get("company_events") or []
    news_items = analysis.get("news_items") or []
    industry_membership = analysis.get("industry_membership") or {}
    comparison_results = analysis.get("comparison_results") or []

    event_sources = sorted({str(item.get("source")) for item in company_events if item.get("source")})
    news_sources = sorted({str(item.get("data_source") or item.get("source")) for item in news_items if item.get("data_source") or item.get("source")})
    comparison_titles = sorted(
        {
            str(item.get("title") or item.get("name"))
            for item in comparison_results
            if item.get("available") and (item.get("title") or item.get("name"))
        }
    )

    summary_rows = [
        {
            "环节": "价格 / 行情",
            "当前来源": str(analysis.get("data_source") or "unknown"),
            "缓存策略": "CSV 本地缓存优先，必要时刷新",
            "降级说明": "网络失败时回退历史缓存",
        },
        {
            "环节": "基本面 / 估值",
            "当前来源": " / ".join(
                [
                    str((fundamental_summary.get("source") or {}).get("fundamental") or "-"),
                    str((fundamental_summary.get("source") or {}).get("valuation") or "-"),
                ]
            ),
            "缓存策略": "SQLite 快照缓存",
            "降级说明": "实时刷新失败时保留最近快照",
        },
        {
            "环节": "市场情绪",
            "当前来源": str(market_sentiment_snapshot.get("source") or "-"),
            "缓存策略": "SQLite 情绪快照",
            "降级说明": "抓取失败时回退最近快照",
        },
        {
            "环节": "事件驱动",
            "当前来源": " / ".join(event_sources) if event_sources else "-",
            "缓存策略": "事件快照 + 函数缓存",
            "降级说明": "单事件源失败时保留其他事件分支",
        },
        {
            "环节": "实时新闻",
            "当前来源": " / ".join(news_sources) if news_sources else "-",
            "缓存策略": "SQLite 新闻快照",
            "降级说明": "实时抓取失败时回退最近新闻快照",
        },
        {
            "环节": "行业归属",
            "当前来源": str(industry_membership.get("source") or "-"),
            "缓存策略": "SQLite 行业归属快照",
            "降级说明": "估值快照无行业字段时回退行业变更数据",
        },
        {
            "环节": "行业横向比较",
            "当前来源": " / ".join(comparison_titles) if comparison_titles else "-",
            "缓存策略": "同行快照缓存优先",
            "降级说明": "同行样本不足时不强出结论",
        },
    ]

    return {
        "headline": "这层借鉴了 TradingAgents-CN 的统一数据源治理思路，把每个分析环节的主来源、缓存方式和降级路径显式展开。",
        "items": summary_rows,
        "platform_matrix": build_platform_data_source_matrix(),
    }
