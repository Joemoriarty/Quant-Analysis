# TradingAgents-CN 借鉴与改造路线图

这份文档用于记录当前项目如何借鉴 `TradingAgents-CN`，并把外部项目能力映射回本项目的渐进式改造路线。

文档入口：
- 首次阅读或准备更新 `docs/` 时，请先看 [README.md](../README.md)。
- 固定更新模板请看 [DOC_UPDATE_TEMPLATES.md](../templates/DOC_UPDATE_TEMPLATES.md)。
- 如果你想看当前缺陷 backlog，请看 [PRIVATE_FUND_GAP_BACKLOG.md](./PRIVATE_FUND_GAP_BACKLOG.md)。
- 如果你想看后续执行计划与当前推进状态，请看 [TRADINGAGENTS_CN_OPTIMIZATION_PLAN.md](./TRADINGAGENTS_CN_OPTIMIZATION_PLAN.md)。

## 文档分工

- 本文件只回答“外部项目哪些能力值得借、为什么值得借、当前怎么吸收”。
- 当前实际判断规则仍以 [STOCK_ANALYSIS_LOGIC.md](./STOCK_ANALYSIS_LOGIC.md) 为准。
- 当前系统模块和数据流仍以 [CODE_ARCHITECTURE.md](./CODE_ARCHITECTURE.md) 为准。
- 当前机构化缺陷仍以 [PRIVATE_FUND_GAP_BACKLOG.md](./PRIVATE_FUND_GAP_BACKLOG.md) 为准。

## 项目定位差异

### 当前项目定位

- 当前 `Quant-Analysis` 仍然以“统一评分投研工具”作为主线。
- 核心价值在于：
  - 单股分析唯一事实来源
  - 候选筛选与组合选股统一口径
  - 回测、策略优化、自动任务与 docs 体系持续沉淀
- 近期不做整站技术栈迁移，也不把多智能体编排当成主目标。

### TradingAgents-CN 的主要特点

- 更强调多智能体编排和研究流程组织。
- 明显更平台化：
  - `tradingagents/graph/setup.py`
  - `tradingagents/graph/trading_graph.py`
  - `web/utils/analysis_runner.py`
  - `web/utils/smart_session_manager.py`
  - `web/components/async_progress_display.py`
  - `web/modules/cache_management.py`
- 更像“AI 研究平台 + 分析工作台”，而不是单纯的统一评分系统。

### 当前总判断

- 不建议整体替换当前项目。
- 建议把 `TradingAgents-CN` 当作“能力模块来源库”：
  - 借工作流
  - 借任务恢复
  - 借页面模块化
  - 不直接照搬它的多智能体主干和完整技术栈

## 高优先级借鉴

### 0. 数据来源治理与统一降级

- 借鉴来源：
  - `tradingagents/dataflows/data_source_manager.py`
  - `tradingagents/dataflows/stock_data_service.py`
  - `tradingagents/dataflows/providers/china/akshare.py`
  - `tradingagents/dataflows/providers/china/tushare.py`
- 借鉴点：
  - 把“主数据源、缓存策略、降级路径”显式化
  - 不再只在代码内部隐式回退，而是把来源治理当成投研底座的一部分
- 为什么适合当前项目：
  - 当前项目已经有缓存和部分回退，但来源路径对页面和 docs 仍然不够透明
  - 这正好能补“工具可解释性”和“工程治理”的中间层
- 当前落地：
  - 单股分析新增：
    - `data_source_summary`
    - `news_summary`
  - 页面新增：
    - 数据来源与降级路径
    - 统一数据来源矩阵
    - 实时新闻摘要
  - 基本面层新增：
    - `AKShare 主源 + Tushare 备源`
- 当前状态：`第二阶段已接入`

### 1. 研究流程层

- 借鉴来源：
  - `tradingagents/graph/setup.py`
  - `tradingagents/graph/trading_graph.py`
  - `tradingagents/agents/utils/agent_states.py`
- 借鉴点：
  - 用“角色分工”思路来组织研究输出
  - 让结论之外还有正方、反方、风险、跟踪和失效条件
- 为什么适合当前项目：
  - 正好对应当前 backlog 里的 `P1-1 研究链条不完整`
  - 不需要真的引入 `LangGraph`，只要先把结构化输出落到单股分析里，就能明显提升机构表达方式
- 当前落地：
  - 单股分析新增：
    - `research_workflow_summary`
    - `risk_committee_summary`
  - 单股页新增：
    - 投资逻辑
    - 看多依据
    - 看空 / 反方依据
    - 跟踪指标
    - 失效条件
    - 风险委员会摘要
- 当前状态：`第一阶段已接入`

### 2. 风险裁决层

- 借鉴来源：
  - `tradingagents/graph/signal_processing.py`
  - `tradingagents/agents/utils/agent_states.py`
- 借鉴点：
  - 不只给“推荐关注 / 中性观察 / 暂不推荐”
  - 还要给“为什么这笔判断脆弱、哪个风险维度最先出问题”
- 为什么适合当前项目：
  - 正好对应当前 backlog 的 `P0-4 组合风险管理弱` 和 `P1-4 风险先行表达不足`
  - 先做规则化风险委员会摘要，能在不引入复杂 agent debate 的前提下提升专业感
- 当前落地：
  - 已新增：
    - 趋势风险
    - 基本面风险
    - 事件风险
    - 行业 / 组合风险
    - 最终风险等级
- 当前状态：`第一阶段已接入`

### 2.1 股票评估框架重组

- 借鉴来源：
  - `tradingagents/agents/analysts/fundamentals_analyst.py`
  - `tradingagents/agents/analysts/market_analyst.py`
  - `tradingagents/agents/analysts/news_analyst.py`
  - `tradingagents/graph/signal_processing.py`
- 借鉴点：
  - 用 analyst 分维思路组织“技术、基本面、情绪、事件、行业、风险”六维判断
  - 让最终结论之前先看到每个维度各自扮演什么角色
- 为什么适合当前项目：
  - 当前项目已经有这些评分，但表达层仍然偏散
  - 这一步不改推荐内核，只重组表达和解释方式，收益高、风险低
- 当前落地：
  - 单股分析新增：
    - `evaluation_framework_summary`
  - 页面新增：
    - 股票评估框架摘要
- 当前状态：`第二阶段已接入`

### 2.1.1 行业景气与板块热度

- 借鉴来源：
  - `TradingAgents-CN` 的行业 / 市场 analyst 分维思路
- 借鉴点：
  - 行业横向比较不能只看财务快照，还要补景气与短期热度
- 为什么适合当前项目：
  - 正好补你现有行业比较里“财务有了、景气没有”的缺口
- 当前落地：
  - 新增 `industry_heat` 子模块
  - 已进入行业横向总分汇总
- 当前状态：`第三阶段已接入`

### 2.2 交易判定层

- 借鉴来源：
  - `tradingagents/agents/managers/research_manager.py`
  - `tradingagents/agents/trader/trader.py`
  - `tradingagents/graph/signal_processing.py`
  - `tradingagents/agents/managers/risk_manager.py`
- 借鉴点：
  - 把研究结论进一步压成交易动作、目标价情景、执行置信度和执行风险
  - 让风险层真正能修改执行动作，而不只是补一句风险提示
- 为什么适合当前项目：
  - 不依赖多 agent，也能明显提升“研究结论 -> 执行计划”的闭环质量
  - 特别适合当前只有单模型、但已经有统一评分主链的阶段
- 当前落地：
  - 单股分析新增：
    - `target_price_scenarios`
    - `execution_plan_summary`
  - 页面新增：
    - 目标价情景
    - 执行计划摘要
  - 候选与组合新增：
    - `execution_confidence`
    - `execution_risk_score`
    - `risk_adjusted_action`
    - `target_price_range`
- 当前状态：`第三阶段已接入`

### 3. 异步任务与会话恢复

- 借鉴来源：
  - `web/utils/analysis_runner.py`
  - `web/utils/smart_session_manager.py`
  - `web/components/async_progress_display.py`
- 借鉴点：
  - 长任务不再强依赖当前页面等待
  - 页面刷新后仍能恢复最近任务和结果摘要
- 为什么适合当前项目：
  - 当前 Render 环境下，长任务和页面刷新很容易打断使用体验
  - 先做 SQLite / `app_settings` + 线程池的轻量版本，就能解决第一阶段痛点
- 当前落地：
  - 新增后台任务中心：
    - 量价代理候选扫描
    - 中线候选扫描
    - 当前股票池行情同步
    - 日常数据更新
    - 行业归属刷新
    - 参数优化
    - 每周自动优化
  - 已支持：
    - 最近任务 ID
    - 参数摘要
    - 结果摘要
    - 页面内恢复最近结果
- 当前状态：`第一阶段已接入`

### 4. 页面模块化拆分

- 借鉴来源：
  - `web/components/*`
  - `web/modules/*`
- 借鉴点：
  - 把大页面按职责切成分析展示、任务工作流、文档管理等模块
- 为什么适合当前项目：
  - 当前 `web/app.py` 过大，已经影响维护效率
  - 先做轻量拆分，不改 UI 语义，是成本最低的第一步
- 当前落地：
  - 新增：
    - [analysis.py](../../web/panels/analysis.py)
    - [workflow.py](../../web/panels/workflow.py)
    - [docs_admin.py](../../web/panels/docs_admin.py)
  - `web/app.py` 已开始把研究展示、任务中心、Docs 看板分流到新模块
- 当前状态：`第一阶段已接入`

## 条件性借鉴

### 1. 配置中心

- 借鉴来源：
  - `tradingagents/config/config_manager.py`
- 可借之处：
  - 把模型、数据源、系统配置做集中治理
- 当前不直接照搬的原因：
  - 本项目近期重点不是多模型平台化
  - 现在更适合保留轻量配置入口，不要引入过重的 provider 管理层
- 当前建议：
  - 先统一“数据源与任务配置”入口
  - 暂不引入完整模型供应商管理

### 2. 报告导出

- 借鉴来源：
  - TradingAgents-CN 的结构化研究输出与报告组织方式
- 可借之处：
  - 结构化研究结果天然适合导出 Markdown / PDF
- 当前不直接照搬的原因：
  - 当前先把研究链条和风险裁决补齐更重要
- 当前建议：
  - 等 `research_workflow_summary` 与 `risk_committee_summary` 稳定后再做导出

### 3. 多级缓存

- 借鉴来源：
  - `web/modules/cache_management.py`
- 可借之处：
  - 分层缓存与缓存治理视图
- 当前不直接照搬的原因：
  - 本项目现阶段 `文件缓存 + SQLite 设置表` 已足够
  - 只有在 Render 并发或多用户成为明显瓶颈时，才值得继续上 Redis

## 暂不建议照搬

### 1. 完整多智能体主干

- 不建议直接迁移：
  - `TradingAgentsGraph`
  - `StateGraph`
  - bull / bear / risk debate 全链条
- 原因：
  - 当前项目的核心竞争力还是统一评分主链
  - 直接引入多智能体编排会明显抬高系统复杂度与维护成本

### 2. FastAPI + Vue + MongoDB + Redis 整体平台

- 不建议当前阶段整站替换。
- 原因：
  - 当前目标是增强现有系统，不是重建一套新平台
  - 当前 Streamlit + SQLite + docs 体系还能继续承载下一阶段改造

### 3. 多 LLM 供应商配置中心

- 不建议作为当前主改造方向。
- 原因：
  - 现在更重要的是研究流程、任务恢复和页面结构
  - 多模型治理的收益还排不到当前最前面

## 对当前 backlog 的映射

- `P1-1 研究框架偏输出结论`：
  - 已通过 `research_workflow_summary` 开始补“投资逻辑 - 反方证据 - 跟踪 - 失效条件”
- `P1-3 单股页决策层级不够机构化`：
  - 已通过结构化研究区块和风险委员会摘要开始修补
- `P1-2 页面结构更像工具集合`：
  - 已开始模块化拆分，但真正的投研工作台首页仍未开始
- `P0-4 组合风险管理明显弱于选股能力`：
  - 当前只补到了单票风险委员会摘要，组合层约束仍未开始
- `P1-5 数据来源与评估框架可解释性不足`：
  - 已通过 `data_source_summary` 和 `evaluation_framework_summary` 开始修补

## 分阶段改造路线

### 阶段 1：轻量吸收，保持现项目主干

- 已完成：
  - 研究流程摘要
  - 风险委员会摘要
  - 后台任务与结果恢复
  - 页面模块初步拆分
- 仍未完成：
  - 组合层风险约束
  - 真正的投研工作台首页
  - 候选池变化追踪

### 阶段 2：补齐组合层与跟踪层

- 目标：
  - 组合风险约束
  - 候选变化对比
  - 待复核清单
  - 研究状态流转

### 阶段 3：再考虑更重的工作流能力

- 条件：
  - 当前研究链条和任务恢复先稳定
  - 多用户、并发、缓存治理确实成为主要瓶颈
- 到那时再评估：
  - 更强的配置中心
  - 更重的缓存体系
  - 更复杂的 agent workflow
