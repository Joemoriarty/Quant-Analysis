# 股票分析系统专业化修补追踪表

这份文档用于持续记录系统从“多维选股工具”向“更专业的投研分析系统”演进的状态。

规则：
- 每次增加或修改会影响研究结论、候选筛选、组合选股、风险判断、事件解释、市场环境判断的功能时，必须同步更新本文件。
- 每次更新至少要写清楚：
  - 本次新增了什么
  - 本次没有覆盖什么
  - 哪些模块已经统一
  - 哪些模块仍然缺失
- 如果本次改动影响股票最终判断逻辑，还必须同步更新 [STOCK_ANALYSIS_LOGIC.md](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/docs/STOCK_ANALYSIS_LOGIC.md)。
- 如果本次改动影响模块职责、数据流、插件结构、UI入口或存储结构，还必须同步更新 [CODE_ARCHITECTURE.md](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/docs/CODE_ARCHITECTURE.md)。

## 状态说明

- `未开始`：还没有设计或落地
- `进行中`：已经有部分实现，但还不完整
- `已接入`：已经进入主流程，但还不够完善
- `已统一`：已经接入主流程，且与统一评分口径保持一致

## 修补顺序

### 当前进展总览

1. 研究结论可解释性：`已接入`
2. 价值判断深度：`进行中`
3. 行业横向比较：`已统一`
4. 事件驱动分析：`已接入`
5. 市场情绪稳定性：`进行中`
6. 组合风险管理：`未开始`
7. 回测评价体系：`进行中`
8. 因子归因与敏感性分析：`未开始`
9. 跟踪提醒与状态变化：`未开始`
10. 产品工作流整合：`进行中`

### 行业横向比较专项进展

- `行业成分长期缓存与本地沉淀`：`已统一`
- `行业内估值分位`：`已统一`
- `行业内增长性比较`：`已统一`
- `比较结果进入最终评分`：`已统一`
- 说明：
  - 当前行业横向分由 `行业质量比较 + 行业估值分位 + 行业增长性比较` 汇总得到
  - 汇总后的 `industry_comparison_score` 已进入单股结论、候选筛选、组合选股和策略进化

建议严格按这个顺序推进，不要同时散改太多方向：

1. 研究结论可解释性
2. 价值判断深度
3. 行业横向比较
4. 事件驱动分析
5. 市场情绪稳定性
6. 组合风险管理
7. 回测评价体系
8. 因子归因与敏感性分析
9. 跟踪提醒与状态变化
10. 产品工作流整合

---

## 1. 研究结论可解释性

- 状态：`已接入`
- 当前已增加：
  - 单股分析已输出 `technical_recommendation`
  - 单股分析已输出 `fundamental_score`
  - 单股分析已输出 `market_sentiment_state`
  - 单股分析已输出 `industry_comparison_score`
  - 单股分析已输出 `recommendation`
  - 页面已增加“分析方案”页
  - 页面可展示统一评分公式、阈值、权重、加减项
  - 单股页已展示行业横向评分摘要和子模块评分
- 当前还未增加：
  - 单票级因子贡献明细
  - “为什么是 82 分不是 74 分”的逐项拆解
  - 结论失效条件的结构化展示
- 主要文件：
  - [portfolio/single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)
  - [web/app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)
  - [docs/STOCK_ANALYSIS_LOGIC.md](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/docs/STOCK_ANALYSIS_LOGIC.md)

## 2. 价值判断深度

- 状态：`进行中`
- 当前已增加：
  - 已接入基础财务字段抓取和落库
  - 已在单股分析中接入基础基本面评分
  - 当前价值判断已经使用：
    - `ROE`
    - `营业总收入`
    - `净利润`
    - `资产负债率`
    - `经营现金流`
    - `总市值`
  - 已支持估值字段抓取与回退估算：
    - `PE`
    - `PB`
  - 已支持增长字段抓取：
    - `营收同比`
    - `净利润同比`
  - 已支持行业内估值分位和增长性比较，但这部分当前通过行业横向比较进入统一总分
- 当前还未增加：
  - 历史估值分位
  - 增长与估值匹配分析
  - 自由现金流质量
  - 利润稳定性与连续性评分
- 主要文件：
  - [data/fundamental_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/fundamental_loader.py)
  - [db/market_db.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/db/market_db.py)
  - [portfolio/single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)

## 3. 行业横向比较

- 状态：`已统一`
- 当前已增加：
  - 已新增插件化比较类型注册机制
  - 已新增行业归属主表：`industry_membership`
  - 已新增行业归属历史表：`industry_membership_history`
  - 已新增行业归属批量刷新与回退链
  - 已新增行业质量比较插件：`industry_peers`
  - 已新增行业估值分位插件：`industry_valuation`
  - 已新增行业增长性比较插件：`industry_growth`
  - 单股分析页已可展示行业同行对比结果和子模块评分
  - 行业横向总分已进入最终推荐、候选筛选、组合选股和策略进化
- 当前还未增加：
  - 行业景气度比较
  - 行业事件驱动比较
  - 组合层行业暴露约束
- 主要文件：
  - [portfolio/comparison_plugins.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/comparison_plugins.py)
  - [data/fundamental_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/fundamental_loader.py)
  - [portfolio/single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)
  - [db/market_db.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/db/market_db.py)

## 4. 事件驱动分析

- 状态：`已接入`
- 当前已增加：
  - 事件表结构已经正式启用
  - 已新增事件加载器，接入公告、业绩预告、财报预约披露
  - 已新增 `company_events` 落库与近期事件读取
  - 已在单股分析中接入 `event_score / event_state / event_summary`
  - 已在单股页展示事件驱动摘要
  - 已在统一评分中接入事件权重和事件阈值
  - 已在候选筛选、组合选股、策略进化中统一使用事件分
- 当前还未增加：
  - 事件重要性分级
  - 事件变化提醒
  - 更完整的公告分类覆盖
  - 事件与行业景气度联动分析
- 主要文件：
  - [data/events_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/events_loader.py)
  - [db/market_db.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/db/market_db.py)
  - [portfolio/single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)
  - [strategies/unified_selection.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/strategies/unified_selection.py)

## 5. 市场情绪稳定性

- 状态：`进行中`
- 当前已增加：
  - 市场情绪评分结构已进入统一评分口径
  - 抓取失败时可回退到中性状态
  - 情绪分已进入单股最终推荐、候选筛选、组合选股和策略进化
- 当前还未增加：
  - 稳定可靠的情绪快照来源
  - 历史情绪快照的持续积累
  - 情绪状态切换提醒
- 风险说明：
  - 当前情绪模块在数据抓取失败时会回退为中性，这意味着结构有了，但可靠性还不够强
- 主要文件：
  - [data/sentiment_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/sentiment_loader.py)
  - [portfolio/single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)

## 6. 组合风险管理

- 状态：`未开始`
- 当前已增加：
  - 统一组合评分入口
  - 行业横向分已可进入组合选股总分
- 当前还未增加：
  - 行业暴露分析
  - 风格暴露分析
  - 集中度限制
  - 组合相关性分析
  - 持仓级风险预警
- 主要文件：
  - [strategies/unified_selection.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/strategies/unified_selection.py)

## 7. 回测评价体系

- 状态：`进行中`
- 当前已增加：
  - 收益、回撤、正收益占比等基础指标
  - 统一评分配置已进入策略优化流程
  - 行业横向分已进入策略优化搜索空间
- 当前还未增加：
  - 基准对比
  - 超额收益
  - 夏普
  - 卡玛
  - 成本和换手影响
  - 分市场阶段统计
- 主要文件：
  - [portfolio/strategy_optimizer.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/strategy_optimizer.py)
  - [db/market_db.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/db/market_db.py)

## 8. 因子归因与敏感性分析

- 状态：`未开始`
- 当前已增加：
  - 页面已展示统一评分公式
- 当前还未增加：
  - 个股级评分拆解
  - 因子贡献排名
  - 参数变化对结果的敏感性分析
  - 入选/淘汰原因明细
- 主要文件：
  - [web/app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)

## 9. 跟踪提醒与状态变化

- 状态：`未开始`
- 当前已增加：
  - 自选股分析能力
- 当前还未增加：
  - 结论升降级提醒
  - 公告/财报提醒
  - 情绪转弱提醒
  - 基本面恶化提醒
- 主要文件：
  - [portfolio/watchlist.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/watchlist.py)

## 10. 产品工作流整合

- 状态：`进行中`
- 当前已增加：
  - 单股分析、自选股分析、吸筹候选、中线候选、组合选股、策略进化已统一评分口径
  - 分析方案页已展示统一公式、阈值、行业横向拆解
  - 单股页已展示行业横向评分摘要
  - 事件驱动已进入统一评分主链，并在单股页与分析方案页展示
- 当前还未增加：
  - 真正的投研工作台首页
  - 每日研究清单
  - 风险变化看板
  - 研究到跟踪到组合的闭环导航
- 主要文件：
  - [web/app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)

---

## 变更记录说明

- 本文件只保留“当前状态、当前缺口、当前统一范围”。
- 历史迭代流水、修补记录、阶段性发布说明统一写入 [RELEASE_NOTES.md](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/docs/RELEASE_NOTES.md)。
- 后续新增功能时：
  - 先更新本文件中的当前状态区
  - 再把变更过程追加到 `RELEASE_NOTES.md`
