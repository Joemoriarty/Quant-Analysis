# 股票分析逻辑跟踪

这份文档用于记录系统当前如何分析股票。以后只要修改会影响 `recommendation` 的规则，就必须同步更新这里。

## 版本历史

### v0.1.0
- 初始状态：系统主要依据技术面和量价代理信号做结论。

### v0.1.1
- 扩展数据库与依赖，但未改变股票结论。

### v0.1.2
- 新增基本面抓取与存储链路，但尚未进入最终结论。

### v0.2.0
- 基本面正式进入单股分析，用于对技术结论做加减分校正。

### v0.3.0
- 市场情绪正式进入最终结论，用于做顺风/逆风环境校正。

### v0.3.1
- 补齐缺失值容错，`NaN` 不再直接参与整数转换；自选股仓位字段和市场情绪快照缺失时按安全默认值处理。

### v0.4.0
- 主力建仓候选和中线候选改为复用单股最终判断链路，不再单独使用旧的纯技术面启发式排序。
- 新增“分析方案”页面，用于展示当前筛选方案和价值判断规则。

### v0.4.1
- 组合选股入口改为复用统一判断口径。
- 在“分析方案”页明确展示组合评分公式、权重和筛选阈值。

### v0.5.0
- 统一评分参数改为页面可调。
- 组合选股、主力建仓候选、中线候选、策略进化改为共用同一份评分配置。

### v0.6.0
- 行业横向比较进入统一评分口径。
- 行业质量比较、行业估值分位、行业增长性比较汇总为 `industry_comparison_score`。
- `industry_comparison_score` 进入单股最终推荐、候选筛选、组合选股和策略进化。

### v0.7.1
- 行业归属改为“主表 + 历史表 + 回退链”正式结构。
- 行业内估值分位支持 `PE/PB` 直接抓取和回退估算。
- 行业内增长性比较支持 `营收同比 / 净利润同比`。
- 单股页和分析方案页新增行业横向评分拆解展示。

### v0.8.0
- 事件驱动正式进入统一评分口径。
- 当前事件面使用 `公告 + 业绩预告 + 财报预约披露` 生成 `event_score / event_state`。
- `event_score` 进入单股最终推荐、候选筛选、组合选股和策略进化。
- 单股页和分析方案页新增事件驱动摘要与事件权重展示。

## 当前版本

### v0.8.0

状态：当前线上逻辑

## 当前最终判断链路

单股最终结论现在由 5 层组成：

1. 技术面
2. 基本面
3. 市场情绪
4. 事件驱动
5. 行业横向比较

最终页面上应能看到这些核心输出：

1. `technical_recommendation`
2. `fundamental_score`
3. `market_sentiment_state`
4. `event_score`
5. `industry_comparison_score`
6. `recommendation`
7. `final_decision_basis`

## 当前候选筛选规则

现在以下模块已经和单股判断保持一致：

1. 单股分析
2. 自选股分析
3. 主力建仓候选
4. 中线候选
5. 组合选股
6. 策略进化

统一方式是：

1. 先跑单股分析，得到 `recommendation`
2. 再读取 `trend_score`
3. 再读取 `fundamental_score`
4. 再读取 `market_sentiment_state`
5. 再读取 `event_score`
6. 再读取 `industry_comparison_score`
7. 再读取 `accumulation_score`

也就是说，候选池不再额外维护一套和单股页不同的判断口径。

## 技术面依据

技术面负责给出买卖时机的第一层判断，当前使用：

1. `MA20 / MA60`
2. `RSI14`
3. `MACD`
4. `20日涨跌幅`
5. `10日量比`
6. `20日支撑位 / 压力位`
7. `ATR14 / 当日振幅`

技术面会先生成：

1. `trend_score`
2. `technical_recommendation`
3. `sell_guidance`
4. `sell_plan`
5. `add_position_guidance`

## 基本面依据

基本面当前用于校正技术面结论，使用的字段有：

1. `ROE`
2. `资产负债率`
3. `经营现金流`
4. `归母净利润`
5. `营业总收入`
6. `总市值`
7. `PE`
8. `PB`
9. `营收同比`
10. `净利润同比`

基本面会生成：

1. `fundamental_score`
2. `fundamental_explanations`
3. `fundamental_risks`
4. `fundamental_summary`

基本面不会单独定义买点，但会影响最终推荐级别。

## 市场情绪依据

市场情绪当前用于判断是顺风环境还是逆风环境，使用：

1. `上涨家数`
2. `下跌家数`
3. `涨停家数`
4. `跌停家数`
5. `市场广度`
6. `market_sentiment_score`
7. `market_sentiment_state`

市场情绪会生成：

1. `market_sentiment_summary`
2. `market_sentiment_explanations`
3. `market_sentiment_risks`

市场情绪不直接替代个股技术面和基本面，只做最终结论校正。
如果当前拿不到情绪快照，则按 `中性` 环境处理，不做额外升降档。

## 当前最终结论规则

### 基础结论

先由技术面和基本面形成基础结论：

1. 技术面强，且基本面不差：可得 `推荐关注`
2. 技术面强，但基本面明显弱：下调为 `中性观察`
3. 技术面中性，但基本面明显强：可上调为 `推荐关注`
4. 技术面弱，但基本面较强：通常保留为 `中性观察`
5. 技术面弱，基本面也不强：`暂不推荐`

### 市场情绪校正

在基础结论之上，再做环境校正：

1. 如果基础结论是 `推荐关注`，但市场情绪为 `偏弱`，则下调一档到 `中性观察`
2. 如果基础结论是 `中性观察`，且市场情绪为 `偏弱`，则不强制下调，但会在依据中提示逆风环境
3. 如果基础结论是 `中性观察`，且原始技术结论为 `推荐关注`，同时市场情绪为 `偏强`，则允许上调到 `推荐关注`
4. 如果基础结论是 `暂不推荐`，但基本面较强、趋势不差、且市场情绪偏强，则允许上调到 `中性观察`

## 当前候选池如何排序

### 主力建仓候选

筛选条件：

1. 单股最终结论不能是 `暂不推荐`
2. `量价吸筹评分` 需要达到候选阈值

排序依据：

1. 最终结论优先级
2. 综合评分
3. 量价吸筹评分
4. 趋势评分

### 中线候选

筛选条件：

1. 单股最终结论不能是 `暂不推荐`
2. 潜力评分需要达到中线候选阈值

潜力评分当前由以下维度组成：

1. 趋势评分
2. 基本面评分
3. 量价吸筹评分
4. 市场情绪得分
5. 20日动量

## 当前组合选股评分算法

组合入口现在不再使用旧的独立因子组合作为主选股口径，而是直接复用统一判断链路。

当前组合综合分公式：

`组合综合分 = 趋势评分*0.35 + 基本面评分*0.30 + 量价吸筹评分*0.20 + 市场情绪得分*0.15 + 结论加减项`

其中结论加减项为：

1. `推荐关注`：`+8`
2. `中性观察`：`+0`
3. `暂不推荐`：`-12`

当前组合入口筛选阈值：

1. 最低推荐级别：`中性观察`
2. 最低趋势评分：`55`
3. 最低基本面评分：`45`

也就是说，组合入口、单股分析、自选股分析、主力建仓候选和中线候选，现在都建立在同一套判断口径之上。

## 当前动态调参机制

“分析方案”页现在可以动态调整以下内容：

1. 趋势权重
2. 基本面权重
3. 量价吸筹权重
4. 市场情绪权重
5. 最低推荐级别
6. 最低趋势评分
7. 最低基本面评分
8. 主力建仓候选最低吸筹评分
9. 中线候选最低潜力评分
10. 不同最终结论的加减分

这些参数当前会同时作用于：

1. 组合选股入口
2. 主力建仓候选
3. 中线候选
4. 分析方案页展示公式
5. 策略进化中的统一评分优化入口

这些参数会保存到系统设置中，因此刷新页面后仍然会继续生效。

## 这次改动的真实影响

从 v0.3.0 开始，市场情绪已经真正参与 `recommendation`。

因此现在可能出现以下变化：

1. `推荐关注 -> 中性观察`
2. `中性观察 -> 推荐关注`
3. `暂不推荐 -> 中性观察`

## 当前还没有纳入最终结论的维度

1. 公司公告与事件驱动
2. 行业比较
3. 宏观经济过滤
4. 真实主力资金流
5. 独立的估值高低评分体系

## 校验要求

以后每次修改任何会影响 `recommendation` 的规则，都必须补充：

1. 这次修改了什么规则
2. 新增了哪些分析依据
3. 哪些情况下会改变最终结论
4. 页面上应该如何核对

## 补充说明

### v0.3.1 缺失值回退

以下字段如果为空、缺失或为 `NaN`，当前会自动回退到安全默认值，而不是抛出异常：

1. 自选股里的 `shares`
2. 自选股里的 `cost_price`
3. 自选股里的 `target_weight`
4. 市场情绪快照里的计数字段和得分字段

这个改动不会改变正常数据下的判断逻辑，只是避免因为脏数据导致分析失败。
## 2026-04-04 - Comparison Plugin Update

### v0.6.0

- Added a plugin-style comparison layer on top of the unified stock analysis result.
- The first enabled comparison type is `industry_peers`.
- Comparison results are returned through:
  - `comparison_results`
  - `comparison_overview`

## 2026-04-04 - v0.8.1 当前有效补充说明

以下内容为当前真实生效的最新逻辑，用于覆盖上方历史乱码段落。

### 1. 中线候选池当前使用的统一算法

中线候选池不再使用独立旧策略，当前完全复用统一评分口径：

1. 先跑单股分析，得到 `recommendation`
2. 再读取 `trend_score`
3. 再读取 `fundamental_score`
4. 再读取 `market_sentiment_score`
5. 再读取 `event_score`
6. 再读取 `industry_comparison_score`
7. 最后叠加 `20日动量` 形成 `潜力评分`

### 2. 当前默认阈值

当前默认阈值调整为：

- `min_recommendation = 中性观察`
- `min_trend_score = 55`
- `min_fundamental_score = 38`
- `min_accumulation_score = 50`
- `min_growth_score = 48`
- `min_industry_score = 30`
- `min_event_score = 0`

### 3. 这次为什么调整

之前中线候选池只有 6 只，排查后确认不是程序异常，而是统一口径下的默认过滤偏严，主要是：

1. 基本面阈值会过滤掉一批趋势尚可但财务分略低的股票
2. 行业横向阈值会进一步压缩样本
3. 事件分做硬过滤会和最终推荐形成重复约束

因此这次把事件面默认改为“以加减分为主，不做默认一票否决”，同时适度放宽基本面、行业和潜力阈值。

### 4. 当前结论

当前中线候选池仍然属于偏审慎口径，不是越多越好。默认口径下数量偏少，通常意味着：

1. 统一评分体系更重视基本面、行业位置和事件风险
2. 候选池更像“中线跟踪池”，不是“宽松观察池”
3. 如果需要扩大覆盖范围，应优先在“分析方案”页动态调整阈值，而不是单独改中线模块逻辑

### Current rule

- Comparison plugins are part of the unified analysis output.
- Comparison plugins currently do **not** change:
  - `trend_score`
  - `fundamental_score`
  - `market_sentiment_score`
  - `recommendation`
- Comparison plugins currently only enhance explanation and cross-checking.

### First enabled comparison type

- `industry_peers`
  - compares the stock against same-industry peers
  - shows relative position of:
    - `ROE`
    - `debt_ratio`
    - `net_profit`
    - `market_value`

### Not added yet

- valuation percentile comparison
- growth comparison
- comparison-driven score adjustment
- portfolio-level comparison plugins

## 2026-04-04 - Industry Membership Fallback Update

### v0.6.1

- 行业归属不再只依赖实时接口
- 当前单股分析新增 `industry_membership`
- 行业归属当前使用回退链：
  1. `industry_membership`
  2. `valuation_snapshots.industry`
  3. 实时 `stock_individual_info_em`

### 当前规则

- 行业归属如果存在且未过期：
  - 用于补全 `valuation_snapshot["industry"]`
  - 用于驱动 `industry_peers` 插件
- 行业归属如果不存在或已过期：
  - 不会修改最终推荐
  - 行业对比插件可能返回不可用

### 当前影响范围

- 影响：
  - `comparison_results`
  - `industry_membership`
  - 行业横向比较解释结果
- 不影响：
  - `trend_score`
  - `fundamental_score`
  - `market_sentiment_state`
  - `recommendation`

## 2026-04-04 - Industry Membership Batch Refresh Update

### v0.6.2

- 新增行业归属批量刷新任务 `industry-membership-refresh`
- 任务会批量调用 `resolve_industry_membership`，并记录：
  1. `from_membership`
  2. `from_valuation`
  3. `from_live`
  4. `stale`
  5. `missing`
  6. `failed`
- 刷新结果会保存到：
  - `last_industry_membership_refresh`
  - `automation_runs`

### 当前影响范围

- 影响：
  - 行业归属主表的覆盖率
  - `comparison_results` 的可用率
  - 页面中的行业归属刷新状态展示
- 不影响：
  - `trend_score`
  - `fundamental_score`
  - `market_sentiment_state`
  - `recommendation`

### 当前规则

- 批量刷新属于数据治理任务，不直接参与最终推荐加减分
- 如果刷新过程中上游接口失败：
  - 任务会保留已有主表沉淀
  - 缺失和失败会被显式记录
  - 页面会展示降级结果，而不是假装刷新成功

## 2026-04-04 - Second Industry Data Source Update

### v0.6.3

- 行业归属新增第二数据源：`ak.stock_industry_change_cninfo`
- 当前行业归属回退链更新为：
  1. `industry_membership`
  2. `valuation_snapshots.industry`
  3. `ak.stock_industry_change_cninfo`
  4. 实时 `ak.stock_individual_info_em`
- 批量刷新结果新增：
  - `from_cninfo`

### 当前影响范围

- 影响：
  - 行业归属解析成功率
  - 行业归属批量刷新统计
  - `comparison_results` 的行业字段可用率
- 不影响：
  - `trend_score`
  - `fundamental_score`
  - `market_sentiment_state`
  - `recommendation`

### 当前规则

- `CNInfo` 只用于补行业归属，不直接改变最终推荐
- 当主表和估值快照都没有行业字段时，才尝试 `CNInfo`
- 当 `CNInfo` 也失败时，才继续尝试东方财富实时接口
## 2026-04-04 - Industry Comparison Added To Final Scoring

### v0.7.0

- 新增五维统一评分：
  - `trend_score`
  - `fundamental_score`
  - `accumulation_score`
  - `market_sentiment_score`
  - `industry_comparison_score`
- 当前统一公式：
  - `portfolio_score = trend*0.30 + fundamental*0.25 + accumulation*0.18 + sentiment*0.12 + industry*0.15 + recommendation_bonus`
  - 页面可动态调整这五个权重和阈值
- 行业横向分来源：
  - 比较插件 `industry_peers`
  - 同行样本中使用 `ROE / 资产负债率 / 净利润 / 营收 / 市值 / PE / PB`
- 行业横向分当前如何影响最终结论：
  - `industry_score >= 75` 时，如果原结论是“中性观察”且趋势、基本面不弱，可上调为“推荐关注”
  - `industry_score >= 75` 时，如果原结论是“暂不推荐”但基本面和趋势已接近观察门槛，可上调为“中性观察”
  - `industry_score <= 30` 时，如果原结论是“推荐关注”且并非极强趋势+极强基本面，会下调为“中性观察”
  - `industry_score <= 30` 时，如果原结论是“中性观察”且基本面仍偏弱，会下调为“暂不推荐”
- 行业横向分当前如何影响候选池和组合：
  - 吸筹候选总分纳入 `industry_score`
  - 中线候选潜力分纳入 `industry_score`
  - 组合选股总分纳入 `industry_score`
  - 统一阈值新增 `min_industry_score`

### 当前影响范围

- 影响：
  - `recommendation`
  - `final_decision_basis`
  - `industry_comparison_score`
  - `portfolio_score`
  - `growth_score`
- 不影响：
  - `trend_score` 的计算方式
  - `fundamental_score` 的计算方式
  - `market_sentiment_score` 的计算方式

### 当前仍未接入

- 行业估值分位单独插件
- 行业成长性单独插件
- 事件驱动分数
- 组合层行业暴露约束
## 2026-04-04 - 行业横向比较收口

### v0.7.1

- 行业横向分不再只来自一个插件，当前改为三类子评分汇总：
  - `industry_peers`：行业质量比较
  - `industry_valuation`：行业估值分位
  - `industry_growth`：行业增长性比较
- 当前行业横向汇总权重：
  - 行业质量比较 `0.45`
  - 行业估值分位 `0.25`
  - 行业增长性比较 `0.30`
- 当前行业横向分如何生成：
  - 先拿同行样本
  - 再分别计算质量分、估值分、增长分
  - 最后汇总成 `industry_comparison_score`
- 同行样本当前如何沉淀：
  - 当前主表：`industry_membership`
  - 历史快照：`industry_membership_history`
  - 读取顺序优先使用主表，不足时会回退到历史快照和补抓逻辑
- 当前估值分位口径：
  - 优先使用现成 `PE / PB`
  - 如果实时估值接口不可用，则回退为：
    - `PE = 最新收盘价 / 基本每股收益`
    - `PB = 最新收盘价 / 每股净资产`
- 当前增长性比较口径：
  - `营收同比`
  - `净利润同比`
  - 若接口未直接给同比，则用同报告期上一年数据回推增长率

### 当前影响范围

- 影响：
  - `industry_comparison_score`
  - `recommendation`
  - `final_decision_basis`
  - `portfolio_score`
  - `growth_score`
- 不影响：
  - `trend_score` 原始计算方法
  - `fundamental_score` 原始计算方法
  - `market_sentiment_score` 原始计算方法
