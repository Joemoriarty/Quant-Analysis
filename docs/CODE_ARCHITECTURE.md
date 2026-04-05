# 代码架构说明

这份文档用于记录当前股票分析系统的代码架构。

规则：
- 只要代码生成或重构影响了模块职责、数据流、插件结构、页面入口、存储结构，就必须同步更新本文件。
- 如果本次改动还影响股票判断逻辑，必须同步更新 [STOCK_ANALYSIS_LOGIC.md](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/docs/STOCK_ANALYSIS_LOGIC.md)。
- 如果本次改动还影响专业化能力覆盖范围，必须同步更新 [PROFESSIONALIZATION_TRACKER.md](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/docs/PROFESSIONALIZATION_TRACKER.md)。

## 当前架构

### 1. 数据层

- 作用：
  - 抓取行情、基本面、情绪、辅助对比数据
  - 提供带回退逻辑的 `load_or_fetch` 接口
- 主要文件：
  - [akshare_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/akshare_loader.py)
  - [fundamental_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/fundamental_loader.py)
  - [events_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/events_loader.py)
  - [sentiment_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/sentiment_loader.py)
- 当前状态：
  - 行情数据链路最完整
  - 基本面数据已进入统一分析主链
  - 事件驱动已接入公告、业绩预告、财报预约披露三类数据
  - 行业同行样本链路已建好，并支持主表、历史表和回退抓取
  - 市场情绪依赖外部接口，失败时会回退为中性

### 2. 存储层

- 作用：
  - 存储市场快照、价格历史、推荐结果、回测结果、优化结果、配置和自动任务记录
- 主要文件：
  - [market_db.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/db/market_db.py)
- 当前表结构：
  - 行情和价格历史
  - 推荐记录和回测记录
  - 优化结果和自动任务结果
  - 基本面快照
  - 估值快照
  - 公司事件
  - 市场情绪快照
  - 行业归属
  - 行业归属历史快照
  - 宏观指标快照

### 3. 统一分析层

- 作用：
  - 输出股票分析的唯一标准结果
  - 当前是所有股票判断的单一事实来源
- 主要文件：
  - [single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)
- 当前输出：
  - `technical_recommendation`
  - `fundamental_score`
  - `market_sentiment_state`
  - `event_score`
  - `industry_comparison_score`
  - `recommendation`
  - `final_decision_basis`
  - `comparison_results`

### 4. 对比插件层

- 作用：
  - 在不改主判断链的前提下，扩展横向对比类型
- 主要文件：
  - [comparison_plugins.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/comparison_plugins.py)
- 当前机制：
  - 通过注册表注册新的对比类型
  - 插件读取统一分析上下文
  - 插件结果会先汇总成 `industry_comparison_score`
  - `industry_comparison_score` 已进入最终推荐和统一评分
- 当前已启用插件：
  - `industry_peers`
  - `industry_valuation`
  - `industry_growth`

### 5. 统一评分层

- 作用：
  - 为所有在用策略入口提供一套统一评分配置
- 主要文件：
  - [scoring_config.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/scoring_config.py)
- 当前被这些模块共用：
  - 单股最终推荐
  - 单股相关筛选视图
  - 吸筹候选
  - 中线候选
  - 组合选股
  - 策略优化
  - 自动工作流
- 当前统一组件：
  - `trend`
  - `fundamental`
  - `accumulation`
  - `sentiment`
  - `event`
  - `industry`

### 6. 筛选与策略层

- 作用：
  - 复用统一分析结果，生成候选和组合
- 主要文件：
  - [candidate_screener.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/candidate_screener.py)
  - [unified_selection.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/strategies/unified_selection.py)
  - [strategy_optimizer.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/strategy_optimizer.py)
- 当前规则：
  - 所有在用策略入口都应围绕统一评分配置工作

### 7. 工作流层

- 作用：
  - 自选股分析
  - 模拟交易
  - 自动化任务
- 主要文件：
  - [watchlist.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/watchlist.py)
  - [paper_trading.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/paper_trading.py)
  - [automation_workflows.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/automation_workflows.py)

### 8. 页面层

- 作用：
  - 展示分析、筛选、自选股、优化、解释和调试视图
- 主要文件：
  - [app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)
- 当前问题：
  - 文件仍然过大
  - 页面逻辑和业务逻辑耦合偏重
  - 后续应拆成页面模块和共享组件

## 当前架构判断

### 优点

- 股票判断逻辑已经集中
- 策略入口已统一评分口径
- 对比类型已经可以插件化扩展
- 数据库存储结构已经为后续能力预留空间

### 缺点

- 页面层过于集中
- 外部数据源脆弱
- 行业横向比较结构已经有了，但样本数据稳定性不足
- 事件驱动已接入主分析，但第一版数据源覆盖面还不完整

## 行业同行样本为什么现在不可用

当前根因不是比较插件逻辑错误，而是上游数据源可用性不足：

1. `ak.stock_board_industry_name_em()` 当前环境下会报代理错误
2. `stock_individual_info_em()` 当前环境下经常拿不到 `industry`
3. 结果是：
  - 当前股票可能没有行业字段
  - 行业板块列表拿不到
  - 行业成分股列表拿不到
  - 同行样本快照自然也拿不到

所以当前问题首先是数据可得性问题，不是评分公式问题。

## 行业同行样本如何补强

建议按下面顺序做：

### 1. 本地沉淀行业归属

- 把 `symbol -> industry_name` 落到 `industry_membership`
- 不要让单股分析每次都实时去请求行业接口

### 2. 增加行业解析回退链

- 行业归属获取顺序建议为：
  1. 本地 `industry_membership`
  2. `valuation_snapshots` 中最近一次 `industry`
  3. 实时 API
  4. 全部失败则标记不可用

### 3. 增加行业同行样本缓存

- 按 `industry_name -> peer_symbols` 缓存同行股票列表
- 避免每次分析都重新拉板块成分

### 4. 增加周期刷新任务

- 单独做一个行业样本刷新任务
- 只要接口有一次成功，就把结果持久化
- 后续在接口失败时继续使用本地旧样本

### 5. 增加最小样本门槛

- 同行样本数量不足时，只给“样本不足”的提示
- 不要把小样本对比当成强结论

### 6. 增加备用数据源

- 如果一个行业接口长期不稳定，就引入第二数据源或人工映射兜底

## 每次架构变更后的记录模板

### YYYY-MM-DD - 架构更新

- 本次目标：
- 新增模块：
- 修改模块：
- 数据流变化：
- 插件结构变化：
- 页面入口变化：
- 存储结构变化：
- 当前风险：

### 2026-04-04 - 架构更新

- 本次目标：
  - 把行业归属改成主表优先、带时效控制和回退链的正式架构
- 新增模块：
  - 无新增独立模块
- 修改模块：
  - [market_db.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/db/market_db.py)
  - [fundamental_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/fundamental_loader.py)
  - [single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)
  - [comparison_plugins.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/comparison_plugins.py)
- 数据流变化：
  - 行业归属获取顺序改为：
    1. `industry_membership`
    2. `valuation_snapshots.industry`
    3. 实时 `stock_individual_info_em`
  - 行业同行样本读取改为优先查询本地 `industry_membership`
- 插件结构变化：
  - `industry_peers` 插件开始读取 `industry_membership` 上下文
- 页面入口变化：
  - 无新增页面入口
- 存储结构变化：
  - 继续复用 `industry_membership` 表作为行业归属主表
  - 新增主表读写与查询接口
- 当前风险：
  - 如果主表还没有历史沉淀，行业比较仍会不可用
  - 当前环境下实时行业接口仍然可能失败

### 2026-04-04 - 架构更新

- 本次目标：
  - 把行业归属批量刷新做成正式自动化任务，而不是只在单股分析时被动触发
- 新增模块：
  - 无新增独立文件，扩展现有自动化工作流
- 修改模块：
  - [automation_workflows.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/automation_workflows.py)
  - [app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)
- 数据流变化：
  - 自动化任务现在新增 `industry-membership-refresh`
  - 任务执行顺序为：
    1. 获取股票池
    2. 批量调用 `resolve_industry_membership`
    3. 统计主表命中、估值回填、实时补齐、过期、缺失、失败
    4. 把结果写入 `app_settings.last_industry_membership_refresh`
    5. 把运行状态写入 `automation_runs`
- 页面入口变化：
  - “策略进化与数据治理”页新增“立即刷新行业归属”按钮
  - 同页新增最近一次行业归属刷新的摘要展示和样本结果展示
- 存储结构变化：
  - 无新增表
  - 新增设置键：`last_industry_membership_refresh`
- 当前风险：
  - 批量刷新只能提升历史沉淀覆盖率，不能保证上游实时行业接口始终可用
  - 当前仍未建立独立的行业同行样本主表，同行列表仍主要依赖 `industry_membership`

### 2026-04-04 - 架构更新

- 本次目标：
  - 为行业归属回退链接入第二数据源，降低对东方财富实时接口的单点依赖
- 新增模块：
  - 无新增独立文件
- 修改模块：
  - [fundamental_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/fundamental_loader.py)
  - [automation_workflows.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/automation_workflows.py)
  - [app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)
- 数据流变化：
  - 行业归属解析顺序更新为：
    1. `industry_membership`
    2. `valuation_snapshots.industry`
    3. `ak.stock_industry_change_cninfo`
    4. 实时 `ak.stock_individual_info_em`
- 页面入口变化：
  - 行业归属刷新结果新增 `from_cninfo` 统计展示
- 存储结构变化：
  - 无新增表
  - 无新增主键
- 当前风险：
  - 第二数据源当前只解决单股票行业归属，不直接提供同行样本主表
  - 若后续要做行业同行分位，仍需继续补行业样本沉淀

### 2026-04-04 - 架构更新

- 本次目标：
  - 把行业横向比较从“仅有插件壳”补成“有同行样本、有分位、有综合分、有结论”的完整逻辑
- 新增模块：
  - 无新增独立文件
- 修改模块：
  - [market_db.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/db/market_db.py)
  - [fundamental_loader.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/data/fundamental_loader.py)
  - [comparison_plugins.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/comparison_plugins.py)
  - [app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)
- 数据流变化：
  - 行业横向比较现在优先读取本地 `industry_membership + fundamental_snapshots + valuation_snapshots` 联合结果
  - 当本地同行样本缺少可用财务快照时，会按需补抓同行基础快照
  - 当本地行业样本仍不足时，会从股票代码表中继续补行业归属，直到形成最小同行样本
- 页面入口变化：
  - 单股页的行业横向比较新增行业综合分和样本提示
- 存储结构变化：
  - 无新增表
  - 新增本地行业同行快照查询接口 `get_industry_peer_snapshots`
- 当前风险：
  - 行业比较现在已形成逻辑闭环，但仍未直接参与最终推荐加减分
  - `PE/PB/市值` 在部分股票上仍可能缺失，因此行业综合分会按可用指标加权计算
### 2026-04-04 - 架构更新

- 本次目标：
  - 把行业横向比较从解释层正式接进统一评分主链
- 修改模块：
  - [scoring_config.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/scoring_config.py)
  - [single_stock_analysis.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/single_stock_analysis.py)
  - [candidate_screener.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/candidate_screener.py)
  - [unified_selection.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/strategies/unified_selection.py)
  - [strategy_optimizer.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/portfolio/strategy_optimizer.py)
  - [app.py](/C:/Users/18197/Documents/project/股票/quant_system_pro/quant_system_pro/web/app.py)
- 数据流变化：
  - 单股分析在生成 `comparison_results` 后，会提取 `industry_comparison_score`
  - `industry_comparison_score` 进入最终推荐升降档逻辑
  - 候选筛选、组合选股、策略进化统一读取 `weights.industry` 和 `thresholds.min_industry_score`
- 页面入口变化：
  - “分析方案”页新增行业横向权重、最低行业横向分、五维公式展示
- 存储结构变化：
  - 无新增表
  - 统一配置结构新增：
    - `weights.industry`
    - `thresholds.min_industry_score`
- 当前风险：
  - 行业横向分已经进入统一评分，但仍依赖同行样本质量
  - 若行业样本不足，当前会回退为不参与阈值过滤，组合总分按 50 分中性值处理
### 2026-04-04 - 架构更新

- 本次目标：
  - 把行业横向比较剩余缺口补齐，包括历史沉淀、估值分位、增长性比较
- 新增或增强的结构：
  - `industry_membership_history`
  - `industry_peers`
  - `industry_valuation`
  - `industry_growth`
- 数据流变化：
  - 行业归属现在同时写入当前主表和历史快照表
  - 同行样本读取顺序变为：
    1. `industry_membership`
    2. `industry_membership_history`
    3. 板块成分接口
    4. 股票代码表补抓
  - 行业横向总分现在是三类插件分数的加权汇总
- 评分链变化：
  - 单股分析先生成 `comparison_results`
  - 再汇总得到 `industry_comparison_score`
  - 最后把该分数接入最终推荐和统一评分
- 当前风险：
  - 估值分位仍有部分股票依赖回退估算
  - 行业历史沉淀已经落库，但还没有独立的历史审查页面
