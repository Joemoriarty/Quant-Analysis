# Docs 索引与更新规则

这份文档是 `docs/` 目录的总入口。

目的：
- 帮助后续快速找到“该先看哪份文档”
- 统一以后改功能时的文档更新规则
- 避免不同文档重复记录、口径冲突或漏更新
- 现在网页端也已增加“Docs 看板”入口，可直接查看最近改动、专业化方向、问题清单和文档原文

---

## 文档总览

## 目录结构

```text
docs/
├── README.md
├── current/
│   ├── FIELD_GLOSSARY.md
│   ├── STOCK_ANALYSIS_LOGIC.md
│   ├── CODE_ARCHITECTURE.md
│   ├── PROFESSIONALIZATION_TRACKER.md
│   └── PRIVATE_FUND_GAP_BACKLOG.md
├── history/
│   └── RELEASE_NOTES.md
└── templates/
    └── DOC_UPDATE_TEMPLATES.md
```

- `docs/README.md`
  - 总入口，先看这份
- `docs/current/`
  - 放“当前状态类”文档
  - 回答系统现在是什么状态
- `docs/history/`
  - 放“历史流水类”文档
  - 回答每次迭代改了什么
- `docs/templates/`
  - 放强约束模板
  - 回答以后应该按什么格式更新文档

### 快速定位规则

- 想在网页里直接看文档摘要：
  - 去 `web/app.py` 的“Docs 看板”页签
- 想看“现在怎么判断”：
  - 去 `docs/current/STOCK_ANALYSIS_LOGIC.md`
- 想查某个字段是什么意思：
  - 去 `docs/current/FIELD_GLOSSARY.md`
- 想看“现在怎么组织”：
  - 去 `docs/current/CODE_ARCHITECTURE.md`
- 想看“能力建设做到哪一步”：
  - 去 `docs/current/PROFESSIONALIZATION_TRACKER.md`
- 想看“私募视角下还差什么”：
  - 去 `docs/current/PRIVATE_FUND_GAP_BACKLOG.md`
- 想看“这次具体改了什么”：
  - 去 `docs/history/RELEASE_NOTES.md`
- 想按固定格式补文档：
  - 去 `docs/templates/DOC_UPDATE_TEMPLATES.md`

### 目录命名约束

- `docs/current/`
  - 只放“现状类”文档
  - 文件名应表达“当前系统是什么”
- `docs/history/`
  - 只放“流水类”文档
  - 文件名应表达“历史发生了什么”
- `docs/templates/`
  - 只放“模板类”文档
  - 文件名应表达“以后应该怎么写”

不建议再把新的核心文档直接丢回 `docs/` 根目录。
根目录原则上只保留：
- `README.md`

## 核心输出字段中文说明

下面这些名称是系统里最常出现的核心输出。完整词典请看 [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)。

以后在 `docs/` 中，推荐优先写成“中文解释（内部参数名）”的形式，避免只写英文键名。

| 中文解释 | 内部参数名 | 建议文档写法 |
| --- | --- | --- |
| 技术面结论 | `technical_recommendation` | 技术面结论（`technical_recommendation`） |
| 基本面评分 | `fundamental_score` | 基本面评分（`fundamental_score`） |
| 市场情绪状态 | `market_sentiment_state` | 市场情绪状态（`market_sentiment_state`） |
| 事件驱动评分 | `event_score` | 事件驱动评分（`event_score`） |
| 行业横向比较评分 | `industry_comparison_score` | 行业横向比较评分（`industry_comparison_score`） |
| 最终推荐结论 | `recommendation` | 最终推荐结论（`recommendation`） |
| 最终结论依据 | `final_decision_basis` | 最终结论依据（`final_decision_basis`） |

补充理解：
- “结论”通常是离散判断，例如 `推荐关注 / 中性观察 / 暂不推荐`
- “评分”通常是 0-100 的数值结果
- “依据”通常是把本次结论为什么成立，用一句话归纳出来

### 1. [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)

- 回答的问题：
  - 当前系统到底如何得出股票结论
  - 哪些规则会影响 `recommendation`
  - 候选筛选、组合评分、升降档逻辑当前如何运作
- 适合什么时候看：
  - 想确认当前判断逻辑
  - 想核对买卖结论为什么变化
  - 想修改评分、阈值、推荐链路

### 2. [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)

- 回答的问题：
  - 系统模块如何拆分
  - 数据流、插件结构、页面入口、存储结构怎么组织
  - 代码职责边界是否变化
- 适合什么时候看：
  - 想做重构
  - 想改模块职责
  - 想补新数据源、插件、页面、存储结构

### 3. [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)

- 回答的问题：
  - 某个字段到底是什么意思
  - 这个字段属于评分、结论、状态还是指标
  - 后续文档里应该用什么中文名
- 适合什么时候看：
  - 看文档时遇到内部参数名
  - 想统一字段中文写法
  - 新增字段后准备补词典

### 4. [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)

- 回答的问题：
  - 某项能力有没有接入
  - 距离机构可用还有多远
  - 当前系统接入状态和机构成熟度分别处在哪个阶段
- 适合什么时候看：
  - 想评估系统现在做到哪一步
  - 想排专业化能力建设优先级
  - 想更新“系统接入状态 / 机构成熟度”

### 5. [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)

- 回答的问题：
  - 从私募 / 机构使用者视角看，当前主要缺陷是什么
  - 哪些问题最影响研究可信度、组合可用性和产品专业感
  - 后续 backlog 应该优先补什么
- 适合什么时候看：
  - 想站在私募大佬视角审视产品
  - 想做产品化、机构化升级
  - 想补“为什么机构现在还不会完全信这套系统”

### 6. [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)

### 7. [DOC_UPDATE_TEMPLATES.md](./templates/DOC_UPDATE_TEMPLATES.md)

- 回答的问题：
  - 每类文档应该按什么模板更新
  - 最少要填哪些字段
  - 什么情况下算更新不完整
- 适合什么时候看：
  - 准备补文档但不想临时想格式
  - 想把文档维护变成固定动作

- 回答的问题：
  - 某次迭代具体改了什么
  - 哪些模块受影响
  - 这次改动留下了什么风险和遗留问题
- 适合什么时候看：
  - 想回看变更历史
  - 想写阶段性发布说明
  - 想追某项功能是什么时候进来的

---

## 推荐阅读顺序

### 场景 1：第一次接手这个项目

1. 先看 [README.md](./README.md)
2. 再看 [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)
3. 再看 [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)
4. 再看 [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
5. 再看 [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)
6. 最后按需看 [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)

### 场景 2：想改股票判断逻辑

1. 先看 [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)
2. 再看 [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
3. 再看 [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)
4. 如果改动影响机构视角判断，再看 [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)
5. 改完后补 [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)

### 场景 3：想改页面或产品结构

1. 先看 [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)
2. 再看 [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)
3. 然后看 [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)
4. 如果页面会改变解释口径，再同步看 [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)

### 场景 4：想看某次迭代到底改了什么

1. 先看 [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
2. 再回到对应的现状文档核对：
  - 逻辑问题看 [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)
  - 字段定义看 [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
  - 架构问题看 [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)
  - 能力状态看 [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)
  - 私募 gap 看 [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)

---

## 更新规则

### 一条总原则

- 任何一次改动，至少要更新“一个现状文档 + 一个历史文档”。
- 现状文档负责回答“现在是什么状态”。
- 历史文档负责回答“这次改了什么”。
- 如果本次改动引入了新的对外字段或关键内部字段，还要同步更新字段词典。

也就是说，通常至少要更新：
- 某一份现状文档
- [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
- 如涉及新增字段，再更新 [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)

### 按改动类型更新

#### 1. 改了股票判断逻辑

- 必须更新：
  - [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)
  - [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
- 视情况更新：
  - [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
  - [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)
  - [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)

#### 2. 改了模块职责、数据流、插件结构、页面入口、存储结构

- 必须更新：
  - [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)
  - [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
- 视情况更新：
  - [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
  - [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)
  - [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)

#### 3. 新增或补强专业化能力

- 必须更新：
  - [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)
  - [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
- 视情况更新：
  - [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
  - [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)
  - [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)
  - [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)

#### 4. 修补私募视角下的产品缺陷

- 必须更新：
  - [PRIVATE_FUND_GAP_BACKLOG.md](./current/PRIVATE_FUND_GAP_BACKLOG.md)
  - [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
- 视情况更新：
  - [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
  - [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)
  - [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)
  - [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)

#### 5. 新增了字段、改了字段中文解释、或者字段对外含义变化

- 必须更新：
  - [FIELD_GLOSSARY.md](./current/FIELD_GLOSSARY.md)
  - [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
- 视情况更新：
  - [STOCK_ANALYSIS_LOGIC.md](./current/STOCK_ANALYSIS_LOGIC.md)
  - [CODE_ARCHITECTURE.md](./current/CODE_ARCHITECTURE.md)
  - [PROFESSIONALIZATION_TRACKER.md](./current/PROFESSIONALIZATION_TRACKER.md)

---

## 最小更新检查清单

以后每次改完功能，再回来看 `docs` 时，至少检查下面 6 个问题：

1. 这次改动有没有影响股票最终结论或评分口径
2. 这次改动有没有影响模块职责、数据流、插件、页面入口或存储结构
3. 这次改动有没有让某项专业化能力的状态发生变化
4. 这次改动有没有新增字段、改字段名、改字段含义
5. 这次改动有没有让某个私募视角缺陷被修补、降级或新增
6. 这次改动有没有值得记录到历史流水里
7. 本次改动后，几份核心文档之间的描述是否仍然一致

只要有任意一个问题回答为“有”，就要更新对应文档。

---

## 推荐维护方式

为了避免以后文档再次长歪，建议固定按下面方式维护：

1. 先改代码
2. 再判断改动类型
3. 先更新现状文档
4. 再更新 [RELEASE_NOTES.md](./history/RELEASE_NOTES.md)
5. 最后回到 [README.md](./README.md) 检查这套规则是否仍然适用

---

## 后续约定

以后再次阅读或更新 `docs/` 时，优先从 [README.md](./README.md) 进入。

如果后续新增新的 `docs/*.md` 文档，必须同步补充：
- 它解决什么问题
- 它和现有 5 份核心文档如何分工
- 它在什么改动场景下必须更新

新增文档前，优先判断它应该放在哪一层：
- `docs/current/`
- `docs/history/`
- `docs/templates/`
