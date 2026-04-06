# quant_system_pro

这是一个基于 `Streamlit + AkShare` 的 A 股多策略组合回测示例。

## 启动方式

请优先使用项目虚拟环境，不要直接使用系统 `python3`。

```bash
.venv/bin/python run.py
```

Render 部署建议直接使用：

```bash
streamlit run web/app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false
```

不要把 `run.py` 再包一层 `streamlit run`，也不要填写不存在的 `web/run.py`。仓库已提供 `render.yaml`，可以直接按 Blueprint 导入。

## 已处理的稳定性问题

- 首屏不再在模块导入时直接请求股票列表
- AkShare 请求失败时会优先尝试本地缓存
- 股票列表、行情数据、回测流程都增加了空数据和异常提示
- 组合策略对零方差和空结果做了保护

## 数据库与自动优化

- 项目现在会在 [`db/market_data.db`](/Users/jack.ma/Documents/SVNDocument/AI_Test/股市/quant_system_pro/db/market_data.db) 中持久化行情、推荐结果、回测记录和参数优化结果
- Web 页面新增了“策略进化与数据库”区域，可以：
  - 把当前股票池同步进数据库
  - 执行一次滚动参数优化
  - 在后续运行策略时自动套用数据库中的最优参数
- 当前“自动进化”属于第一阶段：
  - 已支持参数评估结果入库
  - 已支持最优参数自动回写
  - 还没有做后台定时任务和真正的全市场增量更新

## 自动任务脚本

如果你想从命令行或外部调度器触发任务，可以使用：

```bash
.venv/bin/python automation_runner.py daily-update --pool-size 300
.venv/bin/python automation_runner.py weekly-optimize --pool-size 300
```

这两个命令会分别执行：

- 每日更新：拉取更大股票池、同步行情到数据库
- 每周优化：基于当前股票池重新评估参数，并把最优参数写回数据库

## 常见问题

- 如果提示无法连接行情源，请检查网络、DNS、代理设置
- 如果首次运行没有缓存且网络不可用，页面会给出错误提示，但不会直接崩溃
