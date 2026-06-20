# Dexter 接入说明

Dexter 在本项目中作为美股辅助研究层，位置在初筛之后、TradingAgents 之前：

```text
OpenBB / TickDB / stock-api
↓
Kronos
↓
daily_stock_analysis 初筛 Top50
↓
Dexter 美股辅助研究
↓
TradingAgents
↓
UZI-Skill
↓
Top10 / Buy3
```

## 作用边界

- Dexter 只研究美股候选，不处理港股，也不处理 A 股。
- Dexter 输出基本面、近期公司信息、财务质量、风险和 1-3 个月观察点。
- Dexter 最多给初筛分 `+2`，偏谨慎时最多扣 `-3`。
- Dexter 不直接触发买入。
- 如果 Dexter 明确偏谨慎且置信度较高，会阻止该股票进入买入池，避免报告信号互相打架。
- 如果 Dexter 未安装、缺 key、超时或运行失败，主流程继续执行。

## 配置

```text
DEXTER_ENABLED=1
DEXTER_DIR=/opt/ai-stock-combo/work/repos/dexter
DEXTER_COMMAND=bun
DEXTER_MODEL=gpt-5.5
DEXTER_TOP_N=10
DEXTER_TIMEOUT=900
DEXTER_MAX_ITERATIONS=4
DEXTER_TOOL_ALLOWLIST=get_financials,get_market_data,read_filings,web_search
DEXTER_REQUIRE_FINANCIAL_DATASETS=1
DEXTER_BUY_GATE=1
```

Dexter 需要：

```text
OPENAI_API_KEY
FINANCIAL_DATASETS_API_KEY
```

如果暂时没有 `FINANCIAL_DATASETS_API_KEY`，可以把 `DEXTER_REQUIRE_FINANCIAL_DATASETS=0`，但研究质量会明显依赖模型自身和其他可用工具。

## 输出

```text
outputs/dexter_context.json
work/dexter_context.log
```

命中美股候选后，`candidates_top50.json` 会增加：

```json
{
  "dexter_signal": {
    "status": "ok",
    "stance": "看多",
    "confidence": 0.72,
    "key_points": ["收入增长改善", "现金流质量稳定"],
    "risks": ["估值偏高"],
    "watch_items": ["下一次财报指引"],
    "summary": "财务质量尚可，但需要观察估值和财报兑现。",
    "score_adjustment": 1.44
  }
}
```
