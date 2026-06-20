# Kronos 接入说明

Kronos 在本项目中作为 K 线趋势预测层，位置在数据中台之后、初筛排序之前：

```text
OpenBB / TickDB / stock-api
↓
Kronos
↓
daily_stock_analysis 初筛
↓
TradingAgents
↓
UZI-Skill
```

## 作用边界

- Kronos 只读取最近 K 线，预测未来 5 个交易周期的趋势。
- 上行预测最多给初筛分 `+3`。
- 下行预测最多给初筛分 `-5`。
- Kronos 不直接触发买入。
- 如果 Kronos 未安装、模型未下载、数据不足或超时，流程继续运行。

## 配置

```text
KRONOS_ENABLED=1
KRONOS_DIR=/opt/ai-stock-combo/work/repos/Kronos
KRONOS_PYTHON=/opt/ai-stock-combo/.kronos-venv/bin/python
KRONOS_MODEL=/opt/ai-stock-combo/work/models/Kronos-small
KRONOS_TOKENIZER=/opt/ai-stock-combo/work/models/Kronos-Tokenizer-base
KRONOS_DEVICE=cpu
KRONOS_PRED_LEN=5
KRONOS_MIN_LOOKBACK=40
KRONOS_LOOKBACK=90
KRONOS_SYMBOL_LIMIT=120
KRONOS_TIMEOUT=900
```

## 输出

```text
outputs/kronos_context.json
work/kronos_context.log
```

`candidates_top50.json` 中会为命中的股票增加：

```json
{
  "kronos_signal": {
    "trend": "up",
    "forecast_return_5d": 2.4,
    "confidence": 0.3,
    "score_adjustment": 0.84,
    "model": "/opt/ai-stock-combo/work/models/Kronos-small"
  }
}
```
