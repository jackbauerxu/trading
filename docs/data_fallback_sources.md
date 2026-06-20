# 数据备选源说明

流水线的数据中台顺序是：OpenBB 优先，`global-stock-data` 兜底；在 `global-stock-data` 内部再做多源切换。

## Alpha Vantage

定位：美股日线 K 线和常用金融指标的正式 API 兜底。

配置：

```text
ALPHA_VANTAGE_API_KEY=...
```

当前用途：当 OpenBB 或其他美股源不通时，直接调用 Alpha Vantage 的日线接口，补齐 `close`、20 日涨跌和量能。

## stock-api

定位：轻量行情工具，覆盖 A/HK/US；本项目只用于港股和美股。

配置：

```text
STOCK_API_ENABLED=1
STOCK_API_COMMAND=npx
STOCK_API_TIMEOUT=18
```

当前用途：通过 CLI 自动选择可用数据源，作为港股和美股 K 线备选。若服务器已全局安装 CLI，可以把 `STOCK_API_COMMAND` 改成实际命令名，减少 `npx` 启动开销。

## TickDB

定位：AI 代理友好的多市场实时与历史行情 API。

配置：

```text
TICKDB_API_KEY=...
TICKDB_API_BASE=https://api.tickdb.ai
```

当前用途：作为港股、美股的高优先级 K 线源，可辅助外汇和指数联动信号。

## 质量规则

- 任一数据源失败不会中断流程，会继续尝试下一源。
- 如果只拿到实时价而没有足够 K 线，报告中会标记为 `quote_only`，只做观察参考。
- 买入资格仍由 TradingAgents、UZI-Skill、估值/风险闸门共同决定，数据兜底不能单独触发买入。
