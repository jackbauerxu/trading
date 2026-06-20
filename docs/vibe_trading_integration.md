# Vibe-Trading 接入说明

Vibe-Trading 是一个完整的金融研究工作台，包含：

- 自然语言研究
- 多智能体研究团队
- 回测和 Alpha 因子
- 风控委员会
- Web/API/MCP 服务

在本项目里，它不适合放在初筛阶段，因为它太重；也不适合替代 TradingAgents 或 UZI-Skill，因为会造成多套投研结论互相覆盖。

## 推荐位置

```text
Hermes
↓
OpenBB / TickDB / stock-api
↓
Kronos
↓
daily_stock_analysis 初筛 Top50
↓
Dexter 美股辅助
↓
TradingAgents Top20
↓
UZI-Skill Top10
↓
Vibe-Trading Top10/Buy3 复核
↓
Telegram
```

## 角色分配

| 模块 | 角色 | 是否能直接决定买入 |
|---|---|---|
| daily_stock_analysis | 300 支股票快速初筛 | 否 |
| Kronos | K 线趋势辅助 | 否 |
| Dexter | 美股基本面辅助 | 否 |
| TradingAgents | 多 Agent 投研 | 是，但必须过 UZI |
| UZI-Skill | 投资委员会评分 | 是，但必须过风控闸门 |
| Vibe-Trading | Top10/Buy3 回测与风控复核 | 只能收紧，不能放宽 |

## 为什么放在 Top10 后

Vibe-Trading 的强项是：

- 对少量标的做深度复核
- 回测入场/退出规则
- 检查短期过热和回撤承受度
- 用 `investment_committee`、`risk_committee`、`quant_strategy_desk` 做专题研究

如果把它放在 300 支初筛前，会太慢；放在 Top50 前也会拖长每日任务。因此默认只复核 Top10 里的前 5 只。

## 配置

```text
VIBE_TRADING_ENABLED=1
VIBE_TRADING_DIR=/opt/ai-stock-combo/work/repos/Vibe-Trading
VIBE_TRADING_COMMAND=/opt/ai-stock-combo/.vibe-trading-venv/bin/vibe-trading
VIBE_TRADING_PROVIDER=openai
VIBE_TRADING_MODEL=gpt-4o-mini
VIBE_TRADING_TOP_N=5
VIBE_TRADING_TIMEOUT=1200
VIBE_TRADING_TOOL_TIMEOUT=180
```

现在服务器默认每天开启 Top5 复核：

```text
VIBE_TRADING_ENABLED=1
VIBE_TRADING_TOP_N=5
VIBE_TRADING_WORKERS=2
VIBE_TRADING_TIMEOUT_PER_SYMBOL=420
```

`VIBE_TRADING_WORKERS=2` 表示最多同时复核 2 只股票。先用这个保守值；如果服务器 CPU、内存和模型接口都稳定，再提高到 3。

## 输出

```text
outputs/vibe_trading_review.json
work/vibe_trading_review.log
work/vibe_trading_<symbol>.log
```

如果 Vibe-Trading 对某只股票给出“谨慎”且置信度较高，该股票会被移出买入池，只保留在观察池。
