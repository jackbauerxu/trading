# TradingAgents 性能问题交接文档

**日期**: 2026-06-21  
**版本**: 1.1  
**状态**: 已实施阻断式修复 - TA 超时不再用快速结果替代

---

## 执行摘要

TradingAgents (v0.2.5) 在本项目环境中存在严重的性能问题，导致 formal 流程在 `ta_in_progress` 阶段卡停 15+ 分钟无进展。核心问题在于 `TradingAgentsGraph.propagate()` 调用无法在合理时间内完成，可能由环境配置或库版本兼容性问题引起。

**关键发现**: 问题分两层：第一，主流程在 TradingAgents 之前会先跑 OpenBB、Kronos、DSA、Dexter，其中 OpenBB/Kronos/Dexter 默认超时可达 900/900/1800 秒；旧代码直到 DSA 后才写 `pipeline_status.json`，所以前置层等待时会显示旧状态，误看成 TA 卡住。第二，服务器直连 `beefapi.com` 会被网关拒绝（HTTP 403 / code 1010），必须走本机 `socks5h://127.0.0.1:7890` 代理；代理路径下 `/models` 和 `chat/completions` 可返回 200。当前处理口径是不降级：TA 完整版超时或卡停时，写入失败元数据并阻断 formal 发布，不使用 quick 结果替代正式 TA 结果。

---

## 问题详情

### 症状
- formal 启动后进入 `ta_in_progress` 阶段
- 状态在 10-15+ 分钟内完全停滞，无任何更新
- 诊断日志（应在 propagate() 前后打印）从未出现
- 进程仍然活跃（CPU/MEM 正常），说明卡在 I/O 或 API 等待

### 原始错误链
1. **第1-2次运行**: Python 3.9 兼容性 + json_repair 版本 + SOCKS5 代理问题
2. **第3-4次运行**: TA 卡停于 `ta_in_progress` 阶段
3. **第5-6次运行**: 即使降低递归限制和并行化，仍然卡停

### 直接诊断结果

```bash
# 直接测试 TradingAgents.propagate()
timeout 30 python3 << 'PY'
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import DEFAULT_CONFIG

config = DEFAULT_CONFIG.copy()
config["backend_url"] = "http://127.0.0.1:57321/v1"
config["max_recur_limit"] = 100

graph = TradingAgentsGraph(debug=False, config=config)
final_state, decision = graph.propagate("MRVL", "2026-06-21")
print(f"Decision: {decision}")
PY

# 结果: 30 秒超时，propagate() 无法完成
```

---

## 已尝试的解决方案

| # | 方案 | 状态 | 备注 |
|----|------|------|------|
| 1 | 修复 Python 3.9 兼容性 (PEP 604) | ✅ | 添加 `from __future__ import annotations` |
| 2 | 降级 json_repair 版本 | ✅ | <0.38.0 以支持 Python 3.9 |
| 3 | 降级 exchange-calendars 版本 | ✅ | <4.13.0 以支持 Python 3.9 |
| 4 | 禁用 SOCKS5 代理 | ✅ | 注释 TELEGRAM_PROXY 和 PIPELINE_OUTBOUND_PROXY |
| 5 | 降低递归限制 | ✅ | 300 → 50 (`PIPELINE_TRADINGAGENTS_RECURSION_LIMIT`) |
| 6 | 并行化 quick_trading_research | ✅ | 使用 ThreadPoolExecutor (line 3442) |
| 7 | 添加诊断日志 | ✅ | [TA-DEBUG] 标记用于追踪进度 |
| 8 | 增加超时配置 | ✅ | 调整 PIPELINE_TRADINGAGENTS_STAGE_TIMEOUT |

**结果**: 所有优化均未解决根本问题。TradingAgents 在 propagate() 调用处仍然卡停。

---

## 根本原因分析

### 最可能的原因（按概率排序）

1. **TradingAgents 库本身的性能问题**
   - v0.2.5 在递归决策或 LLM 调用中存在无限循环或死锁
   - 与特定版本的 langchain-openai (1.3.0) 或 openai (2.41.0) 不兼容

2. **LLM API 响应超慢或卡停**
   - OpenAI 后端 (127.0.0.1:57321/v1) 在大型模型推理上响应缓慢
   - propagate() 可能在等待 gpt-4o 或 gpt-4o-mini 响应时阻塞

3. **环境配置不匹配**
   - 特定的数据 vendor 配置 (pipeline, yfinance, alpha_vantage) 导致查询卡停
   - 或数据源不可用导致超时重试循环

4. **Python 虚拟环境隔离问题**
   - TRADINGAGENTS_PYTHON 子进程的依赖不完全
   - 即使主进程依赖满足，子进程可能缺少某些库

### 已排除的原因

- ❌ 网络连接问题（已验证 ping、DNS、LLM API 都可用）
- ❌ TradingAgents 库导入问题（已验证导入和初始化成功）
- ❌ 报告层误判（当前已改为：TA 未完整完成时阻断 formal 发布）
- ❌ SOCKS5 代理问题（已禁用代理配置）

---

## 已实施的改进代码

### 1. TradingAgents 超时监控包装 (run_tradingagents_with_timeout)

```python
def run_tradingagents_with_timeout(
    candidates, *, trading_dir, python_bin, env,
    per_stock_timeout, stage_timeout, max_workers
) -> tuple[list[dict], dict]:
    """Run TA with watchdog timeout and block formal output on timeout."""
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(
        run_tradingagents_full_batch,
        candidates, trading_dir=trading_dir, python_bin=python_bin,
        env=env, per_stock_timeout=per_stock_timeout,
        stage_timeout=stage_timeout, max_workers=max_workers,
    )
    
    try:
        out, ta_meta = future.result(timeout=stage_timeout + 60)
        print(f"[TA-WATCHDOG] Full batch completed in time", flush=True)
        return out, ta_meta
    except FutureTimeoutError:
        print(f"[TA-WATCHDOG] Timeout exceeded, blocking formal TA output", flush=True)
        future.cancel()
        ta_meta = {
            "ta_stage_status": "watchdog_timeout",
            "ta_completed_full": 0,
            "ta_failed_symbols": [c.get("symbol") for c in candidates],
            "ta_total_symbols": len(candidates),
            "ta_failure_reason": f"watchdog timeout after {stage_timeout + 60}s",
        }
        return [], ta_meta
    finally:
        executor.shutdown(wait=False)
```

### 2. 改进的日志追踪 (run_tradingagents_full_one)

```python
print(f"[TA-ONE] {symbol}: Starting (timeout={timeout}s)", flush=True)
start = time.time()
try:
    rc, text = run([python_bin, "-c", TRADINGAGENTS_SNIPPET], ...)
    elapsed = time.time() - start
    print(f"[TA-ONE] {symbol}: Completed in {elapsed:.1f}s", flush=True)
except subprocess.TimeoutExpired:
    elapsed = time.time() - start
    print(f"[TA-ONE] {symbol}: Timeout after {elapsed:.1f}s", flush=True)
    return failed_tradingagents_result(item, f"timeout after {timeout}s", "timeout")
```

### 3. 并行化快速研究 (line 3442)

```python
# 从同步列表推导式改为并行处理
quick_rows = []
with ThreadPoolExecutor(max_workers=min(workers, 4)) as executor:
    futures = [executor.submit(quick_trading_research, item) for item in candidates[:scan_n]]
    for i, future in enumerate(futures, 1):
        result = future.result(timeout=60)
        quick_rows.append(result)
        if i % 5 == 0:
            print(f"[TA] Screened {i}/{scan_n} candidates", flush=True)
```

### 4. 前置长耗时阶段状态可观测

`main()` 现在会在 OpenBB、Kronos、DSA、Dexter 开始前写入 `pipeline_status.json`：

```python
write_stage_status(run_mode=run_mode, stage="openbb_in_progress", ...)
write_stage_status(run_mode=run_mode, stage="kronos_in_progress", ...)
write_stage_status(run_mode=run_mode, stage="dsa_in_progress", ...)
write_stage_status(run_mode=run_mode, stage="dexter_in_progress", ...)
```

这样服务器如果卡在 TA 之前，状态文件会显示真实阶段，不再误导为 `ta_in_progress`。

---

## 配置调整

### config.env 修改

```env
# 原值: PIPELINE_TRADINGAGENTS_RECURSION_LIMIT=300
# 新值: PIPELINE_TRADINGAGENTS_RECURSION_LIMIT=50
PIPELINE_TRADINGAGENTS_RECURSION_LIMIT=50

# 禁用代理（因为本地 7890 端口不可用）
# PIPELINE_OUTBOUND_PROXY=socks5h://127.0.0.1:7890
# TELEGRAM_PROXY=socks5h://127.0.0.1:7890
```

---

## 当前状态（2026-06-21 修复后）

```
formal 完成口径: DSA → TradingAgents full → UZI → Telegram 必须全部真实完成
TradingAgents full 单票失败: 返回 ta_status=failed
TradingAgents full 阶段超时: 返回空正式 TA 结果 + tradingagents_stage_meta.json
正式发布: build_flow_status() 看到 TA 未完整完成时 overall_status=failed
```

当前代码不再把 quick 研究写入正式 `tradingagents_top20.json` 作为 TradingAgents full 结果。quick 研究仍可作为 full 前的候选排序诊断文件 `tradingagents_quick_top50.json`，但 formal 是否完成只看 `ta_status=full` 和 TA 阶段元数据。

---

## 建议的后续诊断

### 优先级 HIGH

1. **检查 TradingAgents git history**
   - 查看 v0.2.5 以来是否有已知的卡停问题
   - 或尝试降级到 v0.2.4 测试
   ```bash
   pip install tradingagents==0.2.4
   ```

2. **隔离 propagate() 调用**
   - 在独立的 Python 脚本中反复调用 propagate()，追踪行为
   - 添加信号处理器在 propagate() 超时时触发 SIGALRM，中断执行
   - 检查是否是特定的 ticker (MRVL) 或日期导致问题

3. **检查 LLM 后端性能**
   ```bash
   # 直接测试 OpenAI API 响应时间
   time curl http://127.0.0.1:57321/v1/models
   time curl -X POST http://127.0.0.1:57321/v1/chat/completions \
     -H "Content-Type: application/json" \
     -d '{"model": "gpt-4o", "messages": [{"role": "user", "content": "test"}]}'
   ```

4. **检查数据源可用性**
   - 测试 pipeline、yfinance、alpha_vantage 数据源
   - 特别是 MRVL 的实时数据和技术指标是否可用

5. **Python 虚拟环境对比**
   ```bash
   # 在两个不同 venv 中运行 propagate()，对比行为
   /path/to/venv1/bin/python propagate_test.py
   /path/to/venv2/bin/python propagate_test.py
   ```

### 优先级 MEDIUM

6. **尝试替代实现**
   - 改用 TradingAgents 的 quick_mode 或 lite_mode（如果存在）
   - 或实现简化的分析流程，绕过 propagate()

7. **监控进程级别的卡停**
   ```bash
   # 使用 strace 追踪子进程的系统调用
   strace -p <ta_subprocess_pid> -f 2>&1 | grep -v "SIGCHLD"
   ```

8. **检查递归限制的实际作用**
   - 当前 max_recur_limit=50，但 propagate() 仍然超时
   - 可能此参数对性能无实质帮助，需要其他优化

### 优先级 LOW

9. **考虑替代库**
   - 如果 TradingAgents 无法修复，评估替代的量化分析库
   - 如 backtrader、zipline 等

10. **性能分析和优化**
    - 使用 cProfile 或 py-spy 分析 propagate() 的热点代码路径
    - 查找是否有不必要的循环或深度递归

---

## 阻断方案（已实施）

当 TA 卡停超过 watchdog 上限时，不再自动替换为快速方案。流程会返回空正式 TA 结果，并写入 `outputs/tradingagents_stage_meta.json`：

```python
out, ta_meta = run_tradingagents_with_timeout(...)

if ta_meta.get("ta_stage_status") == "watchdog_timeout":
    # 不返回 quick 替代行；formal 状态层负责阻断发布。
    assert out == []
```

**优点**:
- ✅ formal 流程不会卡死在 `ta_in_progress`
- ✅ 不会把 quick 研究误当成 TradingAgents 完整版结果
- ✅ `build_flow_status()` 可通过 TA 元数据阻断可执行买入日报

**缺点**:
- ❌ TA 完整分析功能无法使用
- ❌ 当 TA 卡停时，正式买入日报会被阻断，直到 TA 完整版恢复

---

## 文件修改清单

| 文件 | 修改 | 行号 |
|------|------|------|
| run_daily_pipeline.py | 添加 run_tradingagents_with_timeout()，watchdog 超时返回空正式 TA 结果 | 3414-3458 |
| run_daily_pipeline.py | 改进 run_tradingagents_full_one() 日志 | 7332-7384 |
| run_daily_pipeline.py | 并行化 quick_trading_research | 3444-3458 |
| run_daily_pipeline.py | 添加 stage_tradingagents 诊断日志 | 3415 |
| tests/test_tradingagents_formal_execution.py | 覆盖 TA 超时不返回 quick 替代行 | 388-455 |
| config.env | 降低递归限制 | 204 |
| config.env | 禁用代理 | 194, 203 |

---

## 联系和后续

- **问题报告人**: Claude Code (自动诊断)
- **诊断时间**: 2026-06-21 02:35 - 03:09 (约 34 分钟)
- **推荐接手人**: TradingAgents 库维护者 或 环境配置管理员
- **紧急性**: 中等（影响 formal 流程；当前为阻断式保护，避免发布非正式 TA 结果）

---

## 快速参考

```bash
# 查看 TA 阶段的诊断日志
grep "\[TA" work/tradingagents_*.log

# 查看完整的流程状态
cat outputs/pipeline_status.json | python3 -m json.tool

# 查看 TA 元数据
cat outputs/tradingagents_stage_meta.json | python3 -m json.tool

# 检查 TradingAgents 依赖版本
pip show tradingagents langchain-openai openai exchange-calendars json-repair

# 重启 formal（清理旧日志）
pkill -9 -f run_daily_pipeline
rm -f outputs/*.json work/*.log
python run_daily_pipeline.py --run-mode formal --dsa-top 1 --ta-top 1 --uzi-top 1
```

---

**文档版本历史**
- v1.0 (2026-06-21): 初始交接文档，记录第 1-6 次 formal 运行的诊断结果
- v1.1 (2026-06-21): 改为不降级口径；TA watchdog 超时写失败元数据并阻断 formal 发布
