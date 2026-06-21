#!/usr/bin/env python3
"""Hermes-triggered daily stock selection pipeline.

Flow:
Hermes -> OpenBB -> Screen(50) -> Research(20) -> Committee(10) -> Buy(3) -> Telegram.
"""

from __future__ import annotations

import argparse
import datetime as dt
import inspect
import json
import math
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
OUTPUTS = ROOT / "outputs"
WORK = ROOT / "work"
_FUNDAMENTAL_FETCH_CACHE: dict[tuple[str, str], dict[str, Any]] = {}


def load_env_file(path: Path, env: dict[str, str], *, overwrite: bool = False) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and (overwrite or not env.get(key)):
            env[key] = value


def build_env() -> dict[str, str]:
    env = dict(os.environ)
    load_env_file(ROOT / "config.env", env, overwrite=False)
    trading_dir = Path(env.get("TRADINGAGENTS_DIR", ""))
    if trading_dir:
        load_env_file(trading_dir / ".env", env)
    if env.get("OPENAI_BASE_URL") and not env.get("TRADINGAGENTS_LLM_BACKEND_URL"):
        env["TRADINGAGENTS_LLM_BACKEND_URL"] = env["OPENAI_BASE_URL"]
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("NOTIFICATION_REPORT_CHANNELS", "telegram")
    # Override python executables with correct venv paths (config.env often has wrong system python)
    dsa_venv = ROOT / "daily_stock_analysis" / ".venv" / "bin" / "python"
    if dsa_venv.exists():
        env["DAILY_STOCK_ANALYSIS_PYTHON"] = str(dsa_venv)
    if "TRADINGAGENTS_PYTHON" not in env:
        env["TRADINGAGENTS_PYTHON"] = str(ROOT / ".test-venv" / "bin" / "python")
    if "UZI_PYTHON" not in env:
        env["UZI_PYTHON"] = env.get("PYTHON_BIN", "python")
    return env


def read_stock_pool(path: Path) -> list[dict[str, str]]:
    try:
        import yaml  # type: ignore

        data = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader) or {}
        items: list[dict[str, str]] = []
        for group, rows in (data.get("groups") or {}).items():
            for row in rows or []:
                if isinstance(row, str):
                    items.append({"symbol": row.strip(), "name": "", "group": str(group)})
                elif isinstance(row, dict) and row.get("symbol"):
                    items.append({
                        "symbol": str(row.get("symbol", "")).strip(),
                        "name": str(row.get("name", "")).strip(),
                        "group": str(group),
                    })
        return dedupe_pool(items)
    except Exception:
        return read_stock_pool_simple(path)


def read_stock_pool_simple(path: Path) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    group = ""
    current: dict[str, str] | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if line.startswith("  ") and stripped.endswith(":") and not stripped.startswith("-"):
            group = stripped[:-1]
            continue
        if stripped.startswith("- {") and stripped.endswith("}"):
            body = stripped[3:-1]
            fields: dict[str, str] = {}
            for part in re.split(r",\s*", body):
                if ":" not in part:
                    continue
                key, value = part.split(":", 1)
                fields[key.strip()] = value.strip().strip("'\"")
            if fields.get("symbol"):
                items.append({
                    "symbol": fields.get("symbol", ""),
                    "name": fields.get("name", ""),
                    "group": group,
                })
            continue
        if stripped.startswith("- symbol:"):
            if current:
                items.append(current)
            current = {"symbol": stripped.split(":", 1)[1].strip(), "name": "", "group": group}
            continue
        if current and stripped.startswith("name:"):
            current["name"] = stripped.split(":", 1)[1].strip()
    if current:
        items.append(current)
    return dedupe_pool(items)


def dedupe_pool(items: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for item in items:
        symbol = normalize_dsa_symbol(item.get("symbol", ""))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append({**item, "symbol": symbol})
    return out


def normalize_dsa_symbol(symbol: str) -> str:
    s = symbol.strip()
    if not s:
        return ""
    # Remove .HK or .SZ or .SS suffixes
    s = re.sub(r"\.(HK|SZ|SS|HK)$", "", s, flags=re.IGNORECASE)
    if re.fullmatch(r"\d{5}", s):
        return "hk" + s
    return s.upper() if not s.lower().startswith("hk") else "hk" + s[2:].zfill(5)


def to_tradingagents_symbol(symbol: str) -> str:
    s = normalize_dsa_symbol(symbol)
    if s.lower().startswith("hk"):
        return s[2:].zfill(4) + ".HK"
    if re.fullmatch(r"\d{6}", s):
        return s + (".SS" if s.startswith("6") else ".SZ")
    return s.upper()


def to_uzi_symbol(symbol: str) -> str:
    s = normalize_dsa_symbol(symbol)
    if s.lower().startswith("hk"):
        return "hk" + s[2:].zfill(5)
    if re.fullmatch(r"\d{6}", s):
        return s + (".SH" if s.startswith("6") else ".SZ")
    return s.upper()


def run(cmd: list[str], cwd: Path, env: dict[str, str], *, timeout: int | None = None) -> tuple[int, str]:
    print(f"$ {format_cmd(cmd)}")
    WORK.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", errors="replace", dir=WORK, delete=False) as log_file:
        log_path = Path(log_file.name)
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            text=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            terminate_process_group(proc)
            output = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
            raise subprocess.TimeoutExpired(cmd, timeout, output=output)
    output = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
    try:
        log_path.unlink()
    except OSError:
        pass
    return proc.returncode, output or ""


def without_broken_local_proxy(env: dict[str, str]) -> dict[str, str]:
    """Remove dead localhost proxy settings for tools that can reach LLMs directly."""
    cleaned = dict(env)
    for key in ("ALL_PROXY", "all_proxy", "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        value = str(cleaned.get(key) or "")
        if "127.0.0.1:7892" in value or "localhost:7892" in value:
            cleaned.pop(key, None)
    return cleaned


def run_python_snippet(python_bin: str, snippet: str, cwd: Path, env: dict[str, str], *, timeout: int | None = None, name: str = "snippet") -> tuple[int, str]:
    WORK.mkdir(parents=True, exist_ok=True)
    script_path = WORK / f"{safe_name(name)}.py"
    script_path.write_text(snippet, encoding="utf-8")
    try:
        return run([python_bin, str(script_path)], cwd, env, timeout=timeout)
    finally:
        try:
            script_path.unlink()
        except OSError:
            pass


def format_cmd(cmd: list[str]) -> str:
    if len(cmd) >= 3 and cmd[1] == "-c":
        return f"{cmd[0]} -c <inline-python:{len(cmd[2])} chars>"
    text = " ".join(cmd)
    return text if len(text) <= 500 else text[:500] + " ..."


def terminate_process_group(proc: subprocess.Popen[str]) -> None:
    for sig, grace in ((signal.SIGTERM, 3), (signal.SIGKILL, 3)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        except Exception:
            pass
        try:
            proc.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            continue


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def stage_openbb_context(pool: list[dict[str, str]], env: dict[str, str]) -> dict[str, Any]:
    if env.get("PIPELINE_SKIP_OPENBB") == "1":
        existing = read_json_if_exists(OUTPUTS / "openbb_context.json", {})
        if isinstance(existing, dict) and existing.get("symbols"):
            existing = dict(existing)
            existing["status"] = existing.get("status") or "reused"
            existing["reused_due_to_skip_openbb"] = True
            return existing
        return {"status": "skipped", "symbols": [], "benchmarks": []}

    python_bin = env.get("OPENBB_PYTHON") or str(ROOT / ".openbb-venv" / "bin" / "python")
    if not Path(python_bin).exists():
        context = {"status": "fallback", "error": f"OpenBB Python 不存在：{python_bin}", "symbols": [], "benchmarks": []}
        write_json(OUTPUTS / "openbb_context.json", context)
        return context

    run_env = prepare_node_proxy_env(env)
    run_env["PIPELINE_OPENBB_POOL"] = json.dumps(pool, ensure_ascii=False)
    timeout = int(env.get("PIPELINE_OPENBB_TIMEOUT", "900"))
    try:
        rc, text = run([python_bin, "-c", OPENBB_SNIPPET], ROOT, run_env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        text = str(exc.output or "")
        context = {
            "status": "fallback",
            "error": f"OpenBB 超时 {timeout}s",
            "symbols": [],
            "benchmarks": [],
            "log_tail": text[-1500:],
        }
        write_json(OUTPUTS / "openbb_context.json", context)
        write_text(WORK / "openbb_context.log", text)
        return context

    write_text(WORK / "openbb_context.log", text)
    parsed = parse_last_json(text)
    if rc != 0 or not isinstance(parsed, dict):
        context = {
            "status": "fallback",
            "error": summarize_failure(text, rc),
            "symbols": [],
            "benchmarks": [],
            "log_tail": text[-1500:],
        }
        write_json(OUTPUTS / "openbb_context.json", context)
        return context

    parsed.setdefault("status", "ok")
    if int(parsed.get("symbol_count") or 0) == 0:
        parsed["status"] = "no_data"
    write_json(OUTPUTS / "openbb_context.json", parsed)
    return parsed


def stage_kronos_context(openbb_context: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    if env.get("KRONOS_ENABLED", "0") != "1":
        context = {"status": "skipped", "symbols": []}
        write_json(OUTPUTS / "kronos_context.json", context)
        return context

    python_bin = env.get("KRONOS_PYTHON") or str(ROOT / ".kronos-venv" / "bin" / "python")
    kronos_dir = config_path(env.get("KRONOS_DIR", str(ROOT / "work" / "repos" / "Kronos")))
    if not Path(python_bin).exists():
        context = {"status": "unavailable", "error": f"Kronos Python 不存在：{python_bin}", "symbols": []}
        write_json(OUTPUTS / "kronos_context.json", context)
        return context
    if not kronos_dir.exists():
        context = {"status": "unavailable", "error": f"Kronos 仓库不存在：{kronos_dir}", "symbols": []}
        write_json(OUTPUTS / "kronos_context.json", context)
        return context

    min_rows = int(env.get("KRONOS_MIN_LOOKBACK", "40"))
    symbol_limit = int(env.get("KRONOS_SYMBOL_LIMIT", "120"))
    enriched = read_json_if_exists(OUTPUTS / "enriched_stock_data.json", {})
    source_rows = (enriched.get("symbols") or []) if isinstance(enriched, dict) and enriched.get("symbols") else (openbb_context.get("symbols") or [])
    candidates = [
        row for row in source_rows
        if len(row.get("klines") or []) >= min_rows
    ][:symbol_limit]
    if not candidates:
        context = {"status": "no_data", "error": "没有足够 K 线供 Kronos 预测", "symbols": []}
        write_json(OUTPUTS / "kronos_context.json", context)
        return context

    payload_path = WORK / "kronos_payload.json"
    write_json(payload_path, candidates)
    run_env = prepare_node_proxy_env(env)
    run_env.pop("PIPELINE_KRONOS_PAYLOAD", None)
    run_env["PIPELINE_KRONOS_PAYLOAD_FILE"] = str(payload_path)
    run_env["KRONOS_DIR"] = str(kronos_dir)
    timeout = int(env.get("KRONOS_TIMEOUT", "900"))
    try:
        rc, text = run_python_snippet(python_bin, KRONOS_SNIPPET, ROOT, run_env, timeout=timeout, name="kronos_context_runner")
    except subprocess.TimeoutExpired as exc:
        text = str(exc.output or "")
        context = {
            "status": "fallback",
            "error": f"Kronos 超时 {timeout}s",
            "symbols": [],
            "log_tail": text[-1500:],
        }
        write_json(OUTPUTS / "kronos_context.json", context)
        write_text(WORK / "kronos_context.log", text)
        return context

    write_text(WORK / "kronos_context.log", text)
    parsed = parse_last_json(text)
    if rc != 0 or not isinstance(parsed, dict):
        context = {
            "status": "fallback",
            "error": summarize_failure(text, rc),
            "symbols": [],
            "log_tail": text[-1500:],
        }
        write_json(OUTPUTS / "kronos_context.json", context)
        return context
    parsed.setdefault("status", "ok")
    write_json(OUTPUTS / "kronos_context.json", parsed)
    return parsed


def stage_dexter_context(candidates: list[dict[str, Any]], env: dict[str, str]) -> dict[str, Any]:
    if env.get("DEXTER_ENABLED", "0") != "1":
        context = {"status": "skipped", "symbols": []}
        write_json(OUTPUTS / "dexter_context.json", context)
        return context

    dexter_dir = config_path(env.get("DEXTER_DIR", str(ROOT / "work" / "repos" / "dexter")))
    command = env.get("DEXTER_COMMAND", "bun")
    if not dexter_dir.exists():
        context = {"status": "unavailable", "error": f"Dexter 仓库不存在：{dexter_dir}", "symbols": []}
        write_json(OUTPUTS / "dexter_context.json", context)
        return context
    if not env.get("OPENAI_API_KEY"):
        context = {"status": "unavailable", "error": "Dexter 缺少 OPENAI_API_KEY", "symbols": []}
        write_json(OUTPUTS / "dexter_context.json", context)
        return context
    missing_financial_datasets_key = (
        env.get("DEXTER_REQUIRE_FINANCIAL_DATASETS", "1") == "1"
        and not env.get("FINANCIAL_DATASETS_API_KEY")
    )
    degraded_reason = ""

    top_n = int(env.get("DEXTER_TOP_N", "10"))
    enriched = read_json_if_exists(OUTPUTS / "enriched_stock_data.json", {})
    enriched_map = {
        normalize_dsa_symbol(str(row.get("symbol") or "")): row
        for row in (enriched.get("symbols") or []) if isinstance(enriched, dict) and isinstance(row, dict)
    }
    kronos_map = build_kronos_symbol_map(read_json_if_exists(OUTPUTS / "kronos_context.json", {}))
    payload = []
    for candidate in candidates:
        if not is_us_stock_symbol(str(candidate.get("symbol") or "")):
            continue
        symbol = normalize_dsa_symbol(str(candidate.get("symbol") or ""))
        item = dict(candidate)
        shared_data = build_dexter_shared_data(item, enriched_map.get(symbol, {}), kronos_map.get(symbol, {}))
        item["shared_data"] = shared_data
        item["pipeline_data"] = shared_data.get("enriched") or {}
        payload.append(item)
        if len(payload) >= top_n:
            break
    if not payload:
        context = {"status": "no_data", "error": "没有美股候选供 Dexter 研究", "symbols": []}
        write_json(OUTPUTS / "dexter_context.json", context)
        return context

    shared_data_complete = all(dexter_shared_data_complete(item.get("shared_data") or {}) for item in payload)
    if missing_financial_datasets_key and not shared_data_complete:
        degraded_reason = "Dexter 缺少 FINANCIAL_DATASETS_API_KEY，且共享数据包财务/行情字段不完整"

    runner = ensure_dexter_runner(dexter_dir)
    run_env = prepare_node_proxy_env(env)
    payload_path = WORK / "dexter_payload.json"
    write_json(payload_path, payload)
    run_env.pop("PIPELINE_DEXTER_PAYLOAD", None)
    run_env["PIPELINE_DEXTER_PAYLOAD_FILE"] = str(payload_path)
    if degraded_reason:
        run_env["DEXTER_DEGRADED_REASON"] = degraded_reason
        run_env["DEXTER_TOOL_ALLOWLIST"] = dexter_tool_allowlist_without_financials(env.get("DEXTER_TOOL_ALLOWLIST", ""))
    run_env.setdefault("DEXTER_MODEL", env.get("OPENAI_MODEL", "gpt-4o-mini"))
    timeout = int(env.get("DEXTER_TIMEOUT", "900"))
    try:
        rc, text = run([command, "run", str(runner.relative_to(dexter_dir))], dexter_dir, run_env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        text = str(exc.output or "")
        context = {
            "status": "fallback",
            "error": f"Dexter 超时 {timeout}s",
            "symbols": [],
            "log_tail": text[-1500:],
        }
        write_json(OUTPUTS / "dexter_context.json", context)
        write_text(WORK / "dexter_context.log", text)
        return context

    write_text(WORK / "dexter_context.log", text)
    parsed = parse_last_json(text)
    if rc != 0 or not isinstance(parsed, dict):
        context = {
            "status": "fallback",
            "error": summarize_failure(text, rc),
            "symbols": [],
            "log_tail": text[-1500:],
        }
        write_json(OUTPUTS / "dexter_context.json", context)
        return context
    parsed.setdefault("status", "ok")
    if degraded_reason:
        parsed["degraded_reason"] = degraded_reason
    write_json(OUTPUTS / "dexter_context.json", parsed)
    return parsed


def prepare_node_proxy_env(env: dict[str, str]) -> dict[str, str]:
    run_env = dict(env)
    outbound_proxy = run_env.get("PIPELINE_OUTBOUND_PROXY") or run_env.get("ALL_PROXY") or run_env.get("all_proxy")
    if outbound_proxy:
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            run_env.pop(key, None)
        run_env["ALL_PROXY"] = outbound_proxy
        run_env["all_proxy"] = outbound_proxy
    return run_env


def dexter_shared_data_complete(shared_data: dict[str, Any]) -> bool:
    enriched = shared_data.get("enriched") or {}
    candidate = shared_data.get("candidate") or {}
    fundamentals = enriched.get("fundamentals") or {}
    has_price = bool(enriched.get("close") or candidate.get("close"))
    has_market_context = bool(enriched.get("recent_klines") or enriched.get("technical"))
    has_fundamentals = any(
        fundamentals.get(key)
        for key in ("pe", "pb", "market_cap_yi", "revenue_history", "net_profit_history", "roe_history", "financial_health")
    )
    return has_price and has_market_context and has_fundamentals


def dexter_tool_allowlist_without_financials(value: str) -> str:
    tools = [
        item.strip()
        for item in str(value or "get_financials,get_market_data,read_filings,web_search").split(",")
        if item.strip() and item.strip() != "get_financials"
    ]
    if not tools:
        tools = ["get_market_data", "read_filings", "web_search"]
    return ",".join(dict.fromkeys(tools))


def build_dexter_shared_data(
    candidate: dict[str, Any],
    enriched_row: dict[str, Any],
    kronos_row: dict[str, Any],
) -> dict[str, Any]:
    enriched_compact = compact_agent_data(enriched_row)
    candidate_compact = {
        "symbol": candidate.get("symbol"),
        "name": candidate.get("name"),
        "score": candidate.get("score"),
        "status": candidate.get("status"),
        "close": candidate.get("close"),
        "ret_20d": candidate.get("ret_20d"),
        "volume_ratio": candidate.get("volume_ratio"),
        "reason": candidate.get("reason"),
    }
    return {
        "candidate": candidate_compact,
        "enriched": enriched_compact,
        "kronos": {
            "trend": kronos_row.get("trend"),
            "forecast_return_5d": kronos_row.get("forecast_return_5d"),
            "confidence": kronos_row.get("confidence"),
            "model": kronos_row.get("model"),
        } if kronos_row else {},
        "external_signal": candidate.get("external_signal") or {},
        "serenity_signal": candidate.get("serenity_signal") or {},
        "data_quality": {
            "field_sources": enriched_row.get("field_sources") or {},
            "flags": enriched_row.get("data_quality_flags") or [],
            "provider": enriched_row.get("provider") or "",
        },
    }


def ensure_dexter_runner(dexter_dir: Path) -> Path:
    runner = dexter_dir / "scripts" / "pipeline_research.ts"
    content = DEXTER_BATCH_RUNNER.strip() + "\n"
    if not runner.exists() or runner.read_text(encoding="utf-8", errors="ignore") != content:
        runner.parent.mkdir(parents=True, exist_ok=True)
        runner.write_text(content, encoding="utf-8")
    return runner


def is_us_stock_symbol(symbol: str) -> bool:
    s = normalize_dsa_symbol(symbol)
    if not s or s.lower().startswith("hk") or re.fullmatch(r"\d+", s):
        return False
    return bool(re.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", s))


DEXTER_BATCH_RUNNER = r'''
import { config } from 'dotenv';
import { readFileSync } from 'node:fs';

config();

async function installOutboundProxyFetch() {
  const outboundProxy = process.env.PIPELINE_OUTBOUND_PROXY || process.env.ALL_PROXY || process.env.all_proxy;
  if (!outboundProxy || !outboundProxy.startsWith('socks')) return;

  const { SocksProxyAgent } = await import('socks-proxy-agent');
  const dispatcher = new SocksProxyAgent(outboundProxy);
  const nativeFetch = globalThis.fetch;
  globalThis.fetch = (input, init = {}) => nativeFetch(input, { ...init, dispatcher });
}

await installOutboundProxyFetch();
const { initializeOutboundProxyFetch } = await import('../src/model/llm.js');
await initializeOutboundProxyFetch();
const { Agent } = await import('../src/agent/index.js');

const payloadText = process.env.PIPELINE_DEXTER_PAYLOAD_FILE
  ? readFileSync(process.env.PIPELINE_DEXTER_PAYLOAD_FILE, 'utf8')
  : (process.env.PIPELINE_DEXTER_PAYLOAD || '[]');
const payload = JSON.parse(payloadText);
const model = process.env.DEXTER_MODEL || process.env.OPENAI_MODEL || 'gpt-4o-mini';
const maxIterations = Number(process.env.DEXTER_MAX_ITERATIONS || '4');
const workers = Math.max(1, Number(process.env.DEXTER_WORKERS || '4'));
const toolAllowlist = (process.env.DEXTER_TOOL_ALLOWLIST || 'get_financials,get_market_data,read_filings,web_search')
  .split(',')
  .map((x) => x.trim())
  .filter(Boolean);

const systemPrompt = `你是美股辅助研究员。只输出中文。你的任务是补充基本面、财务质量、近期催化、风险和未来1-3个月观察点。你只能输出JSON，不要输出Markdown。`;

function parseJsonAnswer(answer) {
  const text = String(answer || '').trim();
  try {
    return JSON.parse(text);
  } catch (_) {
    const match = text.match(/\{[\s\S]*\}/);
    if (match) {
      try {
        return JSON.parse(match[0]);
      } catch (_) {}
    }
  }
  return null;
}

async function researchOne(item) {
  const symbol = String(item.symbol || '').toUpperCase();
  const name = String(item.name || '');
  const sharedData = item.shared_data ? JSON.stringify(item.shared_data).slice(0, 12000) : '{}';
  const query = `请研究美股 ${symbol} ${name}。结合可用财务、价格、公司文件和新闻资料，输出严格JSON：
系统已经提供一份共享数据包，里面包含 daily_stock_analysis 初筛、OpenBB/enriched 行情财务、Kronos 趋势、外部/Serenity 信号、字段来源和数据质量标记。
你必须把这份共享数据包视为正式数据源并优先使用；外部工具只作为补充，取数失败不影响基于共享数据完成研究。data_sources_used 中不要把共享数据称为 fallback。
共享数据如下：
${sharedData}

{
  "symbol": "${symbol}",
  "name": "${name}",
  "stance": "看多|中性|谨慎",
  "confidence": 0.0到1.0,
  "data_sources_used": ["最多5个实际使用的数据源，例如shared_data.enriched.fundamentals、shared_data.kronos"],
  "key_points": ["最多4条选择理由"],
  "risks": ["最多4条主要风险"],
  "watch_items": ["未来1-3个月最多4个观察点"],
  "summary": "120字以内中文总结"
}
不要加入JSON之外的文字。`;
  const agent = await Agent.create({
    model,
    maxIterations,
    memoryEnabled: false,
    toolAllowlist,
    systemPromptOverride: systemPrompt,
  });
  let answer = '';
  let iterations = 0;
  for await (const event of agent.run(query)) {
    if (event.type === 'done') {
      answer = event.answer || '';
      iterations = event.iterations || 0;
    }
  }
  const parsed = parseJsonAnswer(answer);
  if (parsed && typeof parsed === 'object') {
    return {
      symbol,
      name,
      status: 'ok',
      stance: parsed.stance || '中性',
      confidence: Number(parsed.confidence || 0),
      data_sources_used: Array.isArray(parsed.data_sources_used) ? parsed.data_sources_used.slice(0, 5) : [],
      key_points: Array.isArray(parsed.key_points) ? parsed.key_points.slice(0, 4) : [],
      risks: Array.isArray(parsed.risks) ? parsed.risks.slice(0, 4) : [],
      watch_items: Array.isArray(parsed.watch_items) ? parsed.watch_items.slice(0, 4) : [],
      summary: String(parsed.summary || '').slice(0, 220),
      iterations,
    };
  }
  return {
    symbol,
    name,
    status: 'fallback',
    stance: '中性',
    confidence: 0,
    key_points: [],
    risks: [],
    watch_items: [],
    summary: answer.slice(0, 220),
    iterations,
  };
}

const results = new Array(payload.length);
let nextIndex = 0;

async function runWorker() {
  while (true) {
    const index = nextIndex++;
    if (index >= payload.length) return;
    const item = payload[index];
    try {
      results[index] = await researchOne(item);
    } catch (error) {
      results[index] = {
        symbol: String(item.symbol || '').toUpperCase(),
        name: String(item.name || ''),
        status: 'fallback',
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }
}

await Promise.all(
  Array.from({ length: Math.min(workers, Math.max(1, payload.length)) }, () => runWorker()),
);

console.log('__PIPELINE_JSON__');
console.log(JSON.stringify({
  status: results.some((x) => x.status === 'ok') ? 'ok' : 'fallback',
  model,
  symbol_count: results.filter((x) => x.status === 'ok').length,
  symbols: results,
}, null, 0));
'''


OPENBB_SNIPPET = r'''
import json, os, math, datetime as dt, socket, re, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import subprocess, shutil
from openbb import obb

pool = json.loads(os.environ.get("PIPELINE_OPENBB_POOL", "[]"))
limit = int(os.environ.get("PIPELINE_OPENBB_SYMBOL_LIMIT", str(len(pool) or 300)))
providers = [
    p.strip()
    for p in os.environ.get("OPENBB_PRICE_PROVIDER", "fmp,tiingo,polygon,alpha_vantage,yfinance").split(",")
    if p.strip()
]
hk_providers = [
    p.strip()
    for p in os.environ.get("OPENBB_HK_PRICE_PROVIDER", "yfinance").split(",")
    if p.strip()
]
benchmarks = [s.strip() for s in os.environ.get("PIPELINE_OPENBB_BENCHMARKS", "SPY,QQQ,DIA,IWM").split(",") if s.strip()]
GLOBAL_STOCK_DATA_ENABLED = os.environ.get("GLOBAL_STOCK_DATA_ENABLED", "1") == "1"
GLOBAL_STOCK_DATA_FIRST = os.environ.get("GLOBAL_STOCK_DATA_FIRST", "0") == "1"
GLOBAL_STOCK_DATA_TIMEOUT = float(os.environ.get("GLOBAL_STOCK_DATA_TIMEOUT", "12"))
KRONOS_LOOKBACK = int(os.environ.get("PIPELINE_KRONOS_LOOKBACK", os.environ.get("KRONOS_LOOKBACK", "90")))
ALPHA_VANTAGE_API_KEY = (
    os.environ.get("ALPHA_VANTAGE_API_KEY")
    or os.environ.get("ALPHAVANTAGE_API_KEY")
    or os.environ.get("ALPHA_API_KEY")
    or ""
)
STOCK_API_ENABLED = os.environ.get("STOCK_API_ENABLED", "1") == "1"
STOCK_API_COMMAND = os.environ.get("STOCK_API_COMMAND", "npx")
STOCK_API_TIMEOUT = float(os.environ.get("STOCK_API_TIMEOUT", "18"))
TICKDB_API_KEY = os.environ.get("TICKDB_API_KEY") or os.environ.get("TICKDB_KEY") or ""
TICKDB_API_BASE = os.environ.get("TICKDB_API_BASE", "https://api.tickdb.ai").rstrip("/")
OPENBB_WORKERS = max(1, int(os.environ.get("PIPELINE_OPENBB_WORKERS", "12")))
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

def yahoo_symbol(symbol):
    s = str(symbol or "").strip()
    if s.lower().startswith("hk"):
        return s[2:].zfill(4) + ".HK"
    return s.upper()

def provider_candidates(symbol):
    return hk_providers if str(symbol).upper().endswith(".HK") else providers

def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        value = float(value)
        return default if math.isnan(value) else value
    except Exception:
        return default

def valid_positive_number(value):
    try:
        if value is None:
            return None
        number = float(value)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number) or number <= 0:
        return None
    return number

def http_get(url, *, params=None, headers=None, timeout=None):
    merged_headers = {"User-Agent": UA, **(headers or {})}
    r = requests.get(url, params=params, headers=merged_headers, timeout=timeout or GLOBAL_STOCK_DATA_TIMEOUT)
    r.raise_for_status()
    return r

def hk_code_from_symbol(symbol):
    return str(symbol).split(".", 1)[0].zfill(5)

def global_yahoo_kline(symbol, range_="6mo", interval="1d"):
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
    r = http_get(url, params={"interval": interval, "range": range_})
    data = r.json()
    result = (data.get("chart") or {}).get("result") or []
    if not result:
        err = (data.get("chart") or {}).get("error")
        raise RuntimeError(f"Yahoo chart 无结果：{err}")
    chart = result[0]
    timestamps = chart.get("timestamp") or []
    quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
    rows = []
    for idx, ts in enumerate(timestamps):
        close = safe_float((quote.get("close") or [None])[idx] if idx < len(quote.get("close") or []) else None)
        if close <= 0:
            continue
        rows.append({
            "date": dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d"),
            "open": safe_float((quote.get("open") or [None])[idx] if idx < len(quote.get("open") or []) else None),
            "high": safe_float((quote.get("high") or [None])[idx] if idx < len(quote.get("high") or []) else None),
            "low": safe_float((quote.get("low") or [None])[idx] if idx < len(quote.get("low") or []) else None),
            "close": close,
            "volume": safe_float((quote.get("volume") or [None])[idx] if idx < len(quote.get("volume") or []) else None),
        })
    if not rows:
        raise RuntimeError("Yahoo chart 返回空 K 线")
    return rows

def global_us_kline_sina(ticker):
    url = "https://stock.finance.sina.com.cn/usstock/api/jsonp.php/var/US_MinKService.getDailyK"
    r = http_get(url, params={"symbol": ticker.upper(), "num": int(os.environ.get("PIPELINE_OPENBB_LOOKBACK_DAYS", "45")) + 20}, headers={"Referer": "https://finance.sina.com.cn/"})
    match = re.search(r"\((\[.+\])\)", r.text)
    if not match:
        raise RuntimeError("新浪美股 K 线 JSONP 解析失败")
    items = json.loads(match.group(1))
    rows = []
    for item in items:
        close = safe_float(item.get("c"))
        if close <= 0:
            continue
        rows.append({
            "date": item.get("d"),
            "open": safe_float(item.get("o")),
            "high": safe_float(item.get("h")),
            "low": safe_float(item.get("l")),
            "close": close,
            "volume": safe_float(item.get("v")),
        })
    if not rows:
        raise RuntimeError("新浪美股 K 线为空")
    return rows

def global_hk_quote_tencent(code):
    r = http_get(f"https://qt.gtimg.cn/q=r_hk{code}", timeout=8)
    r.encoding = "gbk"
    match = re.search(r'"(.+)"', r.text)
    if not match:
        raise RuntimeError("腾讯港股行情解析失败")
    fields = match.group(1).split("~")
    if len(fields) < 40:
        raise RuntimeError("腾讯港股字段不足")
    return {
        "close": safe_float(fields[3]),
        "open": safe_float(fields[5]),
        "high": safe_float(fields[33]),
        "low": safe_float(fields[34]),
        "volume": safe_float(fields[6]),
        "change_pct": safe_float(fields[32]),
        "name": fields[1],
        "provider": "global/tencent",
    }

def global_hk_quote_sina(code):
    r = http_get(f"https://hq.sinajs.cn/list=rt_hk{code}", headers={"Referer": "https://finance.sina.com.cn/"}, timeout=8)
    r.encoding = "gbk"
    match = re.search(r'"(.+)"', r.text)
    if not match:
        raise RuntimeError("新浪港股行情解析失败")
    fields = match.group(1).split(",")
    if len(fields) < 13:
        raise RuntimeError("新浪港股字段不足")
    return {
        "close": safe_float(fields[6]),
        "open": safe_float(fields[2]),
        "high": safe_float(fields[4]),
        "low": safe_float(fields[5]),
        "volume": safe_float(fields[12]),
        "change_pct": safe_float(fields[8]),
        "name": fields[1],
        "provider": "global/sina",
    }

def global_eastmoney_quote(code, prefix):
    r = http_get("https://push2.eastmoney.com/api/qt/stock/get", params={
        "secid": f"{prefix}.{code}",
        "fields": "f43,f44,f45,f46,f47,f48,f55,f57,f58,f59,f60,f170",
    }, timeout=8)
    data = (r.json() or {}).get("data")
    if not data:
        raise RuntimeError("东财 push2 无数据")
    dec = int(data.get("f59") or 3)
    divisor = 10 ** dec
    def price(key):
        val = data.get(key)
        if val is None or val == "-":
            return 0.0
        return safe_float(val) / divisor
    return {
        "close": price("f43"),
        "open": price("f46"),
        "high": price("f44"),
        "low": price("f45"),
        "volume": safe_float(data.get("f47")),
        "change_pct": safe_float(data.get("f170")) / 100,
        "name": data.get("f58") or "",
        "provider": "global/eastmoney",
    }

def alpha_vantage_daily(symbol):
    if not ALPHA_VANTAGE_API_KEY:
        raise RuntimeError("Alpha Vantage key 未配置")
    if str(symbol).upper().endswith(".HK"):
        raise RuntimeError("Alpha Vantage 仅作为美股日线源使用")
    ticker = str(symbol).upper().replace(".US", "")
    r = http_get(
        "https://www.alphavantage.co/query",
        params={
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": ticker,
            "outputsize": "compact",
            "apikey": ALPHA_VANTAGE_API_KEY,
        },
        timeout=18,
    )
    data = r.json()
    series = data.get("Time Series (Daily)") or data.get("Time Series (Daily Adjusted)") or {}
    if not series:
        message = data.get("Note") or data.get("Information") or data.get("Error Message") or "无日线"
        raise RuntimeError(f"Alpha Vantage 返回异常：{str(message)[:120]}")
    rows = []
    for date, item in sorted(series.items()):
        close = safe_float(item.get("5. adjusted close") or item.get("4. close"))
        if close <= 0:
            continue
        rows.append({
            "date": date,
            "open": safe_float(item.get("1. open")),
            "high": safe_float(item.get("2. high")),
            "low": safe_float(item.get("3. low")),
            "close": close,
            "volume": safe_float(item.get("6. volume") or item.get("5. volume")),
        })
    if not rows:
        raise RuntimeError("Alpha Vantage 日线为空")
    return rows

def stock_api_code(symbol):
    s = str(symbol).strip()
    if s.upper().endswith(".HK"):
        return "HK" + s.split(".", 1)[0].zfill(5)
    if re.fullmatch(r"\d{6}\.(SS|SH)", s.upper()):
        return "SH" + s[:6]
    if re.fullmatch(r"\d{6}\.SZ", s.upper()):
        return "SZ" + s[:6]
    return "US" + s.upper().replace(".US", "")

def stock_api_cli_klines(symbol):
    if not STOCK_API_ENABLED:
        raise RuntimeError("stock-api disabled")
    if STOCK_API_COMMAND == "npx":
        cmd = ["npx", "-y", "stock-api", "get-klines", stock_api_code(symbol), "--period", "day", "--count", str(int(os.environ.get("PIPELINE_OPENBB_LOOKBACK_DAYS", "45")) + 20)]
    else:
        if not shutil.which(STOCK_API_COMMAND):
            raise RuntimeError(f"stock-api command 不存在：{STOCK_API_COMMAND}")
        cmd = [STOCK_API_COMMAND, "get-klines", stock_api_code(symbol), "--period", "day", "--count", str(int(os.environ.get("PIPELINE_OPENBB_LOOKBACK_DAYS", "45")) + 20)]
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=STOCK_API_TIMEOUT)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "stock-api CLI 失败")[:180])
    data = json.loads(proc.stdout)
    if not isinstance(data, list) or not data:
        raise RuntimeError("stock-api 返回空 K 线")
    rows = []
    for item in data:
        close = safe_float(item.get("close") or item.get("now"))
        if close <= 0:
            continue
        rows.append({
            "date": item.get("date") or dt.date.today().isoformat(),
            "open": safe_float(item.get("open")),
            "high": safe_float(item.get("high")),
            "low": safe_float(item.get("low")),
            "close": close,
            "volume": safe_float(item.get("volume")),
        })
    if not rows:
        raise RuntimeError("stock-api 无有效收盘价")
    return rows

def tickdb_symbol(symbol):
    s = str(symbol).strip()
    if s.upper().endswith(".HK"):
        return s.split(".", 1)[0].zfill(3) + ".HK"
    if s.upper().endswith(".US"):
        return s.upper()
    if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", s.upper()):
        return s.upper() + ".US"
    return s.upper()

def normalize_kline_payload(data):
    payload = data.get("data") if isinstance(data, dict) else data
    if isinstance(payload, dict):
        for key in ("items", "klines", "candles", "list", "rows"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
    if not isinstance(payload, list):
        raise RuntimeError("K 线响应格式不可识别")
    rows = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        close = safe_float(item.get("close") or item.get("c"))
        if close <= 0:
            continue
        ts = item.get("date") or item.get("time") or item.get("timestamp") or item.get("t") or ""
        if isinstance(ts, (int, float)):
            ts = dt.datetime.fromtimestamp(ts / 1000 if ts > 10**12 else ts).strftime("%Y-%m-%d")
        rows.append({
            "date": str(ts)[:10] or dt.date.today().isoformat(),
            "open": safe_float(item.get("open") or item.get("o")),
            "high": safe_float(item.get("high") or item.get("h")),
            "low": safe_float(item.get("low") or item.get("l")),
            "close": close,
            "volume": safe_float(item.get("volume") or item.get("v")),
        })
    if not rows:
        raise RuntimeError("K 线响应无有效收盘价")
    return rows

def tickdb_kline(symbol):
    if not TICKDB_API_KEY:
        raise RuntimeError("TickDB key 未配置")
    r = http_get(
        f"{TICKDB_API_BASE}/v1/market/kline",
        params={"symbol": tickdb_symbol(symbol), "interval": "1d", "limit": int(os.environ.get("PIPELINE_OPENBB_LOOKBACK_DAYS", "45")) + 20},
        headers={"X-API-Key": TICKDB_API_KEY},
        timeout=18,
    )
    return normalize_kline_payload(r.json())

def summarize_klines(symbol, rows, provider):
    if len(rows) < 20:
        raise RuntimeError(f"{provider} K 线数量不足：{len(rows)}")
    rows = normalize_us_kline_scale(symbol, rows, provider)
    dates: list[dt.date] = []
    for item in rows:
        raw_date = str(item.get("date") or "")[:10]
        try:
            dates.append(dt.date.fromisoformat(raw_date))
        except Exception:
            pass
    if len(dates) >= 2 and (dates[-1] - dates[-min(len(dates), 22)]).days > 60:
        raise RuntimeError(f"{provider} K 线日期不连续，疑似旧数据混入")
    closes = [safe_float(x.get("close")) for x in rows[-22:]]
    closes = [x for x in closes if x > 0]
    if not closes:
        raise RuntimeError(f"{provider} 无有效收盘价")
    ret_20d = ((closes[-1] / closes[0] - 1) * 100) if len(closes) >= 2 and closes[0] else 0.0
    if abs(ret_20d) > float(os.environ.get("PIPELINE_MAX_REASONABLE_RET20D", "80")):
        raise RuntimeError(f"{provider} 20日涨幅异常：{ret_20d:.1f}%")
    volumes = [valid_positive_number(x.get("volume")) for x in rows[-22:]]
    volumes = [x for x in volumes if x is not None]
    latest_volume = volumes[-1] if volumes else 0.0
    avg_volume = sum(volumes[:-1]) / max(1, len(volumes) - 1) if len(volumes) > 1 else 0.0
    volume_ratio = latest_volume / avg_volume if avg_volume else None
    compact_rows = []
    for item in rows[-KRONOS_LOOKBACK:]:
        close = safe_float(item.get("close"))
        if close <= 0:
            continue
        compact_rows.append({
            "date": item.get("date", ""),
            "open": round(safe_float(item.get("open"), close), 6),
            "high": round(safe_float(item.get("high"), close), 6),
            "low": round(safe_float(item.get("low"), close), 6),
            "close": round(close, 6),
            "volume": round(safe_float(item.get("volume")), 6),
            "amount": round(safe_float(item.get("amount")), 6),
        })
    return {
        "symbol": symbol,
        "close": round(closes[-1], 4),
        "ret_20d": round(ret_20d, 2),
        "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
        "rows": len(rows),
        "provider": provider,
        "date": rows[-1].get("date", ""),
        "klines": compact_rows,
    }

def normalize_us_kline_scale(symbol, rows, provider):
    """Correct known 10x USD price scaling from some global-stock-data US feeds."""
    ticker = str(symbol or "").upper()
    if ticker.endswith(".HK") or ticker.startswith("HK"):
        return rows
    closes = [safe_float(x.get("close")) for x in rows[-22:] if safe_float(x.get("close")) > 0]
    if not closes:
        return rows
    latest = closes[-1]
    scale = infer_us_price_scale(ticker, latest, provider)
    if scale == 1:
        return rows
    out = []
    for item in rows:
        fixed = dict(item)
        for key in ("open", "high", "low", "close"):
            val = safe_float(fixed.get(key))
            if val > 0:
                fixed[key] = val / scale
        out.append(fixed)
    return out

def infer_us_price_scale(ticker, latest, provider):
    if latest <= 0:
        return 1
    normalized = ticker.replace(".US", "")
    forced = {x.strip().upper() for x in os.environ.get("PIPELINE_FORCE_US_PRICE_DIV10_TICKERS", "").split(",") if x.strip()}
    if normalized in forced:
        return 10
    known_div10 = set()
    if normalized in known_div10 and latest >= 1000:
        return 10
    known_high_price = {
        "ASML", "AZO", "BKNG", "BLK", "BRK.A", "BRK.B", "CMG", "COST",
        "FICO", "GS", "LMT", "LLY", "MELI", "MSTR", "NVR", "REGN",
    }
    if normalized in known_high_price:
        return 1
    return 1

def summarize_quote_only(symbol, quote):
    close = safe_float(quote.get("close"))
    if close <= 0:
        raise RuntimeError(f"{quote.get('provider', 'global')} 行情无有效价格")
    return {
        "symbol": symbol,
        "close": round(close, 4),
        "ret_20d": round(safe_float(quote.get("change_pct")), 2),
        "volume_ratio": 1.0,
        "rows": 1,
        "provider": quote.get("provider", "global/quote"),
        "date": dt.date.today().isoformat(),
        "quote_only": True,
    }

def global_stock_data_summary(symbol):
    if not GLOBAL_STOCK_DATA_ENABLED:
        raise RuntimeError("global-stock-data fallback disabled")
    if str(symbol).upper().endswith(".HK"):
        code = hk_code_from_symbol(symbol)
        failures = []
        for fetcher in (
            lambda: summarize_klines(symbol, tickdb_kline(symbol), "tickdb/kline"),
            lambda: summarize_klines(symbol, stock_api_cli_klines(symbol), "stock-api/auto"),
            lambda: summarize_quote_only(symbol, global_hk_quote_tencent(code)),
            lambda: summarize_quote_only(symbol, global_hk_quote_sina(code)),
            lambda: summarize_quote_only(symbol, global_eastmoney_quote(code, 116)),
            lambda: summarize_klines(symbol, global_yahoo_kline(symbol, range_="6mo", interval="1d"), "global/yahoo_chart"),
        ):
            try:
                return fetcher()
            except Exception as exc:
                failures.append(str(exc)[:140])
        raise RuntimeError("global-stock-data 港股源均失败：" + " | ".join(failures[:4]))

    ticker = str(symbol).upper()
    failures = []
    for fetcher in (
        lambda: summarize_klines(symbol, tickdb_kline(ticker), "tickdb/kline"),
        lambda: summarize_klines(symbol, alpha_vantage_daily(ticker), "alpha_vantage/direct"),
        lambda: summarize_klines(symbol, stock_api_cli_klines(ticker), "stock-api/auto"),
        lambda: summarize_klines(symbol, global_us_kline_sina(ticker), "global/sina_kline"),
        lambda: summarize_quote_only(symbol, global_eastmoney_quote(ticker, 105)),
        lambda: summarize_quote_only(symbol, global_eastmoney_quote(ticker, 106)),
        lambda: summarize_quote_only(symbol, global_eastmoney_quote(ticker, 107)),
        lambda: summarize_klines(symbol, global_yahoo_kline(ticker, range_="6mo", interval="1d"), "global/yahoo_chart"),
    ):
        try:
            return fetcher()
        except Exception as exc:
            failures.append(str(exc)[:140])
    raise RuntimeError("global-stock-data 美股源均失败：" + " | ".join(failures[:5]))

def historical(symbol, provider):
    start_date = (dt.date.today() - dt.timedelta(days=int(os.environ.get("PIPELINE_OPENBB_LOOKBACK_DAYS", "45")))).isoformat()
    data = obb.equity.price.historical(symbol=symbol, start_date=start_date, provider=provider)
    if hasattr(data, "to_df"):
        return data.to_df()
    if hasattr(data, "results"):
        import pandas as pd
        return pd.DataFrame([x.model_dump() if hasattr(x, "model_dump") else dict(x) for x in data.results])
    raise RuntimeError("OpenBB 返回格式不可识别")

def futu_historical(symbol):
    if not str(symbol).upper().endswith(".HK"):
        raise RuntimeError("Futu 仅作为港股行情源使用")
    try:
        from futu import OpenQuoteContext, RET_OK, KLType, AuType
    except Exception as exc:
        raise RuntimeError("futu-api 未安装：" + str(exc)) from exc

    host = os.environ.get("FUTU_OPEND_HOST", "127.0.0.1")
    port = int(os.environ.get("FUTU_OPEND_PORT", "11111"))
    socket.create_connection((host, port), timeout=float(os.environ.get("FUTU_CONNECT_TIMEOUT", "3"))).close()
    start_date = (dt.date.today() - dt.timedelta(days=int(os.environ.get("PIPELINE_OPENBB_LOOKBACK_DAYS", "45")))).isoformat()
    end_date = dt.date.today().isoformat()
    code = "HK." + str(symbol).split(".", 1)[0].zfill(5)
    quote_ctx = OpenQuoteContext(host=host, port=port)
    try:
        ret, data, _ = quote_ctx.request_history_kline(
            code,
            start=start_date,
            end=end_date,
            ktype=KLType.K_DAY,
            autype=AuType.QFQ,
            max_count=1000,
        )
        if ret != RET_OK:
            raise RuntimeError(str(data))
        if data is None or len(data) == 0:
            raise RuntimeError("Futu OpenD 返回空 K 线")
        return data, "futu"
    finally:
        quote_ctx.close()

def summarize(symbol):
    failures = []
    if GLOBAL_STOCK_DATA_FIRST and GLOBAL_STOCK_DATA_ENABLED:
        try:
            return global_stock_data_summary(symbol)
        except Exception as exc:
            failures.append({"provider": "global-stock-data", "error": str(exc)[:180]})

    if str(symbol).upper().endswith(".HK") and GLOBAL_STOCK_DATA_ENABLED:
        try:
            return global_stock_data_summary(symbol)
        except Exception as exc:
            failures.append({"provider": "global-stock-data", "error": str(exc)[:180]})

    if str(symbol).upper().endswith(".HK") and os.environ.get("FUTU_ENABLED", "0") == "1":
        try:
            df, provider = futu_historical(symbol)
        except Exception as exc:
            failures.append({"provider": "futu", "error": str(exc)[:180]})
        else:
            pass
            # Skip generic provider loop when Futu succeeds.
            if df is not None and len(df) > 0:
                return summarize_dataframe(symbol, df, provider)

    for provider in provider_candidates(symbol):
        try:
            df = historical(symbol, provider)
            if df is None or len(df) == 0:
                raise RuntimeError("无价格数据")
            break
        except Exception as exc:
            failures.append({"provider": provider, "error": str(exc)[:180]})
    else:
        if GLOBAL_STOCK_DATA_ENABLED:
            try:
                return global_stock_data_summary(symbol)
            except Exception as exc:
                failures.append({"provider": "global-stock-data", "error": str(exc)[:180]})
        raise RuntimeError("所有行情 provider 均失败：" + json.dumps(failures[:6], ensure_ascii=False))

    if df is None or len(df) == 0:
        raise RuntimeError("无价格数据")
    return summarize_dataframe(symbol, df, provider)

def summarize_dataframe(symbol, df, provider):
    close_col = "close" if "close" in df.columns else "Close"
    volume_col = "volume" if "volume" in df.columns else "Volume" if "Volume" in df.columns else None
    closes = [safe_float(x) for x in df[close_col].tail(22).tolist()]
    closes = [x for x in closes if x > 0]
    latest = closes[-1] if closes else 0.0
    ret_20d = ((closes[-1] / closes[0] - 1) * 100) if len(closes) >= 2 and closes[0] else 0.0
    volumes = [safe_float(x) for x in df[volume_col].tail(22).tolist()] if volume_col else []
    latest_volume = volumes[-1] if volumes else 0.0
    avg_volume = sum(volumes[:-1]) / max(1, len(volumes) - 1) if len(volumes) > 1 else 0.0
    volume_ratio = latest_volume / avg_volume if avg_volume else 1.0
    return {
        "symbol": symbol,
        "close": round(latest, 4),
        "ret_20d": round(ret_20d, 2),
        "volume_ratio": round(volume_ratio, 2),
        "rows": int(len(df)),
        "provider": provider,
    }

def summarize_item(item):
    original = item.get("symbol", "")
    y_symbol = yahoo_symbol(original)
    try:
        row = summarize(y_symbol)
        row.update({"original_symbol": original, "name": item.get("name", ""), "group": item.get("group", "")})
        return row, None
    except Exception as exc:
        return None, {"symbol": original, "openbb_symbol": y_symbol, "error": str(exc)[:220]}

symbols = []
errors = []
items = pool[:limit]
with ThreadPoolExecutor(max_workers=OPENBB_WORKERS) as executor:
    future_map = {executor.submit(summarize_item, item): item for item in items}
    for future in as_completed(future_map):
        row, err = future.result()
        if row:
            symbols.append(row)
        if err:
            errors.append(err)

symbol_order = {item.get("symbol", ""): idx for idx, item in enumerate(items)}
symbols.sort(key=lambda row: symbol_order.get(row.get("original_symbol", ""), 10**9))

benchmark_rows = []
for symbol in benchmarks:
    try:
        benchmark_rows.append(summarize(symbol))
    except Exception as exc:
        errors.append({"symbol": symbol, "error": str(exc)[:220]})

print("__PIPELINE_JSON__")
print(json.dumps({
    "status": "ok",
    "providers": providers,
    "global_stock_data_enabled": GLOBAL_STOCK_DATA_ENABLED,
    "global_stock_data_first": GLOBAL_STOCK_DATA_FIRST,
    "auxiliary_data_sources": {
        "alpha_vantage_direct": bool(ALPHA_VANTAGE_API_KEY),
        "stock_api_cli": STOCK_API_ENABLED,
        "tickdb": bool(TICKDB_API_KEY),
    },
    "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    "symbol_count": len(symbols),
    "error_count": len(errors),
    "symbols": symbols,
    "benchmarks": benchmark_rows,
    "errors": errors[:50],
}, ensure_ascii=False))
'''


KRONOS_SNIPPET = r'''
import json, os, sys, math
import json as jsonlib
import pandas as pd
from pathlib import Path

kronos_dir = os.environ["KRONOS_DIR"]
if kronos_dir not in sys.path:
    sys.path.insert(0, kronos_dir)

from huggingface_hub import hf_hub_download
from safetensors.torch import load_file
from model import Kronos, KronosTokenizer, KronosPredictor

payload_file = os.environ.get("PIPELINE_KRONOS_PAYLOAD_FILE")
if payload_file:
    payload = json.loads(Path(payload_file).read_text(encoding="utf-8"))
else:
    payload = json.loads(os.environ.get("PIPELINE_KRONOS_PAYLOAD", "[]"))
tokenizer_name = os.environ.get("KRONOS_TOKENIZER", "NeoQuasar/Kronos-Tokenizer-base")
model_name = os.environ.get("KRONOS_MODEL", "NeoQuasar/Kronos-small")
device = os.environ.get("KRONOS_DEVICE", "cpu")
max_context = int(os.environ.get("KRONOS_MAX_CONTEXT", "512"))
pred_len = int(os.environ.get("KRONOS_PRED_LEN", "5"))
sample_count = int(os.environ.get("KRONOS_SAMPLE_COUNT", "1"))
temperature = float(os.environ.get("KRONOS_TEMPERATURE", "1.0"))
top_p = float(os.environ.get("KRONOS_TOP_P", "0.9"))

def load_component(repo_id, cls):
    local_path = Path(repo_id)
    if local_path.exists():
        config_path = local_path / "config.json"
        weights_path = local_path / "model.safetensors"
    else:
        config_path = Path(hf_hub_download(repo_id=repo_id, filename="config.json"))
        weights_path = Path(hf_hub_download(repo_id=repo_id, filename="model.safetensors"))
    with open(config_path, "r", encoding="utf-8") as fh:
        config = jsonlib.load(fh)
    component = cls(**config)
    state = load_file(weights_path, device=device)
    component.load_state_dict(state, strict=True)
    component.to(device)
    component.eval()
    return component

tokenizer = load_component(tokenizer_name, KronosTokenizer)
model = load_component(model_name, Kronos)
predictor = KronosPredictor(model, tokenizer, device=device, max_context=max_context)

def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        value = float(value)
        return default if math.isnan(value) else value
    except Exception:
        return default

def forecast_one(row):
    rows = row.get("klines") or []
    df = pd.DataFrame(rows)
    required = ["open", "high", "low", "close"]
    if df.empty or any(col not in df.columns for col in required):
        raise RuntimeError("Kronos 输入缺少 OHLC")
    df["timestamps"] = pd.to_datetime(df.get("date"), errors="coerce")
    df = df.dropna(subset=["timestamps"])
    if len(df) < 20:
        raise RuntimeError("Kronos 输入 K 线不足")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = df.tail(max_context).reset_index(drop=True)
    last_close = safe_float(df["close"].iloc[-1])
    last_ts = df["timestamps"].iloc[-1]
    y_timestamp = pd.Series(pd.date_range(last_ts + pd.Timedelta(days=1), periods=pred_len, freq="D"))
    pred_df = predictor.predict(
        df=df[["open", "high", "low", "close", "volume", "amount"]],
        x_timestamp=df["timestamps"],
        y_timestamp=y_timestamp,
        pred_len=pred_len,
        T=temperature,
        top_p=top_p,
        sample_count=sample_count,
    )
    pred_close = safe_float(pred_df["close"].iloc[-1])
    forecast_return = ((pred_close / last_close - 1) * 100) if last_close > 0 else 0.0
    if forecast_return >= 2:
        trend = "up"
    elif forecast_return <= -2:
        trend = "down"
    else:
        trend = "flat"
    confidence = min(1.0, abs(forecast_return) / 8.0)
    return {
        "symbol": row.get("original_symbol") or row.get("symbol"),
        "market_symbol": row.get("symbol"),
        "provider": row.get("provider"),
        "status": "ok",
        "lookback": int(len(df)),
        "pred_len": pred_len,
        "last_close": round(last_close, 6),
        "forecast_close": round(pred_close, 6),
        "forecast_return_5d": round(forecast_return, 4),
        "trend": trend,
        "confidence": round(confidence, 4),
    }

symbols = []
errors = []
for row in payload:
    try:
        symbols.append(forecast_one(row))
    except Exception as exc:
        errors.append({"symbol": row.get("original_symbol") or row.get("symbol"), "error": str(exc)[:220]})

print("__PIPELINE_JSON__")
print(json.dumps({
    "status": "ok" if symbols else "no_data",
    "model": model_name,
    "tokenizer": tokenizer_name,
    "device": device,
    "symbol_count": len(symbols),
    "error_count": len(errors),
    "symbols": symbols,
    "errors": errors[:50],
}, ensure_ascii=False))
'''


def stage_daily_stock_analysis(
    pool: list[dict[str, str]],
    env: dict[str, str],
    top_n: int,
    openbb_context: dict[str, Any] | None = None,
    kronos_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    dsa_dir = Path(env["DAILY_STOCK_ANALYSIS_DIR"])
    python_bin = env.get("DAILY_STOCK_ANALYSIS_PYTHON") or env["PYTHON_BIN"]
    batch_size = max(1, int(env.get("PIPELINE_DSA_BATCH_SIZE", "50")))
    batch_timeout = int(env.get("PIPELINE_DSA_BATCH_TIMEOUT", env.get("PIPELINE_DSA_TIMEOUT", "300")))
    batches = list(chunked(pool, batch_size))
    logs: list[str] = []

    market_primary_count = sum(1 for item in pool if prefer_market_data_source(item.get("symbol", "")))
    skip_dsa_fetch = (
        env.get("PIPELINE_DSA_SKIP_IF_MARKET_PRIMARY", "1") == "1"
        and market_primary_count == len(pool)
    )
    if skip_dsa_fetch:
        message = (
            "daily_stock_analysis fetch skipped: all symbols are HK/US and are scored from "
            "OpenBB/TickDB/Alpha/stock-api market data primary sources."
        )
        print(message)
        logs.append(message)
    else:
        for idx, batch in enumerate(batches, 1):
            symbols = [item["symbol"] for item in batch]
            header = f"===== DSA batch {idx}/{len(batches)}: {len(symbols)} symbols ====="
            print(header)
            run_env = dict(env)
            run_env.update({
                "SCHEDULE_ENABLED": "false",
                "SCHEDULE_RUN_IMMEDIATELY": "false",
                "RUN_IMMEDIATELY": "true",
            })
            # Ensure daily_stock_analysis can import data_provider module
            # by adding its directory to PYTHONPATH
            dsa_real_path = str(dsa_dir.resolve())
            existing_pythonpath = run_env.get("PYTHONPATH", "")
            run_env["PYTHONPATH"] = f"{dsa_real_path}:{existing_pythonpath}" if existing_pythonpath else dsa_real_path
            try:
                rc, output = run(
                    [
                        python_bin,
                        "main.py",
                        "--stocks",
                        ",".join(symbols),
                        "--dry-run",
                        "--no-notify",
                        "--no-market-review",
                        "--force-run",
                    ],
                    dsa_dir,
                    run_env,
                    timeout=batch_timeout,
                )
            except subprocess.TimeoutExpired as exc:
                output = str(exc.output or "")
                rc = 124
                output += f"\n[PIPELINE] DSA batch timeout after {batch_timeout}s; continuing with next batch.\n"
            batch_log = header + "\n" + output
            logs.append(batch_log)
            write_text(WORK / f"daily_stock_analysis_batch_{idx:02d}.log", batch_log)
            if rc != 0:
                print(f"daily_stock_analysis batch {idx} exited with {rc}; continuing with available database rows.")

    write_text(WORK / "daily_stock_analysis.log", "\n\n".join(logs))

    db_path = dsa_dir / "data" / "stock_analysis.db"
    scored = score_from_stock_daily(db_path, pool, openbb_context or {})

    # Fail immediately if DSA had failures and no fallback data was retrieved
    if not skip_dsa_fetch and not scored:
        reason = "Daily Stock Analysis failed and no fallback data available from database"
        print(reason)
        raise RuntimeError(reason)
    external_signals = load_external_signals(env)
    write_json(OUTPUTS / "external_signals_loaded.json", list(external_signals.values()))
    scored = apply_external_signals(scored, external_signals)
    scored = apply_kronos_signals(scored, kronos_context or {})
    build_serenity_bottleneck_watchlist(pool, scored, external_signals, int(env.get("SERENITY_BOTTLENECK_TOP_N", "20")))
    top = sorted(scored, key=lambda x: x["score"], reverse=True)[:top_n]
    write_json(OUTPUTS / "candidates_top50.json", top)
    return top


def load_external_signals(env: dict[str, str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    path = config_path(env.get("EXTERNAL_SIGNALS_FILE", str(ROOT / "external_signals.yaml")))
    if path.exists():
        out = merge_external_signal_maps(out, read_external_signal_file(path))
    out = merge_external_signal_maps(out, load_external_brief_signals(env))
    out = merge_external_signal_maps(out, load_external_x_post_signals(env))
    out = {symbol: enrich_serenity_signal(symbol, signal) for symbol, signal in out.items()}
    return out


def merge_external_signal_maps(
    base: dict[str, dict[str, Any]],
    incoming: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    out = dict(base)
    for symbol, signal in incoming.items():
        current = dict(out.get(symbol) or {})
        merged = merge_external_signal(current, signal)
        out[symbol] = merged
    return out


def merge_external_signal(current: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    if not current:
        return dict(incoming)
    if not incoming:
        return current
    merged = dict(current)
    old_priority = safe_float(current.get("priority"))
    new_priority = safe_float(incoming.get("priority"))
    if new_priority >= old_priority:
        for key, value in incoming.items():
            if key in {"themes"}:
                continue
            if value not in (None, "", []):
                merged[key] = value
    merged["priority"] = max(old_priority, new_priority)
    merged["themes"] = merge_unique_lists(current.get("themes") or [], incoming.get("themes") or [])
    sources = merge_unique_lists(split_source(current.get("source")), split_source(incoming.get("source")))
    if sources:
        merged["source"] = " + ".join(sources[:4])
    reasons = [str(x).strip() for x in (current.get("reason"), incoming.get("reason")) if str(x or "").strip()]
    if reasons:
        merged["reason"] = complete_excerpt("；".join(dict.fromkeys(reasons)), 520)
    merged["requires_verification"] = bool(current.get("requires_verification", True) or incoming.get("requires_verification", True))
    return merged


def merge_unique_lists(left: list[Any], right: list[Any]) -> list[Any]:
    out: list[Any] = []
    for item in list(left) + list(right):
        if item and item not in out:
            out.append(item)
    return out


def split_source(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"\s+\+\s+", text) if part.strip()]


def read_external_signal_file(path: Path) -> dict[str, dict[str, Any]]:
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
        else:
            import yaml  # type: ignore

            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in data.get("signals") or []:
        if not isinstance(row, dict) or not row.get("symbol"):
            continue
        symbol = normalize_dsa_symbol(str(row.get("symbol")))
        out[symbol] = row
    return out


def load_external_brief_signals(env: dict[str, str]) -> dict[str, dict[str, Any]]:
    brief_file = env.get("EXTERNAL_BRIEF_FILE")
    if brief_file:
        path = config_path(brief_file)
    else:
        candidates = list_external_brief_candidates(env)
        path = max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None
    if not path or not path.exists():
        return {}
    text = path.read_text(encoding="utf-8", errors="ignore")
    signals = parse_external_brief(text, path.name)
    write_json(OUTPUTS / "external_brief_signals.json", list(signals.values()))
    write_json(OUTPUTS / "external_brief_source.json", {"path": str(path), "mtime": path.stat().st_mtime})
    return signals


def list_external_brief_candidates(env: dict[str, str]) -> list[Path]:
    dirs = [config_path(env.get("EXTERNAL_BRIEF_DIR", str(ROOT / "external_briefs")))]
    for raw in env.get("EXTERNAL_BRIEF_EXTRA_DIRS", "").split(os.pathsep):
        if raw.strip():
            dirs.append(config_path(raw.strip()))
    default_dokobot_dirs = [
        Path("/Users/g90/Documents/Codex/2026-06-10/https-dokobot-ai/work"),
        Path("/Users/g90/Documents/Codex/2026-06-10/https-dokobot-ai/outputs"),
    ]
    if env.get("EXTERNAL_BRIEF_USE_DOKOBOT_DEFAULTS", "1") != "0":
        dirs.extend(default_dokobot_dirs)

    candidates: list[Path] = []
    seen: set[Path] = set()
    name_patterns = (
        "daily_analysissite",
        "daily_aleabitoreddit",
        "serenity_stock",
        "analysissite_dokobot",
        "dokobot",
    )
    for directory in dirs:
        if not directory.exists() or not directory.is_dir():
            continue
        for path in directory.iterdir():
            if not path.is_file() or path.suffix.lower() not in {".md", ".txt"}:
                continue
            if path.name.lower() in {"readme.md", "readme.txt"}:
                continue
            if directory.name == "external_briefs" or any(pattern in path.name.lower() for pattern in name_patterns):
                resolved = path.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    candidates.append(path)
    return candidates


def load_external_x_post_signals(env: dict[str, str]) -> dict[str, dict[str, Any]]:
    post_dir = config_path(env.get("EXTERNAL_X_POST_DIR", str(ROOT / "external_x_posts")))
    if not post_dir.exists():
        return {}
    handles = [
        handle.strip().lstrip("@")
        for handle in env.get("EXTERNAL_X_HANDLES", "").split(",")
        if handle.strip()
    ]
    allowed = {handle.lower() for handle in handles}
    candidates = [
        p for p in post_dir.iterdir()
        if p.is_file() and p.suffix.lower() in {".md", ".txt", ".json"}
    ]
    signals: dict[str, dict[str, Any]] = {}
    for path in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[:30]:
        handle = infer_x_handle_from_path(path)
        if allowed and handle.lower() not in allowed:
            continue
        text = read_x_post_file(path)
        for symbol, signal in parse_x_posts(text, handle or path.stem, path.name).items():
            current = signals.get(symbol)
            if not current or safe_float(signal.get("priority")) > safe_float(current.get("priority")):
                signals[symbol] = signal
    write_json(OUTPUTS / "external_x_post_signals.json", list(signals.values()))
    return signals


def infer_x_handle_from_path(path: Path) -> str:
    name = path.stem
    for part in re.split(r"[_\-\s]+", name):
        if part.startswith("@"):
            return part.lstrip("@")
    return name.split(".", 1)[0].lstrip("@")


def read_x_post_file(path: Path) -> str:
    if path.suffix.lower() == ".json":
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return ""
        if isinstance(data, list):
            return "\n".join(str(item.get("text") if isinstance(item, dict) else item) for item in data)
        if isinstance(data, dict):
            posts = data.get("posts") or data.get("items") or []
            if isinstance(posts, list):
                return "\n".join(str(item.get("text") if isinstance(item, dict) else item) for item in posts)
            return str(data.get("text") or "")
    return path.read_text(encoding="utf-8", errors="ignore")


def parse_x_posts(text: str, handle: str, source_name: str) -> dict[str, dict[str, Any]]:
    signals: dict[str, dict[str, Any]] = {}
    chunks = split_x_post_chunks(text)
    for chunk in chunks:
        symbols = extract_symbols(chunk)
        if not symbols:
            continue
        stance = infer_x_stance(chunk)
        themes = infer_x_themes(chunk)
        priority = x_priority(handle, chunk, stance)
        reason = clean_brief_cell(chunk).replace("\n", " ")[:260]
        for symbol in symbols:
            signals[symbol] = {
                "symbol": symbol,
                "source": f"X @{handle}:{source_name}",
                "stance": stance,
                "priority": priority,
                "themes": themes,
                "requires_verification": True,
                "reason": reason or f"@{handle} 帖子提及该标的，需结合一手事实确认。",
            }
    return signals


def split_x_post_chunks(text: str) -> list[str]:
    raw_chunks = re.split(r"\n\s*\n|^[-*]\s+", text, flags=re.M)
    return [chunk.strip() for chunk in raw_chunks if chunk.strip()]


def extract_symbols(text: str) -> list[str]:
    symbols = {normalize_dsa_symbol(match.group(1)) for match in re.finditer(r"\$([A-Z][A-Z0-9.\-]{0,9})\b", text)}
    return sorted(symbol for symbol in symbols if symbol)


def infer_x_stance(text: str) -> str:
    lower = text.lower()
    positive = ("bull", "buy", "long", "強気", "買い", "買増", "上昇", "好決算", "beat", "upgrade")
    negative = ("bear", "sell", "short", "弱気", "売り", "下落", "警戒", "caution", "downgrade", "miss")
    if any(word in lower or word in text for word in negative):
        return "谨慎 / 待验证"
    if any(word in lower or word in text for word in positive):
        return "看多 / 待验证"
    return "外部提及 / 待验证"


def infer_x_themes(text: str) -> list[str]:
    themes = infer_brief_themes(text)
    checks = [
        ("決算", "财报"),
        ("earnings", "财报"),
        ("半導体", "半导体"),
        ("semiconductor", "半导体"),
        ("AI", "AI"),
        ("為替", "日元汇率"),
        ("ドル円", "日元汇率"),
        ("NISA", "日本资金流"),
        ("NVDA", "AI 半导体"),
        ("MU", "存储"),
        ("HBM", "HBM"),
        ("DRAM", "DRAM"),
        ("NAND", "NAND"),
    ]
    for needle, theme in checks:
        if needle in text and theme not in themes:
            themes.append(theme)
    return themes[:6]


def x_priority(handle: str, text: str, stance: str) -> float:
    handle_key = handle.strip().lstrip("@").lower()
    base = {
        "usstockhanako": 210,
        "yukimamax": 240,
        "market_letter_": 220,
        "jicchamatome": 215,
        "himekako3150": 190,
        "butamaru_butako": 220,
    }.get(handle_key, 180)
    if "MU" in extract_symbols(text) or "$MU" in text or "$MUU" in text:
        base += 25
    if "看多" in stance:
        base += 10
    if "谨慎" in stance:
        base -= 5
    return float(base)


def parse_external_brief(text: str, source_name: str) -> dict[str, dict[str, Any]]:
    signals: dict[str, dict[str, Any]] = {}
    for line in text.splitlines():
        cells = [clean_brief_cell(part) for part in line.strip().strip("|").split("|")]
        if len(cells) >= 5 and re.fullmatch(r"\d+", cells[0] or ""):
            symbol = normalize_dsa_symbol(cells[1].split()[0])
            if symbol:
                priority = safe_float(cells[3], 100.0)
                signals[symbol] = {
                    "symbol": symbol,
                    "source": f"dokobot.ai daily brief:{source_name}",
                    "stance": cells[2],
                    "priority": priority,
                    "themes": infer_brief_themes(text),
                    "requires_verification": True,
                    "reason": cells[4][:260],
                }
    for match in re.finditer(r"\$([A-Z][A-Z0-9.\-]{0,9})\b", text):
        symbol = normalize_dsa_symbol(match.group(1))
        signals.setdefault(symbol, {
            "symbol": symbol,
            "source": f"dokobot.ai daily brief:{source_name}",
            "stance": "外部提及 / 待验证",
            "priority": 180,
            "themes": infer_brief_themes(text),
            "requires_verification": True,
            "reason": "简报正文提及该标的，需进一步核验一手事实。",
        })
    for symbol in extract_serenity_brief_symbols(text):
        signals.setdefault(symbol, {
            "symbol": symbol,
            "source": f"dokobot.ai daily brief:{source_name}",
            "stance": infer_brief_symbol_stance(text, symbol),
            "priority": 190,
            "themes": infer_brief_themes(text),
            "requires_verification": True,
            "reason": "dokobot/Serenity 简报提及该标的，按外部线索提高研究优先级，仍需一手事实确认。",
        })
    return signals


def extract_serenity_brief_symbols(text: str) -> list[str]:
    whitelist = set(SERENITY_PROFILES)
    found: set[str] = set()
    for match in re.finditer(r"\b([A-Z][A-Z0-9.]{1,9})\s*(?:\[\d+\])?\b", text):
        symbol = normalize_dsa_symbol(match.group(1))
        if symbol.upper() in whitelist:
            found.add(symbol)
    return sorted(found)


def infer_brief_symbol_stance(text: str, symbol: str) -> str:
    pattern = re.compile(rf"(.{{0,80}}\b{re.escape(symbol.upper())}\b.{{0,160}})", re.S)
    match = pattern.search(text)
    chunk = match.group(1) if match else text[:500]
    if any(term in chunk for term in ("看空", "谨慎", "风险", "ATM", "稀释")):
        return "谨慎 / 待验证"
    if any(term in chunk for term in ("看多", "积极", "偏多", "受益", "核心")):
        return "看多 / 待验证"
    return "外部提及 / 待验证"


def clean_brief_cell(value: str) -> str:
    value = re.sub(r"[*`_]", "", value or "").strip()
    value = re.sub(r"<[^>]+>", "", value)
    return value


def infer_brief_themes(text: str) -> list[str]:
    themes = []
    checks = [
        ("800V", "800V DC"),
        ("CPO", "CPO/光互连"),
        ("光互连", "CPO/光互连"),
        ("AI capex", "AI capex"),
        ("数据中心电力", "数据中心电力链"),
        ("出口管制", "出口管制"),
        ("材料", "材料/光子瓶颈"),
        ("ATM", "股本供给风险"),
    ]
    for needle, theme in checks:
        if needle in text and theme not in themes:
            themes.append(theme)
    return themes[:6]


SERENITY_PROFILES: dict[str, dict[str, Any]] = {
    "LITE": {
        "serenity_tier": "第一优先级",
        "serenity_role": "光子瓶颈核心",
        "bottleneck": "1.6T/CPO/外部激光源与光互连上游",
        "chain_tier": "芯片/器件",
        "evidence_level": "中",
        "action_bias": "等回踩，不追垂直拉升",
        "kill_criteria": "客户认证、出货节奏或毛利率兑现低于预期；竞品扩产明显填补供给缺口",
    },
    "VRT": {
        "serenity_tier": "第一优先级",
        "serenity_role": "电力架构瓶颈核心",
        "bottleneck": "AI 数据中心 800V DC、液冷与电力基础设施",
        "chain_tier": "基础设施",
        "evidence_level": "中",
        "action_bias": "中线跟踪，等订单和 backlog 验证",
        "kill_criteria": "800V DC 时间窗推迟、订单低于预期或大型竞品快速追平",
    },
    "MRVL": {
        "serenity_tier": "第一优先级",
        "serenity_role": "AI fabric 瓶颈核心",
        "bottleneck": "custom silicon、800G/1.6T optics、NVLink/Fabric 连接层",
        "chain_tier": "芯片/器件",
        "evidence_level": "中",
        "action_bias": "只在估值和订单兑现匹配时提高权重",
        "kill_criteria": "客户集中度恶化、毛利率受压或 AI 订单兑现低于预期",
    },
    "AAOI": {
        "serenity_tier": "第二优先级",
        "serenity_role": "光互连高弹性观察",
        "bottleneck": "800G/1.6T 光模块弹性环节",
        "chain_tier": "模块/子系统",
        "evidence_level": "弱",
        "action_bias": "必须等收入、订单或客户进展确认",
        "kill_criteria": "只有热度没有订单，或价格波动显示拥挤交易退潮",
    },
    "COHR": {
        "serenity_tier": "第二优先级",
        "serenity_role": "LITE 替代/配对观察",
        "bottleneck": "光通信材料、激光器与光互连器件",
        "chain_tier": "芯片/器件",
        "evidence_level": "中",
        "action_bias": "作为 LITE 替代和产业验证参照",
        "kill_criteria": "CPO 延迟、客户验证低于预期或毛利率无改善",
    },
    "NVDA": {
        "serenity_tier": "第二优先级",
        "serenity_role": "AI 主线风向标",
        "bottleneck": "GPU/平台需求发动机，不是隐藏上游瓶颈",
        "chain_tier": "下游需求",
        "evidence_level": "强",
        "action_bias": "用于验证 AI capex 强度，不按隐藏瓶颈股处理",
        "kill_criteria": "AI capex 预期降温或客户自研替代节奏加快",
    },
    "SNDK": {
        "serenity_tier": "第三优先级",
        "serenity_role": "存储供需观察",
        "bottleneck": "AI 存储和 enterprise SSD 供需紧张",
        "chain_tier": "模块/子系统",
        "evidence_level": "中",
        "action_bias": "情绪过热时降仓位，只做验证链跟踪",
        "kill_criteria": "供需紧张缓解、ASP 转弱或市场已充分重定价",
    },
    "AMD": {
        "serenity_tier": "第三优先级",
        "serenity_role": "AI capex 受益观察",
        "bottleneck": "GPU/先进封装需求扩散",
        "chain_tier": "芯片/器件",
        "evidence_level": "中",
        "action_bias": "需比较 NVDA/MRVL/LITE 的相对资金吸引力",
        "kill_criteria": "AI GPU 份额、供应链产能或毛利兑现不及预期",
    },
    "GOOGL": {
        "serenity_tier": "第三优先级",
        "serenity_role": "需求方验证源",
        "bottleneck": "800V DC 和 AI capex 需求侧验证",
        "chain_tier": "下游需求",
        "evidence_level": "中",
        "action_bias": "作为电力链需求验证，不当瓶颈供应商",
        "kill_criteria": "AI 降价压力扩大、监管压力增强或 capex 计划放缓",
    },
    "IREN": {
        "serenity_tier": "警惕名单",
        "serenity_role": "股本供给约束",
        "bottleneck": "AI 数据中心叙事与融资压力并存",
        "chain_tier": "基础设施",
        "evidence_level": "中",
        "action_bias": "先读原始融资文件，不进入主动买入",
        "kill_criteria": "股本供给继续压制普通股回报",
    },
    "RKLB": {
        "serenity_tier": "警惕名单",
        "serenity_role": "主题偏离与股本供给约束",
        "bottleneck": "航天链条，和 AI 基建主线距离较远",
        "chain_tier": "系统集成",
        "evidence_level": "中",
        "action_bias": "不和 AI 光互连/电力链同池处理",
        "kill_criteria": "大额股本供给安排继续影响风险回报",
    },
    "MSTR": {
        "serenity_tier": "警惕名单",
        "serenity_role": "高波动代理标的",
        "bottleneck": "不属于 AI 供应链瓶颈",
        "chain_tier": "下游需求",
        "evidence_level": "中",
        "action_bias": "排除出 Serenity 瓶颈买入框架",
        "kill_criteria": "高波动资产回撤放大普通股风险",
    },
    "AXTI": {
        "serenity_tier": "第二优先级",
        "serenity_role": "上游材料瓶颈观察",
        "bottleneck": "InP/化合物半导体衬底与光模块上游材料",
        "chain_tier": "材料耗材",
        "evidence_level": "弱",
        "action_bias": "只在客户、订单、ASP 或产能事实增强后提高权重",
        "kill_criteria": "替代材料路线清晰或订单验证不足",
    },
    "IQE": {
        "serenity_tier": "第二优先级",
        "serenity_role": "外延片材料观察",
        "bottleneck": "Epiwafers/化合物半导体材料链",
        "chain_tier": "材料耗材",
        "evidence_level": "弱",
        "action_bias": "等待价格、客户和产能验证",
        "kill_criteria": "涨价无法传导到财务科目或需求端验证不足",
    },
}


def enrich_serenity_signal(symbol: str, signal: dict[str, Any]) -> dict[str, Any]:
    item = dict(signal)
    normalized = normalize_dsa_symbol(symbol)
    profile = SERENITY_PROFILES.get(normalized.upper())
    source = str(item.get("source") or "")
    themes = [str(x) for x in (item.get("themes") or [])]
    looks_serenity = profile or "Serenity" in source or "dokobot.ai daily brief" in source or any(
        theme in {"800V DC", "CPO/光互连", "材料/光子瓶颈", "数据中心电力链", "AI capex"}
        for theme in themes
    )
    if profile:
        for key, value in profile.items():
            item.setdefault(key, value)
    elif looks_serenity:
        item.setdefault("serenity_tier", "外部线索")
        item.setdefault("serenity_role", infer_serenity_role(themes))
        item.setdefault("bottleneck", infer_serenity_bottleneck(themes))
        item.setdefault("chain_tier", infer_serenity_chain_tier(themes))
        item.setdefault("evidence_level", "弱")
        item.setdefault("action_bias", "只提高研究优先级，不直接触发买入")
        item.setdefault("kill_criteria", "缺少一手事实验证时，不进入主动买入")
    if looks_serenity:
        item["serenity_method"] = True
        if "白毛/Serenity外部参考" not in themes:
            item["themes"] = merge_unique_lists(themes, ["白毛/Serenity外部参考"])
    return item


def infer_serenity_role(themes: list[str]) -> str:
    if "数据中心电力链" in themes or "800V DC" in themes:
        return "电力架构瓶颈观察"
    if "CPO/光互连" in themes or "材料/光子瓶颈" in themes:
        return "光互连瓶颈观察"
    if "存储" in themes or "HBM" in themes:
        return "存储瓶颈观察"
    return "AI 供应链外部线索"


def infer_serenity_bottleneck(themes: list[str]) -> str:
    if "800V DC" in themes:
        return "AI 数据中心 800V DC 电力架构"
    if "CPO/光互连" in themes:
        return "CPO/光互连供应链"
    if "材料/光子瓶颈" in themes:
        return "光子材料与器件上游"
    if "数据中心电力链" in themes:
        return "数据中心电力设备链"
    return "待确认的 AI 供应链环节"


def infer_serenity_chain_tier(themes: list[str]) -> str:
    if "材料/光子瓶颈" in themes:
        return "材料耗材"
    if "CPO/光互连" in themes:
        return "芯片/器件"
    if "800V DC" in themes or "数据中心电力链" in themes:
        return "基础设施"
    return "待确认"


def serenity_score_adjustment(signal: dict[str, Any]) -> float:
    if not signal.get("serenity_method"):
        return 0.0
    tier = str(signal.get("serenity_tier") or "")
    role = str(signal.get("serenity_role") or "")
    evidence = str(signal.get("evidence_level") or "弱")
    delta = {
        "第一优先级": 4.0,
        "第二优先级": 2.5,
        "第三优先级": 1.0,
        "外部线索": 1.0,
        "警惕名单": -5.0,
    }.get(tier, 0.0)
    if evidence == "强":
        delta += 1.0
    elif evidence == "弱":
        delta -= 1.0
    if "风向标" in role or "需求方" in role:
        delta -= 1.5
    return delta


def config_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def apply_external_signals(rows: list[dict[str, Any]], signals: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if not signals:
        return rows
    adjusted: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        signal = signals.get(normalize_dsa_symbol(str(row.get("symbol") or "")))
        if signal:
            priority = safe_float(signal.get("priority"))
            stance = str(signal.get("stance") or "").lower()
            boost = clamp(priority / 60.0, 0, 7)
            if "bear" in stance or "谨慎" in stance or "看空" in stance:
                boost = -min(6, max(2, boost))
            elif "neutral" in stance or "观察" in stance or "中性" in stance:
                boost = min(3, boost)
            if signal.get("serenity_method"):
                boost = 0.0
            old_score = safe_float(item.get("score"))
            item["score"] = round(clamp(old_score + boost, 0, 100), 2)
            item["external_signal"] = {
                "source": signal.get("source", "external"),
                "priority": priority,
                "stance": signal.get("stance", ""),
                "themes": signal.get("themes", []),
                "requires_verification": bool(signal.get("requires_verification", True)),
                "reason": signal.get("reason", ""),
                "serenity_method": bool(signal.get("serenity_method")),
                "serenity_tier": signal.get("serenity_tier", ""),
                "serenity_role": signal.get("serenity_role", ""),
                "bottleneck": signal.get("bottleneck", ""),
                "chain_tier": signal.get("chain_tier", ""),
                "evidence_level": signal.get("evidence_level", ""),
                "action_bias": signal.get("action_bias", ""),
                "kill_criteria": signal.get("kill_criteria", ""),
                "score_adjustment": round(boost, 2),
            }
            item["reason"] = (
                f"{item.get('reason', '')}；外部研究信号：{signal.get('stance', '')}，"
                f"优先级 {priority:.0f}，调分 {boost:+.1f}，{strip_actionable_price_sentences(signal.get('reason', ''))}"
            ).strip("；")
        adjusted.append(item)
    return adjusted


def build_serenity_bottleneck_watchlist(
    pool: list[dict[str, str]],
    candidates: list[dict[str, Any]],
    external_signals: dict[str, dict[str, Any]] | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Build an independent Serenity/Baimao bottleneck list without changing Top10 scoring."""
    signals = external_signals or load_external_signals(build_env())
    pool_map = {normalize_dsa_symbol(str(x.get("symbol") or "")): x for x in pool}
    candidate_map = {normalize_dsa_symbol(str(x.get("symbol") or "")): x for x in candidates}
    symbols = set(candidate_map) | {
        normalize_dsa_symbol(symbol)
        for symbol, signal in signals.items()
        if (signal or {}).get("serenity_method") or normalize_dsa_symbol(symbol).upper() in SERENITY_PROFILES
    }
    rows: list[dict[str, Any]] = []
    for symbol in sorted(symbols):
        if not symbol:
            continue
        signal = enrich_serenity_signal(symbol, signals.get(symbol, {"symbol": symbol, "source": "SERENITY_PROFILES"}))
        if not signal.get("serenity_method") and symbol.upper() not in SERENITY_PROFILES:
            continue
        candidate = candidate_map.get(symbol, {})
        pool_item = pool_map.get(symbol, {})
        row = build_serenity_bottleneck_row(symbol, pool_item, candidate, signal)
        rows.append(row)
    rows.sort(key=lambda row: (serenity_tier_rank(row.get("tier")), -safe_float(row.get("serenity_score")), -safe_float(row.get("dsa_score"))))
    out = rows[:limit]
    write_json(OUTPUTS / "serenity_bottleneck_watchlist.json", out)
    write_text(OUTPUTS / "serenity_bottleneck_watchlist.md", render_serenity_bottleneck_markdown(out))
    return out


def build_serenity_bottleneck_row(
    symbol: str,
    pool_item: dict[str, Any],
    candidate: dict[str, Any],
    signal: dict[str, Any],
) -> dict[str, Any]:
    tier = str(signal.get("serenity_tier") or "外部线索")
    role = str(signal.get("serenity_role") or infer_serenity_role(signal.get("themes") or []))
    bottleneck = str(signal.get("bottleneck") or infer_serenity_bottleneck(signal.get("themes") or []))
    evidence = str(signal.get("evidence_level") or "弱")
    dsa_score = safe_float(candidate.get("score"))
    ret_20 = safe_float(candidate.get("ret_20d"))
    volume_ratio = valid_volume_ratio(candidate.get("volume_ratio"))
    score = 50.0
    score += {"第一优先级": 22, "第二优先级": 15, "第三优先级": 9, "外部线索": 5, "警惕名单": -12}.get(tier, 0)
    score += {"强": 10, "中": 5, "弱": 0}.get(evidence, 0)
    if dsa_score:
        score += clamp((dsa_score - 50) * 0.25, -8, 10)
    if ret_20 >= 45:
        score -= 9
    elif ret_20 >= 30:
        score -= 5
    elif ret_20 <= -15:
        score -= 4
    if volume_ratio is not None and volume_ratio >= 1.8:
        score += 2
    if tier == "警惕名单":
        score = min(score, 49)
    score = round(clamp(score, 0, 100), 2)
    monthly = build_serenity_monthly_screen(signal, candidate)
    quarterly = build_serenity_quarterly_review(signal, candidate)
    red_team = build_serenity_red_team(signal, candidate)
    milestones = build_serenity_milestones(signal)
    baimao_secondary = build_baimao_secondary_analysis(
        {
            "symbol": symbol,
            "tier": tier,
            "role": role,
            "bottleneck": bottleneck,
            "chain_tier": signal.get("chain_tier") or infer_serenity_chain_tier(signal.get("themes") or []),
            "evidence_level": evidence,
            "serenity_score": score,
            "dsa_score": round(dsa_score, 2),
            "ret_20d": round(ret_20, 2),
            "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
            "themes": signal.get("themes", []),
            "monthly_screen": monthly,
            "quarterly_review": quarterly,
            "red_team": red_team,
            "milestones": milestones,
            "kill_criteria": signal.get("kill_criteria", ""),
        }
    )
    red_block = any(item.get("block_buy") for item in red_team)
    action = "Watch"
    if tier in {"第一优先级", "第二优先级"} and evidence in {"中", "强"} and not red_block and score >= 65 and baimao_secondary.get("verdict") != "skip":
        action = "进入主流程深度研究"
    if tier == "警惕名单" or red_block or baimao_secondary.get("verdict") == "skip":
        action = "Watch Only"
    return {
        "symbol": symbol,
        "name": pool_item.get("name") or candidate.get("name") or symbol,
        "tier": tier,
        "role": role,
        "bottleneck": bottleneck,
        "chain_tier": signal.get("chain_tier") or infer_serenity_chain_tier(signal.get("themes") or []),
        "evidence_level": evidence,
        "serenity_score": score,
        "dsa_score": round(dsa_score, 2),
        "ret_20d": round(ret_20, 2),
        "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
        "source": signal.get("source", ""),
        "themes": signal.get("themes", []),
        "monthly_screen": monthly,
        "quarterly_review": quarterly,
        "red_team": red_team,
        "milestones": milestones,
        "baimao_secondary": baimao_secondary,
        "action": action,
        "kill_criteria": signal.get("kill_criteria", ""),
        "note": "独立白毛/Serenity 瓶颈体系候选；只单列研究，不参与 Top10/Buy3 正式评分。",
    }


def build_serenity_monthly_screen(signal: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    themes = [str(x) for x in (signal.get("themes") or [])]
    return {
        "focus": "BOM/供应链瓶颈",
        "criteria": [
            "非主流共识",
            "供应紧",
            "客户依赖度高",
            "扩产周期长",
            "成本占比小但替代难",
            "集中度高",
        ],
        "matched": [
            signal.get("bottleneck") or infer_serenity_bottleneck(themes),
            signal.get("serenity_role") or infer_serenity_role(themes),
            f"外部线索：{signal.get('source') or 'SERENITY_PROFILES'}",
        ],
        "crowding_check": serenity_crowding_check(candidate, signal),
    }


def build_serenity_quarterly_review(signal: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "focus": "四季度财报穿透",
        "checks": [
            "毛利率是否连续扩张",
            "CapEx / 在建工程是否对应瓶颈环节",
            "订单 / backlog 是否验证需求",
            "客户集中度是否可控",
            "研报/社媒覆盖是否过热",
        ],
        "current_snapshot": {
            "dsa_score": round(safe_float(candidate.get("score")), 2),
            "ret_20d": round(safe_float(candidate.get("ret_20d")), 2),
            "volume_ratio": (
                round(valid_volume_ratio(candidate.get("volume_ratio")), 2)
                if valid_volume_ratio(candidate.get("volume_ratio")) is not None
                else None
            ),
            "evidence_level": signal.get("evidence_level", "弱"),
        },
    }


def build_serenity_red_team(signal: dict[str, Any], candidate: dict[str, Any]) -> list[dict[str, Any]]:
    tier = str(signal.get("serenity_tier") or "")
    role = str(signal.get("serenity_role") or "")
    ret_20 = safe_float(candidate.get("ret_20d"))
    checks = [
        ("大客户是否自研", "若大客户自研替代进入量产路径，降级 Watch"),
        ("是否引入二供", "若第二供应商拿到认证或价格优势，降级 Watch"),
        ("18个月内技术路线是否替代", "若 CPO/800V/材料路线被替代路线绕开，降级 Watch"),
        ("同行能否价格战", "若竞品扩产导致 ASP 或毛利率下行，降级 Watch"),
    ]
    out = []
    for name, threshold in checks:
        block = False
        if "风向标" in role and name == "大客户是否自研":
            block = True
        if tier == "警惕名单":
            block = True
        if ret_20 >= 45 and name == "同行能否价格战":
            block = True
        out.append({
            "risk": name,
            "threshold": threshold,
            "block_buy": block,
            "status": "需证伪" if not block else "红队阻断",
        })
    return out


def build_serenity_milestones(signal: dict[str, Any]) -> list[dict[str, str]]:
    bottleneck = str(signal.get("bottleneck") or "")
    if "800V" in bottleneck or "电力" in bottleneck:
        return [
            {"event": "800V DC 客户试点 / 小批量出货确认", "deadline": "未来2个季度内", "kill_threshold": "未见订单、backlog 或客户披露则降级"},
            {"event": "AI 数据中心电力/液冷订单兑现", "deadline": "下一季报", "kill_threshold": "订单增速或毛利率不支持叙事则移出"},
            {"event": "竞品扩产和价格压力复核", "deadline": "每月", "kill_threshold": "ASP 或毛利率连续转弱则降级"},
        ]
    if "光" in bottleneck or "CPO" in bottleneck or "1.6T" in bottleneck:
        return [
            {"event": "客户认证 / 设计导入 / 采购承诺验证", "deadline": "未来2个季度内", "kill_threshold": "无客户或订单事实则保持 Watch Only"},
            {"event": "1.6T/CPO 出货节奏", "deadline": "下一季报", "kill_threshold": "收入或 backlog 未体现则降级"},
            {"event": "替代路线与二供进展", "deadline": "每月", "kill_threshold": "二供放量或路线替代成立则移出"},
        ]
    if "存储" in bottleneck or "SSD" in bottleneck:
        return [
            {"event": "ASP / 供需紧张是否延续", "deadline": "下一季报", "kill_threshold": "ASP 转弱或库存上升则降级"},
            {"event": "AI 数据中心订单 / enterprise SSD 需求", "deadline": "未来2个季度内", "kill_threshold": "订单未验证则保持 Watch"},
            {"event": "拥挤度复核", "deadline": "每月", "kill_threshold": "涨幅过热且社媒/研报拥挤则不进 Buy3"},
        ]
    return [
        {"event": "一手订单 / 客户 / 产能事实验证", "deadline": "未来2个季度内", "kill_threshold": "无一手事实则不进 Buy3"},
        {"event": "毛利率或 backlog 兑现", "deadline": "下一季报", "kill_threshold": "未兑现则降级"},
        {"event": "替代路线和二供风险复核", "deadline": "每月", "kill_threshold": "红队证伪成立则移出"},
    ]


def serenity_crowding_check(candidate: dict[str, Any], signal: dict[str, Any]) -> str:
    ret_20 = safe_float(candidate.get("ret_20d"))
    if ret_20 >= 45:
        return f"过热：20日涨幅 {ret_20:.1f}%，只观察不追"
    if ret_20 >= 30:
        return f"偏热：20日涨幅 {ret_20:.1f}%，需等回踩或事实增强"
    if str(signal.get("evidence_level") or "弱") == "弱":
        return "证据弱：只作为外部线索"
    return "未见明显过热，仍需一手事实确认"


def serenity_tier_rank(tier: Any) -> int:
    return {"第一优先级": 0, "第二优先级": 1, "第三优先级": 2, "外部线索": 3, "警惕名单": 4}.get(str(tier), 9)


def build_baimao_secondary_analysis(row: dict[str, Any]) -> dict[str, Any]:
    """Second-pass analysis based on the installed serenity-skill methodology."""
    evidence = str(row.get("evidence_level") or "弱")
    tier = str(row.get("tier") or "")
    chain_tier = str(row.get("chain_tier") or "")
    bottleneck = str(row.get("bottleneck") or "")
    role = str(row.get("role") or "")
    ret_20 = safe_float(row.get("ret_20d"))
    red_team = list(row.get("red_team") or [])
    monthly = row.get("monthly_screen") or {}
    milestones = list(row.get("milestones") or [])

    dims = {
        "certainty": baimao_certainty_score(evidence, tier, red_team),
        "clarity": baimao_clarity_score(bottleneck, role),
        "purity": baimao_purity_score(chain_tier, role),
        "elasticity": baimao_elasticity_score(chain_tier, tier),
        "timeframe": baimao_timeframe_score(evidence, ret_20, monthly),
    }
    raw_score = sum(dims.values()) / max(len(dims), 1) * 20.0
    evidence_multiplier = {"强": 1.0, "中": 0.85, "弱": 0.70}.get(evidence, 0.70)
    chain_multiplier = baimao_chain_multiplier(chain_tier)
    penalties = baimao_penalties(row)
    penalty_discount = clamp(1.0 - sum(p["discount"] for p in penalties), 0.40, 1.0)
    score = round(clamp(raw_score * evidence_multiplier * (0.70 + 0.30 * chain_multiplier) * penalty_discount, 0, 100), 2)
    rating = baimao_rating(score)
    verdict = baimao_verdict(score, rating, penalties, red_team)
    conclusion = baimao_conclusion(row, score, rating, verdict, penalties)
    evidence_table = baimao_evidence_table(row)
    return {
        "method": "serenity-skill",
        "thesis": baimao_thesis(row),
        "supply_chain_map": baimao_supply_chain_map(row),
        "candidate_bottleneck": bottleneck or "待确认瓶颈",
        "evidence_table": evidence_table,
        "contradictions_and_missing_proof": baimao_missing_proof(row, penalties),
        "catalysts": baimao_catalysts(row),
        "kill_criteria": row.get("kill_criteria") or "红队证伪成立、替代路线量产、ASP/毛利率转弱或一手事实不足",
        "dimensions": {k: round(v, 2) for k, v in dims.items()},
        "raw_score": round(raw_score, 2),
        "evidence_multiplier": evidence_multiplier,
        "chain_multiplier": chain_multiplier,
        "penalties": penalties,
        "secondary_score": score,
        "rating": rating,
        "verdict": verdict,
        "conclusion": conclusion,
        "positioning": baimao_positioning(rating, verdict),
        "validation_checklist": [
            {
                "event": m.get("event", ""),
                "deadline": m.get("deadline", ""),
                "status": "黄",
                "kill_threshold": m.get("kill_threshold", ""),
            }
            for m in milestones[:3]
        ],
    }


def baimao_certainty_score(evidence: str, tier: str, red_team: list[dict[str, Any]]) -> float:
    base = {"强": 4.4, "中": 3.4, "弱": 2.2}.get(evidence, 2.0)
    if tier == "第一优先级":
        base += 0.3
    if any(x.get("block_buy") for x in red_team):
        base -= 0.8
    return clamp(base, 0, 5)


def baimao_clarity_score(bottleneck: str, role: str) -> float:
    text = f"{bottleneck} {role}"
    if any(term in text for term in ("InP", "CPO", "1.6T", "800V", "HBM", "光", "电力", "材料", "fabric")):
        return 4.2
    if bottleneck and bottleneck != "待确认的 AI 供应链环节":
        return 3.2
    return 2.0


def baimao_purity_score(chain_tier: str, role: str) -> float:
    if chain_tier in {"材料耗材", "制程/封装", "设备/测试", "芯片/器件"}:
        return 4.0
    if chain_tier in {"基础设施", "模块/子系统"}:
        return 3.2
    if "风向标" in role or "需求方" in role:
        return 1.8
    return 2.6


def baimao_elasticity_score(chain_tier: str, tier: str) -> float:
    base = {
        "材料耗材": 4.6,
        "制程/封装": 4.2,
        "设备/测试": 3.8,
        "芯片/器件": 3.6,
        "基础设施": 3.1,
        "模块/子系统": 3.3,
        "系统集成": 2.3,
        "下游需求": 1.8,
    }.get(chain_tier, 2.8)
    if tier == "第一优先级":
        base += 0.4
    return clamp(base, 0, 5)


def baimao_timeframe_score(evidence: str, ret_20: float, monthly: dict[str, Any]) -> float:
    base = {"强": 3.2, "中": 3.6, "弱": 2.8}.get(evidence, 2.8)
    crowding = str(monthly.get("crowding_check") or "")
    if ret_20 >= 45 or "过热" in crowding:
        base -= 1.2
    elif ret_20 >= 30 or "偏热" in crowding:
        base -= 0.7
    elif ret_20 <= -15:
        base -= 0.4
    else:
        base += 0.3
    return clamp(base, 0, 5)


def baimao_chain_multiplier(chain_tier: str) -> float:
    return {
        "材料耗材": 1.00,
        "制程/封装": 0.92,
        "设备/测试": 0.85,
        "芯片/器件": 0.78,
        "基础设施": 0.70,
        "模块/子系统": 0.62,
        "系统集成": 0.50,
        "下游需求": 0.40,
    }.get(chain_tier, 0.60)


def baimao_penalties(row: dict[str, Any]) -> list[dict[str, Any]]:
    penalties: list[dict[str, Any]] = []
    evidence = str(row.get("evidence_level") or "弱")
    tier = str(row.get("tier") or "")
    role = str(row.get("role") or "")
    chain_tier = str(row.get("chain_tier") or "")
    ret_20 = safe_float(row.get("ret_20d"))
    red_team = list(row.get("red_team") or [])
    if evidence == "弱":
        penalties.append({"name": "证据弱", "discount": 0.10, "reason": "只有外部线索或 KOL/简报，缺少强/中一手证据"})
    if ret_20 >= 45:
        penalties.append({"name": "拥挤过热", "discount": 0.12, "reason": f"20日涨幅 {ret_20:.1f}%，反身性和追高风险高"})
    elif ret_20 >= 30:
        penalties.append({"name": "偏热", "discount": 0.06, "reason": f"20日涨幅 {ret_20:.1f}%，需等事实增强或回踩"})
    if tier == "警惕名单":
        penalties.append({"name": "警惕名单", "discount": 0.18, "reason": "主题偏离、股本供给或高波动风险"})
    if "风向标" in role or "需求方" in role or chain_tier == "下游需求":
        penalties.append({"name": "不是隐藏瓶颈", "discount": 0.10, "reason": "更适合验证需求，不是最窄物理卡点"})
    if any(x.get("block_buy") for x in red_team):
        penalties.append({"name": "红队阻断", "discount": 0.18, "reason": "买前证伪存在阻断项"})
    return penalties


def baimao_rating(score: float) -> str:
    if score >= 80:
        return "强"
    if score >= 60:
        return "中"
    if score >= 40:
        return "弱"
    return "无"


def baimao_verdict(score: float, rating: str, penalties: list[dict[str, Any]], red_team: list[dict[str, Any]]) -> str:
    if any(p["name"] == "红队阻断" for p in penalties) or any(x.get("block_buy") for x in red_team):
        return "watch_only"
    if rating == "强":
        return "strong_candidate"
    if rating == "中":
        return "validation_candidate"
    if rating == "弱":
        return "watch_only"
    return "skip"


def baimao_thesis(row: dict[str, Any]) -> str:
    return (
        f"{row.get('symbol')} 的白毛法核心假设是：它可能卡在 {row.get('bottleneck') or '待确认瓶颈'}，"
        "只有当客户/订单/产能/毛利率证据兑现时，才从线索升级为可交易 thesis。"
    )


def baimao_supply_chain_map(row: dict[str, Any]) -> str:
    chain = str(row.get("chain_tier") or "待确认")
    bottleneck = str(row.get("bottleneck") or "待确认瓶颈")
    return f"AI 基建需求 -> {chain} -> {bottleneck} -> 下游客户/系统厂商"


def baimao_evidence_table(row: dict[str, Any]) -> list[dict[str, str]]:
    evidence = str(row.get("evidence_level") or "弱")
    source = str(row.get("source") or "外部线索")
    monthly = row.get("monthly_screen") or {}
    quarterly = row.get("quarterly_review") or {}
    return [
        {"claim": "供应链瓶颈存在", "evidence": evidence, "source": source, "status": "待一手验证" if evidence == "弱" else "可进入验证链"},
        {"claim": "非主流共识/供应紧/扩产慢", "evidence": "方法论初筛", "source": "serenity-skill", "status": str(monthly.get("crowding_check") or "待复核")},
        {"claim": "财务科目可兑现", "evidence": "季度复审", "source": "毛利率/CapEx/backlog/客户集中度", "status": str((quarterly.get("current_snapshot") or {}).get("evidence_level") or evidence)},
    ]


def baimao_missing_proof(row: dict[str, Any], penalties: list[dict[str, Any]]) -> list[str]:
    missing = [
        "至少两条独立来源验证关键瓶颈 claim",
        "客户认证、长协订单、backlog 或量产交付证据",
        "毛利率 / ASP / CapEx / 在建工程与瓶颈环节的映射",
    ]
    missing.extend(str(p.get("reason")) for p in penalties[:3])
    return list(dict.fromkeys(x for x in missing if x))


def baimao_catalysts(row: dict[str, Any]) -> list[str]:
    milestones = row.get("milestones") or []
    out = [str(m.get("event")) for m in milestones[:3] if m.get("event")]
    if not out:
        out = ["下一季报验证毛利率、订单/backlog、客户认证和扩产节奏"]
    return out


def baimao_positioning(rating: str, verdict: str) -> str:
    if verdict == "strong_candidate":
        return "白毛法强候选；仍需主流程和价格闸门，不直接进入 Buy3"
    if verdict == "validation_candidate":
        return "验证链候选；小仓/观察前必须补一手证据"
    if verdict == "watch_only":
        return "只观察；等待红队证伪解除或证据增强"
    return "跳过；不满足白毛法瓶颈要求"


def baimao_conclusion(row: dict[str, Any], score: float, rating: str, verdict: str, penalties: list[dict[str, Any]]) -> str:
    penalty_text = "；".join(str(p.get("name")) for p in penalties[:3]) or "暂无主要扣分"
    return (
        f"白毛二次分析给 {row.get('symbol')} {score:.1f} 分，卡位评级 {rating}，结论 {verdict}。"
        f"核心瓶颈是 {row.get('bottleneck') or '待确认'}；主要扣分：{penalty_text}。"
    )


def public_bottleneck_action(value: Any) -> str:
    text = str(value or "").strip()
    mapping = {
        "Watch Only": "只观察",
        "watch_only": "只观察",
        "skip": "跳过",
        "进入主流程深度研究": "进入主流程深度研究",
        "观察": "观察",
        "只观察": "只观察",
    }
    return mapping.get(text, text or "观察")


def public_bottleneck_validation(row: dict[str, Any]) -> str:
    red_blocks = [x for x in (row.get("red_team") or []) if x.get("block_buy")]
    evidence = str(row.get("evidence_level") or "弱")
    if red_blocks:
        return "红队阻断，暂不进入买入候选"
    if evidence in {"强", "高"}:
        return "可进入主流程验证；仍需价格和风控确认"
    if evidence == "中":
        return "可跟踪，需订单/客户/产能事实确认"
    return "外部线索，证据偏弱，仅观察"


def public_bottleneck_next_step(row: dict[str, Any]) -> str:
    milestones = row.get("milestones") or []
    if milestones:
        first = milestones[0] or {}
        event = str(first.get("event") or "订单/客户/产能事实验证")
        deadline = str(first.get("deadline") or "未来2个季度内")
        kill = str(first.get("kill_threshold") or "无一手事实则保持只观察")
        return f"{event}｜{deadline}｜{kill}"
    return "订单/客户/产能事实验证｜未来2个季度内｜无一手事实则保持只观察"


def public_bottleneck_bullet(row: dict[str, Any]) -> list[str]:
    symbol = str(row.get("symbol") or "")
    name = str(row.get("name") or symbol)
    title = f"{name} {symbol}" if name and name != symbol else symbol
    return [
        f"- {title}：{row.get('tier') or '外部线索'} / {row.get('role') or '供应链瓶颈观察'}；体系分 {row.get('serenity_score') or '-'}；动作 {public_bottleneck_action(row.get('action'))}",
        f"  观察理由：{row.get('bottleneck') or '待确认瓶颈'}；证据 {row.get('evidence_level') or '弱'}；{public_bottleneck_validation(row)}",
        f"  下一步：{public_bottleneck_next_step(row)}",
    ]


def render_serenity_bottleneck_markdown(rows: list[dict[str, Any]]) -> str:
    today = dt.date.today().isoformat()
    lines = [
        "# 供应链瓶颈观察清单",
        "",
        f"生成日期：{today}",
        "",
        "口径：这是外部线索清单，只提示潜在供应链瓶颈；不参与正式评分，不决定十大观察池，不触发三只买入候选。买入仍需初筛层、深度投研层、投委复核、量化风控复核和价位确认。",
        "",
    ]
    if not rows:
        lines += ["今日没有独立瓶颈候选。", ""]
        return "\n".join(lines).strip() + "\n"
    for idx, row in enumerate(rows, 1):
        bullet = public_bottleneck_bullet(row)
        bullet[0] = f"{idx}. " + bullet[0].lstrip("- ")
        lines += bullet + [""]
    return "\n".join(lines).strip() + "\n"


def build_kronos_symbol_map(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in context.get("symbols") or []:
        symbol = normalize_dsa_symbol(str(row.get("symbol") or ""))
        if symbol:
            out[symbol] = row
    return out


def kronos_score_adjustment(signal: dict[str, Any]) -> float:
    forecast_return = safe_float(signal.get("forecast_return_5d"))
    confidence = clamp(safe_float(signal.get("confidence")), 0, 1)
    if forecast_return >= 1.5:
        return round(min(3.0, forecast_return * 0.35) * max(0.35, confidence), 2)
    if forecast_return <= -1.5:
        return round(max(-5.0, forecast_return * 0.55) * max(0.35, confidence), 2)
    return 0.0


def apply_kronos_signals(rows: list[dict[str, Any]], context: dict[str, Any]) -> list[dict[str, Any]]:
    signals = build_kronos_symbol_map(context)
    if not signals:
        return rows
    adjusted: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        signal = signals.get(normalize_dsa_symbol(str(row.get("symbol") or "")))
        if signal:
            delta = kronos_score_adjustment(signal)
            old_score = safe_float(item.get("score"))
            item["score"] = round(clamp(old_score + delta, 0, 100), 2)
            item["kronos_signal"] = {
                "trend": signal.get("trend", ""),
                "forecast_return_5d": safe_float(signal.get("forecast_return_5d")),
                "confidence": safe_float(signal.get("confidence")),
                "score_adjustment": delta,
                "model": context.get("model", ""),
            }
            item["reason"] = (
                f"{item.get('reason', '')}；Kronos预测：5日 {safe_float(signal.get('forecast_return_5d')):.1f}%"
                f"，趋势 {signal.get('trend', '')}，调分 {delta:+.1f}"
            ).strip("；")
        adjusted.append(item)
    return adjusted


def build_dexter_symbol_map(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in context.get("symbols") or []:
        symbol = normalize_dsa_symbol(str(row.get("symbol") or ""))
        if symbol:
            out[symbol] = row
    return out


def dexter_score_adjustment(signal: dict[str, Any]) -> float:
    stance = str(signal.get("stance") or "")
    confidence = clamp(safe_float(signal.get("confidence")), 0, 1)
    if "看多" in stance:
        return round(min(2.0, 2.0 * confidence), 2)
    if any(term in stance for term in ("谨慎", "看空", "回避")):
        return round(max(-3.0, -3.0 * max(0.35, confidence)), 2)
    return 0.0


def apply_dexter_signals(rows: list[dict[str, Any]], context: dict[str, Any]) -> list[dict[str, Any]]:
    signals = build_dexter_symbol_map(context)
    if not signals:
        return rows
    adjusted: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        signal = signals.get(normalize_dsa_symbol(str(row.get("symbol") or "")))
        if signal:
            delta = dexter_score_adjustment(signal)
            old_score = safe_float(item.get("score"))
            item["score"] = round(clamp(old_score + delta, 0, 100), 2)
            item["dexter_signal"] = {
                "status": signal.get("status", ""),
                "stance": signal.get("stance", ""),
                "confidence": safe_float(signal.get("confidence")),
                "data_sources_used": signal.get("data_sources_used", []),
                "key_points": signal.get("key_points", []),
                "risks": signal.get("risks", []),
                "watch_items": signal.get("watch_items", []),
                "summary": signal.get("summary", ""),
                "score_adjustment": delta,
            }
            summary = str(signal.get("summary") or "")
            item["reason"] = (
                f"{item.get('reason', '')}；Dexter美股辅助：{signal.get('stance', '中性')}"
                f"，置信度 {safe_float(signal.get('confidence')):.2f}，调分 {delta:+.1f}，{complete_excerpt(strip_actionable_price_sentences(summary), 160)}"
            ).strip("；")
        adjusted.append(item)
    return adjusted


def stage_vibe_trading_review(top10: list[dict[str, Any]], env: dict[str, str]) -> dict[str, Any]:
    if env.get("VIBE_TRADING_ENABLED", "0") != "1":
        context = {"status": "skipped", "symbols": []}
        write_json(OUTPUTS / "vibe_trading_review.json", context)
        return context

    command = env.get("VIBE_TRADING_COMMAND", "vibe-trading")
    top_n = max(1, int(env.get("VIBE_TRADING_TOP_N", "5")))
    timeout = int(env.get("VIBE_TRADING_TIMEOUT_PER_SYMBOL", env.get("VIBE_TRADING_TIMEOUT", "420")))
    workers = max(1, int(env.get("VIBE_TRADING_WORKERS", "2")))
    review_rows = top10[:top_n]
    if not review_rows:
        context = {"status": "no_data", "symbols": []}
        write_json(OUTPUTS / "vibe_trading_review.json", context)
        return context

    def review_one(row: dict[str, Any]) -> dict[str, Any]:
        symbol = normalize_dsa_symbol(str(row.get("symbol") or ""))
        prompt = build_vibe_trading_prompt([row])
        run_env = without_broken_local_proxy(env)
        run_env.setdefault("LANGCHAIN_PROVIDER", env.get("VIBE_TRADING_PROVIDER", "openai"))
        run_env.setdefault("LANGCHAIN_MODEL_NAME", env.get("VIBE_TRADING_MODEL", env.get("OPENAI_MODEL", "gpt-4o-mini")))
        run_env.setdefault("TIMEOUT_SECONDS", env.get("VIBE_TRADING_TOOL_TIMEOUT", "180"))
        try:
            rc, text = run([command, "run", "-p", prompt], ROOT, run_env, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            text = str(exc.output or "")
            write_text(WORK / f"vibe_trading_{safe_name(symbol)}.log", text)
            return {
                "symbol": symbol,
                "name": row.get("name", ""),
                "status": "fallback",
                "error": f"Vibe-Trading 超时 {timeout}s",
                "log_tail": text[-1000:],
            }

        write_text(WORK / f"vibe_trading_{safe_name(symbol)}.log", text)
        parsed = parse_last_json(text)
        if rc != 0 or not isinstance(parsed, dict):
            return {
                "symbol": symbol,
                "name": row.get("name", ""),
                "status": "fallback",
                "error": summarize_failure(text, rc),
                "log_tail": text[-1000:],
            }
        symbols = parsed.get("symbols") if isinstance(parsed.get("symbols"), list) else []
        if symbols and isinstance(symbols[0], dict):
            out = dict(symbols[0])
            out.setdefault("symbol", symbol)
            out.setdefault("name", row.get("name", ""))
            out.setdefault("status", "ok")
            return out
        parsed.setdefault("symbol", symbol)
        parsed.setdefault("name", row.get("name", ""))
        parsed.setdefault("status", "ok")
        return parsed

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(workers, len(review_rows))) as executor:
        future_map = {executor.submit(review_one, row): row for row in review_rows}
        for future in as_completed(future_map):
            try:
                results.append(future.result())
            except Exception as exc:
                row = future_map[future]
                results.append({
                    "symbol": normalize_dsa_symbol(str(row.get("symbol") or "")),
                    "name": row.get("name", ""),
                    "status": "fallback",
                    "error": str(exc),
                })

    ok_count = sum(1 for row in results if row.get("status") == "ok")
    context = {
        "status": "ok" if ok_count else "fallback",
        "symbol_count": ok_count,
        "workers": min(workers, len(review_rows)),
        "timeout_per_symbol": timeout,
        "symbols": sorted(results, key=lambda row: [r.get("symbol") for r in review_rows].index(row.get("symbol")) if row.get("symbol") in [r.get("symbol") for r in review_rows] else 999),
    }
    write_json(OUTPUTS / "vibe_trading_review.json", context)
    write_text(WORK / "vibe_trading_review.log", json.dumps(context, ensure_ascii=False, indent=2))
    return context


def build_vibe_trading_prompt(rows: list[dict[str, Any]]) -> str:
    enriched = read_json_if_exists(OUTPUTS / "enriched_stock_data.json", {})
    enriched_map = {
        normalize_dsa_symbol(str(row.get("symbol") or "")): row
        for row in (enriched.get("symbols") or []) if isinstance(enriched, dict) and isinstance(row, dict)
    }
    payload = []
    for row in rows:
        enriched_row = enriched_map.get(normalize_dsa_symbol(str(row.get("symbol") or "")), {})
        payload.append({
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "total_score": row.get("total_score"),
            "rating": row.get("rating"),
            "action": row.get("action"),
            "risk": row.get("risk"),
            "buy_zone": row.get("buy_zone"),
            "breakout_price": row.get("breakout_price"),
            "stop_loss": row.get("stop_loss"),
            "reason": complete_excerpt(str(row.get("reason") or ""), 700),
            "pipeline_data": compact_agent_data(enriched_row),
        })
    return (
        "你是 Vibe-Trading 复核层。请只用中文。对下面候选股做回测/技术条件/风控复核，"
        "重点检查当前价位是否适合买入、是否需要等待回踩、止损是否合理、是否存在短期过热。"
        "不要改变原始排序，不要新增股票。最后必须输出严格 JSON，格式如下："
        '{"status":"ok","symbols":[{"symbol":"NVDA","stance":"通过|观察|谨慎",'
        '"confidence":0.0,"summary":"120字以内复核结论","entry_check":"入场条件",'
        '"exit_check":"退出条件","backtest_note":"回测或因子证据简述","risks":["最多3条"]}]}。'
        "JSON 之外不要输出其他文字。候选股："
        + json.dumps(payload, ensure_ascii=False)
    )


def apply_vibe_trading_review(rows: list[dict[str, Any]], context: dict[str, Any]) -> list[dict[str, Any]]:
    review_map: dict[str, dict[str, Any]] = {}
    for row in context.get("symbols") or []:
        symbol = normalize_dsa_symbol(str(row.get("symbol") or ""))
        if symbol:
            review_map[symbol] = row
    if not review_map:
        return rows

    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        review = review_map.get(normalize_dsa_symbol(str(item.get("symbol") or "")))
        if review:
            stance = str(review.get("stance") or "")
            confidence = safe_float(review.get("confidence"))
            item["vibe_trading_review"] = {
                "stance": stance,
                "confidence": confidence,
                "summary": strip_actionable_price_sentences(str(review.get("summary", ""))),
                "entry_check": "执行价位以最终价格计划为准，Vibe 原始入场价位不作为下单依据",
                "exit_check": "执行价位以最终价格计划为准，Vibe 原始退出价位不作为下单依据",
                "backtest_note": strip_actionable_price_sentences(str(review.get("backtest_note", ""))),
                "risks": review.get("risks", []),
            }
            if any(term in stance for term in ("谨慎", "回避")) and confidence >= 0.60:
                gates = list(item.get("quality_gates") or [])
                gates.append("Vibe-Trading 复核偏谨慎")
                item["quality_gates"] = gates
                item["quality_note"] = "；".join(gates)
                item["buy_eligible"] = False
                item["action"] = "观察" if str(item.get("action")) == "买入" else item.get("action", "观察")
                item["total_score"] = round(min(safe_float(item.get("total_score")), 64.0), 2)
                item["risk_adjusted_score"] = item["total_score"]
                raw_total = safe_float(item.get("raw_total_score"), item["total_score"])
                item["score_cap_reason"] = "；".join(gates) if item["total_score"] < raw_total else item.get("score_cap_reason", "")
            item["reason"] = (
                f"{item.get('reason', '')}；Vibe-Trading复核：{stance}，"
                f"{complete_excerpt(strip_actionable_price_sentences(str(review.get('summary', ''))), 180)}"
            ).strip("；")
            normalize_execution_text(item)
        out.append(prepare_report_row(item))
    return sorted(out, key=lambda x: safe_float(x.get("total_score")), reverse=True)


def chunked(items: list[dict[str, str]], size: int) -> list[list[dict[str, str]]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def score_from_stock_daily(
    db_path: Path,
    pool: list[dict[str, str]],
    openbb_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    names = {item["symbol"]: item.get("name", "") for item in pool}
    groups = {item["symbol"]: item.get("group", "") for item in pool}
    openbb_map = build_openbb_symbol_map(openbb_context or {})
    enriched_context = read_json_if_exists(OUTPUTS / "enriched_stock_data.json", {})
    enriched_map = build_enriched_symbol_map(enriched_context if isinstance(enriched_context, dict) else {})
    for symbol, row in enriched_map.items():
        if row and symbol not in openbb_map:
            openbb_map[symbol] = row
    results: list[dict[str, Any]] = []
    con = sqlite3.connect(str(db_path)) if db_path.exists() else None
    if con:
        con.row_factory = sqlite3.Row
        try:
            has_stock_daily = bool(con.execute(
                "select 1 from sqlite_master where type='table' and name='stock_daily'"
            ).fetchone())
        except sqlite3.Error:
            has_stock_daily = False
    else:
        has_stock_daily = False
    try:
        for symbol in names:
            openbb_row = openbb_map.get(symbol)
            if prefer_market_data_source(symbol) and openbb_row:
                results.append(score_from_market_data(symbol, names[symbol], groups[symbol], openbb_row, primary=True))
                continue
            if not con or not has_stock_daily:
                if openbb_row:
                    results.append(score_from_market_data(symbol, names[symbol], groups[symbol], openbb_row, primary=False))
                else:
                    results.append({
                        "symbol": symbol,
                        "name": names[symbol],
                        "group": groups[symbol],
                        "score": 0.0,
                        "status": "no_data",
                        "reason": "行情中台与 daily_stock_analysis 均未取到有效日线数据",
                    })
                continue
            code_variants = stock_daily_code_variants(symbol)
            placeholders = ",".join("?" for _ in code_variants)
            rows = con.execute(
                f"""
                select date, close, pct_chg, ma5, ma10, ma20, volume, volume_ratio
                from stock_daily
                where code in ({placeholders})
                order by date desc
                limit 30
                """,
                tuple(code_variants),
            ).fetchall()
            if not rows:
                if openbb_row:
                    results.append(score_from_market_data(symbol, names[symbol], groups[symbol], openbb_row, primary=False))
                else:
                    results.append({
                        "symbol": symbol,
                        "name": names[symbol],
                        "group": groups[symbol],
                        "score": 0.0,
                        "status": "no_data",
                        "reason": "行情中台与 daily_stock_analysis 均未取到有效日线数据",
                    })
                continue
            latest = rows[0]
            closes = [safe_float(r["close"]) for r in reversed(rows) if safe_float(r["close"]) > 0]
            pct_1d = safe_float(latest["pct_chg"])
            ret_20 = ((closes[-1] / closes[0] - 1) * 100) if len(closes) >= 2 else 0.0
            vol_ratio = valid_volume_ratio(latest["volume_ratio"])
            ma20 = safe_float(latest["ma20"])
            close = safe_float(latest["close"])
            trend_bonus = 12 if ma20 and close > ma20 else -8
            momentum = clamp(50 + ret_20 * 1.8 + pct_1d * 1.2, 0, 100)
            volume_score = volume_score_from_ratio(vol_ratio)
            stability = clamp(100 - volatility(closes) * 5, 0, 100)
            score = clamp(momentum * 0.45 + volume_score * 0.20 + stability * 0.20 + (50 + trend_bonus) * 0.15)
            results.append({
                "symbol": symbol,
                "name": names[symbol],
                "group": groups[symbol],
                "score": round(score, 2),
                "status": "ok",
                "latest_date": latest["date"],
                "close": close,
                "ma20": round(ma20, 2),
                "pct_1d": round(pct_1d, 2),
                "ret_20d": round(ret_20, 2),
                "volume_ratio": round(vol_ratio, 2) if vol_ratio is not None else None,
                "reason": f"20日涨跌 {ret_20:.1f}%，{format_volume_ratio(vol_ratio)}，收盘{'高于' if ma20 and close > ma20 else '低于'}MA20",
            })
    finally:
        if con:
            con.close()
    return results


def prefer_market_data_source(symbol: str) -> bool:
    s = normalize_dsa_symbol(symbol)
    if s.lower().startswith("hk"):
        return True
    if s.endswith(".SH") or s.endswith(".SZ"):
        return False
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9.\-]{0,9}", s))


def build_openbb_symbol_map(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in context.get("symbols") or []:
        original = normalize_dsa_symbol(str(row.get("original_symbol") or ""))
        if original:
            out[original] = row
        symbol = normalize_dsa_symbol(str(row.get("symbol") or ""))
        if symbol:
            out.setdefault(symbol, row)
    return out


def build_enriched_symbol_map(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in context.get("symbols") or []:
        if not isinstance(row, dict):
            continue
        symbol = normalize_dsa_symbol(str(row.get("symbol") or ""))
        if symbol:
            out[symbol] = row
        original = normalize_dsa_symbol(str(row.get("original_symbol") or ""))
        if original:
            out.setdefault(original, row)
    return out


def compact_agent_data(row: dict[str, Any]) -> dict[str, Any]:
    if not row:
        return {}
    fundamentals = row.get("fundamentals") or {}
    technical = row.get("technical") or {}
    klines = row.get("klines") or []
    return {
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "close": row.get("close"),
        "ret_20d": row.get("ret_20d"),
        "volume_ratio": row.get("volume_ratio"),
        "technical": technical,
        "fundamentals": {
            "source": fundamentals.get("source"),
            "pe": fundamentals.get("pe"),
            "pb": fundamentals.get("pb"),
            "market_cap_yi": fundamentals.get("market_cap_yi"),
            "revenue_history": fundamentals.get("revenue_history"),
            "net_profit_history": fundamentals.get("net_profit_history"),
            "roe_history": fundamentals.get("roe_history"),
            "financial_health": fundamentals.get("financial_health"),
            "target_price": fundamentals.get("target_price"),
            "coverage_count": fundamentals.get("coverage_count"),
        },
        "recent_klines": klines[-30:] if isinstance(klines, list) else [],
        "field_sources": row.get("field_sources") or {},
        "data_quality_flags": row.get("data_quality_flags") or [],
    }


def score_from_market_data(symbol: str, name: str, group: str, row: dict[str, Any], *, primary: bool) -> dict[str, Any]:
    ret_20 = safe_float(row.get("ret_20d"))
    vol_ratio = valid_volume_ratio(row.get("volume_ratio"))
    close = safe_float(row.get("close"))
    provider = row.get("provider") or "OpenBB"
    momentum = clamp(50 + ret_20 * 1.8, 0, 100)
    volume_score = volume_score_from_ratio(vol_ratio)
    trend_score = clamp(50 + ret_20 * 1.2, 0, 100)
    score = clamp(momentum * 0.50 + volume_score * 0.20 + trend_score * 0.20 + 50 * 0.10)
    return {
        "symbol": symbol,
        "name": name,
        "group": group,
        "score": round(score, 2),
        "status": "market_data_primary" if primary else "market_data_fill",
        "latest_date": row.get("date") or row.get("generated_at") or "",
        "close": close,
        "ma20": safe_float(row.get("ma20")),
        "pct_1d": 0.0,
        "ret_20d": round(ret_20, 2),
        "volume_ratio": round(vol_ratio, 2) if vol_ratio is not None else None,
        "reason": (
            f"行情中台 OpenBB/{provider} 提供日线：20日涨跌 {ret_20:.1f}%，{format_volume_ratio(vol_ratio)}"
            if primary else
            f"daily_stock_analysis 本地日线未覆盖该代码，采用行情中台 OpenBB/{provider}：20日涨跌 {ret_20:.1f}%，{format_volume_ratio(vol_ratio)}"
        ),
    }


def build_enriched_stock_data(
    pool: list[dict[str, str]],
    env: dict[str, str],
    *,
    openbb_context: dict[str, Any] | None = None,
    candidates: list[dict[str, Any]] | None = None,
    trading: list[dict[str, Any]] | None = None,
    uzi: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the shared stock data package consumed by every agent.

    The package is intentionally source-rich: each agent can complete its work
    from this file even when its own preferred vendor is rate-limited.
    """
    openbb_map = build_openbb_symbol_map(openbb_context or read_json_if_exists(OUTPUTS / "openbb_context.json", {}))
    candidate_map = {
        normalize_dsa_symbol(str(row.get("symbol") or "")): row
        for row in (candidates if candidates is not None else read_json_if_exists(OUTPUTS / "candidates_top50.json", []))
        if isinstance(row, dict)
    }
    trading_map = {
        normalize_dsa_symbol(str(row.get("symbol") or "")): row
        for row in (trading if trading is not None else read_json_if_exists(OUTPUTS / "tradingagents_top20.json", []))
        if isinstance(row, dict)
    }
    uzi_map = {
        normalize_dsa_symbol(str(row.get("symbol") or "")): row
        for row in (uzi if uzi is not None else read_json_if_exists(OUTPUTS / "uzi_top10.json", []))
        if isinstance(row, dict)
    }
    pool_map = {normalize_dsa_symbol(item.get("symbol", "")): item for item in pool}
    symbols = list(dict.fromkeys([
        *pool_map.keys(),
        *candidate_map.keys(),
        *trading_map.keys(),
        *uzi_map.keys(),
    ]))
    max_workers = max(1, int(env.get("PIPELINE_ENRICH_WORKERS", "8")))
    existing_context = read_json_if_exists(OUTPUTS / "enriched_stock_data.json", {})
    existing_map = build_enriched_symbol_map(existing_context if isinstance(existing_context, dict) else {})
    fetch_fundamentals = env.get("PIPELINE_ENRICH_FUNDAMENTALS", env.get("PIPELINE_UZI_FUNDAMENTAL_FETCH", "1")) == "1"
    fundamental_scope = env.get("PIPELINE_ENRICH_FUNDAMENTAL_SCOPE", "candidates").strip().lower()

    def build_one(symbol: str) -> dict[str, Any]:
        pool_item = pool_map.get(symbol, {})
        candidate = candidate_map.get(symbol, {})
        market = openbb_map.get(symbol) or openbb_map.get(normalize_dsa_symbol(str(candidate.get("symbol") or ""))) or {}
        existing = existing_map.get(symbol) or {}
        fundamentals = dict(existing.get("fundamentals") or {})
        close = first_positive(market.get("close"), candidate.get("close"), fundamentals.get("price"))
        fundamentals = anchor_fundamentals_to_market_price(fundamentals, close, market.get("provider") or candidate.get("provider"))
        should_fetch_fundamentals = (
            fetch_fundamentals
            and (fundamental_scope == "all" or symbol in candidate_map or symbol in trading_map or symbol in uzi_map)
            and fundamentals_need_completion(fundamentals)
        )
        if should_fetch_fundamentals:
            fundamentals = complete_pipeline_fundamentals(symbol, fundamentals, env)
        klines = normalize_pipeline_klines(market.get("klines") or market.get("candles") or [])
        if not klines:
            klines = synthesize_pipeline_klines(candidate or market or {"price": fundamentals.get("price")})
        if not klines and fundamentals:
            klines = synthesize_pipeline_klines({"price": fundamentals.get("price"), "ret_20d": candidate.get("ret_20d") or market.get("ret_20d")})
        field_sources = dict(fundamentals.get("_field_sources") or {})
        if market.get("provider"):
            field_sources.setdefault("klines", str(market.get("provider")))
            field_sources.setdefault("price", str(market.get("provider")))
        if candidate:
            field_sources.setdefault("score", "daily_stock_analysis")
        quality = data_quality_flags(candidate, market, fundamentals, klines)
        return {
            "symbol": symbol,
            "name": pool_item.get("name") or candidate.get("name") or fundamentals.get("name") or symbol,
            "group": pool_item.get("group") or candidate.get("group") or "",
            "close": close,
            "ret_20d": safe_float(candidate.get("ret_20d"), safe_float(market.get("ret_20d"))),
            "volume_ratio": (
                valid_volume_ratio(candidate.get("volume_ratio"))
                if valid_volume_ratio(candidate.get("volume_ratio")) is not None
                else valid_volume_ratio(market.get("volume_ratio"))
            ),
            "score": safe_float(candidate.get("score")),
            "status": candidate.get("status") or ("market_data_fill" if market else "enriched"),
            "reason": candidate.get("reason") or "",
            "provider": market.get("provider") or ("candidates/synthetic" if klines and klines[-1].get("synthetic") else "pipeline/enriched"),
            "original_symbol": market.get("original_symbol") or symbol,
            "klines": klines,
            "technical": build_enriched_technical(candidate, market, klines),
            "fundamentals": fundamentals,
            "tradingagents": trading_map.get(symbol, {}),
            "uzi": uzi_map.get(symbol, {}),
            "external_signals": candidate.get("external_signal") or candidate.get("serenity_signal") or {},
            "field_sources": field_sources,
            "data_quality_flags": quality,
        }

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(max_workers, max(1, len(symbols)))) as executor:
        future_map = {executor.submit(build_one, symbol): symbol for symbol in symbols if symbol}
        for future in as_completed(future_map):
            try:
                rows.append(future.result())
            except Exception as exc:
                symbol = future_map[future]
                item = pool_map.get(symbol, {})
                rows.append({
                    "symbol": symbol,
                    "name": item.get("name") or symbol,
                    "group": item.get("group") or "",
                    "status": "enrich_error",
                    "error": str(exc)[:240],
                    "klines": [],
                    "fundamentals": {},
                    "field_sources": {},
                    "data_quality_flags": ["enrich_error"],
                })
    order = {symbol: idx for idx, symbol in enumerate(symbols)}
    rows.sort(key=lambda row: order.get(row.get("symbol"), 10**9))
    context = {
        "status": "ok",
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "symbol_count": len(rows),
        "complete_count": sum(1 for row in rows if not row.get("data_quality_flags")),
        "symbols": rows,
    }
    write_json(OUTPUTS / "enriched_stock_data.json", context)
    return context


def normalize_pipeline_klines(rows: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        close = first_positive(row.get("close"), row.get("Close"), row.get("收盘"))
        if close <= 0:
            continue
        out.append({
            "date": str(row.get("date") or row.get("Date") or row.get("日期") or ""),
            "open": first_positive(row.get("open"), row.get("Open"), row.get("开盘"), close),
            "high": first_positive(row.get("high"), row.get("High"), row.get("最高"), close),
            "low": first_positive(row.get("low"), row.get("Low"), row.get("最低"), close),
            "close": close,
            "volume": safe_float(row.get("volume") or row.get("Volume") or row.get("成交量")),
            **({"synthetic": True} if row.get("synthetic") else {}),
        })
    return out


def synthesize_pipeline_klines(row: dict[str, Any]) -> list[dict[str, Any]]:
    fundamentals = row.get("fundamentals") or {}
    close = first_positive(row.get("close"), row.get("price"), fundamentals.get("price"))
    if close <= 0:
        return []
    ret_20 = safe_float(row.get("ret_20d"))
    start = close / (1 + ret_20 / 100) if ret_20 > -95 else close
    today = dt.date.today()
    volume_ratio = valid_volume_ratio(row.get("volume_ratio")) or 1.0
    out = []
    for idx in range(22):
        date = today - dt.timedelta(days=21 - idx)
        price = start + (close - start) * idx / 21
        spread = max(price * 0.006, 0.01)
        out.append({
            "date": date.isoformat(),
            "open": round(price, 4),
            "high": round(price + spread, 4),
            "low": round(max(price - spread, 0.01), 4),
            "close": round(price, 4),
            "volume": round(1_000_000 * volume_ratio),
            "synthetic": True,
        })
    return out


def build_enriched_technical(candidate: dict[str, Any], market: dict[str, Any], klines: list[dict[str, Any]]) -> dict[str, Any]:
    closes = [safe_float(row.get("close")) for row in klines if safe_float(row.get("close")) > 0]
    close = closes[-1] if closes else first_positive(candidate.get("close"), market.get("close"))
    ma20 = safe_float(candidate.get("ma20")) or (sum(closes[-20:]) / min(len(closes), 20) if closes else 0)
    return {
        "close": close,
        "ma20": round(ma20, 4) if ma20 else 0,
        "ret_20d": safe_float(candidate.get("ret_20d"), safe_float(market.get("ret_20d"))),
        "volume_ratio": (
            valid_volume_ratio(candidate.get("volume_ratio"))
            if valid_volume_ratio(candidate.get("volume_ratio")) is not None
            else valid_volume_ratio(market.get("volume_ratio"))
        ),
        "rsi_estimate": round(estimate_rsi(closes), 2) if closes else 50.0,
        "synthetic_kline": bool(klines and klines[-1].get("synthetic")),
    }


def data_quality_flags(
    candidate: dict[str, Any],
    market: dict[str, Any],
    fundamentals: dict[str, Any],
    klines: list[dict[str, Any]],
) -> list[str]:
    flags: list[str] = []
    if not market:
        flags.append("market_row_missing")
    if not klines:
        flags.append("klines_missing")
    elif klines[-1].get("synthetic"):
        flags.append("klines_synthetic_from_candidate")
    if not fundamentals:
        flags.append("fundamentals_missing")
    else:
        if not (fundamentals.get("revenue_history") or fundamentals.get("net_profit_history") or fundamentals.get("roe_history")):
            flags.append("financial_statement_thin")
        if not first_positive(fundamentals.get("pe"), fundamentals.get("pb"), fundamentals.get("market_cap_raw"), fundamentals.get("market_cap_yi")):
            flags.append("valuation_thin")
    if candidate and candidate.get("status") == "no_data":
        flags.append("dsa_no_data")
    return flags


def score_from_openbb(symbol: str, name: str, group: str, row: dict[str, Any]) -> dict[str, Any]:
    return score_from_market_data(symbol, name, group, row, primary=False)


def stock_daily_code_variants(symbol: str) -> list[str]:
    s = normalize_dsa_symbol(symbol)
    if s.lower().startswith("hk"):
        digits = s[2:].zfill(5)
        return [f"HK{digits}", f"hk{digits}", digits]
    return [s, s.upper(), s.lower()]


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        f = float(value)
        return default if math.isnan(f) else f
    except Exception:
        return default


def determine_run_mode(env: dict[str, str], args: object) -> str:
    cli_mode = str(getattr(args, "run_mode", "") or "").strip().lower()
    if cli_mode in {"formal", "smoke", "diagnostic"}:
        return cli_mode
    if bool(getattr(args, "smoke", False)):
        return "smoke"
    if bool(getattr(args, "diagnostic", False)):
        return "diagnostic"
    env_mode = str(env.get("PIPELINE_RUN_MODE", "") or "").strip().lower()
    if env_mode in {"formal", "smoke", "diagnostic"}:
        return env_mode
    return "formal"


def status_is_full_tradingagents(row: dict[str, object]) -> bool:
    return str(row.get("ta_status") or "") == "full"


def status_is_ok_uzi(row: dict[str, object]) -> bool:
    if str(row.get("status") or "") != "ok":
        return False
    hard_flags = {
        "UZI 未返回有效投委结果",
        "UZI 原始数据缺失",
        "UZI 财务维度不足",
        "UZI 基础行情缺失",
        "UZI 输出疑似默认低分模板",
        "UZI 批量重复低分",
        "UZI 二次复核仍未通过",
    }
    flags = {str(flag) for flag in (row.get("quality_flags") or [])}
    return not any(any(marker in flag for marker in hard_flags) for flag in flags)


def valid_volume_ratio(value: Any) -> float | None:
    try:
        if value is None:
            return None
        ratio = float(value)
    except Exception:
        return None
    if math.isnan(ratio) or math.isinf(ratio):
        return None
    if ratio <= 0:
        return None
    if ratio > 50:
        return None
    return ratio


def valid_positive_number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number) or number <= 0:
        return None
    return number


def format_volume_ratio(value: Any) -> str:
    ratio = valid_volume_ratio(value)
    if ratio is None:
        return "量能数据缺失，量比不可用"
    return f"量比 {ratio:.2f}"


def normalize_volume_ratio_text(text: Any) -> str:
    clean = str(text or "")

    def repl(match: re.Match[str]) -> str:
        return format_volume_ratio(match.group(1))

    return re.sub(r"(?:成交量比(?:仅)?|量比)\s*(\d+(?:\.\d*)?)", repl, clean)


def volume_score_from_ratio(value: Any) -> float:
    ratio = valid_volume_ratio(value)
    if ratio is None:
        return 50.0
    return clamp(50 + (ratio - 1.0) * 18, 0, 100)


def volatility(values: list[float]) -> float:
    if len(values) < 3:
        return 10.0
    rets = []
    for a, b in zip(values, values[1:]):
        if a:
            rets.append((b / a - 1) * 100)
    if not rets:
        return 10.0
    mean = sum(rets) / len(rets)
    return math.sqrt(sum((r - mean) ** 2 for r in rets) / len(rets))


def run_tradingagents_with_timeout(
    candidates: list[dict[str, Any]],
    *,
    trading_dir: str,
    python_bin: str,
    env: dict[str, str],
    per_stock_timeout: int,
    stage_timeout: int,
    max_workers: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run TA with watchdog timeout and block formal output on timeout."""
    import concurrent.futures
    from concurrent.futures import TimeoutError as FutureTimeoutError

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(
        run_tradingagents_full_batch,
        candidates,
        trading_dir=trading_dir,
        python_bin=python_bin,
        env=env,
        per_stock_timeout=per_stock_timeout,
        stage_timeout=stage_timeout,
        max_workers=max_workers,
    )

    try:
        out, ta_meta = future.result(timeout=stage_timeout + 60)
        print(f"[TA-WATCHDOG] Full batch completed in time", flush=True)
        return out, ta_meta
    except FutureTimeoutError:
        print(f"[TA-WATCHDOG] Timeout exceeded ({stage_timeout + 60}s), blocking formal TA output", flush=True)
        future.cancel()

        failed_symbols = [c.get("symbol", "?") for c in candidates]
        ta_meta = {
            "ta_stage_status": "watchdog_timeout",
            "ta_completed_full": 0,
            "ta_failed_symbols": failed_symbols,
            "ta_total_symbols": len(candidates),
            "ta_failure_reason": f"watchdog timeout after {stage_timeout + 60}s",
        }
        return [], ta_meta
    finally:
        executor.shutdown(wait=False)


def stage_tradingagents(candidates: list[dict[str, Any]], env: dict[str, str], top_n: int) -> list[dict[str, Any]]:
    print(f"[TA] Starting TradingAgents stage with {len(candidates)} candidates, top_n={top_n}", flush=True)
    if env.get("PIPELINE_SKIP_TRADINGAGENTS") == "1":
        out = [fallback_trading(item, "skipped") for item in candidates[:top_n]]
        write_json(OUTPUTS / "tradingagents_stage_meta.json", {
            "ta_stage_status": "skipped",
            "ta_completed_full": 0,
            "ta_failed_symbols": [str(item.get("symbol") or "?") for item in candidates[:top_n]],
            "ta_total_symbols": min(len(candidates), top_n),
        })
        write_json(OUTPUTS / "tradingagents_top20.json", out)
        return out

    trading_dir = Path(env["TRADINGAGENTS_DIR"])
    python_bin = env["TRADINGAGENTS_PYTHON"]
    if not trading_dir.exists() or not Path(python_bin).exists():
        out = [fallback_trading(item, "TradingAgents 未安装或路径不可用") for item in candidates[:top_n]]
        write_json(OUTPUTS / "tradingagents_stage_meta.json", {
            "ta_stage_status": "unavailable",
            "ta_completed_full": 0,
            "ta_failed_symbols": [str(item.get("symbol") or "?") for item in candidates[:top_n]],
            "ta_total_symbols": min(len(candidates), top_n),
        })
        write_json(OUTPUTS / "tradingagents_top20.json", out)
        return out
    scan_n = min(len(candidates), int(env.get("PIPELINE_TRADINGAGENTS_SCAN_N", str(max(50, top_n)))))
    full_n = min(scan_n, max(1, int(env.get("PIPELINE_TRADINGAGENTS_FULL_N", str(top_n)))))
    timeout = int(env.get("PIPELINE_TRADINGAGENTS_FULL_TIMEOUT_PER_STOCK", env.get("PIPELINE_TRADINGAGENTS_TIMEOUT_PER_STOCK", "900")))
    workers = max(1, int(env.get("PIPELINE_TRADINGAGENTS_WORKERS", "2")))

    # Parallelize quick_trading_research to speed up screening
    print(f"[TA] Screening {scan_n} candidates with quick research...", flush=True)
    quick_rows = []
    with ThreadPoolExecutor(max_workers=min(workers, 4)) as executor:
        futures = [executor.submit(quick_trading_research, item) for item in candidates[:scan_n]]
        for i, future in enumerate(futures, 1):
            try:
                result = future.result(timeout=60)
                quick_rows.append(result)
                if i % 5 == 0:
                    print(f"[TA] Screened {i}/{scan_n} candidates", flush=True)
            except Exception as e:
                print(f"[TA] Screening error for candidate {i}: {e}", flush=True)
                quick_rows.append(quick_trading_research(candidates[i-1]))
    print(f"[TA] Screening complete: {len(quick_rows)} results", flush=True)

    quick_ranked = sorted(quick_rows, key=lambda x: trading_rank_score(x), reverse=True)
    write_json(OUTPUTS / "tradingagents_quick_top50.json", quick_ranked[:scan_n])

    candidate_map = {item["symbol"]: item for item in candidates}
    scan_items = [candidate_map[row["symbol"]] for row in quick_ranked[:full_n] if row["symbol"] in candidate_map]

    ta_full_timeout = int(env.get("PIPELINE_TRADINGAGENTS_STAGE_TIMEOUT", "3600"))
    print(f"[TA] Running {len(scan_items)} full analyses with {ta_full_timeout}s stage timeout", flush=True)
    out, ta_meta = run_tradingagents_with_timeout(
        scan_items,
        trading_dir=str(trading_dir),
        python_bin=python_bin,
        env=env,
        per_stock_timeout=timeout,
        stage_timeout=ta_full_timeout,
        max_workers=workers,
    )
    write_json(OUTPUTS / "tradingagents_stage_meta.json", ta_meta)

    allow_incomplete = env.get("PIPELINE_TRADINGAGENTS_ALLOW_INCOMPLETE_FINAL", "0") == "1"
    if allow_incomplete and len(out) < top_n:
        completed = {row.get("symbol") for row in out}
        for row in quick_ranked:
            if len(out) >= top_n:
                break
            symbol = row.get("symbol")
            if symbol not in completed and symbol in candidate_map:
                out.append(row)
                completed.add(symbol)

    full_rows = [row for row in out if row.get("ta_status") == "full"]
    ranked_source = out if allow_incomplete else full_rows
    ranked = sorted(ranked_source, key=lambda x: trading_rank_score(x), reverse=True)[:top_n]
    write_json(OUTPUTS / "tradingagents_full_top20.json", out)
    write_json(OUTPUTS / "tradingagents_top20.json", ranked)
    return ranked


def quick_trading_research(item: dict[str, Any], note: str = "") -> dict[str, Any]:
    score = safe_float(item.get("score"))
    ret_20 = safe_float(item.get("ret_20d"))
    vol_ratio = valid_volume_ratio(item.get("volume_ratio"))
    overheated = ret_20 >= 30
    momentum_component = clamp(ret_20, -20, 18) * 0.20
    volume_component = clamp((vol_ratio - 1.0) * 5, -5, 8) if vol_ratio is not None else 0.0
    overheat_penalty = 18 if ret_20 >= 50 else 12 if overheated else 0
    failure_penalty = 10 if note else 0
    confidence = clamp((score + momentum_component + volume_component - overheat_penalty - failure_penalty) / 100.0, 0, 1)
    if score >= 78 and 0 <= ret_20 <= 25 and not note:
        action = "BUY"
    elif score >= 58:
        action = "HOLD"
    else:
        action = "WATCH"
    if ret_20 >= 35 or (vol_ratio is not None and vol_ratio >= 4):
        risk = "high"
    elif ret_20 >= 18 or (vol_ratio is not None and vol_ratio >= 1.8):
        risk = "medium"
    else:
        risk = "low" if score >= 65 else "medium"
    source = "行情中台/初筛"
    suffix = f"；完整版 TradingAgents {translate_failure_note(note)}，暂用快速研究分" if note else "；未进入 TradingAgents 完整版名额"
    heat_note = "；短期涨幅过热，只能列入观察池" if overheated else ""
    return {
        "symbol": item["symbol"],
        "ta_symbol": to_tradingagents_symbol(item["symbol"]),
        "name": item.get("name", ""),
        "dsa_score": score,
        "action": action,
        "confidence": round(confidence, 4),
        "risk": risk,
        "reason": (
            f"TradingAgents 快速研究：基于{source}，初筛分 {score:.1f}，"
            f"20日涨跌 {ret_20:.1f}%，{format_volume_ratio(vol_ratio)}{heat_note}{suffix}"
        ),
        "ta_status": "quick" if not note else "quick_fallback",
        "ta_note": note or "not_scheduled_full",
    }


def failed_tradingagents_result(item: dict[str, Any], note: str, error_type: str) -> dict[str, Any]:
    score = safe_float(item.get("score"))
    return {
        "symbol": item["symbol"],
        "ta_symbol": to_tradingagents_symbol(item["symbol"]),
        "name": item.get("name", ""),
        "dsa_score": score,
        "action": "WATCH",
        "confidence": 0.0,
        "risk": "high",
        "reason": f"TradingAgents 完整版未完成：{translate_failure_note(note)}",
        "ta_status": "failed",
        "ta_note": note,
        "ta_error_type": error_type,
    }


def trading_rank_score(row: dict[str, Any]) -> float:
    risk_penalty = {"low": 0, "medium": 3, "high": 10, "低": 0, "中": 3, "高": 10}.get(str(row.get("risk", "medium")), 3)
    action_bonus = {"BUY": 6, "HOLD": 2, "WATCH": 0, "SELL": -20}.get(str(row.get("action", "")).upper(), 0)
    return safe_float(row.get("confidence")) * 100 + action_bonus - risk_penalty


TRADINGAGENTS_SNIPPET = r'''
import json, os, socket, urllib.parse
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import DEFAULT_CONFIG

outbound_proxy = (
    os.environ.get("PIPELINE_OUTBOUND_PROXY")
    or os.environ.get("ALL_PROXY")
    or os.environ.get("HTTPS_PROXY")
    or os.environ.get("TELEGRAM_PROXY")
)
if outbound_proxy:
    if outbound_proxy.startswith(("socks5://", "socks5h://")):
        try:
            import socks
            parsed = urllib.parse.urlparse(outbound_proxy)
            socks.set_default_proxy(
                socks.SOCKS5,
                parsed.hostname,
                parsed.port or 1080,
                username=urllib.parse.unquote(parsed.username) if parsed.username else None,
                password=urllib.parse.unquote(parsed.password) if parsed.password else None,
                rdns=outbound_proxy.startswith("socks5h://"),
            )
            socket.socket = socks.socksocket
        except Exception as exc:
            print(f"TradingAgents outbound proxy bootstrap skipped: {type(exc).__name__}: {exc}", flush=True)
for _proxy_key in ("ALL_PROXY", "all_proxy", "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
    os.environ.pop(_proxy_key, None)

config = DEFAULT_CONFIG.copy()
config["output_language"] = os.environ.get("TRADINGAGENTS_OUTPUT_LANGUAGE", "Chinese")
config["llm_provider"] = os.environ.get("TRADINGAGENTS_LLM_PROVIDER", config.get("llm_provider", "openai"))
config["backend_url"] = os.environ.get("TRADINGAGENTS_LLM_BACKEND_URL") or os.environ.get("OPENAI_BASE_URL") or config.get("backend_url")
config["deep_think_llm"] = os.environ.get("TRADINGAGENTS_DEEP_THINK_LLM") or config.get("deep_think_llm", "gpt-4o")
config["quick_think_llm"] = os.environ.get("TRADINGAGENTS_QUICK_THINK_LLM") or config.get("quick_think_llm", "gpt-4o-mini")
config["resolve_memory_outcomes"] = os.environ.get("TRADINGAGENTS_RESOLVE_MEMORY_OUTCOMES", "0").strip().lower() in ("1", "true", "yes", "on")
config["max_recur_limit"] = int(os.environ.get("PIPELINE_TRADINGAGENTS_RECURSION_LIMIT", config.get("max_recur_limit", 100)))
config["data_vendors"] = {
    "core_stock_apis": os.environ.get("TRADINGAGENTS_CORE_DATA_VENDOR", "pipeline,yfinance,alpha_vantage"),
    "technical_indicators": os.environ.get("TRADINGAGENTS_TECH_DATA_VENDOR", "pipeline,yfinance,alpha_vantage"),
    "fundamental_data": os.environ.get("TRADINGAGENTS_FUND_DATA_VENDOR", "pipeline,yfinance,alpha_vantage"),
    "news_data": os.environ.get("TRADINGAGENTS_NEWS_DATA_VENDOR", "pipeline,yfinance,alpha_vantage"),
}
print(f"[TA-DEBUG] Config ready. Backend: {config.get('backend_url')}", flush=True)
print(f"[TA-DEBUG] Initializing TradingAgentsGraph...", flush=True)
import time
t0 = time.time()
graph = TradingAgentsGraph(debug=False, config=config)
print(f"[TA-DEBUG] TradingAgentsGraph initialized in {time.time()-t0:.1f}s", flush=True)
ticker = os.environ["PIPELINE_TA_TICKER"]
date = os.environ["PIPELINE_TA_DATE"]
print(f"[TA-DEBUG] Starting propagate({ticker}, {date})...", flush=True)
t1 = time.time()
final_state, decision = graph.propagate(ticker, date)
print(f"[TA-DEBUG] propagate() completed in {time.time()-t1:.1f}s", flush=True)
payload = {
    "action": decision,
    "decision": decision,
    "final_trade_decision": final_state.get("final_trade_decision"),
    "market_report": final_state.get("market_report"),
    "fundamentals_report": final_state.get("fundamentals_report"),
    "sentiment_report": final_state.get("sentiment_report"),
    "news_report": final_state.get("news_report"),
    "investment_debate_state": final_state.get("investment_debate_state"),
    "risk_debate_state": final_state.get("risk_debate_state"),
}
print("__PIPELINE_JSON__")
print(json.dumps(payload, ensure_ascii=False, default=str))
'''


def parse_last_json(text: str) -> Any:
    marker = "__PIPELINE_JSON__"
    if marker in text:
        tail = text.split(marker, 1)[1].strip()
        try:
            return json.loads(tail.splitlines()[0])
        except Exception:
            pass
    decoder = json.JSONDecoder()
    parsed_values: list[Any] = []
    for match in re.finditer(r"[{\[]", text):
        try:
            value, _ = decoder.raw_decode(text[match.start():])
        except Exception:
            continue
        parsed_values.append(value)
    for value in reversed(parsed_values):
        if isinstance(value, dict) and isinstance(value.get("symbols"), list):
            return value
    for value in reversed(parsed_values):
        if isinstance(value, dict):
            return value
    for value in reversed(parsed_values):
        return value
    matches = re.findall(r"(\{.*?\})", text, flags=re.S)
    for match in reversed(matches):
        try:
            return json.loads(match)
        except Exception:
            continue
    return None


def normalize_trading_result(item: dict[str, Any], parsed: Any, raw: str) -> dict[str, Any]:
    if isinstance(parsed, dict):
        raw_action = str(parsed.get("action") or parsed.get("decision") or parsed.get("recommendation") or "")
        action = normalize_ta_action(raw_action)
        confidence = safe_float(parsed.get("confidence"), item["score"] / 100.0)
        if confidence > 1:
            confidence = confidence / 100.0
        risk = str(parsed.get("risk") or parsed.get("risk_level") or "medium")
        reason = build_tradingagents_reason(parsed, raw)
    else:
        action = normalize_ta_action(str(parsed))
        confidence = item["score"] / 100.0
        risk = "medium"
        reason = complete_excerpt(str(parsed), 500)
    if not action:
        action = "BUY" if confidence >= 0.75 else "HOLD" if confidence >= 0.55 else "WATCH"
    return {
        "symbol": item["symbol"],
        "ta_symbol": to_tradingagents_symbol(item["symbol"]),
        "name": item.get("name", ""),
        "dsa_score": item["score"],
        "action": action,
        "confidence": round(clamp(confidence, 0, 1), 4),
        "risk": risk,
        "reason": complete_excerpt(reason, 1000),
        "ta_status": "full",
        "ta_note": "full_completed",
    }


def normalize_ta_action(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if any(token in text for token in ("strong buy", "buy", "overweight", "outperform", "accumulate")):
        return "BUY"
    if any(token in text for token in ("sell", "underweight", "underperform", "reduce")):
        return "SELL"
    if any(token in text for token in ("hold", "neutral", "market weight", "equal weight")):
        return "HOLD"
    return text.upper()


def build_tradingagents_reason(parsed: dict[str, Any], raw: str) -> str:
    direct = parsed.get("reason") or parsed.get("rationale") or parsed.get("summary")
    final_decision = parsed.get("final_trade_decision")
    blocks = []
    if final_decision:
        blocks.append(f"最终投研结论：{str(final_decision).strip()}")
    for key, title in (
        ("market_report", "技术/市场"),
        ("fundamentals_report", "基本面"),
        ("sentiment_report", "情绪"),
        ("news_report", "新闻"),
    ):
        value = parsed.get(key)
        if value:
            text = " ".join(str(value).split())
            blocks.append(f"{title}：{complete_excerpt(text, 520)}")
    if direct:
        blocks.insert(0, str(direct).strip())
    if blocks:
        return "\n".join(blocks)
    return raw[-1000:]


def fallback_trading(item: dict[str, Any], note: str) -> dict[str, Any]:
    score = safe_float(item.get("score"))
    confidence = clamp(score / 100.0, 0, 1)
    return {
        "symbol": item["symbol"],
        "ta_symbol": to_tradingagents_symbol(item["symbol"]),
        "name": item.get("name", ""),
        "dsa_score": score,
        "action": "BUY" if score >= 80 else "HOLD" if score >= 60 else "WATCH",
        "confidence": round(confidence, 4),
        "risk": "medium",
        "reason": f"TradingAgents {translate_failure_note(note)}；使用 DSA 初筛分降级估算",
        "ta_status": "fallback",
        "ta_note": note,
    }


def translate_failure_note(note: str) -> str:
    text = str(note or "")
    if "insufficient_user_quota" in text or "额度不足" in text:
        return "额度不足"
    if "timeout" in text:
        return "超时"
    if "RateLimit" in text or "rate limit" in text.lower() or "Too Many Requests" in text:
        return "数据源限流"
    if "YFRateLimitError" in text or "Alpha Vantage rate limit" in text:
        return "Yahoo/Alpha Vantage 限流"
    if "skipped" in text:
        return "已跳过"
    if "failed" in text:
        return "失败"
    return text


def summarize_failure(text: str, rc: int) -> str:
    body = str(text or "")
    if "insufficient_user_quota" in body or "额度不足" in body:
        return "额度不足"
    if "PermissionDeniedError" in body or "403" in body:
        return "权限或额度不足"
    if "timeout" in body.lower():
        return "超时"
    if "YFRateLimitError" in body or "Too Many Requests" in body or "Alpha Vantage rate limit" in body:
        return "Yahoo/Alpha Vantage 数据源限流"
    return f"失败 rc={rc}"


def stage_uzi(trading: list[dict[str, Any]], env: dict[str, str], top_n: int) -> list[dict[str, Any]]:
    if env.get("PIPELINE_SKIP_UZI") == "1":
        out = [fallback_uzi(item, "skipped") for item in trading[:top_n]]
        write_json(OUTPUTS / "uzi_top10.json", out)
        return out

    candidate_n = int(env.get("PIPELINE_UZI_CANDIDATE_N", str(max(top_n, top_n * 2))))
    selected = trading[: min(len(trading), max(top_n, candidate_n))]
    uzi_dir = Path(env["UZI_SKILL_DIR"])
    python_bin = env.get("UZI_PYTHON") or env["PYTHON_BIN"]
    timeout = resolve_uzi_timeout(env)
    depth = env.get("UZI_DEPTH", "lite")
    breaker = max(1, int(env.get("PIPELINE_UZI_FAILURE_BREAKER", "2")))
    cache_max_age_hours = safe_float(env.get("PIPELINE_UZI_CACHE_MAX_AGE_HOURS"), 18.0)

    if env.get("PIPELINE_UZI_SEED_CACHE", "1") == "1":
        seed_summary = seed_uzi_cache_from_pipeline(selected, uzi_dir, env)
        write_json(OUTPUTS / "uzi_seed_summary.json", seed_summary)

    out: list[dict[str, Any]] = []
    consecutive_failures = 0
    for idx, item in enumerate(selected):
        symbol = to_uzi_symbol(item["symbol"])
        cached = read_uzi_cache(uzi_dir, symbol)
        if cached and uzi_cache_reusable(cached, cache_max_age_hours, item, symbol, env):
            cached = ensure_uzi_agent_review(item, symbol, cached, uzi_dir, python_bin, env)
            cached_row = normalize_uzi_result(item, symbol, cached, uzi_dir, env)
            if str(cached_row.get("status")) != "degraded" or env.get("PIPELINE_UZI_RERUN_DEGRADED_CACHE", "1") == "0":
                out.append(cached_row)
                consecutive_failures = 0
                continue
        try:
            rc, text = run(
                [python_bin, "run.py", symbol, "--no-browser", "--depth", depth],
                uzi_dir,
                env,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            out.append(fallback_uzi(item, "timeout"))
            consecutive_failures += 1
            if consecutive_failures >= breaker:
                out.extend(fallback_uzi(row, "连续超时，触发熔断") for row in selected[idx + 1:])
                break
            continue
        write_text(WORK / f"uzi_{safe_name(symbol)}.log", text)
        parsed = read_uzi_cache(uzi_dir, symbol)
        if not parsed:
            out.append(fallback_uzi(item, f"failed rc={rc}"))
            consecutive_failures += 1
            if consecutive_failures >= breaker:
                out.extend(fallback_uzi(row, "连续失败，触发熔断") for row in selected[idx + 1:])
                break
            continue
        parsed = ensure_uzi_agent_review(item, symbol, parsed, uzi_dir, python_bin, env)
        out.append(normalize_uzi_result(item, symbol, parsed, uzi_dir, env))
        consecutive_failures = 0

    out = annotate_uzi_batch_quality(out, env)
    out = recheck_degraded_uzi_rows(out, selected, uzi_dir, python_bin, env, timeout, depth)
    out = annotate_uzi_batch_quality(out, env)
    ranked = sorted(out, key=lambda x: uzi_rank_score(x), reverse=True)[:top_n]
    write_json(OUTPUTS / "uzi_top10.json", ranked)
    return ranked


def resolve_uzi_timeout(env: dict[str, str]) -> int:
    timeout = int(env.get("PIPELINE_UZI_TIMEOUT_PER_STOCK", "300"))
    hard_cap_text = str(env.get("PIPELINE_UZI_TIMEOUT_HARD_CAP", "0")).strip()
    hard_cap = int(hard_cap_text) if hard_cap_text else 0
    if hard_cap > 0:
        return min(timeout, hard_cap)
    return timeout


def uzi_cache_reusable(
    parsed: dict[str, Any],
    max_age_hours: float,
    item: dict[str, Any],
    symbol: str,
    env: dict[str, str],
) -> bool:
    if not cache_is_fresh(parsed, max_age_hours):
        return False
    row = normalize_uzi_result(item, symbol, parsed)
    if str(row.get("status")) != "degraded":
        return True
    return env.get("PIPELINE_UZI_RERUN_DEGRADED_CACHE", "1") == "0"


def seed_uzi_cache_from_pipeline(selected: list[dict[str, Any]], uzi_dir: Path, env: dict[str, str]) -> dict[str, Any]:
    """Pre-fill UZI raw_data cache with pipeline market/fundamental data.

    UZI-Skill was built around its own fetchers. For US/HK names those fetchers
    can return sparse data, which pushes many investors into the same low-score
    template. This seed keeps UZI's scoring logic intact, but gives it the
    market, kline, valuation and financial fields we already collected.
    """
    cache_root = uzi_dir / "skills" / "deep-analysis" / "scripts" / ".cache"
    openbb_context = read_json_if_exists(OUTPUTS / "openbb_context.json", {})
    enriched_context = read_json_if_exists(OUTPUTS / "enriched_stock_data.json", {})
    candidates = read_json_if_exists(OUTPUTS / "candidates_top50.json", [])
    candidate_map = {str(row.get("symbol")): row for row in candidates if isinstance(row, dict)}
    openbb_map = build_openbb_symbol_map(openbb_context if isinstance(openbb_context, dict) else {})
    enriched_map = build_enriched_symbol_map(enriched_context if isinstance(enriched_context, dict) else {})
    seeded: list[dict[str, Any]] = []

    for item in selected:
        symbol = str(item.get("symbol") or "")
        uzi_symbol = to_uzi_symbol(symbol)
        dsa_item = candidate_map.get(symbol, {})
        market_row = (
            enriched_map.get(symbol)
            or enriched_map.get(normalize_dsa_symbol(symbol))
            or openbb_map.get(symbol)
            or openbb_map.get(normalize_dsa_symbol(symbol))
            or {}
        )
        fundamentals = dict((market_row.get("fundamentals") or {}) if isinstance(market_row, dict) else {})
        fundamentals = complete_pipeline_fundamentals(symbol, fundamentals, env)
        raw = build_uzi_seed_raw(symbol, uzi_symbol, item, dsa_item, market_row, fundamentals)
        aliases = uzi_cache_aliases(uzi_symbol)
        for alias in aliases:
            path = cache_root / alias / "raw_data.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(raw, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        seeded.append({
            "symbol": symbol,
            "uzi_symbol": uzi_symbol,
            "aliases": aliases,
            "dimensions": sorted((raw.get("dimensions") or {}).keys()),
            "fundamental_source": fundamentals.get("source", "none"),
            "has_financials": bool(((raw.get("dimensions") or {}).get("1_financials") or {}).get("data", {}).get("revenue_history")),
            "has_kline": bool(((raw.get("dimensions") or {}).get("2_kline") or {}).get("data", {}).get("candles_60d")),
        })
    return {"status": "ok", "seeded": seeded, "count": len(seeded)}


def build_uzi_seed_raw_for_item(item: dict[str, Any], uzi_dir: Path, env: dict[str, str]) -> dict[str, Any]:
    openbb_context = read_json_if_exists(OUTPUTS / "openbb_context.json", {})
    enriched_context = read_json_if_exists(OUTPUTS / "enriched_stock_data.json", {})
    candidates = read_json_if_exists(OUTPUTS / "candidates_top50.json", [])
    symbol = str(item.get("symbol") or "")
    uzi_symbol = to_uzi_symbol(symbol)
    candidate_map = {str(row.get("symbol")): row for row in candidates if isinstance(row, dict)}
    openbb_map = build_openbb_symbol_map(openbb_context if isinstance(openbb_context, dict) else {})
    enriched_map = build_enriched_symbol_map(enriched_context if isinstance(enriched_context, dict) else {})
    dsa_item = candidate_map.get(symbol, {})
    market_row = (
        enriched_map.get(symbol)
        or enriched_map.get(normalize_dsa_symbol(symbol))
        or openbb_map.get(symbol)
        or openbb_map.get(normalize_dsa_symbol(symbol))
        or {}
    )
    fundamentals = dict((market_row.get("fundamentals") or {}) if isinstance(market_row, dict) else {})
    fundamentals = complete_pipeline_fundamentals(symbol, fundamentals, env)
    return build_uzi_seed_raw(symbol, uzi_symbol, item, dsa_item, market_row, fundamentals)


def restore_pipeline_seed_dimensions(parsed: dict[str, Any], item: dict[str, Any], uzi_dir: Path, env: dict[str, str]) -> dict[str, Any]:
    """Merge formal pipeline seed fields back after UZI fetchers overwrite HK basic data."""
    raw = parsed.get("raw") or {}
    dims = raw.get("dimensions") or {}
    seed_raw = build_uzi_seed_raw_for_item(item, uzi_dir, env)
    seed_dims = seed_raw.get("dimensions") or {}
    changed = False

    for dim_name in ("0_basic", "1_financials", "2_kline", "6_research", "10_valuation"):
        seed_dim = seed_dims.get(dim_name)
        if not isinstance(seed_dim, dict):
            continue
        current_dim = dims.get(dim_name)
        if not isinstance(current_dim, dict):
            dims[dim_name] = seed_dim
            changed = True
            continue
        current_data = current_dim.get("data") if isinstance(current_dim.get("data"), dict) else {}
        seed_data = seed_dim.get("data") if isinstance(seed_dim.get("data"), dict) else {}
        if dim_name == "0_basic":
            for key in ("name", "industry", "price", "market_cap", "market_cap_raw", "pe_ttm", "pb"):
                if not current_data.get(key) and seed_data.get(key):
                    current_data[key] = seed_data[key]
                    changed = True
        elif dim_name == "10_valuation":
            for key, value in seed_data.items():
                if not current_data.get(key) and value:
                    current_data[key] = value
                    changed = True
        current_dim["data"] = current_data
        if current_dim.get("source") != "pipeline_seed" and seed_dim.get("source"):
            current_dim["pipeline_seed_source"] = seed_dim.get("source")
        dims[dim_name] = current_dim

    if changed:
        raw["dimensions"] = dims
        raw["pipeline_seed_restored"] = True
        parsed = dict(parsed)
        parsed["raw"] = raw
    return parsed


def read_json_if_exists(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default


def uzi_cache_aliases(uzi_symbol: str) -> list[str]:
    aliases = [uzi_symbol]
    if uzi_symbol.lower().startswith("hk"):
        digits = uzi_symbol[2:].zfill(5)
        aliases.append(f"{digits}.HK")
    if uzi_symbol.endswith(".SH") or uzi_symbol.endswith(".SZ"):
        aliases.append(uzi_symbol.split(".", 1)[0])
    out: list[str] = []
    for alias in aliases:
        if alias and alias not in out:
            out.append(alias)
    return out


def build_uzi_seed_raw(
    symbol: str,
    uzi_symbol: str,
    item: dict[str, Any],
    dsa_item: dict[str, Any],
    market_row: dict[str, Any],
    fundamentals: dict[str, Any],
) -> dict[str, Any]:
    now = dt.datetime.now().isoformat(timespec="seconds")
    dims: dict[str, Any] = {}
    name = str(item.get("name") or dsa_item.get("name") or fundamentals.get("name") or symbol)
    close = first_positive(market_row.get("close"), dsa_item.get("close"), fundamentals.get("price"))
    ret_20 = safe_float(dsa_item.get("ret_20d"), safe_float(market_row.get("ret_20d")))
    volume_ratio = valid_volume_ratio(dsa_item.get("volume_ratio"))
    if volume_ratio is None:
        volume_ratio = valid_volume_ratio(market_row.get("volume_ratio"))
    pe = first_positive(fundamentals.get("pe"), dsa_item.get("pe"), market_row.get("pe"))
    pb = first_positive(fundamentals.get("pb"), dsa_item.get("pb"), market_row.get("pb"))
    market_cap = first_positive(fundamentals.get("market_cap_yi"), market_row.get("market_cap_yi"))
    market_cap_raw = first_positive(fundamentals.get("market_cap_raw"), market_row.get("market_cap_raw"))
    if not market_cap and market_cap_raw:
        market_cap = market_cap_raw / 1e8
    eps = first_positive(fundamentals.get("eps"))
    if not pe and close and eps:
        pe = round(close / eps, 4)
        fundamentals = dict(fundamentals)
        fundamentals["pe"] = pe
        fundamentals["pe_derived_from_price_eps"] = True
        field_sources = dict(fundamentals.get("_field_sources") or {})
        field_sources["pe"] = "pipeline_derived_price_eps"
        fundamentals["_field_sources"] = field_sources
    field_sources = fundamentals.get("_field_sources") or {}

    dims["0_basic"] = {
        "ticker": uzi_symbol,
        "market": "H" if normalize_dsa_symbol(symbol).lower().startswith("hk") else "U",
        "data": {
            "code": uzi_symbol,
            "ticker": uzi_symbol,
            "name": name,
            "industry": fundamentals.get("industry") or dsa_item.get("group") or "综合",
            "price": close,
            "change_pct": safe_float(dsa_item.get("pct_1d")),
            "market_cap": market_cap,
            "market_cap_raw": market_cap_raw,
            "pe_ttm": pe,
            "pb": pb,
            "eps": eps or 0,
            "dividend_yield_ttm": fundamentals.get("dividend_yield") or 0,
            "security_type": "stock",
            "field_sources": field_sources,
        },
        "source": "pipeline_seed",
        "fallback": False,
    }

    financials = build_uzi_financial_dim(fundamentals)
    if financials:
        dims["1_financials"] = {"ticker": uzi_symbol, "data": financials, "source": fundamentals.get("source", "pipeline_seed"), "fallback": False}

    kline = build_uzi_kline_dim(market_row, dsa_item, close, ret_20, volume_ratio)
    if kline:
        dims["2_kline"] = {"ticker": uzi_symbol, "data": kline, "source": market_row.get("provider") or "pipeline_seed", "fallback": False}

    research = build_uzi_research_dim(fundamentals, close)
    if research:
        dims["6_research"] = {"ticker": uzi_symbol, "data": research, "source": fundamentals.get("source", "pipeline_seed"), "fallback": False}

    valuation = build_uzi_valuation_dim(pe, pb, market_cap, fundamentals)
    if valuation:
        dims["10_valuation"] = {"ticker": uzi_symbol, "data": valuation, "source": fundamentals.get("source", "pipeline_seed"), "fallback": False}

    return {
        "ticker": uzi_symbol,
        "market": "H" if normalize_dsa_symbol(symbol).lower().startswith("hk") else "U",
        "fetched_at": now,
        "pipeline_seeded": True,
        "field_sources": field_sources,
        "dimensions": dims,
    }


def first_positive(*values: Any) -> float:
    for value in values:
        num = safe_float(value)
        if num > 0:
            return num
    return 0.0


def build_uzi_financial_dim(fundamentals: dict[str, Any]) -> dict[str, Any]:
    revenue = [safe_float(x) for x in fundamentals.get("revenue_history", []) if safe_float(x) > 0]
    profit = [safe_float(x) for x in fundamentals.get("net_profit_history", []) if safe_float(x) != 0]
    roe = [safe_float(x) for x in fundamentals.get("roe_history", []) if safe_float(x) != 0]
    if not any((revenue, profit, roe)):
        return {}
    health = fundamentals.get("financial_health") or {}
    out = {
        "roe_history": roe,
        "revenue_history": revenue,
        "net_profit_history": profit,
        "financial_years": fundamentals.get("financial_years", []),
        "financial_health": health,
        "roe": f"{roe[-1]:.1f}%" if roe else "",
        "revenue_growth": "",
        "net_margin": "",
    }
    if len(revenue) >= 2 and revenue[-2]:
        out["revenue_growth"] = f"{(revenue[-1] / revenue[-2] - 1) * 100:+.1f}%"
    if revenue and profit and revenue[-1]:
        out["net_margin"] = f"{profit[-1] / revenue[-1] * 100:.1f}%"
    return out


def build_uzi_kline_dim(
    market_row: dict[str, Any],
    dsa_item: dict[str, Any],
    close: float,
    ret_20: float,
    volume_ratio: float,
) -> dict[str, Any]:
    rows = market_row.get("klines") or market_row.get("candles") or []
    candles = []
    for row in rows[-60:] if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        row_close = first_positive(row.get("close"), row.get("Close"), row.get("收盘"))
        if row_close <= 0:
            continue
        candles.append({
            "date": row.get("date") or row.get("日期") or "",
            "open": first_positive(row.get("open"), row.get("Open"), row.get("开盘"), row_close),
            "high": first_positive(row.get("high"), row.get("High"), row.get("最高"), row_close),
            "low": first_positive(row.get("low"), row.get("Low"), row.get("最低"), row_close),
            "close": row_close,
            "volume": safe_float(row.get("volume") or row.get("Volume") or row.get("成交量")),
        })
    closes = [safe_float(row.get("close")) for row in candles if safe_float(row.get("close")) > 0]
    if not closes and close > 0:
        start = close / (1 + ret_20 / 100) if ret_20 > -95 else close
        closes = [start + (close - start) * i / 21 for i in range(22)]
        candles = [{"date": "", "open": c, "high": c, "low": c, "close": c, "volume": 0} for c in closes]
    if not closes:
        return {}
    ma20 = safe_float(dsa_item.get("ma20")) or (sum(closes[-20:]) / min(len(closes), 20))
    rsi = estimate_rsi(closes)
    stage = "Stage 2 上升" if close > ma20 and ret_20 >= 0 else "Stage 1 底部" if close > ma20 else "Stage 4 下跌"
    return {
        "stage": stage,
        "ma_align": "多头排列" if close > ma20 and ret_20 >= 0 else "均线待确认",
        "macd": "金叉水上" if ret_20 > 0 else "弱势震荡",
        "rsi": round(rsi, 1),
        "candles_60d": candles[-60:],
        "kline_stats": {"ytd_return": ret_20, "volatility": 0, "max_drawdown": 0},
        "indicators": {"obv_trend_up": ret_20 > 0, "williams_r": -35 if ret_20 > 0 else -65},
        "volume_ratio": volume_ratio,
    }


def estimate_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < 2:
        return 50.0
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = diffs[-period:]
    gains = sum(max(x, 0) for x in recent) / max(1, len(recent))
    losses = sum(max(-x, 0) for x in recent) / max(1, len(recent))
    if losses <= 0:
        return 70.0 if gains > 0 else 50.0
    rs = gains / losses
    return clamp(100 - 100 / (1 + rs), 0, 100)


def build_uzi_research_dim(fundamentals: dict[str, Any], close: float) -> dict[str, Any]:
    target = safe_float(fundamentals.get("target_price"))
    coverage = int(safe_float(fundamentals.get("coverage_count")))
    if not target and not coverage:
        return {}
    upside = (target / close - 1) * 100 if close and target else 0
    return {
        "report_count": coverage,
        "coverage_count": coverage,
        "buy_rating_pct": safe_float(fundamentals.get("buy_rating_pct")),
        "target_price_avg": target,
        "upside": round(upside, 1),
        "consensus_eps_2026": safe_float(fundamentals.get("eps_next_year")),
        "consensus_pe_2026": safe_float(fundamentals.get("forward_pe")),
    }


def build_uzi_valuation_dim(pe: float, pb: float, market_cap: float, fundamentals: dict[str, Any] | None = None) -> dict[str, Any]:
    fundamentals = fundamentals or {}
    revenue = [safe_float(x) for x in fundamentals.get("revenue_history", []) if safe_float(x) > 0]
    profit = [safe_float(x) for x in fundamentals.get("net_profit_history", []) if safe_float(x) != 0]
    target = safe_float(fundamentals.get("target_price"))
    forward_pe = safe_float(fundamentals.get("forward_pe"))
    ev_to_sales = safe_float(fundamentals.get("ev_to_sales") or fundamentals.get("enterprise_value_to_revenue"))
    price_to_sales = safe_float(fundamentals.get("price_to_sales") or fundamentals.get("ps"))
    if not any((pe, pb, market_cap, revenue, profit, target, forward_pe, ev_to_sales, price_to_sales)):
        return {}
    pe_quantile = 80 if pe >= 50 else 65 if pe >= 30 else 50 if pe >= 15 else 35
    out = {
        "pe": pe,
        "pb": pb,
        "pe_quantile": (
            "由 price/EPS 推导，需后续以正式估值源校验"
            if fundamentals.get("pe_derived_from_price_eps")
            else f"5年 {pe_quantile} 分位" if pe else "PE 不适用"
        ),
        "industry_pe": 0,
        "dcf": f"{market_cap:.2f} 亿" if market_cap else "",
        "market_cap_yi": market_cap,
        "target_price_avg": target,
        "forward_pe": forward_pe,
        "price_to_sales": price_to_sales,
        "ev_to_sales": ev_to_sales,
    }
    if revenue and market_cap and not price_to_sales:
        out["price_to_sales"] = round(market_cap / revenue[-1], 2) if revenue[-1] else 0
    if revenue and profit and revenue[-1]:
        out["net_margin"] = round(profit[-1] / revenue[-1] * 100, 2)
    if fundamentals.get("pe_derived_from_price_eps"):
        out["valuation_note"] = "PE 由 pipeline 已有现价与 EPS 推导；市值/PB 若数据源未返回总股本或每股净资产，暂不硬填。"
    if revenue and not pe and any(x < 0 for x in profit[-3:]):
        out["valuation_note"] = "公司仍处亏损或利润不稳定，PE 不适用；UZI 应改用收入增长、亏损收窄、现金流和 P/S 评估。"
    return out


def import_akshare_module() -> Any:
    try:
        import akshare as ak  # type: ignore
    except Exception:
        version = f"python{sys.version_info.major}.{sys.version_info.minor}"
        site_packages = ROOT / ".akshare-venv" / "lib" / version / "site-packages"
        if site_packages.exists() and str(site_packages) not in sys.path:
            sys.path.insert(0, str(site_packages))
        try:
            import akshare as ak  # type: ignore
        except Exception:
            return None
    return ak


def is_hk_symbol(symbol: str) -> bool:
    return normalize_dsa_symbol(symbol).lower().startswith("hk")


def hk_symbol_digits(symbol: str) -> str:
    s = normalize_dsa_symbol(symbol)
    return s[2:].zfill(5) if s.lower().startswith("hk") else s.zfill(5)


def parse_chinese_number(value: Any) -> float:
    if isinstance(value, (int, float)):
        return safe_float(value)
    text = str(value or "").strip().replace(",", "")
    if not text or text in {"-", "--", "None", "nan"}:
        return 0.0
    multiplier = 1.0
    if "万亿" in text:
        multiplier = 1e12
    elif "亿" in text:
        multiplier = 1e8
    elif "万" in text:
        multiplier = 1e4
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    return safe_float(match.group(0)) * multiplier if match else 0.0


def latest_akshare_number(rows: Any, field_names: tuple[str, ...] = ()) -> float:
    records: list[dict[str, Any]] = []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        if getattr(rows, "empty", True):
            return 0.0
        records = rows.to_dict("records")
    elif isinstance(rows, list):
        records = [row for row in rows if isinstance(row, dict)]
    elif isinstance(rows, dict):
        records = [rows]
    if not records:
        return 0.0
    for row in reversed(records):
        keys = field_names or tuple(str(k) for k in row.keys())
        for key in keys:
            if key in row:
                value = parse_chinese_number(row.get(key))
                if value:
                    return value
        if not field_names:
            for value in row.values():
                parsed = parse_chinese_number(value)
                if parsed:
                    return parsed
    return 0.0


def normalize_market_cap_raw(value: float) -> float:
    value = safe_float(value)
    if value <= 0:
        return 0.0
    return value * 1e8 if value < 1e7 else value


def akshare_call_indicator(fn: Any, symbol: str, indicator: str) -> Any:
    try:
        params = inspect.signature(fn).parameters
    except Exception:
        params = {}
    if "indicator" in params:
        kwargs = {"symbol": symbol, "indicator": indicator}
        if "period" in params:
            kwargs["period"] = "近一年"
        return fn(**kwargs)
    return fn(symbol=symbol)


def fetch_akshare_hk_valuation(symbol: str) -> dict[str, Any]:
    if not is_hk_symbol(symbol):
        return {}
    ak = import_akshare_module()
    if ak is None:
        return {}
    code = hk_symbol_digits(symbol)
    source = "AKShareHKValuation"
    out: dict[str, Any] = {"source": source}

    eniu = getattr(ak, "stock_hk_eniu_indicator", None)
    if eniu:
        for indicator, target_key, fields in (
            ("市盈率", "pe", ("市盈率", "PE", "pe", "value")),
            ("市净率", "pb", ("市净率", "PB", "pb", "value")),
            ("市值", "market_cap_raw", ("总市值", "市值", "market_cap", "value")),
        ):
            try:
                rows = akshare_call_indicator(eniu, code, indicator)
            except Exception:
                rows = None
            value = latest_akshare_number(rows, fields)
            if value:
                out[target_key] = normalize_market_cap_raw(value) if target_key == "market_cap_raw" else value

    baidu = getattr(ak, "stock_hk_valuation_baidu", None)
    if baidu:
        for indicator, target_key, fields in (
            ("总市值", "market_cap_raw", ("总市值", "市值", "value")),
            ("市盈率", "pe", ("市盈率", "PE", "value")),
            ("市净率", "pb", ("市净率", "PB", "value")),
        ):
            if first_positive(out.get(target_key)):
                continue
            try:
                rows = akshare_call_indicator(baidu, code, indicator)
            except Exception:
                rows = None
            value = latest_akshare_number(rows, fields)
            if value:
                out[target_key] = normalize_market_cap_raw(value) if target_key == "market_cap_raw" else value

    market_cap_raw = first_positive(out.get("market_cap_raw"))
    if market_cap_raw:
        out["market_cap_raw"] = market_cap_raw
        out["market_cap_yi"] = round(market_cap_raw / 1e8, 4)
    out["_field_sources"] = {
        key: source
        for key in ("market_cap_raw", "market_cap_yi", "pe", "pb")
        if first_positive(out.get(key))
    }
    return out if fundamental_payload_has_signal(out) else {}


def fetch_pipeline_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    if env.get("PIPELINE_UZI_FUNDAMENTAL_FETCH", "1") != "1":
        return {}
    merged: dict[str, Any] = {}
    sources: list[str] = []
    for cache_key, fetcher in (
        ("fmp", fetch_fmp_fundamentals),
        ("alpha_overview", fetch_alpha_overview),
        ("yahoo", fetch_yahoo_fundamentals),
        ("eastmoney", fetch_eastmoney_fundamentals),
        ("sec", fetch_sec_fundamentals),
    ):
        data = cached_fundamental_fetch(cache_key, symbol, lambda fetcher=fetcher: fetcher(symbol, env))
        if not data:
            continue
        merge_fundamental_payload(merged, data)
        source_name = str(data.get("source") or fetcher.__name__).strip()
        if source_name:
            sources.append(source_name)
    if fundamentals_need_valuation(merged):
        valuation = fetch_marketdata_valuation_fallback(symbol, env, merged)
        if valuation:
            merge_fundamental_payload(merged, valuation)
            source_name = str(valuation.get("source") or "MarketDataValuation").strip()
            if source_name:
                sources.append(source_name)
    if is_hk_symbol(symbol) and fundamentals_need_completion(merged):
        valuation = cached_fundamental_fetch("akshare_hk_valuation", symbol, lambda: fetch_akshare_hk_valuation(symbol))
        if valuation:
            merge_fundamental_payload(merged, valuation)
            source_name = str(valuation.get("source") or "AKShareHKValuation").strip()
            if source_name:
                sources.append(source_name)
    if env.get("PIPELINE_UZI_QUOTE_FALLBACK", "1") == "1":
        quote = fetch_quote_fallback_fundamentals(symbol)
        if quote:
            merge_fundamental_payload(merged, quote)
            source_name = str(quote.get("source") or "QuoteFallback").strip()
            if source_name:
                sources.append(source_name)
    if not merged:
        return {}
    if sources:
        merged["source"] = "+".join(dict.fromkeys(sources))
    return merged


def complete_pipeline_fundamentals(symbol: str, existing: dict[str, Any] | None, env: dict[str, str]) -> dict[str, Any]:
    current = dict(existing or {})
    if not fundamentals_need_completion(current):
        return current
    fresh = fetch_pipeline_fundamentals(symbol, env)
    if fresh:
        merge_fundamental_payload(current, fresh)
        current_sources = [str(x).strip() for x in str((existing or {}).get("source") or "").split("+") if str(x).strip()]
        fresh_sources = [str(x).strip() for x in str(fresh.get("source") or "").split("+") if str(x).strip()]
        sources = list(dict.fromkeys([*current_sources, *fresh_sources]))
        if sources:
            current["source"] = "+".join(sources)
    return current


def anchor_fundamentals_to_market_price(fundamentals: dict[str, Any], market_price: Any, source: Any = "market") -> dict[str, Any]:
    price = first_positive(market_price)
    if not price:
        return fundamentals
    current = dict(fundamentals or {})
    old_price = first_positive(current.get("price"))
    if old_price and (price / old_price >= 8 or old_price / price >= 8):
        for key in ("market_cap_raw", "market_cap_yi", "pe", "price_to_sales"):
            current.pop(key, None)
    current["price"] = price
    field_sources = dict(current.get("_field_sources") or {})
    field_sources["price"] = str(source or "market")
    current["_field_sources"] = field_sources
    shares = first_positive(current.get("shares_outstanding"))
    eps = first_positive(current.get("eps"))
    if shares and not first_positive(current.get("market_cap_raw")):
        market_cap_raw = price * shares
        current["market_cap_raw"] = market_cap_raw
        current["market_cap_yi"] = market_cap_raw / 1e8
        field_sources["market_cap_raw"] = "pipeline_derived_price_shares"
        field_sources["market_cap_yi"] = "pipeline_derived_price_shares"
    if eps and not first_positive(current.get("pe")):
        current["pe"] = price / eps
        current["pe_derived_from_price_eps"] = True
        field_sources["pe"] = "pipeline_derived_price_eps"
    return current


def fundamentals_need_completion(data: dict[str, Any]) -> bool:
    if not data:
        return True
    if fundamentals_need_valuation(data):
        return True
    if not first_positive(data.get("pb")):
        return True
    if not first_positive(data.get("market_cap_raw"), data.get("market_cap_yi")):
        return True
    if not (data.get("revenue_history") or data.get("net_profit_history") or data.get("roe_history")):
        return True
    return False


def cached_fundamental_fetch(cache_key: str, symbol: str, fetcher: Any) -> dict[str, Any]:
    key = (cache_key, normalize_dsa_symbol(symbol).upper())
    if key in _FUNDAMENTAL_FETCH_CACHE:
        return dict(_FUNDAMENTAL_FETCH_CACHE[key])
    data = fetcher()
    _FUNDAMENTAL_FETCH_CACHE[key] = dict(data or {})
    return data or {}


def fundamentals_need_valuation(data: dict[str, Any]) -> bool:
    return not first_positive(
        data.get("pe"),
        data.get("pb"),
        data.get("market_cap_raw"),
        data.get("market_cap_yi"),
        data.get("price_to_sales"),
        data.get("forward_pe"),
        data.get("target_price"),
    )


def fetch_fmp_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    api_key = env.get("FMP_API_KEY") or env.get("FINANCIAL_MODELING_PREP_API_KEY") or ""
    if not api_key:
        return {}
    ticker = fmp_symbol(symbol)
    profile = safe_http_json(
        f"https://financialmodelingprep.com/api/v3/profile/{urllib.parse.quote(ticker)}",
        {"apikey": api_key},
    )
    income = safe_http_json(
        f"https://financialmodelingprep.com/api/v3/income-statement/{urllib.parse.quote(ticker)}",
        {"period": "annual", "limit": "6", "apikey": api_key},
    )
    ratios = safe_http_json(
        f"https://financialmodelingprep.com/api/v3/ratios/{urllib.parse.quote(ticker)}",
        {"period": "annual", "limit": "6", "apikey": api_key},
    )
    estimates = safe_http_json(
        f"https://financialmodelingprep.com/api/v3/analyst-estimates/{urllib.parse.quote(ticker)}",
        {"limit": "3", "apikey": api_key},
    )
    p0 = profile[0] if isinstance(profile, list) and profile else {}
    income_rows = sort_financial_rows(income)
    ratio_rows = sort_financial_rows(ratios)
    est0 = estimates[0] if isinstance(estimates, list) and estimates else {}
    revenue = [safe_float(row.get("revenue")) / 1e8 for row in income_rows if safe_float(row.get("revenue")) > 0]
    profit = [safe_float(row.get("netIncome")) / 1e8 for row in income_rows if safe_float(row.get("netIncome")) != 0]
    roe = [safe_float(row.get("returnOnEquity")) * 100 for row in ratio_rows if safe_float(row.get("returnOnEquity")) != 0]
    health = {}
    if ratio_rows:
        last_ratio = ratio_rows[-1]
        health = {
            "current_ratio": safe_float(last_ratio.get("currentRatio")),
            "debt_ratio": safe_float(last_ratio.get("debtRatio")) * 100,
            "fcf_margin": safe_float(last_ratio.get("freeCashFlowOperatingCashFlowRatio")) * 100,
            "roic": safe_float(last_ratio.get("returnOnCapitalEmployed")) * 100,
        }
    out = {
        "source": "FMP",
        "name": p0.get("companyName") or p0.get("symbol") or ticker,
        "industry": p0.get("industry") or p0.get("sector") or "",
        "price": safe_float(p0.get("price")),
        "market_cap_raw": safe_float(p0.get("mktCap")),
        "market_cap_yi": safe_float(p0.get("mktCap")) / 1e8,
        "pe": safe_float(p0.get("pe")),
        "pb": safe_float(p0.get("priceToBookRatio")),
        "eps": safe_float(p0.get("eps")),
        "dividend_yield": safe_float(p0.get("lastDiv")),
        "revenue_history": revenue,
        "net_profit_history": profit,
        "roe_history": roe,
        "financial_years": [str(row.get("calendarYear") or row.get("date") or "")[:4] for row in income_rows],
        "financial_health": health,
        "target_price": safe_float(est0.get("estimatedPriceAvg") or est0.get("estimatedPriceHigh")),
        "eps_next_year": safe_float(est0.get("estimatedEpsAvg")),
        "coverage_count": safe_float(est0.get("numberAnalystEstimatedRevenue") or est0.get("numberAnalystsEstimatedRevenue")),
    }
    return out if fundamental_payload_has_signal(out) else {}


def fetch_alpha_overview(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    api_key = env.get("ALPHA_VANTAGE_API_KEY") or env.get("ALPHAVANTAGE_API_KEY") or env.get("ALPHA_API_KEY") or ""
    if not api_key or normalize_dsa_symbol(symbol).lower().startswith("hk"):
        return {}
    ticker = normalize_dsa_symbol(symbol).upper().replace(".US", "")
    try:
        data = http_json("https://www.alphavantage.co/query", {"function": "OVERVIEW", "symbol": ticker, "apikey": api_key})
    except Exception:
        return {}
    if not isinstance(data, dict) or not data.get("Symbol"):
        return {}
    roe = safe_float(data.get("ReturnOnEquityTTM")) * 100
    revenue_ttm = safe_float(data.get("RevenueTTM")) / 1e8
    profit_margin = safe_float(data.get("ProfitMargin")) * 100
    profit = revenue_ttm * profit_margin / 100 if revenue_ttm and profit_margin else 0
    out = {
        "source": "Alpha Vantage",
        "name": data.get("Name") or ticker,
        "industry": data.get("Industry") or data.get("Sector") or "",
        "market_cap_raw": safe_float(data.get("MarketCapitalization")),
        "market_cap_yi": safe_float(data.get("MarketCapitalization")) / 1e8,
        "pe": safe_float(data.get("PERatio")),
        "pb": safe_float(data.get("PriceToBookRatio")),
        "eps": safe_float(data.get("EPS")),
        "dividend_yield": safe_float(data.get("DividendYield")) * 100,
        "revenue_history": [revenue_ttm] if revenue_ttm else [],
        "net_profit_history": [profit] if profit else [],
        "roe_history": [roe] if roe else [],
        "financial_health": {"net_margin": profit_margin},
        "target_price": safe_float(data.get("AnalystTargetPrice")),
        "forward_pe": safe_float(data.get("ForwardPE")),
    }
    return out if fundamental_payload_has_signal(out) else {}


def fetch_yahoo_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    ticker = yahoo_symbol_for_fundamentals(symbol)
    modules = ",".join([
        "price",
        "summaryProfile",
        "summaryDetail",
        "defaultKeyStatistics",
        "financialData",
        "recommendationTrend",
        "earningsTrend",
        "incomeStatementHistory",
        "incomeStatementHistoryQuarterly",
    ])
    try:
        data = http_json(
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}",
            {"modules": modules},
        )
    except Exception:
        return {}
    result = (((data.get("quoteSummary") or {}).get("result") or [None])[0]) if isinstance(data, dict) else None
    if not isinstance(result, dict):
        return {}
    price = result.get("price") or {}
    profile = result.get("summaryProfile") or {}
    detail = result.get("summaryDetail") or {}
    stats = result.get("defaultKeyStatistics") or {}
    financial = result.get("financialData") or {}
    recommendation = result.get("recommendationTrend") or {}
    earnings_trend = result.get("earningsTrend") or {}
    annual = ((result.get("incomeStatementHistory") or {}).get("incomeStatementHistory") or [])
    quarterly = ((result.get("incomeStatementHistoryQuarterly") or {}).get("incomeStatementHistory") or [])
    income_rows = annual if isinstance(annual, list) and annual else quarterly if isinstance(quarterly, list) else []
    income_rows = [row for row in income_rows if isinstance(row, dict)]
    income_rows = list(reversed(income_rows))
    revenue = [unwrap_finance_number((row.get("totalRevenue") or {})) / 1e8 for row in income_rows if unwrap_finance_number((row.get("totalRevenue") or {})) > 0]
    profit = [unwrap_finance_number((row.get("netIncome") or {})) / 1e8 for row in income_rows if unwrap_finance_number((row.get("netIncome") or {})) != 0]
    years = [extract_finance_year(row.get("endDate")) for row in income_rows]
    roe = first_nonempty_series(
        normalize_number_series(financial.get("returnOnEquity"), scale=100.0),
        normalize_number_series(stats.get("returnOnEquity"), scale=100.0),
    )
    target_price = first_positive(
        unwrap_finance_number(financial.get("targetMeanPrice")),
        unwrap_finance_number(financial.get("targetHighPrice")),
    )
    coverage_count = first_positive(
        unwrap_finance_number(financial.get("numberOfAnalystOpinions")),
        recommendation_count(recommendation),
    )
    out = {
        "source": "Yahoo",
        "name": price.get("longName") or price.get("shortName") or ticker,
        "industry": profile.get("industry") or profile.get("sector") or "",
        "price": unwrap_finance_number(price.get("regularMarketPrice")),
        "market_cap_raw": unwrap_finance_number(price.get("marketCap")),
        "market_cap_yi": unwrap_finance_number(price.get("marketCap")) / 1e8,
        "pe": first_positive(
            unwrap_finance_number(stats.get("trailingPE")),
            unwrap_finance_number(detail.get("trailingPE")),
        ),
        "pb": first_positive(
            unwrap_finance_number(stats.get("priceToBook")),
            unwrap_finance_number(detail.get("priceToBook")),
        ),
        "eps": first_positive(
            unwrap_finance_number(stats.get("trailingEps")),
            unwrap_finance_number(detail.get("trailingEps")),
        ),
        "dividend_yield": unwrap_finance_number(detail.get("dividendYield")) * 100,
        "revenue_history": revenue,
        "net_profit_history": profit,
        "roe_history": roe,
        "financial_years": years,
        "financial_health": {
            "current_ratio": unwrap_finance_number(financial.get("currentRatio")),
            "debt_ratio": unwrap_finance_number(financial.get("debtToEquity")),
            "net_margin": unwrap_finance_number(financial.get("profitMargins")) * 100,
            "fcf_margin": unwrap_finance_number(financial.get("freeCashflow")) / max(unwrap_finance_number(financial.get("totalRevenue")), 1) * 100 if unwrap_finance_number(financial.get("totalRevenue")) else 0,
        },
        "target_price": target_price,
        "eps_next_year": first_positive(
            extract_growth_value(earnings_trend, "epsTrend", "avg"),
            extract_growth_value(earnings_trend, "earningsEstimate", "avg"),
        ),
        "forward_pe": unwrap_finance_number(detail.get("forwardPE")),
        "coverage_count": coverage_count,
        "buy_rating_pct": recommendation_buy_ratio(recommendation),
    }
    return out if fundamental_payload_has_signal(out) else {}


def fetch_eastmoney_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    secucode = eastmoney_secucode(symbol)
    if not secucode:
        return {}
    indicators = safe_http_json(
        "https://datacenter-web.eastmoney.com/api/data/v1/get",
        {
            "reportName": eastmoney_indicator_report_name(secucode),
            "columns": "ALL",
            "filter": f'(SECUCODE="{secucode}")',
            "pageNumber": "1",
            "pageSize": "6",
            "sortColumns": "REPORT_DATE",
            "sortTypes": "-1",
            "source": "WEB",
            "client": "WEB",
        },
        timeout=15,
    )
    rows = (((indicators.get("result") or {}).get("data") or []) if isinstance(indicators, dict) else [])
    if not isinstance(rows, list) or not rows:
        return {}
    rows = [row for row in rows if isinstance(row, dict)]
    rows = list(reversed(rows))
    last = rows[-1]
    revenue = extract_eastmoney_series(rows, ("OPERATE_INCOME", "TOTAL_OPERATE_INCOME"))
    profit = extract_eastmoney_series(rows, ("PARENT_HOLDER_NETPROFIT", "NETPROFIT", "HOLDER_PROFIT"))
    roe = extract_eastmoney_series(rows, ("ROE_AVG", "ROE", "WEIGHTAVG_ROE"), scale=1.0)
    out = {
        "source": "Eastmoney",
        "name": last.get("SECURITY_NAME_ABBR") or "",
        "market_cap_raw": first_positive(last.get("TOTAL_MARKET_CAP"), last.get("MARKET_CAP")),
        "market_cap_yi": first_positive(last.get("TOTAL_MARKET_CAP"), last.get("MARKET_CAP")) / 1e8 if first_positive(last.get("TOTAL_MARKET_CAP"), last.get("MARKET_CAP")) else 0.0,
        "pe": first_positive(last.get("PE_TTM"), last.get("PE9")),
        "pb": first_positive(last.get("PB"), last.get("PB_MRQ")),
        "eps": first_positive(last.get("BASIC_EPS"), last.get("EPSJB")),
        "dividend_yield": first_positive(last.get("DIVI_RATIO")),
        "revenue_history": revenue,
        "net_profit_history": profit,
        "roe_history": roe,
        "financial_years": [extract_finance_year(row.get("REPORT_DATE") or row.get("REPORT")) for row in rows],
        "financial_health": {
            "current_ratio": first_positive(last.get("CURRENT_RATIO")),
            "debt_ratio": first_positive(last.get("DEBT_ASSET_RATIO")),
            "gross_margin": first_positive(last.get("GROSS_PROFIT_RATIO")),
            "net_margin": first_positive(last.get("NET_PROFIT_RATIO")),
            "roa": first_positive(last.get("ROA")),
        },
    }
    return out if fundamental_payload_has_signal(out) else {}


def fetch_sec_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    if normalize_dsa_symbol(symbol).lower().startswith("hk"):
        return {}
    cik = sec_ticker_to_cik(symbol)
    if not cik:
        return {}
    headers = {"User-Agent": env.get("SEC_USER_AGENT") or "ai-stock-combo/1.0 contact=ops@example.com"}
    try:
        data = http_json_with_headers(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            {},
            headers=headers,
            timeout=18,
        )
    except Exception:
        return {}
    us_gaap = ((data.get("facts") or {}).get("us-gaap") or {}) if isinstance(data, dict) else {}
    if not isinstance(us_gaap, dict) or not us_gaap:
        return {}
    revenue = sec_metric_series(us_gaap, [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "Revenues",
    ])
    profit = sec_metric_series(us_gaap, ["NetIncomeLoss", "ProfitLoss"])
    eps = first_series_value(sec_metric_series(us_gaap, ["EarningsPerShareDiluted", "EarningsPerShareBasic"]))
    roe = derive_roe_from_sec(us_gaap)
    out = {
        "source": "SEC",
        "revenue_history": [round(x / 1e8, 4) for x in revenue if x],
        "net_profit_history": [round(x / 1e8, 4) for x in profit if x],
        "roe_history": roe,
        "eps": eps,
        "financial_years": sec_metric_years(us_gaap, "NetIncomeLoss"),
    }
    return out if fundamental_payload_has_signal(out) else {}


def fetch_marketdata_valuation_fallback(symbol: str, env: dict[str, str], base: dict[str, Any] | None = None) -> dict[str, Any]:
    if normalize_dsa_symbol(symbol).lower().startswith("hk"):
        return {}
    merged: dict[str, Any] = dict(base or {})
    sources: list[str] = []
    for fetcher in (fetch_polygon_quote_fundamentals, fetch_tiingo_quote_fundamentals, fetch_sec_valuation_fundamentals):
        if not valuation_fallback_fetcher_needed(fetcher, merged):
            continue
        try:
            data = fetcher(symbol, env)
        except Exception:
            data = {}
        if not data:
            continue
        merge_fundamental_payload(merged, data)
        source_name = str(data.get("source") or fetcher.__name__).strip()
        if source_name:
            sources.append(source_name)
    price = first_positive(merged.get("price"))
    shares = first_positive(merged.get("shares_outstanding"))
    eps = first_positive(merged.get("eps"))
    revenue = [safe_float(x) for x in merged.get("revenue_history", []) if safe_float(x) > 0]
    if price and shares and not first_positive(merged.get("market_cap_raw")):
        market_cap_raw = price * shares
        merged["market_cap_raw"] = market_cap_raw
        merged["market_cap_yi"] = market_cap_raw / 1e8
    if price and eps and not first_positive(merged.get("pe")):
        merged["pe"] = price / eps
    if revenue and first_positive(merged.get("market_cap_yi")) and not first_positive(merged.get("price_to_sales")):
        merged["price_to_sales"] = first_positive(merged.get("market_cap_yi")) / revenue[-1]
    if sources:
        merged["source"] = "+".join(dict.fromkeys(sources))
    return merged if fundamental_payload_has_signal(merged) else {}


def valuation_fallback_fetcher_needed(fetcher: Any, merged: dict[str, Any]) -> bool:
    name = getattr(fetcher, "__name__", "")
    if name in {"fetch_polygon_quote_fundamentals", "fetch_tiingo_quote_fundamentals"}:
        return not first_positive(merged.get("price"))
    if name == "fetch_sec_valuation_fundamentals":
        return not first_positive(merged.get("shares_outstanding"))
    return True


def fetch_polygon_quote_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    api_key = env.get("POLYGON_API_KEY") or env.get("MASSIVE_API_KEY") or ""
    if not api_key:
        return {}
    ticker = normalize_dsa_symbol(symbol).upper().replace(".US", "")
    def do_fetch() -> dict[str, Any]:
        data = http_json(
            f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(ticker)}/prev",
            {"adjusted": "true", "apiKey": api_key},
            timeout=15,
        )
        results = data.get("results") if isinstance(data, dict) else []
        row = results[0] if isinstance(results, list) and results else {}
        close = first_positive(row.get("c"), row.get("close"))
        if close <= 0:
            return {}
        close = normalize_marketdata_quote_price(ticker, close)
        return {"source": "Polygon", "price": close}
    try:
        return cached_fundamental_fetch("polygon_quote", ticker, do_fetch)
    except Exception:
        return {}


def fetch_tiingo_quote_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    api_key = env.get("TIINGO_API_KEY") or env.get("TIINGO_TOKEN") or env.get("OPENBB_TIINGO_TOKEN") or ""
    if not api_key:
        return {}
    ticker = normalize_dsa_symbol(symbol).upper().replace(".US", "")
    def do_fetch() -> dict[str, Any]:
        data = http_json(
            f"https://api.tiingo.com/tiingo/daily/{urllib.parse.quote(ticker)}/prices",
            {"token": api_key},
            timeout=15,
        )
        row = data[0] if isinstance(data, list) and data else {}
        close = first_positive(row.get("adjClose"), row.get("close"))
        if close <= 0:
            return {}
        close = normalize_marketdata_quote_price(ticker, close)
        return {"source": "Tiingo", "price": close}
    try:
        return cached_fundamental_fetch("tiingo_quote", ticker, do_fetch)
    except Exception:
        return {}


def normalize_marketdata_quote_price(ticker: str, close: float) -> float:
    scale = infer_quote_price_scale(ticker, close)
    return close / scale if scale else close


def infer_quote_price_scale(ticker: str, latest: float) -> int:
    if latest <= 0:
        return 1
    normalized = str(ticker or "").upper().replace(".US", "")
    forced = {x.strip().upper() for x in os.environ.get("PIPELINE_FORCE_US_PRICE_DIV10_TICKERS", "").split(",") if x.strip()}
    if normalized in forced:
        return 10
    known_div10 = set()
    if normalized in known_div10 and latest >= 1000:
        return 10
    known_high_price = {"ASML", "BKNG", "BRK.A", "BRK.B", "COST", "FICO", "GS", "LLY", "MELI", "MSTR", "NVR", "REGN"}
    if normalized in known_high_price:
        return 1
    return 1


def fetch_sec_valuation_fundamentals(symbol: str, env: dict[str, str]) -> dict[str, Any]:
    cik = sec_ticker_to_cik(symbol)
    if not cik:
        return {}
    headers = {"User-Agent": env.get("SEC_USER_AGENT") or "ai-stock-combo/1.0 contact=ops@example.com"}
    def do_fetch() -> dict[str, Any]:
        data = http_json_with_headers(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            {},
            headers=headers,
            timeout=18,
        )
        us_gaap = ((data.get("facts") or {}).get("us-gaap") or {}) if isinstance(data, dict) else {}
        if not isinstance(us_gaap, dict) or not us_gaap:
            return {}
        shares = first_series_value(sec_metric_series(us_gaap, [
            "EntityCommonStockSharesOutstanding",
            "CommonStocksIncludingAdditionalPaidInCapitalSharesOutstanding",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "WeightedAverageNumberOfSharesOutstandingBasic",
        ]))
        eps = first_series_value(sec_metric_series(us_gaap, ["EarningsPerShareDiluted", "EarningsPerShareBasic"]))
        revenue = sec_metric_series(us_gaap, [
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueNet",
            "Revenues",
        ])
        out = {
            "source": "SECValuation",
            "shares_outstanding": shares,
            "eps": eps,
            "revenue_history": [round(x / 1e8, 4) for x in revenue if x],
        }
        return out if fundamental_payload_has_signal(out) else {}
    try:
        return cached_fundamental_fetch("sec_valuation", symbol, do_fetch)
    except Exception:
        return {}


def sort_financial_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    parsed = [row for row in rows if isinstance(row, dict)]
    return sorted(parsed, key=financial_row_sort_key)


def financial_row_sort_key(row: dict[str, Any]) -> tuple[int, str]:
    year = str(row.get("calendarYear") or row.get("date") or row.get("endDate") or "")
    match = re.search(r"(20\d{2})", year)
    year_num = int(match.group(1)) if match else 0
    return (year_num, year)


def fmp_symbol(symbol: str) -> str:
    s = normalize_dsa_symbol(symbol)
    if s.lower().startswith("hk"):
        return s[2:].zfill(4) + ".HK"
    return s.upper().replace(".US", "")


def http_json(url: str, params: dict[str, Any], timeout: int = 18) -> Any:
    query = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{url}?{query}", headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def safe_http_json(url: str, params: dict[str, Any], timeout: int = 18) -> Any:
    try:
        return http_json(url, params, timeout=timeout)
    except Exception:
        return {}


def http_json_with_headers(
    url: str,
    params: dict[str, Any],
    *,
    headers: dict[str, str],
    timeout: int = 18,
) -> Any:
    query = urllib.parse.urlencode(params)
    final_url = f"{url}?{query}" if query else url
    req = urllib.request.Request(final_url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def merge_fundamental_payload(target: dict[str, Any], incoming: dict[str, Any]) -> None:
    field_sources = dict(target.get("_field_sources") or {})
    incoming_source = str(incoming.get("source") or "").strip()
    for key, value in incoming.items():
        if key in {"source", "_field_sources"}:
            continue
        if key in {"revenue_history", "net_profit_history", "roe_history", "financial_years"}:
            if incoming.get(key) and len(incoming.get(key) or []) >= len(target.get(key) or []):
                target[key] = list(incoming.get(key) or [])
                if incoming_source:
                    field_sources[key] = incoming_source
            continue
        if key == "financial_health":
            merged = dict(target.get(key) or {})
            for health_key, health_value in (value or {}).items():
                if first_positive(health_value) or (isinstance(health_value, str) and health_value):
                    current = merged.get(health_key)
                    if (not first_positive(current) and not current) or (health_key in {"net_margin", "gross_margin", "roa", "roic"} and first_positive(health_value) > first_positive(current)):
                        merged[health_key] = health_value
                        if incoming_source:
                            field_sources[f"financial_health.{health_key}"] = incoming_source
            if merged:
                target[key] = merged
            continue
        if key in {"name", "industry"}:
            if value and not target.get(key):
                target[key] = value
                if incoming_source:
                    field_sources[key] = incoming_source
            continue
        if isinstance(value, str):
            if value and not target.get(key):
                target[key] = value
                if incoming_source:
                    field_sources[key] = incoming_source
            continue
        if key == "buy_rating_pct":
            if safe_float(value) > safe_float(target.get(key)):
                target[key] = safe_float(value)
                if incoming_source:
                    field_sources[key] = incoming_source
            continue
        if key == "coverage_count":
            if safe_float(value) > safe_float(target.get(key)):
                target[key] = safe_float(value)
                if incoming_source:
                    field_sources[key] = incoming_source
            continue
        if first_positive(value) and not first_positive(target.get(key)):
            target[key] = safe_float(value) if isinstance(value, (int, float, str)) else value
            if incoming_source:
                field_sources[key] = incoming_source
    if field_sources:
        target["_field_sources"] = field_sources


def fundamental_payload_has_signal(data: dict[str, Any]) -> bool:
    return bool(
        first_positive(
            data.get("price"),
            data.get("market_cap_raw"),
            data.get("market_cap_yi"),
            data.get("pe"),
            data.get("pb"),
            data.get("target_price"),
            data.get("coverage_count"),
        )
        or data.get("revenue_history")
        or data.get("net_profit_history")
        or data.get("roe_history")
    )


def unwrap_finance_number(value: Any) -> float:
    if isinstance(value, dict):
        for key in ("raw", "fmt", "longFmt"):
            if key in value:
                return safe_float(value.get(key))
        return 0.0
    return safe_float(value)


def extract_finance_year(value: Any) -> str:
    if isinstance(value, dict):
        raw = value.get("fmt") or value.get("raw") or ""
    else:
        raw = value or ""
    text = str(raw)
    match = re.search(r"(20\d{2})", text)
    return match.group(1) if match else text[:4]


def normalize_number_series(value: Any, *, scale: float = 1.0) -> list[float]:
    if isinstance(value, list):
        out = [unwrap_finance_number(x) * scale for x in value if unwrap_finance_number(x)]
        return [round(x, 4) for x in out if x]
    number = unwrap_finance_number(value)
    return [round(number * scale, 4)] if number else []


def first_nonempty_series(*series_list: list[float]) -> list[float]:
    best: list[float] = []
    for series in series_list:
        cleaned = [safe_float(x) for x in series if safe_float(x) != 0]
        if len(cleaned) > len(best):
            best = cleaned
    return best


def recommendation_count(recommendation: dict[str, Any]) -> float:
    trends = recommendation.get("trend") or []
    if not isinstance(trends, list) or not trends:
        return 0.0
    current = trends[0] if isinstance(trends[0], dict) else {}
    return sum(
        safe_float(current.get(key))
        for key in ("strongBuy", "buy", "hold", "sell", "strongSell")
    )


def recommendation_buy_ratio(recommendation: dict[str, Any]) -> float:
    trends = recommendation.get("trend") or []
    if not isinstance(trends, list) or not trends:
        return 0.0
    current = trends[0] if isinstance(trends[0], dict) else {}
    total = recommendation_count(recommendation)
    if total <= 0:
        return 0.0
    bullish = safe_float(current.get("strongBuy")) + safe_float(current.get("buy"))
    return round(bullish / total * 100, 2)


def extract_growth_value(earnings_trend: dict[str, Any], key: str, field: str) -> float:
    trend = earnings_trend.get("trend") or []
    if not isinstance(trend, list):
        return 0.0
    for row in trend:
        if not isinstance(row, dict):
            continue
        payload = row.get(key) or {}
        value = unwrap_finance_number(payload.get(field))
        if value:
            return value
    return 0.0


def yahoo_symbol_for_fundamentals(symbol: str) -> str:
    s = normalize_dsa_symbol(symbol)
    if s.lower().startswith("hk"):
        return s[2:].zfill(4) + ".HK"
    return s.upper().replace(".US", "")


def eastmoney_secucode(symbol: str) -> str:
    s = normalize_dsa_symbol(symbol)
    if s.lower().startswith("hk"):
        return f"{s[2:].zfill(5)}.HK"
    ticker = s.upper().replace(".US", "")
    try:
        rows = http_json_with_headers(
            "https://searchapi.eastmoney.com/api/suggest/get",
            {
                "input": ticker,
                "type": "14",
                "token": "D43BF722C8E33BDC906FB84D85E326E8",
                "count": "10",
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12,
        )
    except Exception:
        rows = {}
    suggestions = (((rows.get("QuotationCodeTable") or {}).get("Data") or []) if isinstance(rows, dict) else [])
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        code = str(item.get("Code") or "").upper()
        mkt = str(item.get("MktNum") or "")
        if code != ticker:
            continue
        if mkt == "105":
            return f"{ticker}.O"
        if mkt == "106":
            return f"{ticker}.N"
        if mkt == "107":
            return f"{ticker}.AM"
    return f"{ticker}.O"


def eastmoney_indicator_report_name(secucode: str) -> str:
    return "RPT_HKF10_FN_GMAININDICATOR" if secucode.endswith(".HK") else "RPT_USF10_FN_GMAININDICATOR"


def extract_eastmoney_series(rows: list[dict[str, Any]], keys: tuple[str, ...], *, scale: float = 1e8) -> list[float]:
    out: list[float] = []
    for row in rows:
        value = 0.0
        for key in keys:
            value = first_positive(row.get(key))
            if value:
                break
        if value:
            out.append(round(value / scale, 4) if scale else round(value, 4))
    return out


_SEC_TICKER_CACHE: dict[str, str] | None = None


def sec_ticker_to_cik(symbol: str) -> str:
    global _SEC_TICKER_CACHE
    ticker = normalize_dsa_symbol(symbol).upper().replace(".US", "")
    if _SEC_TICKER_CACHE is None:
        try:
            data = http_json_with_headers(
                "https://www.sec.gov/files/company_tickers.json",
                {},
                headers={"User-Agent": "ai-stock-combo/1.0 contact=ops@example.com"},
                timeout=18,
            )
        except Exception:
            _SEC_TICKER_CACHE = {}
        else:
            cache: dict[str, str] = {}
            if isinstance(data, dict):
                for row in data.values():
                    if not isinstance(row, dict):
                        continue
                    t = str(row.get("ticker") or "").upper()
                    cik = str(row.get("cik_str") or "").zfill(10)
                    if t and cik:
                        cache[t] = cik
            _SEC_TICKER_CACHE = cache
    return (_SEC_TICKER_CACHE or {}).get(ticker, "")


def sec_metric_series(us_gaap: dict[str, Any], metric_names: list[str]) -> list[float]:
    for metric_name in metric_names:
        metric = us_gaap.get(metric_name) or {}
        units = metric.get("units") or {}
        series = []
        for unit_rows in units.values():
            if not isinstance(unit_rows, list):
                continue
            annual_rows = [
                row for row in unit_rows
                if isinstance(row, dict) and row.get("form") in {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
            ]
            annual_rows = sorted(annual_rows, key=lambda row: str(row.get("end") or ""))
            values = [safe_float(row.get("val")) for row in annual_rows if safe_float(row.get("val")) != 0]
            if len(values) > len(series):
                series = values
        if series:
            return series
    return []


def sec_metric_years(us_gaap: dict[str, Any], metric_name: str) -> list[str]:
    metric = us_gaap.get(metric_name) or {}
    units = metric.get("units") or {}
    best: list[str] = []
    for unit_rows in units.values():
        if not isinstance(unit_rows, list):
            continue
        annual_rows = [
            row for row in unit_rows
            if isinstance(row, dict) and row.get("form") in {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
        ]
        annual_rows = sorted(annual_rows, key=lambda row: str(row.get("end") or ""))
        years = [extract_finance_year(row.get("end")) for row in annual_rows]
        if len(years) > len(best):
            best = years
    return best


def first_series_value(values: list[float]) -> float:
    return safe_float(values[-1]) if values else 0.0


def derive_roe_from_sec(us_gaap: dict[str, Any]) -> list[float]:
    profit = sec_metric_series(us_gaap, ["NetIncomeLoss", "ProfitLoss"])
    equity = sec_metric_series(us_gaap, [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ])
    if not profit or not equity:
        return []
    size = min(len(profit), len(equity))
    out: list[float] = []
    for ni, eq in zip(profit[-size:], equity[-size:]):
        if eq:
            out.append(round(ni / eq * 100, 4))
    return out


def global_hk_quote_tencent(code: str) -> dict[str, Any]:
    req = urllib.request.Request(f"https://qt.gtimg.cn/q=r_hk{code}", headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=8) as response:
        text = response.read().decode("gb18030", errors="replace")
    match = re.search(r'"(.+)"', text)
    if not match:
        raise RuntimeError("腾讯港股行情解析失败")
    fields = match.group(1).split("~")
    if len(fields) < 40:
        raise RuntimeError("腾讯港股字段不足")
    return {
        "close": safe_float(fields[3]),
        "open": safe_float(fields[5]),
        "high": safe_float(fields[33]),
        "low": safe_float(fields[34]),
        "volume": safe_float(fields[6]),
        "change_pct": safe_float(fields[32]),
        "name": fields[1],
        "provider": "global/tencent",
    }


def global_hk_quote_sina(code: str) -> dict[str, Any]:
    req = urllib.request.Request(
        f"https://hq.sinajs.cn/list=rt_hk{code}",
        headers={"Referer": "https://finance.sina.com.cn/", "User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=8) as response:
        text = response.read().decode("gb18030", errors="replace")
    match = re.search(r'"(.+)"', text)
    if not match:
        raise RuntimeError("新浪港股行情解析失败")
    fields = match.group(1).split(",")
    if len(fields) < 13:
        raise RuntimeError("新浪港股字段不足")
    return {
        "close": safe_float(fields[6]),
        "open": safe_float(fields[2]),
        "high": safe_float(fields[4]),
        "low": safe_float(fields[5]),
        "volume": safe_float(fields[12]),
        "change_pct": safe_float(fields[8]),
        "name": fields[1],
        "provider": "global/sina",
    }


def global_eastmoney_quote(code: str, prefix: int) -> dict[str, Any]:
    result = http_json(
        "https://push2.eastmoney.com/api/qt/stock/get",
        {"secid": f"{prefix}.{code}", "fields": "f43,f44,f45,f46,f47,f48,f55,f57,f58,f59,f60,f170"},
        timeout=8,
    )
    data = result.get("data") if isinstance(result, dict) else None
    if not data:
        raise RuntimeError("东财 push2 无数据")
    divisor = 10 ** int(data.get("f59") or 3)

    def price(key: str) -> float:
        value = data.get(key)
        return 0.0 if value is None or value == "-" else safe_float(value) / divisor

    return {
        "close": price("f43"),
        "open": price("f46"),
        "high": price("f44"),
        "low": price("f45"),
        "volume": safe_float(data.get("f47")),
        "change_pct": safe_float(data.get("f170")) / 100,
        "name": data.get("f58") or "",
        "provider": "global/eastmoney",
    }


def fetch_quote_fallback_fundamentals(symbol: str) -> dict[str, Any]:
    s = normalize_dsa_symbol(symbol)
    try:
        if s.lower().startswith("hk"):
            code = s[2:].zfill(5)
            quote = first_successful_quote(
                ("TencentQuote", lambda: global_hk_quote_tencent(code)),
                ("SinaQuote", lambda: global_hk_quote_sina(code)),
                ("EastmoneyQuote", lambda: global_eastmoney_quote(code, 116)),
            )
        else:
            ticker = s.upper().replace(".US", "")
            quote = first_successful_quote(
                ("EastmoneyQuote", lambda: global_eastmoney_quote(ticker, 105)),
                ("EastmoneyQuote", lambda: global_eastmoney_quote(ticker, 106)),
                ("EastmoneyQuote", lambda: global_eastmoney_quote(ticker, 107)),
            )
    except Exception:
        return {}
    if not quote:
        return {}
    market_cap_yi = first_positive(quote.get("market_cap"))
    market_cap_raw = market_cap_yi * 1e8 if market_cap_yi else 0.0
    source = f"QuoteFallback/{quote.get('_source_name') or quote.get('provider') or 'quote'}"
    out = {
        "source": source,
        "name": quote.get("name") or "",
        "price": first_positive(quote.get("close"), quote.get("price")),
        "market_cap_yi": market_cap_yi,
        "market_cap_raw": market_cap_raw,
        "pe": first_positive(quote.get("pe")),
        "pb": first_positive(quote.get("pb")),
        "eps": first_positive(quote.get("eps")),
    }
    out["_field_sources"] = {
        key: source
        for key in ("price", "market_cap_yi", "market_cap_raw", "pe", "pb", "eps")
        if first_positive(out.get(key))
    }
    return out if fundamental_payload_has_signal(out) else {}


def first_successful_quote(*candidates: tuple[str, Any]) -> dict[str, Any]:
    for source_name, fn in candidates:
        try:
            data = fn()
        except Exception:
            continue
        if isinstance(data, dict) and data:
            out = dict(data)
            out["_source_name"] = source_name
            return out
    return {}


def read_uzi_cache(uzi_dir: Path, symbol: str) -> dict[str, Any] | None:
    cache_root = uzi_dir / "skills" / "deep-analysis" / "scripts" / ".cache"
    candidates = [cache_root / symbol]
    if symbol.lower().startswith("hk"):
        digits = symbol[2:].zfill(5)
        candidates.append(cache_root / f"{digits}.HK")
    if symbol.endswith(".SH") or symbol.endswith(".SZ"):
        candidates.append(cache_root / symbol.split(".", 1)[0])
    for path in candidates:
        syn = path / "synthesis.json"
        panel = path / "panel.json"
        dims = path / "dimensions.json"
        if syn.exists():
            raw = path / "raw_data.json"
            agent = path / "agent_analysis.json"
            return {
                "synthesis": json.loads(syn.read_text(encoding="utf-8")),
                "raw": json.loads(raw.read_text(encoding="utf-8")) if raw.exists() else {},
                "panel": json.loads(panel.read_text(encoding="utf-8")) if panel.exists() else {},
                "dimensions": json.loads(dims.read_text(encoding="utf-8")) if dims.exists() else {},
                "agent_analysis": json.loads(agent.read_text(encoding="utf-8")) if agent.exists() else {},
                "_cache_mtime": syn.stat().st_mtime,
            }
    return None


def cache_is_fresh(parsed: dict[str, Any], max_age_hours: float) -> bool:
    mtime = safe_float(parsed.get("_cache_mtime"))
    if not mtime or max_age_hours <= 0:
        return False
    return (time.time() - mtime) <= max_age_hours * 3600


def normalize_uzi_result(
    item: dict[str, Any],
    symbol: str,
    parsed: dict[str, Any],
    uzi_dir: Path | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    if uzi_dir is not None and env is not None:
        parsed = restore_pipeline_seed_dimensions(parsed, item, uzi_dir, env)
    syn = parsed.get("synthesis") or {}
    panel = parsed.get("panel") or {}
    score = safe_float(syn.get("overall_score"), safe_float(item.get("confidence")) * 100)
    masters = {}
    for inv in (panel.get("investors") or [])[:12]:
        name = inv.get("name") or inv.get("investor_id")
        if name:
            masters[str(name)] = safe_float(inv.get("score"))
    reason = syn.get("verdict_detail") or item.get("reason", "")
    quality_flags = uzi_quality_flags(syn, panel, score, str(reason))
    data_flags = uzi_seed_data_quality_flags(parsed.get("raw") or {})
    quality_flags.extend(data_flags)
    if env_flag_enabled("PIPELINE_UZI_REQUIRE_AGENT_REVIEW", default=True) and not syn.get("agent_reviewed"):
        quality_flags.append("UZI 未完成 agent 投委复核")
    status = "degraded" if quality_flags else "ok"
    return {
        "symbol": item["symbol"],
        "uzi_symbol": symbol,
        "name": syn.get("name") or item.get("name", ""),
        "dsa_score": item.get("dsa_score", 0),
        "tradingagents_confidence": item.get("confidence", 0),
        "uzi_score": round(score, 2),
        "rating": syn.get("verdict_label") or rating_from_score(score),
        "masters": masters,
        "reason": reason,
        "status": status,
        "quality_flags": quality_flags,
    }


def ensure_uzi_agent_review(
    item: dict[str, Any],
    symbol: str,
    parsed: dict[str, Any],
    uzi_dir: Path,
    python_bin: str,
    env: dict[str, str],
) -> dict[str, Any]:
    if env.get("PIPELINE_UZI_AGENT_REVIEW", "1") == "0":
        return parsed
    syn = parsed.get("synthesis") or {}
    if syn.get("agent_reviewed"):
        return parsed
    raw = parsed.get("raw") or {}
    panel = parsed.get("panel") or {}
    if not raw or not panel:
        return parsed
    write_uzi_agent_analysis(uzi_dir, symbol, build_uzi_agent_analysis(item, symbol, raw, panel, parsed.get("dimensions") or {}))
    timeout = int(env.get("PIPELINE_UZI_STAGE2_TIMEOUT", "120"))
    scripts_dir = uzi_dir / "skills" / "deep-analysis" / "scripts"
    snippet = (
        "from run_real_test import stage2\n"
        f"stage2({symbol!r})\n"
    )
    try:
        rc, text = run([python_bin, "-c", snippet], scripts_dir, env, timeout=timeout)
        write_text(WORK / f"uzi_stage2_{safe_name(symbol)}.log", text)
        if rc != 0:
            return parsed
    except subprocess.TimeoutExpired as exc:
        write_text(WORK / f"uzi_stage2_{safe_name(symbol)}.log", str(exc.output or ""))
        return parsed
    return read_uzi_cache(uzi_dir, symbol) or parsed


def write_uzi_agent_analysis(uzi_dir: Path, symbol: str, analysis: dict[str, Any]) -> None:
    cache_root = uzi_dir / "skills" / "deep-analysis" / "scripts" / ".cache"
    for alias in uzi_cache_aliases(symbol):
        path = cache_root / alias / "agent_analysis.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def build_uzi_agent_analysis(
    item: dict[str, Any],
    symbol: str,
    raw: dict[str, Any],
    panel: dict[str, Any],
    dims: dict[str, Any],
) -> dict[str, Any]:
    raw_dims = raw.get("dimensions") or {}
    basic = (raw_dims.get("0_basic") or {}).get("data") or {}
    kline = (raw_dims.get("2_kline") or {}).get("data") or {}
    financials = (raw_dims.get("1_financials") or {}).get("data") or {}
    valuation = (raw_dims.get("10_valuation") or {}).get("data") or {}
    name = str(basic.get("name") or item.get("name") or symbol)
    price = safe_float(basic.get("price"))
    pe = safe_float(basic.get("pe_ttm") or valuation.get("pe"))
    pb = safe_float(basic.get("pb") or valuation.get("pb"))
    stage = str(kline.get("stage") or kline.get("ma_align") or "趋势待确认")
    rsi = safe_float(kline.get("rsi"))
    revenue = financials.get("revenue_history") or []
    profit = financials.get("net_profit_history") or []
    roe = financials.get("roe_history") or []
    ret_20 = safe_float(item.get("ret_20d"))
    confidence = safe_float(item.get("confidence")) * 100

    investors = [x for x in (panel.get("investors") or []) if isinstance(x, dict) and str(x.get("signal")) != "skip"]
    ranked = sorted(investors, key=lambda x: safe_float(x.get("score")), reverse=True)
    bulls = ranked[:3]
    bears = list(reversed(ranked[-3:])) if ranked else []
    bull_text = "；".join(f"{x.get('name')} {safe_float(x.get('score')):.0f}分：{x.get('headline') or x.get('reasoning') or ''}" for x in bulls)
    bear_text = "；".join(f"{x.get('name')} {safe_float(x.get('score')):.0f}分：{x.get('headline') or x.get('reasoning') or ''}" for x in bears)

    dim_commentary = {
        "0_basic": f"{name} 当前参考价 {price or '未知'}，行业为 {basic.get('industry') or '待确认'}，市值与估值字段已由数据中台核对后进入 UZI。",
        "1_financials": f"财务侧收入序列 {len(revenue)} 项、利润序列 {len(profit)} 项、ROE 序列 {len(roe)} 项；若历史样本不足，投委需降低基本面置信度。",
        "2_kline": f"技术侧处于 {stage}，RSI 约 {rsi or '未知'}，20 日涨幅约 {ret_20:.1f}%，追高与回踩确认需要分开判断。",
        "3_macro": "宏观维度按美股半导体/全球风险偏好处理，利率、美元和 AI 资本开支预期会影响估值弹性。",
        "4_peers": "同行对比需重点看同业估值、订单能见度和毛利率兑现，缺同行样本时不应单独推高评分。",
        "5_chain": "产业链维度关注 AI 基建、设备更新周期和上游瓶颈，外部 Serenity 线索只作参考不直接加分。",
        "6_research": f"投研层置信度约 {confidence:.1f}，但 UZI 投委需要独立审查，不能机械继承 TradingAgents 分数。",
        "7_industry": "行业景气度需结合订单、库存周期和资本开支节奏；若仅有价格动量，归入观察而非直接买入。",
        "8_materials": "原材料与供给侧约束目前作为风险/催化双向变量，需等待一手信息确认。",
        "9_futures": "期货或大宗映射对该标的不是主要驱动，除非出现明确成本冲击或供应链价格变化。",
        "10_valuation": f"估值侧 PE 约 {pe or '未知'}、PB 约 {pb or '未知'}，高估值股票必须要求更强增长兑现。",
        "11_governance": "治理维度主要检查回购、股权激励、管理层稳定性和重大稀释事件，暂不单独改变结论。",
        "12_capital_flow": "资金面需观察放量突破还是高位换手，若 20 日涨幅过大，应优先等待缩量回踩。",
        "13_policy": "政策维度重点关注出口管制、AI 数据中心监管和跨境供应链限制，属于需要持续更新的风险项。",
        "14_moat": "护城河需要由客户粘性、技术壁垒、份额和定价权共同验证，单纯热门赛道不能视作护城河。",
        "15_events": "事件催化以财报、指引、订单、客户验证和管理层口径为准，社交流叙事只能列为观察项。",
        "16_lhb": "该维度对美股参考价值有限，不作为核心结论来源。",
        "17_sentiment": "情绪维度用于识别拥挤度，若情绪和涨幅同步过热，买点应后移到回踩或突破确认。",
        "18_trap": "异常波动和过热动量需要进入风控，避免把短线追涨误判为基本面改善。",
        "19_contests": "比赛/短线资金维度只作辅助，不替代基本面、估值和交易计划。",
    }
    risks = [
        "估值或短线涨幅过热时，回撤会放大。",
        "若订单、财报或管理层指引不能兑现，投委评分应下调。",
        "宏观利率和 AI 资本开支预期变化会影响板块估值。",
    ]
    if ret_20 >= 25:
        risks.insert(0, f"20 日涨幅 {ret_20:.1f}% 偏高，当前不适合无条件追入。")
    buy_zone = item.get("buy_zone") or ""
    stop_loss = item.get("stop_loss") or ""
    core = f"{name} 经 UZI agent 复核后，核心矛盾是基本面/赛道质量与当前价位性价比之间的取舍；需结合 {buy_zone or '回踩区'} 和止损 {stop_loss or '关键支撑'} 执行。"
    return {
        "agent_reviewed": True,
        "dim_commentary": dim_commentary,
        "panel_insights": f"投委分歧：看多代表为 {bull_text or '暂无'}；看空代表为 {bear_text or '暂无'}。最终结论不能只看均分，需要同时参考估值、动量和买点质量。",
        "great_divide_override": {
            "punchline": f"{name} 的分歧不在于有没有故事，而在于当前价格是否已经提前反映预期。",
            "bull_say_rounds": [
                f"多方认为 {name} 仍有产业链和趋势支撑，若财报兑现，估值可以被增长消化。",
                f"技术面 {stage}，若突破伴随放量，说明资金仍在确认主线。",
                "多方只接受有价格纪律的分批入场，不主张在过热区一次性买入。",
            ],
            "bear_say_rounds": [
                f"空方强调估值 PE {pe or '未知'} 与短线涨幅 {ret_20:.1f}% 已经压低安全边际。",
                "如果缺少订单、利润率或现金流的进一步确认，当前更多是观察池而非买入池。",
                "空方要求跌破止损位或基本面证据转弱时果断退出。",
            ],
        },
        "narrative_override": {
            "core_conclusion": core,
            "risks": risks[:5],
            "buy_zones": {
                "value": {"price": price, "rationale": "价值派只在估值和基本面安全边际改善后考虑。"},
                "growth": {"price": price, "rationale": "成长派需要收入、利润或订单继续兑现。"},
                "technical": {"price": price, "rationale": "技术派等待回踩企稳或有效突破确认。"},
                "youzi": {"price": price, "rationale": "短线资金只适合小仓验证，不适合追高重仓。"},
            },
        },
        "data_gap_acknowledged": {
            "3_macro": "宏观与政策信息会在 daily pipeline 外部信号中持续补充。",
            "7_industry": "行业景气需结合后续财报、订单和供应链验证。",
            "13_policy": "政策风险保留为持续观察项。",
        },
    }


def fallback_uzi(item: dict[str, Any], note: str) -> dict[str, Any]:
    score = safe_float(item.get("confidence")) * 100
    return {
        "symbol": item["symbol"],
        "uzi_symbol": to_uzi_symbol(item["symbol"]),
        "name": item.get("name", ""),
        "dsa_score": item.get("dsa_score", 0),
        "tradingagents_confidence": item.get("confidence", 0),
        "uzi_score": round(score, 2),
        "rating": rating_from_score(score),
        "masters": {},
        "reason": f"UZI-Skill {translate_failure_note(note)}；使用 TradingAgents 置信度降级估算",
        "status": "fallback",
        "quality_flags": ["UZI 未返回有效投委结果"],
    }


def uzi_seed_data_quality_flags(raw: dict[str, Any]) -> list[str]:
    if not raw:
        return ["UZI 原始数据缺失"]
    dims = raw.get("dimensions") or {}
    flags: list[str] = []
    basic = ((dims.get("0_basic") or {}).get("data") or {}) if isinstance(dims.get("0_basic"), dict) else {}
    financials = ((dims.get("1_financials") or {}).get("data") or {}) if isinstance(dims.get("1_financials"), dict) else {}
    kline = ((dims.get("2_kline") or {}).get("data") or {}) if isinstance(dims.get("2_kline"), dict) else {}
    valuation = ((dims.get("10_valuation") or {}).get("data") or {}) if isinstance(dims.get("10_valuation"), dict) else {}
    if not first_positive(basic.get("price")):
        flags.append("UZI 基础行情缺失")
    if not (financials.get("revenue_history") or financials.get("net_profit_history") or financials.get("roe_history")):
        flags.append("UZI 财务维度不足")
    if not (kline.get("candles_60d") or kline.get("stage")):
        flags.append("UZI K线维度不足")
    valuation_has_signal = any(first_positive(valuation.get(key), basic.get(key)) for key in (
        "pe",
        "pe_ttm",
        "pb",
        "market_cap",
        "market_cap_yi",
        "target_price_avg",
        "forward_pe",
        "price_to_sales",
        "ev_to_sales",
        "net_margin",
    ))
    valuation_has_signal = valuation_has_signal or bool(valuation.get("valuation_note"))
    valuation_has_signal = valuation_has_signal or bool(financials.get("revenue_history") or financials.get("net_profit_history"))
    if not valuation_has_signal:
        flags.append("UZI 估值维度不足")
    return flags


def uzi_quality_flags(syn: dict[str, Any], panel: dict[str, Any], score: float, reason: str) -> list[str]:
    flags: list[str] = []
    investors = panel.get("investors") or []
    if len(investors) < 3:
        flags.append("UZI 投委成员结果不足")
    if "基本面 56.9" in reason or "共识 10.5" in reason:
        flags.append("UZI 输出疑似默认低分模板")
    if not reason.strip():
        flags.append("UZI 缺少投委理由")
    return flags


def annotate_uzi_batch_quality(rows: list[dict[str, Any]], env: dict[str, str]) -> list[dict[str, Any]]:
    min_count = int(env.get("PIPELINE_UZI_REPEATED_SCORE_MIN_COUNT", "3"))
    buckets: dict[float, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row.get("status")) not in {"ok", "degraded"}:
            continue
        score = round(safe_float(row.get("uzi_score")), 1)
        buckets.setdefault(score, []).append(row)
    repeated_low_scores = {
        score for score, grouped in buckets.items()
        if len(grouped) >= min_count and 30 <= score < 60
    }
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        score = round(safe_float(item.get("uzi_score")), 1)
        flags = list(item.get("quality_flags") or [])
        if score in repeated_low_scores:
            flags.append(f"UZI 批量重复低分 {score:.1f}，需重新复核")
        if flags:
            item["quality_flags"] = list(dict.fromkeys(flags))
            item["status"] = "degraded" if item.get("status") == "ok" else item.get("status", "degraded")
            note = "；".join(item["quality_flags"])
            reason = str(item.get("reason") or "")
            if note and note not in reason:
                item["reason"] = f"{reason}；{note}".strip("；")
        out.append(item)
    return out


def recheck_degraded_uzi_rows(
    rows: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    uzi_dir: Path,
    python_bin: str,
    env: dict[str, str],
    timeout: int,
    depth: str,
) -> list[dict[str, Any]]:
    """Retry UZI once when the output looks like a repeated template.

    The retry keeps raw_data.json, but removes UZI's generated synthesis files
    so the second pass must rebuild the committee result from the seeded data.
    """
    if env.get("PIPELINE_UZI_RECHECK_DEGRADED", "1") == "0":
        return rows
    max_rechecks = max(0, int(env.get("PIPELINE_UZI_RECHECK_MAX", "4")))
    if max_rechecks <= 0:
        return rows
    item_map = {str(item.get("symbol")): item for item in selected}
    recheck_depth = env.get("PIPELINE_UZI_RECHECK_DEPTH", depth)
    recheck_timeout = int(env.get("PIPELINE_UZI_RECHECK_TIMEOUT_PER_STOCK", str(timeout)))
    out: list[dict[str, Any]] = []
    rechecked = 0
    for row in rows:
        if rechecked >= max_rechecks or not uzi_needs_recheck(row):
            out.append(row)
            continue
        source_item = item_map.get(str(row.get("symbol")))
        if not source_item:
            out.append(row)
            continue
        symbol = str(row.get("uzi_symbol") or to_uzi_symbol(str(row.get("symbol"))))
        invalidate_uzi_output_cache(uzi_dir, symbol)
        rechecked += 1
        try:
            rc, text = run(
                [python_bin, "run.py", symbol, "--no-browser", "--depth", recheck_depth],
                uzi_dir,
                env,
                timeout=recheck_timeout,
            )
            write_text(WORK / f"uzi_recheck_{safe_name(symbol)}.log", text)
        except subprocess.TimeoutExpired:
            retry = fallback_uzi(source_item, "二次复核超时")
            retry["quality_flags"] = list(dict.fromkeys((row.get("quality_flags") or []) + ["UZI 二次复核仍未通过"]))
            retry["status"] = "degraded"
            out.append(retry)
            continue
        parsed = read_uzi_cache(uzi_dir, symbol)
        if rc != 0 or not parsed:
            retry = fallback_uzi(source_item, f"二次复核失败 rc={rc}")
            retry["quality_flags"] = list(dict.fromkeys((row.get("quality_flags") or []) + ["UZI 二次复核仍未通过"]))
            retry["status"] = "degraded"
            out.append(retry)
            continue
        retry = normalize_uzi_result(source_item, symbol, parsed, uzi_dir, env)
        if uzi_needs_recheck(retry):
            retry_flags = list(retry.get("quality_flags") or [])
            retry_flags.extend([str(x) for x in (row.get("quality_flags") or [])])
            retry_flags.append("UZI 二次复核仍未通过")
            retry["quality_flags"] = list(dict.fromkeys(retry_flags))
            retry["status"] = "degraded"
            reason = str(retry.get("reason") or "")
            note = "；".join(retry["quality_flags"])
            if note and note not in reason:
                retry["reason"] = f"{reason}；{note}".strip("；")
        out.append(retry)
    return out


def uzi_needs_recheck(row: dict[str, Any]) -> bool:
    flags = "；".join(str(x) for x in (row.get("quality_flags") or []))
    markers = ("批量重复低分", "默认低分模板")
    return any(marker in flags for marker in markers)


def invalidate_uzi_output_cache(uzi_dir: Path, symbol: str) -> None:
    cache_root = uzi_dir / "skills" / "deep-analysis" / "scripts" / ".cache"
    for alias in uzi_cache_aliases(symbol):
        path = cache_root / alias
        for name in ("synthesis.json", "panel.json", "dimensions.json"):
            try:
                (path / name).unlink()
            except FileNotFoundError:
                pass
            except PermissionError:
                write_text(WORK / f"uzi_cache_permission_{safe_name(alias)}.log", f"cannot remove {path / name}")


def uzi_rank_score(row: dict[str, Any]) -> float:
    score = safe_float(row.get("uzi_score"))
    if str(row.get("status")) == "degraded":
        score -= 25
    if str(row.get("status")) == "fallback":
        score -= 40
    return score


def rating_from_score(score: float) -> str:
    if score >= 85:
        return "强烈买入"
    if score >= 75:
        return "买入"
    if score >= 60:
        return "观察"
    return "回避"


def merge_scores(candidates: list[dict[str, Any]], trading: list[dict[str, Any]], uzi: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    dsa_map = {x["symbol"]: x for x in candidates}
    ta_map = {x["symbol"]: x for x in trading}
    uzi_map = {x["symbol"]: x for x in uzi}
    rows = []
    for ta_item in trading[:top_n]:
        symbol = ta_item["symbol"]
        uzi_item = uzi_map.get(symbol) or fallback_uzi(ta_item, "未进入 UZI Top10")
        dsa_item = dsa_map.get(symbol, {})
        dsa_score = safe_float(dsa_item.get("score") or uzi_item.get("dsa_score") or ta_item.get("dsa_score"))
        ta_score = safe_float(ta_item.get("confidence") or uzi_item.get("tradingagents_confidence")) * 100
        raw_uzi_score = safe_float(uzi_item.get("uzi_score"))
        committee_source = "UZI-Skill"
        if uzi_has_invalid_committee_result(uzi_item):
            uzi_score = invalid_committee_score(uzi_item)
            committee_source = "UZI无效"
        elif should_use_backup_committee_score(uzi_item):
            uzi_score = backup_committee_score(dsa_item, ta_item, uzi_item)
            committee_source = "备用投委评分"
        else:
            uzi_score = raw_uzi_score
        gate_uzi_item = {**uzi_item, "uzi_score": uzi_score}
        pretrade_audit = pretrade_consistency_audit(dsa_item, dsa_item, ta_item)
        gates = list(pretrade_audit.get("gates") or [])
        gates.extend(gate for gate in evaluate_buy_gates(dsa_item, ta_item, gate_uzi_item) if gate not in gates)
        raw_total = dsa_score * 0.30 + ta_score * 0.40 + uzi_score * 0.30
        total = raw_total
        if gates:
            hard_cap = 82.0
            if any(gate in gates for gate in ("UZI 投委未完成", "UZI 投委分低于 60")):
                hard_cap = 59.0
            elif any(is_tradingagents_gate(gate) for gate in gates) or any(str(gate).startswith("20日涨幅") for gate in gates):
                hard_cap = 64.0
            elif "UZI 投委输出质量不足" in gates and uzi_score >= 60:
                hard_cap = 82.0
            total = min(
                total,
                hard_cap,
            )
        action = normalize_action(ta_item.get("action", ""))
        if gates and action == "买入":
            action = "观察"
        advice = make_trade_advice(total, action, ta_item.get("risk", "medium"), dsa_item)
        audited_item = {**dsa_item, **trade_level_fields_from_advice(advice)}
        pretrade_audit = pretrade_consistency_audit(audited_item, dsa_item, ta_item)
        for gate in pretrade_audit.get("gates") or []:
            if gate not in gates:
                gates.append(gate)
        if pretrade_audit.get("gates"):
            total = min(total, 49.0)
            if action == "买入":
                action = "观察"
        score_cap_reason = "；".join(gates) if total < raw_total else ""
        bucket = classify_trade_bucket(total, action, normalize_risk(ta_item.get("risk", "medium")), gates, advice)
        if bucket["bucket"] == "D" and has_hard_no_buy_gate(gates):
            advice = force_watch_only_advice(advice, gates)
            bucket = classify_trade_bucket(total, action, normalize_risk(ta_item.get("risk", "medium")), gates, advice)
        row = {
            "symbol": symbol,
            "name": uzi_item.get("name") or dsa_item.get("name") or ta_item.get("name", ""),
            "raw_total_score": round(raw_total, 2),
            "risk_adjusted_score": round(total, 2),
            "total_score": round(total, 2),
            "score_cap_reason": score_cap_reason,
            "dsa_score": round(dsa_score, 2),
            "tradingagents_score": round(ta_score, 2),
            "uzi_score": round(uzi_score, 2),
            "raw_uzi_score": round(raw_uzi_score, 2),
            "committee_score_source": committee_source,
            "rating": committee_rating_label(uzi_item, uzi_score, committee_source),
            "action": action,
            "risk": normalize_risk(ta_item.get("risk", "medium")),
            "ta_status": ta_item.get("ta_status", "unknown"),
            "buy_eligible": not gates,
            "trade_bucket": bucket["bucket"],
            "trade_bucket_label": bucket["label"],
            "trade_trigger": bucket["trigger"],
            "quality_gates": gates,
            "quality_note": "；".join(gates),
            "pretrade_audit": pretrade_audit,
            "trade_advice": advice["trade_advice"],
            "buy_advice": advice["buy_advice"],
            "sell_advice": advice["sell_advice"],
            "position_advice": advice["position_advice"],
            "price_plan": advice.get("price_plan", ""),
            "reference_price": advice.get("reference_price", ""),
            "buy_zone": advice.get("buy_zone", ""),
            "breakout_price": advice.get("breakout_price", ""),
            "stop_loss": advice.get("stop_loss", ""),
            "take_profit_1": advice.get("take_profit_1", ""),
            "take_profit_2": advice.get("take_profit_2", ""),
            "dsa_reason": dsa_item.get("reason", ""),
            "external_signal": dsa_item.get("external_signal", {}),
            "serenity_signal": extract_serenity_signal(dsa_item),
            "dexter_signal": dsa_item.get("dexter_signal", {}),
            "tradingagents_reason": ta_item.get("reason", ""),
            "uzi_reason": uzi_item.get("reason", ""),
            "uzi_quality_flags": uzi_item.get("quality_flags", []),
            "reason": build_combined_reason(dsa_item, ta_item, uzi_item),
            "uzi_status": uzi_item.get("status", "unknown"),
        }
        normalize_execution_text(row)
        row["short_watch"] = build_short_watch(row, dsa_item, ta_item, gate_uzi_item, gates)
        rows.append(prepare_report_row(row))
    return sorted(rows, key=lambda x: x["total_score"], reverse=True)[:top_n]


def trade_level_fields_from_advice(advice: dict[str, Any]) -> dict[str, Any]:
    return {
        "reference_price": advice.get("reference_price", ""),
        "buy_zone": advice.get("buy_zone", ""),
        "breakout_price": advice.get("breakout_price", ""),
        "stop_loss": advice.get("stop_loss", ""),
        "take_profit_1": advice.get("take_profit_1", ""),
        "take_profit_2": advice.get("take_profit_2", ""),
    }


def normalize_execution_text(row: dict[str, Any]) -> dict[str, Any]:
    """Keep all actionable text aligned with the final generated price levels."""
    reference = str(row.get("reference_price") or "").strip()
    buy_zone = str(row.get("buy_zone") or "").strip()
    breakout = str(row.get("breakout_price") or "").strip()
    stop_loss = str(row.get("stop_loss") or "").strip()
    tp1 = str(row.get("take_profit_1") or "").strip()
    tp2 = str(row.get("take_profit_2") or "").strip()
    if reference and buy_zone and breakout:
        row["buy_advice"] = (
            f"只按最终口径执行：参考价 {reference}；优先等回踩到 {buy_zone} 后缩量企稳；"
            f"若放量站上 {breakout}，次日不跌回突破位才允许小仓确认。"
        )
    if stop_loss and (tp1 or tp2):
        row["sell_advice"] = (
            f"只按最终口径执行：跌破 {stop_loss} 严格止损；"
            f"第一止盈 {tp1 or '-'}，第二止盈 {tp2 or '-'}；"
            "若价格口径或 ticker 映射异常，立即停止执行。"
        )
    bucket = str(row.get("trade_bucket") or "")
    if bucket == "D":
        row["trade_trigger"] = f"只观察；硬性闸门未通过：{complete_excerpt(str(row.get('quality_note') or '信号未一致'), 180)}。"
    elif bucket == "A" and stop_loss and buy_zone:
        row["trade_trigger"] = f"现价附近只允许小仓；止损 {stop_loss}；若回踩 {buy_zone} 且企稳可加仓。"
    elif bucket == "B" and buy_zone:
        row["trade_trigger"] = f"等待回踩到 {buy_zone} 且缩量企稳；止损 {stop_loss or '按最新支撑位'}。"
    elif bucket == "C" and breakout:
        row["trade_trigger"] = f"等待放量站上 {breakout}，次日不跌回突破位再考虑小仓。"
    row["execution_price_correction"] = build_execution_price_correction(row)
    return row


EXECUTION_PRICE_KEYS = {
    "price_plan",
    "reference_price",
    "buy_zone",
    "breakout_price",
    "stop_loss",
    "take_profit_1",
    "take_profit_2",
    "buy_advice",
    "sell_advice",
    "position_advice",
    "trade_trigger",
    "pretrade_audit",
    "quality_gates",
    "quality_note",
    "gates",
    "checks",
}


def prepare_report_row(row: dict[str, Any]) -> dict[str, Any]:
    item = deepcopy_json(row)
    normalize_execution_text(item)
    item = sanitize_report_free_text(item)
    item["reason"] = compose_report_reason(item)
    return item


def sanitize_report_free_text(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        return {k: sanitize_report_free_text(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_report_free_text(v, key) for v in value]
    if isinstance(value, str) and key not in EXECUTION_PRICE_KEYS:
        return strip_actionable_price_sentences(strip_internal_error_sentences(value))
    return value


def build_execution_price_correction(row: dict[str, Any]) -> str:
    reference = str(row.get("reference_price") or "").strip()
    buy_zone = str(row.get("buy_zone") or "").strip()
    breakout = str(row.get("breakout_price") or "").strip()
    stop_loss = str(row.get("stop_loss") or "").strip()
    tp1 = str(row.get("take_profit_1") or "").strip()
    tp2 = str(row.get("take_profit_2") or "").strip()
    parts = []
    if reference:
        parts.append(f"参考价 {reference}")
    if buy_zone:
        parts.append(f"回踩区 {buy_zone}")
    if breakout:
        parts.append(f"突破确认 {breakout}")
    if stop_loss:
        parts.append(f"止损 {stop_loss}")
    if tp1 or tp2:
        parts.append(f"止盈 {tp1 or '-'} / {tp2 or '-'}")
    if not parts:
        return "执行价位：暂无有效最终价位，禁止按原始 agent 文本下单。"
    return "执行价位修正：" + "；".join(parts) + "。"


def compose_report_reason(row: dict[str, Any]) -> str:
    parts: list[str] = []
    if safe_float(row.get("dsa_score")):
        dsa = [f"初筛分 {safe_float(row.get('dsa_score')):.1f}"]
        if row.get("dsa_reason"):
            parsed = parse_dsa_reason_metrics(str(row.get("dsa_reason")))
            if parsed:
                dsa.extend(parsed)
        elif "volume_ratio" in row:
            dsa.append(format_volume_ratio(row.get("volume_ratio")))
        parts.append("初筛：" + "，".join(dsa) + "。")
    serenity = row.get("serenity_signal") or {}
    if serenity:
        parts.append(
            "Serenity："
            f"{serenity.get('tier') or '外部线索'}，{serenity.get('role') or '-'}；"
            f"瓶颈 {serenity.get('bottleneck') or '-'}；证据 {serenity.get('evidence_level') or '弱'}。"
        )
    external = row.get("external_signal") or {}
    if external:
        themes = "、".join(str(x) for x in (external.get("themes") or [])[:4])
        reason = strip_actionable_price_sentences(str(external.get("reason") or ""))
        parts.append(
            f"外部研究：{external.get('stance') or '-'}"
            f"{'，主题 ' + themes if themes else ''}"
            f"{'；' + complete_excerpt(reason, 220) if reason else ''}。"
        )
    dexter = row.get("dexter_signal") or {}
    if dexter:
        summary = strip_actionable_price_sentences(strip_internal_error_sentences(str(dexter.get("summary") or "")))
        parts.append(
            f"Dexter：{dexter.get('stance') or '-'}，置信度 {safe_float(dexter.get('confidence')):.2f}"
            f"{'；' + complete_excerpt(summary, 180) if summary else ''}。"
        )
    if row.get("tradingagents_score") or row.get("action"):
        ta_summary = strip_actionable_price_sentences(strip_internal_error_sentences(str(row.get("tradingagents_reason") or "")))
        parts.append(
            f"TradingAgents：{row.get('action') or '观察'}，"
            f"置信分 {safe_float(row.get('tradingagents_score')):.1f}，"
            f"风险 {row.get('risk') or '中'}"
            f"{'；摘要 ' + complete_excerpt(ta_summary, 260) if ta_summary else ''}。"
        )
    if row.get("uzi_score") or row.get("rating"):
        uzi_summary = strip_actionable_price_sentences(strip_internal_error_sentences(str(row.get("uzi_reason") or "")))
        parts.append(
            f"UZI：{safe_float(row.get('uzi_score')):.1f} 分，评级 {row.get('rating') or '-'}。"
            f"{' 投委摘要：' + complete_excerpt(uzi_summary, 260) if uzi_summary else ''}"
        )
    correction = row.get("execution_price_correction") or build_execution_price_correction(row)
    parts.append(correction)
    short = row.get("short_watch") or {}
    if short.get("eligible"):
        parts.append(
            "做空观察："
            f"{short.get('status') or '仅观察'}；"
            f"触发 {short.get('entry') or '-'}；"
            f"止损 {short.get('stop') or '-'}；"
            f"回补 {short.get('cover_1') or '-'} / {short.get('cover_2') or '-'}；"
            f"原因 {short.get('reason') or '-'}。"
        )
    if row.get("quality_note"):
        parts.append(f"未买原因：{row.get('quality_note')}。")
    return " ".join(complete_sentence(str(x)) for x in parts if str(x or "").strip())


def parse_dsa_reason_metrics(text: str) -> list[str]:
    out: list[str] = []
    ret = re.search(r"20日涨跌\s*(-?\d+(?:\.\d+)?)%", text)
    vol = re.search(r"(?:成交量比(?:仅)?|量比)\s*(\d+(?:\.\d*)?)", text)
    if ret:
        out.append(f"20日涨跌 {safe_float(ret.group(1)):.1f}%")
    if vol:
        out.append(format_volume_ratio(vol.group(1)))
    elif "量能数据缺失" in text or "量比不可用" in text:
        out.append(format_volume_ratio(None))
    return out


def complete_sentence(text: str) -> str:
    clean = str(text or "").strip()
    clean = re.sub(r"\s+；\s+", "；", clean)
    clean = re.sub(r"。{2,}", "。", clean)
    clean = re.sub(r"\s+", " ", clean)
    if not clean:
        return ""
    if clean.endswith(("。", "！", "？", ".", "!", "?")):
        return clean
    return clean.rstrip("；;，,、：:") + "。"


def deepcopy_json(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, list):
            return list(value)
        return value


def has_hard_no_buy_gate(gates: list[str]) -> bool:
    hard_terms = (
        "价格口径未通过",
        "价位一致性未通过",
        "ticker 映射未确认",
        "TradingAgents 完整版未完成",
        "TradingAgents 未给出买入",
        "UZI 投委未完成",
        "UZI 投委输出质量不足",
        "UZI 投委分低于 60",
        "UZI 投委结论偏谨慎或看空",
    )
    return any(gate in hard_terms or is_tradingagents_gate(str(gate)) or str(gate).startswith("20日涨幅") for gate in gates)


def is_tradingagents_gate(gate: str) -> bool:
    return str(gate).startswith(("TradingAgents ", "深度投研层 "))


def force_watch_only_advice(advice: dict[str, str], gates: list[str]) -> dict[str, str]:
    item = dict(advice)
    item["trade_advice"] = "只观察，不建议新开仓；等待硬性闸门解除后再评估"
    item["buy_advice"] = f"不买入；最终参考价位仍保留用于监控，原因：{'；'.join(gates[:4])}"
    item["position_advice"] = "空仓不买，已有仓位按止损止盈管理"
    return item


def should_use_backup_committee_score(uzi_item: dict[str, Any]) -> bool:
    if os.environ.get("PIPELINE_BACKUP_COMMITTEE_SCORE", "1") == "0":
        return False
    if uzi_has_hard_quality_failure(uzi_item):
        return False
    status = str(uzi_item.get("status") or "")
    flags = "；".join(str(x) for x in (uzi_item.get("quality_flags") or []))
    return status in {"degraded", "fallback"} or "重复低分" in flags or "数据" in flags


def uzi_has_invalid_committee_result(uzi_item: dict[str, Any]) -> bool:
    status = str(uzi_item.get("status") or "")
    flags = "；".join(str(x) for x in (uzi_item.get("quality_flags") or []))
    invalid_markers = (
        "UZI 未返回有效投委结果",
        "UZI 原始数据缺失",
        "UZI 财务维度不足",
        "UZI 基础行情缺失",
        "批量重复低分",
        "默认低分模板",
        "二次复核仍未通过",
    )
    return status == "fallback" or any(marker in flags for marker in invalid_markers)


def invalid_committee_score(uzi_item: dict[str, Any]) -> float:
    raw = safe_float(uzi_item.get("uzi_score"))
    return round(clamp(raw if raw > 0 else 45.0, 35.0, 50.0), 2)


def uzi_has_hard_quality_failure(uzi_item: dict[str, Any]) -> bool:
    flags = "；".join(str(x) for x in (uzi_item.get("quality_flags") or []))
    hard_markers = (
        "批量重复低分",
        "默认低分模板",
        "二次复核仍未通过",
    )
    return any(marker in flags for marker in hard_markers)


def committee_rating_label(uzi_item: dict[str, Any], score: float, source: str) -> str:
    if source == "UZI无效":
        return "观察 · UZI无效"
    if source == "备用投委评分":
        return f"{rating_from_score(score)} · 备用投委"
    return str(uzi_item.get("rating") or rating_from_score(score))


def backup_committee_score(dsa_item: dict[str, Any], ta_item: dict[str, Any], uzi_item: dict[str, Any]) -> float:
    dsa_score = safe_float(dsa_item.get("score") or ta_item.get("dsa_score"))
    ta_score = safe_float(ta_item.get("confidence")) * 100
    if ta_score <= 0:
        ta_score = safe_float(uzi_item.get("tradingagents_confidence")) * 100
    base = dsa_score * 0.42 + ta_score * 0.38 + 50.0 * 0.20
    ret_20 = safe_float(dsa_item.get("ret_20d"))
    if ret_20 >= 30:
        base -= 8.0
    elif ret_20 >= 22:
        base -= 4.0
    risk = normalize_risk(ta_item.get("risk", "中"))
    if risk == "高":
        base -= 5.0
    elif risk == "低":
        base += 1.5
    return round(clamp(base, 35.0, 82.0), 2)


def extract_serenity_signal(dsa_item: dict[str, Any]) -> dict[str, Any]:
    signal = dsa_item.get("external_signal") or {}
    if not signal.get("serenity_method"):
        return {}
    return {
        "tier": signal.get("serenity_tier", ""),
        "role": signal.get("serenity_role", ""),
        "bottleneck": signal.get("bottleneck", ""),
        "chain_tier": signal.get("chain_tier", ""),
        "evidence_level": signal.get("evidence_level", ""),
        "action_bias": signal.get("action_bias", ""),
        "kill_criteria": signal.get("kill_criteria", ""),
        "source": signal.get("source", ""),
    }


def update_watchlist(top10: list[dict[str, Any]], buy_rows: list[dict[str, Any]], env: dict[str, str]) -> dict[str, Any]:
    today = dt.date.today().isoformat()
    state_path = OUTPUTS / "watchlist_state.json"
    previous = read_json_file(state_path, {"positions": {}, "history": []})
    positions: dict[str, Any] = dict(previous.get("positions") or {})
    history: list[dict[str, Any]] = list(previous.get("history") or [])
    top_map = {row["symbol"]: row for row in top10}
    candidate_map = {
        str(row.get("symbol") or ""): row
        for row in read_json_if_exists(OUTPUTS / "candidates_top50.json", [])
        if isinstance(row, dict) and row.get("symbol")
    }
    buy_symbols = {row["symbol"] for row in buy_rows}
    entry_min_score = safe_float(env.get("WATCHLIST_ENTRY_MIN_SCORE"), 55.0)
    exit_absent_days = int(env.get("WATCHLIST_EXIT_ABSENT_DAYS", "3"))
    exit_min_score = safe_float(env.get("WATCHLIST_EXIT_MIN_SCORE"), 45.0)
    max_hold_days = int(env.get("WATCHLIST_MAX_HOLD_DAYS", "20"))
    events: list[dict[str, Any]] = []

    for rank, row in enumerate(top10, 1):
        row = prepare_report_row(row)
        symbol = row["symbol"]
        if safe_float(row.get("total_score")) < entry_min_score and symbol not in positions:
            events.append({
                "type": "未纳入观察池",
                "symbol": symbol,
                "name": row.get("name", ""),
                "reason": f"综合分低于进入线 {entry_min_score:.0f}",
            })
            continue
        pos = positions.get(symbol)
        if not pos:
            pos = {
                "symbol": symbol,
                "name": row.get("name", ""),
                "first_seen": today,
                "seen_count": 0,
                "status": "watching",
                "notes": [],
            }
            events.append({"type": "进入观察池", "symbol": symbol, "name": row.get("name", ""), "reason": watch_entry_reason(row)})
        pos.update({
            "name": row.get("name", pos.get("name", "")),
            "last_seen": today,
            "last_rank": rank,
            "last_score": row.get("total_score"),
            "last_raw_score": row.get("raw_total_score", row.get("total_score")),
            "last_risk_adjusted_score": row.get("risk_adjusted_score", row.get("total_score")),
            "last_rating": row.get("rating"),
            "last_action": row.get("action"),
            "last_risk": row.get("risk"),
            "last_close": row.get("reference_price"),
            "buy_zone": row.get("buy_zone"),
            "breakout_price": row.get("breakout_price"),
            "stop_loss": row.get("stop_loss"),
            "trade_bucket": row.get("trade_bucket"),
            "trade_bucket_label": row.get("trade_bucket_label"),
            "trade_trigger": row.get("trade_trigger"),
            "quality_note": row.get("quality_note", ""),
            "buy_eligible": bool(row.get("buy_eligible")),
            "seen_count": int(pos.get("seen_count") or 0) + 1,
            "absent_days": 0,
            "ret_20d": extract_ret_20d(row),
        })
        if symbol in buy_symbols:
            pos["status"] = "buy_ready"
            events.append({"type": "进入买入池", "symbol": symbol, "name": row.get("name", ""), "reason": row.get("buy_decision", "")})
        elif row.get("buy_eligible"):
            pos["status"] = "ready_watch"
        else:
            pos["status"] = "watching"
        positions[symbol] = pos

    for symbol, pos in list(positions.items()):
        if symbol in top_map:
            continue
        refresh_watch_position_from_market(pos, candidate_map.get(symbol))
        pos["absent_days"] = int(pos.get("absent_days") or 0) + 1
        age_days = days_between(str(pos.get("first_seen") or today), today)
        exit_reason = ""
        if int(pos.get("absent_days") or 0) >= exit_absent_days:
            exit_reason = f"连续 {pos.get('absent_days')} 天未进入 Top10"
        elif safe_float(pos.get("last_score")) < exit_min_score:
            exit_reason = f"最近综合分低于 {exit_min_score:.0f}"
        elif age_days >= max_hold_days and str(pos.get("status")) != "buy_ready":
            exit_reason = f"观察超过 {max_hold_days} 天仍未进入买入池"
        if exit_reason:
            events.append({"type": "移出观察池", "symbol": symbol, "name": pos.get("name", ""), "reason": exit_reason})
            positions.pop(symbol, None)
        else:
            positions[symbol] = pos

    state = {
        "updated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "positions": positions,
        "history": (history + [{"date": today, "top10": [row["symbol"] for row in top10], "buy": list(buy_symbols), "events": events}])[-60:],
    }
    write_json(state_path, state)
    watch_report = public_report_text(render_watchlist_markdown(state, events))
    alert_report = public_report_text(render_buy_alerts_markdown(buy_rows, events))
    write_text(OUTPUTS / "watchlist_today.md", watch_report)
    write_text(OUTPUTS / "buy_alerts.md", alert_report)
    return {"state": state, "events": events, "watch_report": watch_report, "alert_report": alert_report}


def read_json_file(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default


def refresh_watch_position_from_market(pos: dict[str, Any], market: dict[str, Any] | None) -> None:
    if not market:
        return
    close = first_positive(market.get("close"))
    if close <= 0:
        return
    risk = normalize_risk(pos.get("last_risk") or market.get("risk") or "medium")
    levels = build_price_levels({"symbol": pos.get("symbol"), **market}, risk)
    if not levels:
        return
    pos["last_close"] = levels.get("reference_price") or pos.get("last_close")
    pos["buy_zone"] = levels.get("buy_zone") or pos.get("buy_zone")
    pos["breakout_price"] = levels.get("breakout_price") or pos.get("breakout_price")
    pos["stop_loss"] = levels.get("stop_loss") or pos.get("stop_loss")
    pos["take_profit_1"] = levels.get("take_profit_1") or pos.get("take_profit_1")
    pos["take_profit_2"] = levels.get("take_profit_2") or pos.get("take_profit_2")
    pos["ret_20d"] = safe_float(market.get("ret_20d"), safe_float(pos.get("ret_20d")))


def watch_entry_reason(row: dict[str, Any]) -> str:
    if row.get("buy_eligible"):
        return "三层信号接近买入池，继续观察价位确认"
    return row.get("quality_note") or "进入 Top10，等待信号改善"


def format_score_line(row: dict[str, Any]) -> str:
    adjusted = row.get("risk_adjusted_score", row.get("total_score", "-"))
    raw = row.get("raw_total_score", adjusted)
    return f"风控后综合分：{adjusted}；原始综合分：{raw}"


def format_position_advice(row: dict[str, Any]) -> str:
    advice = str(row.get("position_advice") or "").strip()
    action = str(row.get("action") or "")
    if "0% 新仓" in advice or ("建议 0%" in advice and action == "持有"):
        return "空仓不买，已有仓位按止损止盈管理"
    return advice or "-"


def extract_ret_20d(row: dict[str, Any]) -> float:
    text = f"{row.get('dsa_reason', '')} {row.get('reason', '')}"
    match = re.search(r"20日涨跌\s*(-?\d+(?:\.\d+)?)%", text)
    return safe_float(match.group(1)) if match else 0.0


def days_between(start: str, end: str) -> int:
    try:
        return (dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days
    except Exception:
        return 0


def render_watchlist_markdown(watch: dict[str, Any], events: list[dict[str, Any]]) -> str:
    today = dt.date.today().isoformat()
    positions = list((watch.get("state", watch) or {}).get("positions", {}).values())
    positions.sort(key=lambda x: (str(x.get("status")) != "buy_ready", safe_float(x.get("last_rank"), 999)))
    lines = ["# 今日持续追踪池", "", f"生成日期：{today}", "", f"当前追踪：{len(positions)} 只", ""]
    if events:
        lines += ["## 今日变化", ""]
        for event in events[:20]:
            lines.append(f"- {event.get('type')}：{event.get('name') or event.get('symbol')} {event.get('symbol')}，{event.get('reason')}")
        lines.append("")
    lines += ["## 追踪明细", ""]
    for idx, pos in enumerate(positions[:30], 1):
        lines += [
            f"{idx}. {pos.get('name') or pos.get('symbol')} {pos.get('symbol')}",
            f"状态：{watch_status_label(str(pos.get('status')))}",
            f"连续/累计观察：{pos.get('seen_count', 0)} 次；未进 Top10：{pos.get('absent_days', 0)} 天",
            f"最近排名：{pos.get('last_rank', '-')}; 风控后综合分：{pos.get('last_risk_adjusted_score', pos.get('last_score', '-'))}; 原始综合分：{pos.get('last_raw_score', pos.get('last_score', '-'))}; 评级：{pos.get('last_rating', '-')}",
            f"执行分档：{pos.get('trade_bucket_label') or '-'}；触发：{pos.get('trade_trigger') or '-'}",
            f"价格：参考 {pos.get('last_close') or '-'}；买入区 {pos.get('buy_zone') or '-'}；突破 {pos.get('breakout_price') or '-'}；止损 {pos.get('stop_loss') or '-'}",
            f"未买原因：{pos.get('quality_note') or '已通过'}",
            "",
        ]
    return "\n".join(lines).strip() + "\n"


def render_buy_alerts_markdown(buy_rows: list[dict[str, Any]], events: list[dict[str, Any]]) -> str:
    today = dt.date.today().isoformat()
    lines = ["# 今日买入提醒", "", f"生成日期：{today}", ""]
    if not buy_rows:
        lines += ["今日没有新的严格买入提醒。", ""]
    for idx, row in enumerate(buy_rows, 1):
        lines += [
            f"{idx}. {row.get('name') or row.get('symbol')} {row.get('symbol')}",
            f"结论：{row.get('buy_decision')}",
            f"价格计划：{row.get('price_plan')}",
            f"风控闸门：{row.get('quality_note') or '通过'}",
            "",
        ]
    exits = [event for event in events if event.get("type") == "移出观察池"]
    if exits:
        lines += ["## 今日移出", ""]
        for event in exits[:20]:
            lines.append(f"- {event.get('name') or event.get('symbol')} {event.get('symbol')}：{event.get('reason')}")
    return "\n".join(lines).strip() + "\n"


def watch_status_label(status: str) -> str:
    return {
        "buy_ready": "已进入买入池",
        "ready_watch": "接近买入，等待价位/复核",
        "watching": "观察中",
    }.get(status, "观察中")


def select_buy_list(top10: list[dict[str, Any]], buy_n: int) -> list[dict[str, Any]]:
    qualified = [row for row in top10 if should_buy(row)]
    buy_rows = sorted(qualified, key=lambda x: (buy_rank_score(x), x["total_score"]), reverse=True)[:buy_n]
    prepared: list[dict[str, Any]] = []
    for idx, row in enumerate(buy_rows, 1):
        item = prepare_report_row(row)
        item["buy_rank"] = idx
        item["buy_decision"] = make_buy_decision(item)
        prepared.append(prepare_report_row(item))
    return prepared


def should_buy(row: dict[str, Any]) -> bool:
    score = safe_float(row.get("total_score"))
    action = str(row.get("action", ""))
    risk = str(row.get("risk", ""))
    if not row.get("buy_eligible"):
        return False
    if safe_float(row.get("uzi_score")) < 60:
        return False
    if str(row.get("ta_status")) != "full":
        return False
    if risk == "高":
        return score >= 78 and action == "买入"
    return score >= 70 and action == "买入"


def buy_rank_score(row: dict[str, Any]) -> float:
    risk_penalty = {"低": 0, "中": 4, "高": 12}.get(str(row.get("risk", "中")), 4)
    action_bonus = {"买入": 8, "持有": 4, "观察": 0, "卖出": -20}.get(str(row.get("action", "观察")), 0)
    return safe_float(row.get("total_score")) + action_bonus - risk_penalty


def make_buy_decision(row: dict[str, Any]) -> str:
    score = safe_float(row.get("total_score"))
    risk = str(row.get("risk", "中"))
    buy_zone = str(row.get("buy_zone") or "")
    breakout = str(row.get("breakout_price") or "")
    if not row.get("buy_eligible"):
        return f"不进入买入池：{row.get('quality_note') or '三层信号未一致'}"
    if score >= 75 and risk != "高":
        if buy_zone and breakout:
            return f"可分批买入：优先等回踩到 {buy_zone}，强势放量站上 {breakout} 可小仓跟进"
        return "可分批买入"
    if score >= 60:
        if buy_zone and breakout:
            return f"观察买入：回踩到 {buy_zone} 再考虑，突破确认价 {breakout}"
        return "观察买入，等回踩确认"
    return "仅列入候选，不主动买入"


def classify_trade_bucket(total: float, action: str, risk: str, gates: list[str], advice: dict[str, str]) -> dict[str, str]:
    buy_zone = advice.get("buy_zone", "")
    breakout = advice.get("breakout_price", "")
    stop_loss = advice.get("stop_loss", "")
    hard_blocks = [
        gate for gate in gates
        if gate in {
            "价格口径未通过",
            "价位一致性未通过",
            "ticker 映射未确认",
            "TradingAgents 未给出买入",
            "UZI 投委未完成",
            "UZI 投委输出质量不足",
            "UZI 投委分低于 60",
            "UZI 投委结论偏谨慎或看空",
        }
        or is_tradingagents_gate(str(gate))
        or gate.startswith("20日涨幅")
    ]
    if hard_blocks:
        return {
            "bucket": "D",
            "label": "D档 Watch Only",
            "trigger": f"只观察；硬性闸门未通过：{'；'.join(hard_blocks[:3])}。",
        }
    if not gates and total >= 75 and action == "买入" and risk != "高":
        trigger = f"现价附近可小仓；止损 {stop_loss}；若回踩 {buy_zone} 可加仓。" if buy_zone else "现价附近可小仓，严格执行止损。"
        return {"bucket": "A", "label": "A档 Buy Now", "trigger": trigger}
    if buy_zone and total >= 58 and len(hard_blocks) <= 1:
        return {
            "bucket": "B",
            "label": "B档 Buy on Pullback",
            "trigger": f"等待回踩到 {buy_zone} 且缩量企稳；止损 {stop_loss or '按最新支撑位'}。",
        }
    if breakout and total >= 58 and risk != "高":
        return {
            "bucket": "C",
            "label": "C档 Breakout Buy",
            "trigger": f"等待放量站上 {breakout}，次日不跌回突破位再考虑小仓。",
        }
    return {
        "bucket": "D",
        "label": "D档 Watch Only",
        "trigger": f"只观察；未满足条件：{'；'.join(gates[:3]) if gates else '信号强度不足'}。",
    }


def evaluate_buy_gates(dsa_item: dict[str, Any], ta_item: dict[str, Any], uzi_item: dict[str, Any]) -> list[str]:
    gates: list[str] = []
    ta_status = str(ta_item.get("ta_status") or "")
    if ta_status != "full":
        gates.append(tradingagents_gate_label(ta_item))
    uzi_status = str(uzi_item.get("status") or "")
    if uzi_status == "degraded":
        gates.append("UZI 投委输出质量不足")
    elif uzi_status != "ok":
        gates.append("UZI 投委未完成")
    uzi_score = safe_float(uzi_item.get("uzi_score"))
    if uzi_score < 60:
        gates.append("UZI 投委分低于 60")
    if uzi_is_negative(uzi_item):
        gates.append("UZI 投委结论偏谨慎或看空")
    ret_20 = safe_float(dsa_item.get("ret_20d"))
    if ret_20 >= 30:
        gates.append(f"20日涨幅 {ret_20:.1f}% 过热")
    if normalize_action(ta_item.get("action", "")) != "买入":
        gates.append("TradingAgents 未给出买入")
    signal = dsa_item.get("external_signal") or {}
    if signal.get("requires_verification"):
        gates.append("外部研究信号待一手验证")
    dexter = dsa_item.get("dexter_signal") or {}
    dexter_stance = str(dexter.get("stance") or "")
    if (
        env_flag_enabled("DEXTER_BUY_GATE", default=True)
        and any(term in dexter_stance for term in ("谨慎", "看空", "回避"))
        and safe_float(dexter.get("confidence")) >= 0.65
    ):
        gates.append("Dexter 美股辅助偏谨慎")
    return gates


def build_short_watch(
    row: dict[str, Any],
    dsa_item: dict[str, Any],
    ta_item: dict[str, Any],
    uzi_item: dict[str, Any],
    buy_gates: list[str],
) -> dict[str, Any]:
    """Separate bearish consensus from executable short ideas."""
    symbol = str(row.get("symbol") or dsa_item.get("symbol") or ta_item.get("symbol") or "")
    reference = first_positive(dsa_item.get("close"), parse_price_numbers(row.get("reference_price"))[:1][0] if parse_price_numbers(row.get("reference_price")) else 0)
    ma20 = safe_float(dsa_item.get("ma20"))
    ret_20 = safe_float(dsa_item.get("ret_20d"))
    risk = str(row.get("risk") or normalize_risk(ta_item.get("risk")))
    ta_action = normalize_action(ta_item.get("action"))
    uzi_negative = uzi_is_negative(uzi_item)
    dexter = dsa_item.get("dexter_signal") or {}
    dexter_negative = any(term in str(dexter.get("stance") or "") for term in ("谨慎", "看空", "回避")) and safe_float(dexter.get("confidence")) >= 0.60
    audit = row.get("pretrade_audit") or {}
    reasons: list[str] = []

    if not audit.get("passed", True):
        return {
            "eligible": False,
            "status": "不可做空",
            "reason": "先验一致性未通过，不能用于任何方向的交易",
        }
    if reference <= 0:
        return {"eligible": False, "status": "不可做空", "reason": "缺少有效参考价"}
    if uzi_negative:
        reasons.append("UZI 投委偏谨慎或看空")
    if ta_action == "卖出":
        reasons.append("TradingAgents 给出卖出")
    elif "TradingAgents 未给出买入" in buy_gates:
        reasons.append("TradingAgents 未确认多头")
    if dexter_negative:
        reasons.append("Dexter 辅助偏谨慎")
    if ret_20 >= 30:
        reasons.append(f"20日涨幅 {ret_20:.1f}% 过热，存在回撤风险")
    elif ret_20 <= -8:
        reasons.append(f"20日跌幅 {abs(ret_20):.1f}%，趋势偏弱")
    if ma20 > 0 and reference < ma20:
        reasons.append("现价低于20日均线")

    bearish_votes = 0
    bearish_votes += 1 if uzi_negative else 0
    bearish_votes += 1 if ta_action == "卖出" else 0
    bearish_votes += 1 if dexter_negative else 0
    bearish_votes += 1 if ret_20 <= -8 or (ret_20 >= 30 and ta_action != "买入") else 0
    if bearish_votes < 2:
        return {
            "eligible": False,
            "status": "仅多头回避",
            "reason": "看空证据不足；看空多数不会自动转换为做空",
        }

    short_levels = build_short_price_levels(symbol, reference, ma20, risk)
    return {
        "eligible": True,
        "status": "做空观察，不自动下单",
        "entry": short_levels.get("entry", ""),
        "stop": short_levels.get("stop", ""),
        "cover_1": short_levels.get("cover_1", ""),
        "cover_2": short_levels.get("cover_2", ""),
        "borrow_note": "美股需确认券源、借券费率和 Reg SHO 限制；不能确认时优先用看跌期权/价差替代，且必须小仓。",
        "reason": "；".join(dict.fromkeys(reasons)),
    }


def build_short_price_levels(symbol: str, reference: float, ma20: float, risk: str) -> dict[str, str]:
    if reference <= 0:
        return {}
    breakdown = reference * (0.982 if risk == "低" else 0.975 if risk == "中" else 0.965)
    if ma20 > 0:
        breakdown = min(breakdown, ma20 * 0.985)
    stop = reference * (1.035 if risk == "低" else 1.050 if risk == "中" else 1.070)
    cover_1 = reference * (0.925 if risk == "低" else 0.910 if risk == "中" else 0.890)
    cover_2 = reference * (0.860 if risk == "低" else 0.835 if risk == "中" else 0.795)
    return {
        "entry": format_price(symbol, breakdown),
        "stop": format_price(symbol, stop),
        "cover_1": format_price(symbol, cover_1),
        "cover_2": format_price(symbol, cover_2),
    }


def pretrade_consistency_audit(
    market_item: dict[str, Any],
    price_source: dict[str, Any] | None = None,
    ta_item: dict[str, Any] | None = None,
) -> dict[str, Any]:
    symbol = str(market_item.get("symbol") or (ta_item or {}).get("symbol") or "")
    reference = first_positive(market_item.get("close"), market_item.get("reference_price"))
    external = first_positive((price_source or {}).get("close"))
    reason_text = " ".join(str(x or "") for x in [
        market_item.get("reason"),
        market_item.get("dsa_reason"),
        (ta_item or {}).get("reason"),
        (market_item.get("external_signal") or {}).get("summary") if isinstance(market_item.get("external_signal"), dict) else "",
    ])
    checks: list[str] = []
    gates: list[str] = []

    if reference <= 0:
        checks.append("价格真实性：无有效参考价")
        gates.append("价格口径未通过")
    elif external > 0:
        ratio = max(reference, external) / max(min(reference, external), 0.01)
        if ratio >= 4.0:
            checks.append(f"价格真实性：报告价与行情源相差 {ratio:.1f} 倍")
            gates.append("价格口径未通过")
        else:
            checks.append("价格真实性：通过")
    else:
        checks.append("价格真实性：仅有单一行情源，需持续校验")

    level_values = []
    for key in ("buy_zone", "breakout_price", "stop_loss", "take_profit_1", "take_profit_2"):
        level_values.extend(parse_price_numbers(market_item.get(key)))
    if reference > 0 and level_values:
        level_ratios = [max(reference, value) / max(min(reference, value), 0.01) for value in level_values if value > 0]
        if any(r >= 4.0 for r in level_ratios):
            checks.append("价位一致性：交易价位与参考价不在同一数量级")
            gates.append("价位一致性未通过")
        else:
            checks.append("价位一致性：通过")
    elif level_values:
        checks.append("价位一致性：缺少有效参考价")
        gates.append("价位一致性未通过")
    else:
        checks.append("价位一致性：待生成交易价位")

    ticker_gate = ticker_mapping_gate(symbol, reason_text)
    if ticker_gate:
        checks.append(ticker_gate)
        gates.append("ticker 映射未确认")
    else:
        checks.append("ticker 映射：通过")

    gates = list(dict.fromkeys(gates))
    return {
        "passed": not gates,
        "gates": gates,
        "checks": checks,
        "reference_price": reference,
        "source_price": external,
    }


def parse_price_numbers(value: Any) -> list[float]:
    nums = [safe_float(x) for x in re.findall(r"\d+(?:\.\d+)?", str(value or "").replace(",", ""))]
    return [x for x in nums if x > 0]


def ticker_mapping_gate(symbol: str, text: str) -> str:
    ticker = normalize_dsa_symbol(symbol).upper()
    lowered = text.lower()
    if ticker == "GE" and any(term in text for term in ("电力链", "电力设备", "800V", "数据中心电力")):
        return "ticker 映射：GE 与 GEV/GEHC 需区分，电力链主题疑似应核验 GEV"
    if ticker in {"GEV", "GEHC"} and "GE " in text:
        return "ticker 映射：GE 拆分公司需核验"
    return ""


def tradingagents_gate_label(ta_item: dict[str, Any]) -> str:
    status = str(ta_item.get("ta_status") or "")
    note = str(ta_item.get("ta_note") or ta_item.get("reason") or "")
    if status == "quick":
        return "TradingAgents 仅快速研究，未进入完整版名额"
    if status == "quick_fallback":
        detail = translate_failure_note(note)
        return f"TradingAgents 完整版失败：{detail}"
    if status == "failed":
        detail = translate_failure_note(note)
        return f"TradingAgents 完整版未完成：{detail}"
    if status == "fallback":
        detail = translate_failure_note(note)
        return f"TradingAgents 不可用：{detail}"
    return "TradingAgents 完整版未完成"


def env_flag_enabled(name: str, *, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        try:
            value = load_runtime_env_value(name)
        except Exception:
            value = None
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def load_runtime_env_value(name: str) -> str | None:
    env: dict[str, str] = {}
    load_env_file(ROOT / "config.env", env)
    return env.get(name)


def uzi_is_negative(uzi_item: dict[str, Any]) -> bool:
    text = f"{uzi_item.get('rating', '')} {uzi_item.get('reason', '')}"
    negative_terms = ("看空", "谨慎", "回避", "卖出", "不建议", "偏弱", "风险较高")
    return any(term in text for term in negative_terms)


def normalize_action(action: Any) -> str:
    text = str(action or "").upper()
    if "BUY" in text:
        return "买入"
    if "SELL" in text:
        return "卖出"
    if "HOLD" in text:
        return "持有"
    if "WATCH" in text:
        return "观察"
    return "观察"


def normalize_risk(risk: Any) -> str:
    text = str(risk or "").lower()
    if "low" in text or "低" in text:
        return "低"
    if "high" in text or "高" in text:
        return "高"
    return "中"


def make_trade_advice(total: float, action: Any, risk: Any, market: dict[str, Any] | None = None) -> dict[str, str]:
    risk_cn = normalize_risk(risk)
    action_cn = normalize_action(action)
    if total >= 85:
        trade = "优先关注，等待盘中回踩或突破确认后分批买入"
        position = "建议仓位 20%-30%，高风险票降到 10%-15%"
    elif total >= 75:
        trade = "可以进入买入观察区，采用分批低吸"
        position = "建议仓位 10%-20%"
    elif total >= 60:
        trade = "暂不追高，观察回踩支撑和成交量确认"
        position = "建议仓位 0%-10%，只适合试仓"
    else:
        trade = "不建议新开仓，等待评分改善"
        position = "建议空仓或仅保留已有底仓"

    price_levels = build_price_levels(market or {}, risk_cn)
    buy = "放量站上 5/10 日均线，且次日不跌破前一日低点；若高开过多则等回踩。"
    sell = "跌破 20 日均线且放量、综合分跌破 55、或出现重大负面新闻时减仓/止损。"
    price_plan = ""
    if price_levels:
        buy = (
            f"回踩买入区 {price_levels['buy_zone']}；"
            f"若放量站上 {price_levels['breakout_price']}，可按突破确认价小仓跟进。"
        )
        sell = (
            f"跌破 {price_levels['stop_loss']} 严格止损；"
            f"第一止盈 {price_levels['take_profit_1']}，第二止盈 {price_levels['take_profit_2']}。"
        )
        price_plan = (
            f"参考价 {price_levels['reference_price']}；"
            f"回踩区 {price_levels['buy_zone']}；"
            f"突破确认 {price_levels['breakout_price']}；"
            f"止损 {price_levels['stop_loss']}；"
            f"止盈 {price_levels['take_profit_1']} / {price_levels['take_profit_2']}。"
        )
    if risk_cn == "高":
        if price_levels:
            sell = (
                f"高风险只做轻仓：跌破 {price_levels['stop_loss']} 立即止损；"
                f"冲到 {price_levels['take_profit_1']} 先减仓，{price_levels['take_profit_2']} 继续锁定收益。"
            )
        else:
            sell = "跌破 5 日均线或单日放量长阴即减仓；亏损达到 5%-7% 严格止损。"
    elif action_cn == "卖出":
        trade = "以风险控制为主，不建议买入，已有仓位逢反弹降低。"
    return {
        "trade_advice": trade,
        "buy_advice": buy,
        "sell_advice": sell,
        "position_advice": position,
        "price_plan": price_plan,
        "reference_price": price_levels.get("reference_price", "") if price_levels else "",
        "buy_zone": price_levels.get("buy_zone", "") if price_levels else "",
        "breakout_price": price_levels.get("breakout_price", "") if price_levels else "",
        "stop_loss": price_levels.get("stop_loss", "") if price_levels else "",
        "take_profit_1": price_levels.get("take_profit_1", "") if price_levels else "",
        "take_profit_2": price_levels.get("take_profit_2", "") if price_levels else "",
    }


def build_price_levels(market: dict[str, Any], risk_cn: str) -> dict[str, str]:
    close = safe_float(market.get("close"))
    if close <= 0:
        return {}
    symbol = str(market.get("symbol") or "")
    ma20 = safe_float(market.get("ma20"))
    ret_20 = safe_float(market.get("ret_20d"))
    vol_ratio = valid_volume_ratio(market.get("volume_ratio"))
    pullback = {"低": 0.025, "中": 0.035, "高": 0.050}.get(risk_cn, 0.035)
    if ret_20 >= 18:
        pullback += 0.020
    elif ret_20 >= 10:
        pullback += 0.010
    elif ret_20 <= -10:
        pullback -= 0.005
    if vol_ratio is not None and vol_ratio >= 1.8:
        pullback += 0.005
    pullback = clamp(pullback, 0.020, 0.085)

    buy_high = close * (1 - pullback * 0.45)
    buy_low = close * (1 - pullback * 1.35)
    if ma20 > 0:
        support_low = ma20 * 0.985
        support_high = ma20 * 1.015
        buy_low = min(buy_low, support_low)
        buy_high = max(min(buy_high, support_high), buy_low * 1.01)

    breakout = close * (1.018 if risk_cn == "低" else 1.025 if risk_cn == "中" else 1.035)
    stop_loss = buy_low * (0.965 if risk_cn == "低" else 0.945 if risk_cn == "中" else 0.925)
    take_profit_1 = close * (1.075 if risk_cn == "低" else 1.090 if risk_cn == "中" else 1.110)
    take_profit_2 = close * (1.140 if risk_cn == "低" else 1.165 if risk_cn == "中" else 1.205)
    return {
        "reference_price": format_price(symbol, close),
        "buy_zone": f"{format_price(symbol, buy_low)}-{format_price(symbol, buy_high)}",
        "breakout_price": format_price(symbol, breakout),
        "stop_loss": format_price(symbol, stop_loss),
        "take_profit_1": format_price(symbol, take_profit_1),
        "take_profit_2": format_price(symbol, take_profit_2),
    }


def format_price(symbol: str, value: float) -> str:
    prefix = "HK$" if symbol.upper().startswith("HK") or symbol.endswith(".HK") else "$"
    if value >= 100:
        text = f"{value:.2f}"
    elif value >= 10:
        text = f"{value:.2f}"
    else:
        text = f"{value:.3f}"
    return f"{prefix}{text}"


def build_combined_reason(dsa_item: dict[str, Any], ta_item: dict[str, Any], uzi_item: dict[str, Any]) -> str:
    parts = []
    dsa_bits = []
    if safe_float(dsa_item.get("ret_20d")) or "volume_ratio" in dsa_item:
        dsa_bits.append(f"20日涨跌 {safe_float(dsa_item.get('ret_20d')):.1f}%")
        dsa_bits.append(format_volume_ratio(dsa_item.get("volume_ratio")))
    if safe_float(dsa_item.get("score")):
        dsa_bits.append(f"初筛分 {safe_float(dsa_item.get('score')):.1f}")
    if dsa_bits:
        parts.append("初筛：" + "，".join(dsa_bits))
    signal = dsa_item.get("external_signal") or {}
    if signal:
        themes = "、".join(str(x) for x in (signal.get("themes") or [])[:4])
        signal_reason = strip_actionable_price_sentences(str(signal.get("reason", "")))
        parts.append(f"外部研究：{signal.get('stance', '')}，{themes}{'；' + complete_excerpt(signal_reason, 220) if signal_reason else ''}")
        if signal.get("serenity_method"):
            parts.append(
                "白毛/Serenity："
                f"{signal.get('serenity_tier', '外部线索')}，{signal.get('serenity_role', '')}；"
                f"瓶颈：{signal.get('bottleneck', '')}；证据：{signal.get('evidence_level', '弱')}；"
                f"执行：{signal.get('action_bias', '')}"
            )
    if ta_item:
        parts.append(
            "投研："
            f"TradingAgents {normalize_action(ta_item.get('action'))}，"
            f"置信度 {safe_float(ta_item.get('confidence')):.2f}，"
            f"风险 {normalize_risk(ta_item.get('risk'))}；原始投研中的执行价位不作为下单依据。"
        )
    dexter = dsa_item.get("dexter_signal") or {}
    if dexter:
        points = "、".join(str(x) for x in (dexter.get("key_points") or [])[:3])
        risks = "、".join(str(x) for x in (dexter.get("risks") or [])[:2])
        summary = strip_actionable_price_sentences(str(dexter.get("summary", "")))
        parts.append(
            f"Dexter美股辅助：{dexter.get('stance', '中性')}，{complete_excerpt(summary, 180)}"
            f"{'；要点：' + points if points else ''}{'；风险：' + risks if risks else ''}"
        )
    if uzi_item:
        parts.append(
            "投委："
            f"UZI {safe_float(uzi_item.get('uzi_score')):.1f} 分，"
            f"评级 {uzi_item.get('rating') or rating_from_score(safe_float(uzi_item.get('uzi_score')))}。"
        )
    flags = [str(x) for x in (uzi_item.get("quality_flags") or []) if str(x).strip()]
    if flags:
        parts.append(f"投委质量提示：{'；'.join(flags[:3])}")
    return "；".join(parts)


def sanitize_agent_execution_text(text: str) -> str:
    clean = normalize_volume_ratio_text(" ".join(str(text or "").split()))
    if not clean:
        return ""
    labels = (
        "Investment Thesis",
        "Time Horizon",
        "Price Target",
        "核心判断",
        "技术结论",
        "基本面",
        "情绪",
    )
    for label in labels:
        if label in clean:
            clean = clean.split(label, 1)[0]
    clean = strip_actionable_price_sentences(clean)
    clean = re.sub(r"\[\s*(?:；|,|，|、|-|–)*\s*", "", clean)
    clean = re.sub(r"(?:；|,|，|、|-|–)+\s*\]", "", clean)
    clean = re.sub(r"(?:；\s*(?:投委|投研|Dexter美股辅助|Vibe-Trading复核|外部研究|初筛)\s*)+$", "", clean)
    return clean.strip("；:： ,，、-–[]")


def strip_actionable_price_sentences(text: Any) -> str:
    clean = normalize_volume_ratio_text(" ".join(str(text or "").split()))
    if not clean:
        return ""
    action_terms = (
        "当前价", "现价", "价格", "价位", "买区", "买入区", "回踩", "突破", "止损", "止盈",
        "跌破", "站上", "追入", "入场", "退出", "减仓", "加仓", "目标价", "Price Target",
        "EMA", "SMA", "布林", "支撑", "阻力", "当前", "附近", "不追", "追高", "追",
        "区间", "企稳", "上方", "下方", "警戒线", "离场", "仓位", "首笔", "分批",
        "新增资金", "目标仓位", "连续站稳", "收复", "均线",
        "站稳", "提高至", "基准", "纪律线", "浮盈", "兑现", "降至", "收于",
        "第一纪律", "第二纪律", "止盈位", "止损位", "持仓", "新建仓", "建仓",
        "升至", "回落", "高位", "持有期", "战术调整", "时间 horizon", "time horizon", "估值参考",
    )
    sentences = re.split(r"(?<=[。！？；;，,])\s*", clean)
    kept: list[str] = []
    for sentence in sentences:
        s = sentence.strip()
        if not s:
            continue
        if any(term in s for term in action_terms):
            continue
        kept.append(s)
    return " ".join(kept).strip("；;，,、 ")


def strip_internal_error_sentences(text: Any) -> str:
    clean = " ".join(str(text or "").split())
    if not clean:
        return ""
    internal_terms = (
        "架构错误",
        "架构返回错误",
        "工具返回不可用错误",
        "不可用错误",
        "Traceback",
        "Exception",
        "internal error",
        "Internal error",
        "get_verified_market_snapshot",
    )
    sentences = re.split(r"(?<=[。！？；;，,])\s*", clean)
    kept: list[str] = []
    for sentence in sentences:
        s = sentence.strip()
        if not s:
            continue
        if any(term in s for term in internal_terms):
            continue
        kept.append(s)
    return " ".join(kept).strip("；;，,、 ")


def complete_excerpt(text: Any, limit: int = 520) -> str:
    clean = normalize_volume_ratio_text(" ".join(str(text or "").split()))
    if len(clean) <= limit:
        return clean
    window = clean[:limit]
    decimal_dot = re.search(r"\d\.$", window)
    sentence_marks = ("。", "；", ";", "！", "？", "\n") if decimal_dot else ("。", "；", ";", "！", "？", ".", "\n")
    cut = max(window.rfind(mark) for mark in sentence_marks)
    if cut >= max(80, int(limit * 0.45)):
        out = window[:cut + 1].strip()
    else:
        out = window.rstrip("；,，、:：-–[(（") + "…"
    out = re.sub(r"\s*\d+\.(?:…)?$", "…", out)
    if out.endswith(("；", ";", "，", ",", "、", "：", ":")):
        out = out.rstrip("；;，,、：:") + "。"
    return out


def humanize_report_chinese(text: Any) -> str:
    clean = str(text or "").replace("**", "").replace("`", "")
    replacements = (
        ("FINAL TRANSACTION PROPOSAL", "最终交易建议"),
        ("Executive Summary", "摘要"),
        ("Investment Thesis", "投资逻辑"),
        ("Time Horizon", "持有周期"),
        ("Strong Buy", "强烈看多"),
        ("Buy on Pullback", "回踩买入"),
        ("Breakout Buy", "突破买入"),
        ("Watch Only", "只观察"),
        ("Buy Now", "立即买入"),
        ("TradingAgents", "深度投研层"),
        ("tradingagents", "深度投研层"),
        ("Vibe-Trading", "量化风控复核"),
        ("vibe_trading", "量化风控复核"),
        ("Dexter", "辅助研究层"),
        ("dexter", "辅助研究层"),
        ("Serenity", "外部供应链线索"),
        ("dokobot", "外部简报"),
        ("OpenBB", "行情数据层"),
        ("Kronos", "趋势预测层"),
        ("UZI", "投委复核"),
        ("uzi", "投委复核"),
        ("final_report", "最终报告"),
        ("dsa", "初筛层"),
        ("Universe", "股票池"),
        ("Top10", "十大观察池"),
        ("Buy3", "三只买入候选"),
        ("Rating", "评级"),
        ("Overweight", "增持"),
        ("Underweight", "减持"),
        ("CapEx", "资本开支"),
        ("backlog", "在手订单"),
        ("ticker", "股票代码"),
    )
    for source, target in replacements:
        clean = clean.replace(source, target)
    word_replacements = {
        "BUY": "买入",
        "SELL": "卖出",
        "HOLD": "持有",
        "WATCH": "观察",
        "AI": "人工智能",
        "DSA": "初筛层",
        "Agent": "研究模块",
        "fallback": "降级",
        "degraded": "部分数据降级",
        "full": "完整",
        "unknown": "未知",
    }
    for source, target in word_replacements.items():
        clean = re.sub(rf"\b{re.escape(source)}\b", target, clean, flags=re.IGNORECASE)
    clean = re.sub(r"(初筛层|行情数据层|趋势预测层|辅助研究层|深度投研层|投委复核层?|量化风控复核层?|最终报告)\s+(?=[\u4e00-\u9fff])", r"\1", clean)
    clean = clean.replace("投委复核投委", "投委")
    clean = clean.replace("D档 只观察， 只观察", "D档 只观察")
    clean = clean.replace("D档 只观察；触发：只观察", "D档 只观察；触发：保持观察")
    clean = clean.replace("部分数据降级", "部分数据不足")
    clean = clean.replace("：降级", "：数据不足")
    return clean


def public_report_text(text: Any) -> str:
    return humanize_report_chinese(text)


def public_layer_name(value: Any) -> str:
    text = str(value or "").strip()
    mapping = {
        "dsa": "初筛层",
        "daily_stock_analysis": "初筛层",
        "openbb": "行情数据层",
        "kronos": "趋势预测层",
        "dexter": "辅助研究层",
        "tradingagents": "深度投研层",
        "uzi": "投委复核层",
        "vibe_trading": "量化风控复核层",
        "final_report": "最终报告",
        "telegram": "通知发送",
        "telegram_done": "通知已发送",
        "telegram_failed": "通知发送失败",
        "formal": "正式模式",
        "smoke": "连通性测试",
        "diagnostic": "诊断模式",
        "ok": "通过",
        "failed": "失败",
        "in_progress": "进行中",
    }
    return mapping.get(text, text)


def public_layer_list(values: Any) -> str:
    names = [public_layer_name(item) for item in (values or []) if str(item).strip()]
    return "、".join(names) or "无"


def render_bucket_section(top10: list[dict[str, Any]]) -> list[str]:
    lines = ["## 今日执行分档", ""]
    buckets = (
        ("A", "A档 Buy Now，立刻可以买"),
        ("B", "B档 Buy on Pullback，等回踩到价位"),
        ("C", "C档 Breakout Buy，突破确认买"),
        ("D", "D档 Watch Only，只观察"),
    )
    for bucket, title in buckets:
        rows = [row for row in top10 if row.get("trade_bucket") == bucket]
        lines.append(f"### {title}")
        if rows:
            for row in rows[:10]:
                lines.append(
                    f"- {row.get('name') or row['symbol']} {row['symbol']}："
                    f"{row.get('trade_trigger') or row.get('buy_advice') or ''}"
                )
        else:
            lines.append("- 无")
        lines.append("")
    return lines


def render_short_watch_section(top10: list[dict[str, Any]]) -> list[str]:
    rows = [row for row in top10 if (row.get("short_watch") or {}).get("eligible")]
    lines = ["## 做空观察池", ""]
    lines.append("口径：看空多数只会阻断买入，不会自动等于做空。只有先验一致性通过，并且至少两类空头证据同向，才进入做空观察池；真实执行还要确认券源、借券费率、Reg SHO/熔断限制，或改用看跌期权/价差。")
    lines.append("")
    if not rows:
        lines += ["今日没有满足独立做空观察条件的标的。", ""]
        return lines
    for row in rows[:10]:
        short = row.get("short_watch") or {}
        lines += [
            f"- {row.get('name') or row['symbol']} {row['symbol']}：{short.get('status') or '做空观察'}",
            f"  触发：跌破/反弹失败参考 {short.get('entry') or '-'}；止损：{short.get('stop') or '-'}；回补：{short.get('cover_1') or '-'} / {short.get('cover_2') or '-'}",
            f"  原因：{short.get('reason') or '-'}",
            f"  执行限制：{short.get('borrow_note') or '需先确认做空可执行性'}",
        ]
    lines.append("")
    return lines


def render_serenity_section(top10: list[dict[str, Any]]) -> list[str]:
    rows = [row for row in top10 if row.get("serenity_signal")]
    lines = ["## 外部供应链线索参考", ""]
    if not rows:
        lines += [
            "今日十大观察池未命中外部供应链瓶颈线索。",
            "外部简报只用于提示待验证线索，不参与正式评分，也不会直接触发买入。",
            "",
        ]
        return lines

    tier_order = {"第一优先级": 0, "第二优先级": 1, "第三优先级": 2, "外部线索": 3, "警惕名单": 4}
    rows.sort(key=lambda row: (tier_order.get(str(row["serenity_signal"].get("tier")), 9), -safe_float(row.get("total_score"))))
    lines += [
        "口径：外部简报只提示可能的供应链瓶颈和待验证线索；不参与正式评分，不决定十大观察池，不触发三只买入候选。买入仍需初筛层、深度投研层、投委复核、量化风控复核和价位确认。",
        "",
    ]
    for row in rows[:10]:
        sig = row.get("serenity_signal") or {}
        lines += [
            f"- {row.get('name') or row['symbol']} {row['symbol']}：{sig.get('tier') or '外部线索'} / {sig.get('role') or '-'}",
            f"  瓶颈环节：{sig.get('bottleneck') or '-'}；链条层级：{sig.get('chain_tier') or '-'}；证据等级：{sig.get('evidence_level') or '弱'}",
            f"  执行偏向：{sig.get('action_bias') or '-'}",
            f"  退出/降权条件：{sig.get('kill_criteria') or '-'}",
        ]
    lines.append("")
    return lines


def render_serenity_bottleneck_section(rows: list[dict[str, Any]] | None = None) -> list[str]:
    if rows is None:
        rows = read_json_if_exists(OUTPUTS / "serenity_bottleneck_watchlist.json", [])
    lines = ["## 供应链瓶颈观察清单（单列）", ""]
    lines += [
        "口径：这是外部线索清单，只单列候选，不混入十大观察池/三只买入候选评分。它寻找 BOM/供应链中成本小、替代难、扩产慢、集中度高的瓶颈环节；美股/港股重点看非主流共识、供应紧、客户依赖度高、扩产周期长。",
        "",
    ]
    if not rows:
        lines += ["今日没有独立瓶颈候选。", ""]
        return lines
    for row in rows[:8]:
        lines += public_bottleneck_bullet(row)
    lines.append("")
    return lines


def apply_flow_status_to_report(
    top10: list[dict[str, object]],
    buy3: list[dict[str, object]],
    status: dict[str, object],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if status.get("can_publish_buy_report") is True:
        return top10, buy3
    reasons = "；".join(str(reason) for reason in (status.get("blocking_reasons") or []))
    note = f"正式流程未完成，不生成可执行买入结论：{complete_excerpt(reasons, 260)}"
    gated_top10: list[dict[str, object]] = []
    for row in top10:
        item = dict(row)
        item["buy_eligible"] = False
        item["trade_bucket"] = "D"
        item["trade_bucket_label"] = "D档 Watch Only，只观察"
        item["trade_trigger"] = note
        item["quality_note"] = note
        item["buy_decision"] = "只观察"
        gated_top10.append(item)
    return gated_top10, []


def render_flow_status_section(status: dict[str, object] | None) -> list[str]:
    if not status:
        return []
    completed = public_layer_list(status.get("completed_layers") or [])
    missing = public_layer_list(status.get("missing_layers") or [])
    reasons = [
        humanize_report_chinese(reason)
        for reason in (status.get("blocking_reasons") or [])
        if str(reason).strip()
    ]
    lines = [
        "## 正式流程状态",
        "",
        f"正式流程状态：{public_layer_name(status.get('overall_status'))}",
        f"运行模式：{public_layer_name(status.get('run_mode'))}",
        f"已完成层：{completed}",
        f"未完成层：{missing}",
        f"可发布买入日报：{'是' if status.get('can_publish_buy_report') else '否'}",
    ]
    if reasons:
        lines.append("")
        lines.append("阻断原因：")
        for reason in reasons[:12]:
            lines.append(f"- {reason}")
    lines.append("")
    return lines


def render_final_markdown(
    top10: list[dict[str, Any]],
    buy3: list[dict[str, Any]],
    counts: dict[str, int],
    watch: dict[str, Any] | None = None,
    flow_status: dict[str, Any] | None = None,
) -> str:
    top10 = [prepare_report_row(row) for row in top10]
    buy3 = [prepare_report_row(row) for row in buy3]
    today = dt.date.today().isoformat()
    title = "# 今日 AI 选股报告"
    if flow_status and flow_status.get("can_publish_buy_report") is not True:
        title = "# 今日 AI 选股报告（正式流程未完成）"
    lines = [
        title,
        "",
        f"生成日期：{today}",
        "",
    ]
    lines += render_flow_status_section(flow_status)
    lines += [
        "## 漏斗",
        "",
        f"股票池：{counts.get('universe', 0)}",
        f"行情数据层：{counts.get('openbb', 0)}",
        f"趋势预测层：{counts.get('kronos', 0)}",
        f"辅助研究层：{counts.get('dexter', 0)}",
        f"量化风控复核：{counts.get('vibe_trading', 0)}",
        f"供应链瓶颈观察清单（单列）：{counts.get('serenity_bottleneck', 0)}",
        f"初筛：{counts.get('screen', 0)}",
        f"深度研究：{counts.get('research', 0)}",
        f"投资委员会：{counts.get('committee', 0)}",
        f"买入：{len(buy3)}",
        "",
    ]
    lines += render_serenity_section(top10)
    lines += render_serenity_bottleneck_section()
    lines += render_pretrade_audit_section(top10)
    lines += [
        "## 执行口径",
        "",
        "所有可执行价位只以本报告的“价格计划 / 买入条件 / 卖出条件”为准；深度投研、量化风控复核、外部研究原始正文中的旧价位会被剥离，不作为下单依据。",
        "",
    ]
    lines += render_bucket_section(top10)
    lines += render_short_watch_section(top10)
    lines += [f"## 今日买入 {len(buy3)} 只", ""]
    if not buy3:
        lines += [
            "今日无满足严格买入条件的股票。",
            "规则：深度投研完成、投委复核分数不低于 60、投委结论不偏谨慎或看空、20日涨幅不过热、三层信号同向。",
            "",
        ]
    for idx, row in enumerate(buy3, 1):
        lines += [
            f"{idx}. {row.get('name') or row['symbol']} {row['symbol']}",
            f"买入结论：{row.get('buy_decision')}",
            format_score_line(row),
            f"评级：{row['rating']}",
            f"买卖建议：{row.get('trade_advice')}",
            f"价格计划：{row.get('price_plan') or '暂无有效参考价，等下一轮日线数据补齐'}",
            f"买入条件：{row.get('buy_advice')}",
            f"卖出条件：{row.get('sell_advice')}",
            f"仓位建议：{format_position_advice(row)}",
            f"风控闸门：{row.get('quality_note') or '通过'}",
            f"选择理由：{complete_excerpt(row.get('reason', ''), 720)}",
            "",
        ]

    if watch:
        state = watch.get("state") or {}
        positions = state.get("positions") or {}
        events = watch.get("events") or []
        lines += ["## 持续追踪池", "", f"当前追踪：{len(positions)} 只", ""]
        if events:
            for event in events[:8]:
                lines.append(f"- {event.get('type')}：{event.get('name') or event.get('symbol')} {event.get('symbol')}，{event.get('reason')}")
            lines.append("")
        else:
            lines += ["今日无新增或移出。", ""]

    lines += ["## 投资委员会 Top10", ""]
    for idx, row in enumerate(top10, 1):
        lines += [
            f"{idx}. {row.get('name') or row['symbol']} {row['symbol']}",
            format_score_line(row),
            f"评级：{row['rating']}",
            f"操作：{row.get('action') or '观察'}",
            f"执行分档：{row.get('trade_bucket_label') or 'D档 Watch Only'}",
            f"触发条件：{row.get('trade_trigger') or '-'}",
            f"买卖建议：{row.get('trade_advice')}",
            f"价格计划：{row.get('price_plan') or '暂无有效参考价，等下一轮日线数据补齐'}",
            f"买入条件：{row.get('buy_advice')}",
            f"卖出条件：{row.get('sell_advice')}",
            f"仓位建议：{format_position_advice(row)}",
            f"风险：{row.get('risk') or '中'}",
            f"先验一致性：{format_pretrade_audit_line(row)}",
            f"买入资格：{'通过' if row.get('buy_eligible') else '未通过'}",
            f"风控闸门：{row.get('quality_note') or '通过'}",
            f"外部线索：{format_serenity_line(row)}",
            f"投委复核质量：{format_uzi_quality_line(row)}",
            f"做空观察：{format_short_watch_line(row)}",
            f"分解：初筛层 {row['dsa_score']} ×30% / 深度投研层 {row['tradingagents_score']} ×40% / {format_committee_score_line(row)} ×30%",
            f"选择理由：{complete_excerpt(row.get('reason', ''), 720)}",
            "",
        ]
    return "\n".join(lines).strip() + "\n"


def render_pretrade_audit_section(top10: list[dict[str, Any]]) -> list[str]:
    failed = [row for row in top10 if not (row.get("pretrade_audit") or {}).get("passed")]
    lines = ["## 先验一致性审计", ""]
    lines.append("口径：先查价格真实性、价位一致性和 ticker 映射；这些通过后，模型分数和交易建议才有下单意义。")
    lines.append("")
    if not failed:
        lines += ["今日 Top10 价格口径、价位一致性、ticker 映射均通过先验审计。", ""]
        return lines
    for row in failed:
        audit = row.get("pretrade_audit") or {}
        gates = "；".join(audit.get("gates") or ["未通过"])
        checks = "；".join(audit.get("checks") or [])
        lines.append(f"- {row.get('name') or row['symbol']} {row['symbol']}：{gates}。{checks}")
    lines.append("")
    return lines


def format_pretrade_audit_line(row: dict[str, Any]) -> str:
    audit = row.get("pretrade_audit") or {}
    if audit.get("passed"):
        return "通过"
    gates = "；".join(audit.get("gates") or [])
    return gates or "未通过"


def format_serenity_line(row: dict[str, Any]) -> str:
    sig = row.get("serenity_signal") or {}
    if not sig:
        return "未命中外部供应链瓶颈线索"
    return (
        f"{sig.get('tier') or '外部线索'} / {sig.get('role') or '-'}；"
        f"瓶颈 {sig.get('bottleneck') or '-'}；证据 {sig.get('evidence_level') or '弱'}；"
        f"执行 {sig.get('action_bias') or '-'}"
    )


def format_uzi_quality_line(row: dict[str, Any]) -> str:
    flags = [str(x) for x in (row.get("uzi_quality_flags") or []) if str(x).strip()]
    if not flags:
        return "通过"
    return "；".join(flags[:4])


def format_short_watch_line(row: dict[str, Any]) -> str:
    short = row.get("short_watch") or {}
    if not short.get("eligible"):
        return short.get("reason") or "未触发独立做空观察条件"
    return (
        f"{short.get('status') or '做空观察'}；"
        f"触发 {short.get('entry') or '-'}；止损 {short.get('stop') or '-'}；"
        f"回补 {short.get('cover_1') or '-'} / {short.get('cover_2') or '-'}"
    )


def format_committee_score_line(row: dict[str, Any]) -> str:
    source = str(row.get("committee_score_source") or "UZI-Skill")
    score = row.get("uzi_score")
    raw = row.get("raw_uzi_score")
    if source == "备用投委评分":
        return f"备用投委复核 {score}（原始分 {raw}）"
    return f"投委复核 {score}"


def telegram_text_for_status(markdown: str, status: dict[str, object]) -> str:
    if status.get("can_publish_buy_report") is True:
        return markdown
    reasons = [str(reason) for reason in (status.get("blocking_reasons") or []) if str(reason).strip()]
    evidence = [str(path) for path in (status.get("evidence_files") or []) if str(path).strip()]
    lines = [
        "正式流程未完成，今日不发布可执行买入日报。",
        f"运行模式：{status.get('run_mode')}",
        f"整体状态：{status.get('overall_status')}",
        "",
        "阻断原因：",
    ]
    lines.extend(f"- {reason}" for reason in reasons[:10])
    if evidence:
        lines += ["", "证据文件："]
        lines.extend(f"- {path}" for path in evidence[:10])
    return "\n".join(lines)


def telegram_send(env: dict[str, str], text: str) -> None:
    token = env.get("TELEGRAM_BOT_TOKEN")
    chat_id = telegram_chat_id(env)
    if not token or not chat_id:
        raise RuntimeError("Telegram not configured: TELEGRAM_BOT_TOKEN and chat id are required")
    chunks = split_telegram_text(text, max_chars=3600)
    for idx, chunk in enumerate(chunks, 1):
        prefix = f"（{idx}/{len(chunks)}）\n" if len(chunks) > 1 else ""
        payload: dict[str, Any] = {"chat_id": chat_id, "text": prefix + chunk, "disable_web_page_preview": True}
        if env.get("TELEGRAM_MESSAGE_THREAD_ID"):
            payload["message_thread_id"] = env["TELEGRAM_MESSAGE_THREAD_ID"]
        api_base = env.get("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/")
        url = f"{api_base}/bot{token}/sendMessage"
        body = telegram_post(url, payload, env)
        result = json.loads(body)
        if not result.get("ok"):
            detail = str(result.get("description") or body)
            raise RuntimeError(f"Telegram send failed: {detail}")


def telegram_chat_id(env: dict[str, str]) -> str:
    if env.get("TELEGRAM_CHAT_ID"):
        return str(env["TELEGRAM_CHAT_ID"]).strip()
    if env.get("TELEGRAM_HOME_CHANNEL"):
        return str(env["TELEGRAM_HOME_CHANNEL"]).strip()
    allowed = str(env.get("TELEGRAM_ALLOWED_USERS", "")).strip()
    return allowed.split(",", 1)[0].strip() if allowed else ""


def telegram_post(url: str, payload: dict[str, Any], env: dict[str, str]) -> str:
    proxy = env.get("TELEGRAM_PROXY") or env.get("HTTPS_PROXY") or env.get("https_proxy")
    if proxy:
        try:
            import requests  # type: ignore

            response = requests.post(
                url,
                data=payload,
                proxies={"http": proxy, "https": proxy},
                timeout=int(env.get("TELEGRAM_SEND_TIMEOUT", "75")),
            )
            return response.text
        except Exception as exc:
            if "SOCKS" not in str(exc) and "Missing dependencies" not in str(exc):
                raise
            return telegram_post_with_curl(url, payload, proxy, env)
    req = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(payload).encode("utf-8"),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        return response.read().decode("utf-8", errors="replace")


def telegram_post_with_curl(url: str, payload: dict[str, Any], proxy: str, env: dict[str, str]) -> str:
    parsed = urllib.parse.urlparse(proxy)
    proxy_arg = ""
    if parsed.scheme.startswith("socks"):
        proxy_arg = f"{parsed.hostname}:{parsed.port or 1080}"
        proxy_flag = "--socks5-hostname"
    else:
        proxy_arg = proxy
        proxy_flag = "--proxy"
    cmd = ["curl", "-sS", "--max-time", str(int(env.get("TELEGRAM_SEND_TIMEOUT", "75"))), proxy_flag, proxy_arg]
    for key, value in payload.items():
        cmd.extend(["--data-urlencode", f"{key}={value}"])
    cmd.append(url)
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=int(env.get("TELEGRAM_SEND_TIMEOUT", "75")) + 5)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"curl rc={proc.returncode}")
    return proc.stdout


def split_telegram_text(text: str, max_chars: int = 3600) -> list[str]:
    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current = ""
    for para in paragraphs:
        candidate = para if not current else current + "\n\n" + para
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = para
        while len(current) > max_chars:
            chunks.append(current[:max_chars])
            current = current[max_chars:]
    if current:
        chunks.append(current)
    return chunks or [text]


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(text, encoding="utf-8")
    except PermissionError:
        alt = path.with_name(f"{path.stem}_{os.getpid()}_{int(time.time())}{path.suffix}")
        alt.write_text(text, encoding="utf-8")


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


RECOVERABLE_TRADINGAGENTS_OUTPUT_MARKERS = (
    "BrokenPipeError",
    "服务器连接失败",
    "接收数据异常",
)


def recoverable_tradingagents_output_note(text: str) -> str:
    body = str(text or "")
    for marker in RECOVERABLE_TRADINGAGENTS_OUTPUT_MARKERS:
        if marker in body:
            return marker
    return ""


def write_stage_status(
    *,
    run_mode: str,
    stage: str,
    overall_status: str,
    blocking_reasons: list[str] | None = None,
    can_publish_buy_report: bool = False,
    **extra_fields: Any
) -> dict[str, Any]:
    """Write pipeline stage status to outputs/pipeline_status.json"""
    import time
    status: dict[str, Any] = {
        "run_mode": run_mode,
        "overall_status": overall_status,
        "stage": stage,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "blocking_reasons": blocking_reasons or [],
        "can_publish_buy_report": can_publish_buy_report,
    }
    status.update(extra_fields)
    status["public_stage"] = public_layer_name(stage)
    status["public_overall_status"] = public_layer_name(overall_status)
    status["public_blocking_reasons"] = [
        public_report_text(reason)
        for reason in (blocking_reasons or [])
        if str(reason).strip()
    ]
    if "completed_layers" in status:
        status["public_completed_layers"] = public_layer_list(status.get("completed_layers") or [])
    if "missing_layers" in status:
        status["public_missing_layers"] = public_layer_list(status.get("missing_layers") or [])

    OUTPUTS.mkdir(parents=True, exist_ok=True)
    status_file = OUTPUTS / "pipeline_status.json"
    write_json(status_file, status)
    return status


def build_failed_flow_status(
    *,
    run_mode: str,
    stage: str,
    exc: Exception,
    **extra_fields: Any
) -> dict[str, Any]:
    """Build failure status dict from exception"""
    import time
    error_info: dict[str, Any] = {
        "type": type(exc).__name__,
        "message": str(exc)[:300],
    }
    status: dict[str, Any] = {
        "run_mode": run_mode,
        "stage": stage,
        "overall_status": "failed",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "error": error_info,
        "can_publish_buy_report": False,
    }
    status.update(extra_fields)
    return status


def run_tradingagents_full_one(
    item: dict[str, Any],
    *,
    trading_dir: Path,
    python_bin: str = "python3",
    env: dict[str, str] | None = None,
    timeout: int = 1800
) -> dict[str, Any]:
    """Run real TradingAgents full analysis for a single symbol.

    Calls the genuine TRADINGAGENTS_SNIPPET via run() (synchronous, with
    per-stock timeout). In formal flow, incomplete single-symbol runs return
    explicit failed rows so the stage can block publication.
    """
    import time
    if env is None:
        env = dict(os.environ)

    symbol = to_tradingagents_symbol(item["symbol"])
    run_env = dict(env)
    run_env["PIPELINE_TA_TICKER"] = symbol
    run_env["PIPELINE_TA_DATE"] = env.get("PIPELINE_TA_DATE") or dt.date.today().isoformat()

    print(f"[TA-ONE] {symbol}: Starting (timeout={timeout}s)", flush=True)
    start = time.time()

    try:
        rc, text = run([python_bin, "-c", TRADINGAGENTS_SNIPPET], trading_dir, run_env, timeout=timeout)
        elapsed = time.time() - start
        print(f"[TA-ONE] {symbol}: Completed in {elapsed:.1f}s", flush=True)
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        print(f"[TA-ONE] {symbol}: Timeout after {elapsed:.1f}s (limit={timeout}s)", flush=True)
        return failed_tradingagents_result(item, f"timeout after {timeout}s", "timeout")
    except (BrokenPipeError, OSError, subprocess.CalledProcessError) as exc:
        elapsed = time.time() - start
        print(f"[TA-ONE] {symbol}: Error after {elapsed:.1f}s: {type(exc).__name__}", flush=True)
        note = recoverable_tradingagents_output_note(str(exc)) or type(exc).__name__
        return failed_tradingagents_result(item, note, type(exc).__name__)

    recoverable_note = recoverable_tradingagents_output_note(text)
    if recoverable_note:
        print(f"[TA-ONE] {symbol}: Recoverable error: {recoverable_note}", flush=True)
        return failed_tradingagents_result(item, recoverable_note, "recoverable_output")
    write_text(WORK / f"tradingagents_{safe_name(symbol)}.log", text)
    parsed = parse_last_json(text)
    if rc != 0 or not parsed:
        fail_note = summarize_failure(text, rc)
        print(f"[TA-ONE] {symbol}: Parse failed: {fail_note}", flush=True)
        return failed_tradingagents_result(item, fail_note, "invalid_output")
    print(f"[TA-ONE] {symbol}: Success", flush=True)
    return normalize_trading_result(item, parsed, text)


def finalize_tradingagents_stage(rows: list[dict[str, Any]], ta_meta: dict[str, Any]) -> list[dict[str, Any]]:
    """Finalize TA stage: sort by score and write output files"""
    # Sort by score descending
    ranked_rows = sorted(rows, key=lambda x: x.get("score", 0), reverse=True)

    # Ensure outputs directory exists
    OUTPUTS.mkdir(parents=True, exist_ok=True)

    # Write full diagnostic version
    full_file = OUTPUTS / "tradingagents_full_top20.json"
    write_json(full_file, ranked_rows[:20])

    # Write production version
    prod_file = OUTPUTS / "tradingagents_top20.json"
    write_json(prod_file, ranked_rows[:20])

    return ranked_rows


def build_flow_status(
    run_mode: str,
    stage: str | None = None,
    overall_status: str | None = None,
    ta_stage_meta: dict[str, Any] | None = None,
    **extra_fields: Any
) -> dict[str, Any]:
    """Build pipeline flow status.

    Supports both:
    1. Stage-oriented status writer call shape
    2. Existing report-oriented flow-status call shape used by main()
    """
    if "env" in extra_fields:
        env = extra_fields["env"]
        counts = extra_fields["counts"]
        trading = extra_fields["trading"]
        uzi = extra_fields["uzi"]
        vibe_review = extra_fields["vibe_review"]
        final_report_written = extra_fields["final_report_written"]
        telegram_enabled = extra_fields["telegram_enabled"]
        telegram_sent = extra_fields["telegram_sent"]
        ta_stage_meta = ta_stage_meta or extra_fields.get("ta_stage_meta")

        completed_layers: list[str] = []
        missing_layers: list[str] = []
        blocking_reasons: list[str] = []
        evidence_files = [
            "outputs/candidates_top50.json",
            "outputs/tradingagents_top20.json",
            "outputs/uzi_top10.json",
            "outputs/final_top10.md",
            "outputs/final_top10.json",
        ]

        if run_mode != "formal":
            return {
                "run_mode": run_mode,
                "overall_status": "ok",
                "completed_layers": ["smoke" if run_mode == "smoke" else "diagnostic"],
                "missing_layers": [],
                "blocking_reasons": [f"{run_mode} 模式只证明连线或单层诊断，不代表正式日报完成"],
                "evidence_files": evidence_files,
                "can_publish_buy_report": False,
                "telegram_enabled": telegram_enabled,
                "telegram_sent": telegram_sent,
            }

        if env.get("PIPELINE_SKIP_TRADINGAGENTS") == "1":
            missing_layers.append("tradingagents")
            blocking_reasons.append("正式日报禁止 PIPELINE_SKIP_TRADINGAGENTS=1")
        if env.get("PIPELINE_SKIP_UZI") == "1":
            missing_layers.append("uzi")
            blocking_reasons.append("正式日报禁止 PIPELINE_SKIP_UZI=1")
        if env.get("PIPELINE_TRADINGAGENTS_ALLOW_INCOMPLETE_FINAL") == "1":
            missing_layers.append("tradingagents")
            blocking_reasons.append("正式日报禁止 PIPELINE_TRADINGAGENTS_ALLOW_INCOMPLETE_FINAL=1")

        if counts.get("screen", 0) > 0:
            completed_layers.append("dsa")
        else:
            missing_layers.append("dsa")
            blocking_reasons.append("DSA 初筛未产出候选")

        bad_ta = [str(row.get("symbol") or "?") for row in trading if not status_is_full_tradingagents(row)]
        if trading and not bad_ta:
            completed_layers.append("tradingagents")
        else:
            missing_layers.append("tradingagents")
            detail = "、".join(bad_ta[:8]) if bad_ta else "无深度投研层完整版结果"
            blocking_reasons.append(f"深度投研层未正式完成：{detail}")

        if ta_stage_meta:
            ta_stage_status = str(ta_stage_meta.get("ta_stage_status") or "unknown")
            ta_completed_full = int(ta_stage_meta.get("ta_completed_full") or 0)
            ta_total_symbols = int(ta_stage_meta.get("ta_total_symbols") or 0)
            ta_failed_symbols = [str(symbol) for symbol in (ta_stage_meta.get("ta_failed_symbols") or [])]
            if ta_stage_status != "completed" or ta_completed_full != ta_total_symbols or ta_failed_symbols:
                missing_layers.append("tradingagents")
                failed_detail = "、".join(ta_failed_symbols[:8]) if ta_failed_symbols else "无失败 symbol 明细"
                blocking_reasons.append(
                    f"TradingAgents stage meta 未正式完成：{ta_stage_status} "
                    f"full={ta_completed_full}/{ta_total_symbols} failed={failed_detail}"
                )

        bad_uzi = []
        for row in uzi:
            if not status_is_ok_uzi(row):
                flags = "；".join(str(flag) for flag in (row.get("quality_flags") or []))
                bad_uzi.append(f"{row.get('symbol') or '?'} {row.get('status') or 'unknown'} {flags}".strip())
        if uzi and not bad_uzi:
            completed_layers.append("uzi")
        else:
            missing_layers.append("uzi")
            detail = "；".join(bad_uzi[:8]) if bad_uzi else "无 UZI ok 投委结果"
            blocking_reasons.append(f"UZI 未正式完成：{detail}")

        vibe_enabled = env.get("VIBE_TRADING_ENABLED", "0") == "1"
        if vibe_enabled:
            if str(vibe_review.get("status") or "") == "ok":
                completed_layers.append("vibe_trading")
            else:
                missing_layers.append("vibe_trading")
                blocking_reasons.append(f"Vibe-Trading 未正式完成：{vibe_review.get('status') or 'unknown'}")

        if final_report_written:
            completed_layers.append("final_report")
        else:
            missing_layers.append("final_report")
            blocking_reasons.append("最终报告未生成")

        if telegram_enabled:
            if telegram_sent is True:
                completed_layers.append("telegram")
            else:
                missing_layers.append("telegram")
                blocking_reasons.append("Telegram 启用但未发送成功")

        missing_layers = list(dict.fromkeys(missing_layers))
        completed_layers = list(dict.fromkeys(layer for layer in completed_layers if layer not in missing_layers))
        report_overall_status = "ok" if not missing_layers and not blocking_reasons else "failed"
        return {
            "run_mode": "formal",
            "overall_status": report_overall_status,
            "completed_layers": completed_layers,
            "missing_layers": missing_layers,
            "blocking_reasons": blocking_reasons,
            "evidence_files": evidence_files,
            "can_publish_buy_report": report_overall_status == "ok",
            "telegram_enabled": telegram_enabled,
            "telegram_sent": telegram_sent,
        }

    flow_status = {
        "run_mode": run_mode,
        "stage": stage,
        "overall_status": overall_status,
        "can_publish_buy_report": True,
    }

    if run_mode == "formal" and ta_stage_meta:
        if ta_stage_meta.get("ta_stage_status") != "completed":
            flow_status["can_publish_buy_report"] = False

    flow_status.update(extra_fields)
    return flow_status


def run_tradingagents_full_batch(
    candidates: list[dict[str, Any]],
    *,
    trading_dir: str,
    python_bin: str,
    env: dict[str, str],
    per_stock_timeout: int = 1800,
    stage_timeout: int = 3600,
    max_workers: int = 4
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Batch scheduler for real TradingAgents full execution.

    Submits each symbol to run_tradingagents_full_one via a thread pool, then
    polls future.done() (instead of as_completed, which can block the stage
    hard-cap). Enforces an overall stage_timeout. Per-stock timeout is enforced
    inside run() so a single hung process cannot exceed per_stock_timeout.
    """
    stage_timeout = int(env.get("PIPELINE_TRADINGAGENTS_STAGE_TIMEOUT") or os.environ.get("PIPELINE_TRADINGAGENTS_STAGE_TIMEOUT", str(stage_timeout)))

    start = time.time()
    completed_full = 0
    failed_symbols: list[str] = []
    results_by_symbol: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=max_workers)
    timed_out = False

    try:
        future_to_symbol: dict[Any, str] = {}
        for item in candidates:
            symbol = item.get("symbol", "unknown")
            future = executor.submit(
                run_tradingagents_full_one,
                item,
                trading_dir=Path(trading_dir),
                python_bin=python_bin,
                env=env,
                timeout=per_stock_timeout,
            )
            future_to_symbol[future] = symbol
            results_by_symbol[symbol] = None

        remaining_futures = set(future_to_symbol.keys())

        while remaining_futures:
            # Stage-level hard cap: mark unfinished symbols failed and stop polling.
            if time.time() - start > stage_timeout:
                print(f"TradingAgents stage timeout after {stage_timeout}s; force-shutdown executor")
                timed_out = True
                for future in remaining_futures:
                    symbol = future_to_symbol[future]
                    future.cancel()
                    if symbol not in failed_symbols:
                        failed_symbols.append(symbol)
                    results_by_symbol[symbol] = {"ta_status": "stage_timeout"}
                break

            still_remaining = set()
            for future in remaining_futures:
                if future.done():
                    symbol = future_to_symbol[future]
                    try:
                        result = future.result()
                        results_by_symbol[symbol] = result
                        if result.get("ta_status") == "full":
                            completed_full += 1
                        else:
                            if symbol not in failed_symbols:
                                failed_symbols.append(symbol)
                    except Exception as e:
                        if symbol not in failed_symbols:
                            failed_symbols.append(symbol)
                        results_by_symbol[symbol] = {"ta_status": "error", "error": str(e)}
                else:
                    still_remaining.add(future)

            remaining_futures = still_remaining
            if remaining_futures:
                time.sleep(0.5)
    finally:
        if timed_out:
            executor.shutdown(wait=False)
            print("Executor shutdown without waiting for remaining tasks")
        else:
            executor.shutdown(wait=True)

    elapsed = time.time() - start

    if time.time() - start > stage_timeout:
        ta_stage_status = "stage_timeout"
    elif not failed_symbols:
        ta_stage_status = "completed"
    else:
        ta_stage_status = "partial_failure"

    rows: list[dict[str, Any]] = []
    for item in candidates:
        symbol = item.get("symbol", "unknown")
        result = results_by_symbol.get(symbol) or {}
        row = {
            "symbol": symbol,
            "company_name": item.get("company_name", ""),
            "ta_decision": result.get("ta_status", "unknown"),
            "ta_completed_full": completed_full,
            "ta_failed_symbols": failed_symbols,
        }
        row.update(result)
        rows.append(row)

    metadata = {
        "ta_stage_status": ta_stage_status,
        "ta_completed_full": completed_full,
        "ta_failed_symbols": failed_symbols,
        "ta_total_symbols": len(candidates),
        "ta_elapsed_seconds": round(elapsed, 2),
    }

    return rows, metadata


def persist_flow_status(flow_status: dict[str, Any], *, stage: str) -> dict[str, Any]:
    extra_fields = {
        key: value for key, value in flow_status.items()
        if key not in {"run_mode", "stage", "overall_status", "blocking_reasons", "can_publish_buy_report"}
    }
    return write_stage_status(
        run_mode=str(flow_status.get("run_mode") or "formal"),
        stage=stage,
        overall_status=str(flow_status.get("overall_status") or "failed"),
        blocking_reasons=list(flow_status.get("blocking_reasons") or []),
        can_publish_buy_report=bool(flow_status.get("can_publish_buy_report")),
        **extra_fields,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Hermes daily Top10 stock pipeline.")
    parser.add_argument("--pool", default=str(ROOT / "stock_pool.yaml"))
    parser.add_argument("--dsa-top", type=int, default=None)
    parser.add_argument("--ta-top", type=int, default=None)
    parser.add_argument("--uzi-top", type=int, default=None)
    parser.add_argument("--buy-top", type=int, default=None)
    parser.add_argument("--run-mode", choices=["formal", "smoke", "diagnostic"], help="Pipeline run mode. Defaults to PIPELINE_RUN_MODE or formal.")
    parser.add_argument("--smoke", action="store_true", help="Mark this run as a smoke wiring test.")
    parser.add_argument("--diagnostic", action="store_true", help="Mark this run as a diagnostic run.")
    parser.add_argument("--reuse-dsa", action="store_true", help="Reuse outputs/candidates_top50.json instead of rerunning DSA.")
    parser.add_argument("--send-telegram", action="store_true", default=None)
    args = parser.parse_args()

    started = time.time()
    env = build_env()
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    WORK.mkdir(parents=True, exist_ok=True)

    dsa_top = args.dsa_top or int(env.get("PIPELINE_DSA_TOP_N", "50"))
    ta_top = args.ta_top or int(env.get("PIPELINE_TRADINGAGENTS_TOP_N", "20"))
    uzi_top = args.uzi_top or int(env.get("PIPELINE_UZI_TOP_N", "10"))
    buy_top = args.buy_top or int(env.get("PIPELINE_BUY_TOP_N", "3"))
    run_mode = determine_run_mode(env, args)
    env["PIPELINE_RUN_MODE"] = run_mode

    pool = read_stock_pool(Path(args.pool))
    write_json(OUTPUTS / "stock_pool_resolved.json", pool)
    print(f"Loaded stock pool: {len(pool)} symbols")

    write_stage_status(
        run_mode=run_mode,
        stage="openbb_in_progress",
        overall_status="in_progress",
        can_publish_buy_report=False,
        counts={"universe": len(pool)},
    )
    openbb_context = stage_openbb_context(pool, env)
    print(f"OpenBB context: {openbb_context.get('status')} / {openbb_context.get('symbol_count', 0)} symbols")
    enriched_context = build_enriched_stock_data(pool, env, openbb_context=openbb_context)
    print(f"Enriched data: {enriched_context.get('symbol_count', 0)} symbols / complete {enriched_context.get('complete_count', 0)}")

    write_stage_status(
        run_mode=run_mode,
        stage="kronos_in_progress",
        overall_status="in_progress",
        can_publish_buy_report=False,
        counts={
            "universe": len(pool),
            "openbb": int(openbb_context.get("symbol_count") or 0),
        },
    )
    kronos_context = stage_kronos_context(openbb_context, env)
    print(f"Kronos context: {kronos_context.get('status')} / {kronos_context.get('symbol_count', 0)} symbols")

    write_stage_status(
        run_mode=run_mode,
        stage="dsa_in_progress",
        overall_status="in_progress",
        can_publish_buy_report=False,
        counts={
            "universe": len(pool),
            "openbb": int(openbb_context.get("symbol_count") or 0),
            "kronos": int(kronos_context.get("symbol_count") or 0),
        },
    )
    if args.reuse_dsa and (OUTPUTS / "candidates_top50.json").exists():
        candidates = json.loads((OUTPUTS / "candidates_top50.json").read_text(encoding="utf-8"))
    else:
        candidates = stage_daily_stock_analysis(pool, env, dsa_top, openbb_context, kronos_context)
    print(f"DSA candidates: {len(candidates)}")
    enriched_context = build_enriched_stock_data(pool, env, openbb_context=openbb_context, candidates=candidates)
    print(f"Enriched data refreshed after DSA: {enriched_context.get('symbol_count', 0)} symbols")

    write_stage_status(
        run_mode=run_mode,
        stage="dexter_in_progress",
        overall_status="in_progress",
        can_publish_buy_report=False,
        counts={
            "universe": len(pool),
            "openbb": int(openbb_context.get("symbol_count") or 0),
            "kronos": int(kronos_context.get("symbol_count") or 0),
            "screen": len(candidates),
        },
    )
    dexter_context = stage_dexter_context(candidates, env)
    print(f"Dexter context: {dexter_context.get('status')} / {dexter_context.get('symbol_count', 0)} symbols")
    candidates = apply_dexter_signals(candidates, dexter_context)
    write_json(OUTPUTS / "candidates_top50.json", candidates)
    enriched_context = build_enriched_stock_data(pool, env, openbb_context=openbb_context, candidates=candidates)

    write_stage_status(
        run_mode=run_mode,
        stage="ta_in_progress",
        overall_status="in_progress",
        can_publish_buy_report=False,
    )

    trading = stage_tradingagents(candidates, env, ta_top)
    ta_stage_meta = read_json_if_exists(OUTPUTS / "tradingagents_stage_meta.json", {})
    print(f"TradingAgents results: {len(trading)}")
    enriched_context = build_enriched_stock_data(pool, env, openbb_context=openbb_context, candidates=candidates, trading=trading)

    write_stage_status(
        run_mode=run_mode,
        stage="ta_done",
        overall_status="in_progress",
        can_publish_buy_report=False,
        ta_stage_meta=ta_stage_meta,
    )

    write_stage_status(
        run_mode=run_mode,
        stage="uzi_in_progress",
        overall_status="in_progress",
        can_publish_buy_report=False,
        ta_stage_meta=ta_stage_meta,
    )

    uzi = stage_uzi(trading, env, uzi_top)
    print(f"UZI results: {len(uzi)}")
    enriched_context = build_enriched_stock_data(pool, env, openbb_context=openbb_context, candidates=candidates, trading=trading, uzi=uzi)

    final_rows = merge_scores(candidates, trading, uzi, uzi_top)
    vibe_review = stage_vibe_trading_review(final_rows, env)
    print(f"Vibe-Trading review: {vibe_review.get('status')} / {vibe_review.get('symbol_count', len(vibe_review.get('symbols') or []))} symbols")
    final_rows = apply_vibe_trading_review(final_rows, vibe_review)[:uzi_top]
    buy3 = select_buy_list(final_rows, buy_top)
    final_rows = [prepare_report_row(row) for row in final_rows]
    buy3 = [prepare_report_row(row) for row in buy3]
    counts = {
        "universe": len(pool),
        "openbb": int(openbb_context.get("symbol_count") or 0),
        "kronos": int(kronos_context.get("symbol_count") or 0),
        "dexter": int(dexter_context.get("symbol_count") or 0),
        "vibe_trading": int(vibe_review.get("symbol_count") or len(vibe_review.get("symbols") or [])),
        "serenity_bottleneck": len(read_json_if_exists(OUTPUTS / "serenity_bottleneck_watchlist.json", [])),
        "screen": len(candidates),
        "research": len(trading),
        "committee": len(final_rows),
    }
    should_send = args.send_telegram or env.get("PIPELINE_SEND_TELEGRAM", "1") == "1"
    report_flow_status = build_flow_status(
        env=env,
        run_mode=run_mode,
        counts=counts,
        trading=trading,
        uzi=uzi,
        vibe_review=vibe_review,
        final_report_written=True,
        telegram_enabled=False,
        telegram_sent=None,
        ta_stage_meta=ta_stage_meta,
    )
    top10_for_report, buy3_for_report = apply_flow_status_to_report(final_rows, buy3, report_flow_status)
    markdown = humanize_report_chinese(render_final_markdown(top10_for_report, buy3_for_report, counts, watch=None, flow_status=report_flow_status))
    write_text(OUTPUTS / "final_top10.md", markdown)
    write_json(OUTPUTS / "final_top10.json", top10_for_report)
    write_json(OUTPUTS / "buy_top3.json", buy3_for_report)
    write_text(OUTPUTS / "buy_top3.md", render_buy_markdown(buy3_for_report, counts))
    update_watchlist(top10_for_report, buy3_for_report, env)
    flow_status = build_flow_status(
        env=env,
        run_mode=run_mode,
        counts=counts,
        trading=trading,
        uzi=uzi,
        vibe_review=vibe_review,
        final_report_written=True,
        telegram_enabled=should_send,
        telegram_sent=None,
        ta_stage_meta=ta_stage_meta,
    )
    persist_flow_status(flow_status, stage="final_report_done")
    print(f"Final report written: {OUTPUTS / 'final_top10.md'} ({len(markdown)} chars)")

    if should_send:
        try:
            telegram_send(env, telegram_text_for_status(markdown, report_flow_status))
        except Exception:
            failed_status = build_flow_status(
                env=env,
                run_mode=run_mode,
                counts=counts,
                trading=trading,
                uzi=uzi,
                vibe_review=vibe_review,
                final_report_written=True,
                telegram_enabled=True,
                telegram_sent=False,
                ta_stage_meta=ta_stage_meta,
            )
            persist_flow_status(failed_status, stage="telegram_failed")
            raise
        flow_status = build_flow_status(
            env=env,
            run_mode=run_mode,
            counts=counts,
            trading=trading,
            uzi=uzi,
            vibe_review=vibe_review,
            final_report_written=True,
            telegram_enabled=True,
            telegram_sent=True,
            ta_stage_meta=ta_stage_meta,
        )
        persist_flow_status(flow_status, stage="telegram_done")
    print(f"Pipeline completed in {time.time() - started:.1f}s")


def render_buy_markdown(buy3: list[dict[str, Any]], counts: dict[str, int]) -> str:
    buy3 = [prepare_report_row(row) for row in buy3]
    today = dt.date.today().isoformat()
    lines = [
        f"# 今日买入 {len(buy3)} 只",
        "",
        f"生成日期：{today}",
        f"漏斗：股票池 {counts.get('universe', 0)} → 行情数据层 {counts.get('openbb', 0)} → 初筛 {counts.get('screen', 0)} → 深度投研 {counts.get('research', 0)} → 投委复核 {counts.get('committee', 0)} → 买入 {len(buy3)}",
        "",
    ]
    if not buy3:
        lines += [
            "今日无满足严格买入条件的股票。",
            "宁可空仓观察，也不把信号冲突的股票包装成买入。",
            "",
        ]
    for idx, row in enumerate(buy3, 1):
        lines += [
            f"{idx}. {row.get('name') or row['symbol']} {row['symbol']}",
            f"结论：{row.get('buy_decision')}",
            format_score_line(row),
            f"价格计划：{row.get('price_plan') or '暂无有效参考价，等下一轮日线数据补齐'}",
            f"买入：{row.get('buy_advice')}",
            f"卖出：{row.get('sell_advice')}",
            f"仓位：{format_position_advice(row)}",
            f"风控闸门：{row.get('quality_note') or '通过'}",
            f"理由：{complete_excerpt(row.get('reason', ''), 720)}",
            "",
        ]
    return "\n".join(lines).strip() + "\n"


def _persist_failed_status(exc: Exception) -> None:
    try:
        prev = read_json_if_exists(OUTPUTS / "pipeline_status.json", {})
        run_mode = prev.get("run_mode") or "formal"
        stage = prev.get("stage") or "unknown"
        write_stage_status(
            run_mode=run_mode,
            stage=stage,
            overall_status="failed",
            blocking_reasons=[f"{type(exc).__name__}: {exc}"][:1],
            can_publish_buy_report=False,
        )
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        _persist_failed_status(exc)
        raise
