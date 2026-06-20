#!/usr/bin/env python3
"""Glue runner for TradingAgents, UZI-Skill, daily_stock_analysis, Hermes, and Telegram."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_TRADINGAGENTS_DIR = Path("/Users/g90/Documents/Codex/2026-06-10/export-openai-api-key-openai-gpt/TradingAgents")
DEFAULT_UZI_DIR = Path("/Users/g90/Documents/Codex/2026-06-10/bash-git-clone-https-github-com/UZI-Skill")
DEFAULT_DSA_DIR = Path("/Users/g90/Documents/Codex/2026-06-11/zhulinsen-daily-stock-analysis-https-github/work/daily_stock_analysis")
DEFAULT_BUNDLED_PYTHON = Path("/Users/g90/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3")


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
        if key and (overwrite or key not in env or not env[key]):
            env[key] = value


def build_env() -> dict[str, str]:
    env = dict(os.environ)
    load_env_file(ROOT / "config.env", env, overwrite=False)

    trading_dir = Path(env.get("TRADINGAGENTS_DIR") or DEFAULT_TRADINGAGENTS_DIR)
    load_env_file(trading_dir / ".env", env, overwrite=False)

    if env.get("OPENAI_BASE_URL") and not env.get("TRADINGAGENTS_LLM_BACKEND_URL"):
        env["TRADINGAGENTS_LLM_BACKEND_URL"] = env["OPENAI_BASE_URL"]
    if env.get("OPENAI_MODEL") and not env.get("TRADINGAGENTS_DEEP_THINK_LLM"):
        env["TRADINGAGENTS_DEEP_THINK_LLM"] = env["OPENAI_MODEL"]
    if env.get("OPENAI_MODEL") and not env.get("TRADINGAGENTS_QUICK_THINK_LLM"):
        env["TRADINGAGENTS_QUICK_THINK_LLM"] = env["OPENAI_MODEL"]
    env.setdefault("NOTIFICATION_REPORT_CHANNELS", "telegram")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


def path_from_env(env: dict[str, str], key: str, default: Path) -> Path:
    return Path(env.get(key) or default).expanduser()


def split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def mask_status(env: dict[str, str], key: str) -> str:
    return "SET" if env.get(key) else "MISSING"


def run_command(cmd: list[str], *, cwd: Path, env: dict[str, str], check: bool = True) -> int:
    print(f"\n$ {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(cwd), env=env, check=False)
    if check and proc.returncode != 0:
        raise SystemExit(proc.returncode)
    return proc.returncode


def telegram_send(env: dict[str, str], text: str) -> None:
    token = env.get("TELEGRAM_BOT_TOKEN")
    chat_id = env.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise SystemExit("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required.")

    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    thread_id = env.get("TELEGRAM_MESSAGE_THREAD_ID")
    if thread_id:
        payload["message_thread_id"] = thread_id

    data = urllib.parse.urlencode(payload).encode("utf-8")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    request = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        raise SystemExit(f"Telegram send failed: {exc}") from exc

    result = json.loads(body)
    if not result.get("ok"):
        raise SystemExit(f"Telegram send failed: {body}")


def doctor(args: argparse.Namespace) -> None:
    env = build_env()
    trading_dir = path_from_env(env, "TRADINGAGENTS_DIR", DEFAULT_TRADINGAGENTS_DIR)
    uzi_dir = path_from_env(env, "UZI_SKILL_DIR", DEFAULT_UZI_DIR)
    dsa_dir = path_from_env(env, "DAILY_STOCK_ANALYSIS_DIR", DEFAULT_DSA_DIR)
    trading_python = path_from_env(env, "TRADINGAGENTS_PYTHON", trading_dir / ".venv/bin/python")
    python_bin = path_from_env(env, "PYTHON_BIN", DEFAULT_BUNDLED_PYTHON)
    hermes_home = Path(env.get("HERMES_HOME") or "~/.hermes").expanduser()

    checks = [
        ("TradingAgents dir", trading_dir.exists(), str(trading_dir)),
        ("TradingAgents python", trading_python.exists(), str(trading_python)),
        ("UZI-Skill dir", uzi_dir.exists(), str(uzi_dir)),
        ("daily_stock_analysis dir", dsa_dir.exists(), str(dsa_dir)),
        ("Python for DSA", python_bin.exists() or shutil.which(str(python_bin)) is not None, str(python_bin)),
        ("Hermes home", hermes_home.exists(), str(hermes_home)),
        ("hermes command", shutil.which("hermes") is not None, shutil.which("hermes") or "not on PATH"),
    ]

    print("Component checks")
    for name, ok, detail in checks:
        print(f"  {'OK' if ok else 'MISS'}  {name}: {detail}")

    print("\nEnvironment keys")
    for key in [
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_MODEL",
        "TRADINGAGENTS_LLM_PROVIDER",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "NOTIFICATION_REPORT_CHANNELS",
        "MX_APIKEY",
        "TUSHARE_TOKEN",
    ]:
        print(f"  {key}={mask_status(env, key)}")

    print("\nStocks")
    print(f"  STOCK_LIST={env.get('STOCK_LIST', '')}")
    print(f"  TRADINGAGENTS_TICKERS={env.get('TRADINGAGENTS_TICKERS', '')}")
    print(f"  UZI_TICKERS={env.get('UZI_TICKERS', '')}")

    skills_dir = hermes_home / "skills"
    print("\nHermes skill links")
    for name in ["deep-analysis", "investor-panel", "lhb-analyzer", "trap-detector"]:
        link = skills_dir / name
        print(f"  {'OK' if link.exists() else 'MISS'}  {link}")


def telegram_test(args: argparse.Namespace) -> None:
    env = build_env()
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    telegram_send(env, f"Trading combo Telegram test OK\n{now}")
    print("Telegram test message sent.")


def run_daily(args: argparse.Namespace) -> None:
    env = build_env()
    dsa_dir = path_from_env(env, "DAILY_STOCK_ANALYSIS_DIR", DEFAULT_DSA_DIR)
    python_bin = str(path_from_env(env, "PYTHON_BIN", DEFAULT_BUNDLED_PYTHON))
    stocks = args.stocks or env.get("STOCK_LIST")
    extra = list(args.extra)
    if extra and extra[0] == "--":
        extra = extra[1:]
    cmd = [python_bin, "main.py"]
    if stocks:
        cmd += ["--stocks", stocks]
    cmd += extra
    run_command(cmd, cwd=dsa_dir, env=env)


def run_trading(args: argparse.Namespace) -> None:
    env = build_env()
    trading_dir = path_from_env(env, "TRADINGAGENTS_DIR", DEFAULT_TRADINGAGENTS_DIR)
    python_bin = str(path_from_env(env, "TRADINGAGENTS_PYTHON", trading_dir / ".venv/bin/python"))
    tickers = [args.ticker] if args.ticker else split_csv(env.get("TRADINGAGENTS_TICKERS") or env.get("STOCK_LIST") or "NVDA")
    date = args.date or dt.date.today().isoformat()
    code = """
import json, os, sys
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import DEFAULT_CONFIG

config = DEFAULT_CONFIG.copy()
config["output_language"] = os.environ.get("TRADINGAGENTS_OUTPUT_LANGUAGE", config.get("output_language", "Chinese"))
graph = TradingAgentsGraph(debug=True, config=config)
for ticker in sys.argv[1:]:
    _, decision = graph.propagate(ticker, os.environ["TRADINGAGENTS_RUN_DATE"])
    print("\\n===== TradingAgents", ticker, "=====")
    print(decision if isinstance(decision, str) else json.dumps(decision, ensure_ascii=False, indent=2, default=str))
"""
    env["TRADINGAGENTS_RUN_DATE"] = date
    run_command([python_bin, "-c", code, *tickers], cwd=trading_dir, env=env)


def run_uzi(args: argparse.Namespace) -> None:
    env = build_env()
    uzi_dir = path_from_env(env, "UZI_SKILL_DIR", DEFAULT_UZI_DIR)
    python_bin = str(path_from_env(env, "PYTHON_BIN", DEFAULT_BUNDLED_PYTHON))
    tickers = [args.ticker] if args.ticker else split_csv(env.get("UZI_TICKERS") or env.get("STOCK_LIST") or "NVDA")
    for ticker in tickers:
        cmd = [python_bin, "run.py", ticker, "--no-browser"]
        if args.depth:
            cmd += ["--depth", args.depth]
        run_command(cmd, cwd=uzi_dir, env=env)


def run_all(args: argparse.Namespace) -> None:
    run_daily(argparse.Namespace(stocks=args.stocks, extra=["--no-market-review"]))
    run_trading(argparse.Namespace(ticker=None, date=args.date))
    run_uzi(argparse.Namespace(ticker=None, depth=args.depth))
    env = build_env()
    if env.get("TELEGRAM_BOT_TOKEN") and env.get("TELEGRAM_CHAT_ID"):
        telegram_send(env, "Trading combo run completed.")


def install_hermes_symlinks(args: argparse.Namespace) -> None:
    env = build_env()
    uzi_dir = path_from_env(env, "UZI_SKILL_DIR", DEFAULT_UZI_DIR)
    hermes_home = Path(env.get("HERMES_HOME") or "~/.hermes").expanduser()
    skills_dir = hermes_home / "skills"
    skills_dir.mkdir(parents=True, exist_ok=True)
    for name in ["deep-analysis", "investor-panel", "lhb-analyzer", "trap-detector"]:
        source = uzi_dir / "skills" / name
        target = skills_dir / name
        if target.exists() or target.is_symlink():
            target.unlink()
        target.symlink_to(source, target_is_directory=True)
        print(f"linked {target} -> {source}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("doctor", help="Check local paths and required env.")
    p.set_defaults(func=doctor)

    p = sub.add_parser("telegram-test", help="Send a Telegram smoke-test message.")
    p.set_defaults(func=telegram_test)

    p = sub.add_parser("daily", help="Run daily_stock_analysis.")
    p.add_argument("--stocks", help="Override STOCK_LIST for this run.")
    p.add_argument("extra", nargs=argparse.REMAINDER, help="Extra args passed to daily_stock_analysis main.py. Prefix with --.")
    p.set_defaults(func=run_daily)

    p = sub.add_parser("trading", help="Run TradingAgents for one or more tickers.")
    p.add_argument("--ticker", help="Single ticker override.")
    p.add_argument("--date", help="Analysis date, defaults to today.")
    p.set_defaults(func=run_trading)

    p = sub.add_parser("uzi", help="Run UZI-Skill deep-analysis.")
    p.add_argument("--ticker", help="Single ticker override.")
    p.add_argument("--depth", choices=["lite", "medium", "deep"], help="UZI analysis depth.")
    p.set_defaults(func=run_uzi)

    p = sub.add_parser("all", help="Run DSA, TradingAgents, and UZI in sequence.")
    p.add_argument("--stocks", help="Override STOCK_LIST for DSA.")
    p.add_argument("--date", help="TradingAgents analysis date, defaults to today.")
    p.add_argument("--depth", choices=["lite", "medium", "deep"], help="UZI analysis depth.")
    p.set_defaults(func=run_all)

    p = sub.add_parser("install-hermes-symlinks", help="Link local UZI skills into ~/.hermes/skills.")
    p.set_defaults(func=install_hermes_symlinks)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
