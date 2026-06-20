#!/usr/bin/env python3
"""Monitor historical Top10 names and alert only when buy triggers pass checks."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import run_daily_pipeline as pipeline


ROOT = Path(__file__).resolve().parent
OUTPUTS = ROOT / "outputs"
STATE_PATH = OUTPUTS / "buy_trigger_monitor_state.json"
LOG_PATH = OUTPUTS / "buy_trigger_monitor.log"


@dataclass
class TriggerDecision:
    symbol: str
    name: str
    should_alert: bool
    trigger_type: str
    reason: str
    close: float
    blocks: list[str] = field(default_factory=list)
    dedup_key: str = ""


def safe_float(value: Any, default: float = 0.0) -> float:
    return pipeline.safe_float(value, default)


def parse_money(value: Any) -> float:
    text = str(value or "").strip().replace("$", "").replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return safe_float(match.group(0)) if match else 0.0


def parse_price_range(value: Any) -> tuple[float, float]:
    text = str(value or "").replace("$", "").replace(",", "")
    nums = [safe_float(x) for x in re.findall(r"\d+(?:\.\d+)?", text)]
    nums = [x for x in nums if x > 0]
    if len(nums) >= 2:
        low, high = nums[0], nums[1]
        return (min(low, high), max(low, high))
    if len(nums) == 1:
        return nums[0], nums[0]
    return 0.0, 0.0


def normalize_symbol(symbol: str) -> str:
    s = str(symbol or "").strip()
    if s.lower().startswith("hk") and len(s) > 2:
        return s.lower()
    return s.upper()


def normalize_action(action: Any) -> str:
    text = str(action or "").upper()
    if "BUY" in text or "买入" in text:
        return "买入"
    if "SELL" in text or "卖出" in text:
        return "卖出"
    if "HOLD" in text or "持有" in text:
        return "持有"
    return str(action or "观察")


def quality_blocks(row: dict[str, Any], quote: dict[str, Any], *, max_ret20: float = 30.0) -> list[str]:
    blocks: list[str] = []
    quality = str(row.get("quality_note") or "")
    rating = str(row.get("rating") or "")
    action = normalize_action(row.get("action"))
    uzi_score = safe_float(row.get("uzi_score"))
    ret_20d = safe_float(quote.get("ret_20d"), safe_float(row.get("ret_20d")))

    if str(row.get("ta_status") or "full") != "full":
        blocks.append("TradingAgents 未完整完成")
    if uzi_score < 60:
        blocks.append("UZI 投委分低于 60")
    if any(term in quality for term in ("UZI 投委结论偏谨慎或看空", "UZI 投委输出质量不足", "UZI 投委未完成")):
        blocks.append("UZI 投委结论或质量未通过")
    if any(term in rating for term in ("谨慎", "偏空", "看空")):
        blocks.append("评级仍偏谨慎或偏空")
    if "价格口径未通过" in quality or "price_scale_invalid" in str(row.get("data_quality_flags") or ""):
        blocks.append("价格口径未通过")
    if "TradingAgents 未给出买入" in quality or action not in {"买入", "观察"}:
        blocks.append("TradingAgents 未给出可买方向")
    if ret_20d >= max_ret20:
        blocks.append(f"20日涨幅 {ret_20d:.1f}% 仍过热")
    return list(dict.fromkeys(blocks))


def pretrade_blocks(row: dict[str, Any], quote: dict[str, Any]) -> list[str]:
    audit_item = dict(row)
    if safe_float(quote.get("close")) > 0:
        audit_item["close"] = safe_float(quote.get("close"))
    audit = pipeline.pretrade_consistency_audit(audit_item, {"close": quote.get("close")}, row)
    return [str(gate) for gate in (audit.get("gates") or []) if str(gate).strip()]


def evaluate_trigger(row: dict[str, Any], quote: dict[str, Any], env: dict[str, str] | None = None) -> TriggerDecision:
    env = env or {}
    symbol = normalize_symbol(str(row.get("symbol") or ""))
    name = str(row.get("name") or symbol)
    close = safe_float(quote.get("close"))
    buy_low, buy_high = parse_price_range(row.get("buy_zone"))
    breakout = parse_money(row.get("breakout_price"))
    stop_loss = parse_money(row.get("stop_loss"))
    max_ret20 = safe_float(env.get("MONITOR_MAX_RET20D"), 30.0)
    breakout_min_volume = safe_float(env.get("MONITOR_BREAKOUT_MIN_VOLUME_RATIO"), 1.1)
    pullback_max_volume = safe_float(env.get("MONITOR_PULLBACK_MAX_VOLUME_RATIO"), 2.2)
    volume_ratio = safe_float(quote.get("volume_ratio"), 1.0)

    blocks = []
    invalid_price_symbols = {
        normalize_symbol(x)
        for x in str(env.get("MONITOR_PRICE_SCALE_INVALID_SYMBOLS", "")).split(",")
        if x.strip()
    }
    if symbol in invalid_price_symbols:
        blocks.append("价格口径未通过")
    reference_price = parse_money(row.get("reference_price") or row.get("price_plan"))
    max_price_ratio = safe_float(env.get("MONITOR_MAX_PRICE_SCALE_RATIO"), 4.0)
    if close > 0 and reference_price > 0:
        ratio = max(close, reference_price) / max(min(close, reference_price), 0.01)
        if ratio >= max_price_ratio:
            blocks.append(f"价格口径未通过：实时/报告价差 {ratio:.1f} 倍")
    if close <= 0:
        blocks.append("无有效当前价")
    if stop_loss and close <= stop_loss:
        blocks.append(f"当前价已低于止损 {stop_loss:.2f}")
    blocks.extend(quality_blocks(row, quote, max_ret20=max_ret20))
    blocks.extend(pretrade_blocks(row, quote))

    trigger_type = ""
    reason = ""
    if close > 0 and buy_low and buy_low <= close <= buy_high:
        trigger_type = "回踩买入"
        reason = f"当前价 {close:.2f} 进入回踩买入区 {buy_low:.2f}-{buy_high:.2f}"
        if volume_ratio > pullback_max_volume:
            blocks.append(f"回踩量能过大，量比 {volume_ratio:.2f}")
    elif close > 0 and breakout and close >= breakout:
        trigger_type = "突破确认"
        reason = f"当前价 {close:.2f} 站上突破确认价 {breakout:.2f}"
        if volume_ratio < breakout_min_volume:
            blocks.append(f"突破量能不足，量比 {volume_ratio:.2f}")
    else:
        trigger_type = "未触发"
        if close > 0 and buy_low and breakout:
            reason = f"当前价 {close:.2f} 未进入买入区 {buy_low:.2f}-{buy_high:.2f}，也未突破 {breakout:.2f}"
        elif close > 0:
            reason = f"当前价 {close:.2f} 未触发有效买点"
        else:
            reason = "无有效价格，无法判断"

    should_alert = trigger_type in {"回踩买入", "突破确认"} and not blocks
    day = dt.date.today().isoformat()
    dedup_key = f"{symbol}|{trigger_type}|{day}"
    return TriggerDecision(symbol, name, should_alert, trigger_type, reason, close, list(dict.fromkeys(blocks)), dedup_key)


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def append_log(line: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line.rstrip() + "\n")


def load_monitor_rows() -> list[dict[str, Any]]:
    rows = load_json(OUTPUTS / "final_top10.json", [])
    if not isinstance(rows, list):
        rows = []
    watch = load_json(OUTPUTS / "watchlist_state.json", {})
    positions = (watch.get("state") or {}).get("positions") if isinstance(watch, dict) else {}
    for symbol, pos in (positions or {}).items():
        if not any(normalize_symbol(str(row.get("symbol"))) == normalize_symbol(symbol) for row in rows):
            item = dict(pos or {})
            item.setdefault("symbol", symbol)
            item.setdefault("name", symbol)
            rows.append(item)
    return rows


def load_cached_quote(symbol: str) -> dict[str, Any]:
    wanted = {normalize_symbol(symbol), normalize_symbol(symbol).upper(), normalize_symbol(symbol).lower()}
    for filename in ("enriched_stock_data.json", "openbb_context.json"):
        payload = load_json(OUTPUTS / filename, {})
        if not isinstance(payload, dict):
            continue
        candidates: list[dict[str, Any]] = []
        symbols = payload.get("symbols")
        if isinstance(symbols, list):
            candidates.extend(row for row in symbols if isinstance(row, dict))
        elif isinstance(symbols, dict):
            candidates.extend(row for row in symbols.values() if isinstance(row, dict))
        candidates.extend(row for row in payload.values() if isinstance(row, dict))
        for row in candidates:
            row_symbols = {
                normalize_symbol(str(row.get("symbol") or "")),
                normalize_symbol(str(row.get("original_symbol") or "")),
            }
            if wanted.isdisjoint(row_symbols):
                continue
            if safe_float(row.get("close")) > 0:
                return {
                    "close": safe_float(row.get("close")),
                    "ret_20d": safe_float(row.get("ret_20d")),
                    "volume_ratio": safe_float(row.get("volume_ratio"), 1.0),
                    "provider": row.get("provider") or filename,
                }
    return {}


def live_eastmoney_quote(symbol: str) -> dict[str, Any]:
    ticker = normalize_symbol(symbol)
    prefixes = [105, 106, 107]
    if ticker.lower().startswith("hk"):
        code = ticker[2:].zfill(5)
        prefixes = [116]
    else:
        code = ticker.replace(".US", "")
    last_error = ""
    for prefix in prefixes:
        params = urllib.parse.urlencode({
            "secid": f"{prefix}.{code}",
            "fields": "f43,f44,f45,f46,f47,f58,f59,f60,f170",
        })
        url = f"https://push2.eastmoney.com/api/qt/stock/get?{params}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            data = payload.get("data") or {}
            dec = int(data.get("f59") or 3)
            divisor = 10 ** dec
            raw_close = data.get("f43")
            close = safe_float(raw_close) / divisor if raw_close not in (None, "-") else 0.0
            if close > 0:
                return {
                    "close": close,
                    "change_pct": safe_float(data.get("f170")) / 100,
                    "volume": safe_float(data.get("f47")),
                    "provider": f"eastmoney/live/{prefix}",
                }
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
    raise RuntimeError(last_error or "Eastmoney live quote failed")


def merge_live_and_cached_quote(symbol: str, cached: dict[str, Any]) -> dict[str, Any]:
    live = live_eastmoney_quote(symbol)
    out = dict(cached or {})
    out.update({k: v for k, v in live.items() if v not in (None, "", 0)})
    if not out.get("ret_20d") and cached.get("ret_20d"):
        out["ret_20d"] = cached["ret_20d"]
    if not out.get("volume_ratio") and cached.get("volume_ratio"):
        out["volume_ratio"] = cached["volume_ratio"]
    if cached.get("provider"):
        out["provider"] = f"{live.get('provider')} + {cached.get('provider')}"
    return out


def fetch_quote(symbol: str, env: dict[str, str], mock_quotes: dict[str, Any] | None = None) -> dict[str, Any]:
    if mock_quotes:
        row = mock_quotes.get(symbol) or mock_quotes.get(symbol.upper()) or mock_quotes.get(symbol.lower())
        if isinstance(row, dict):
            return row
    quote = load_cached_quote(symbol)
    if env.get("MONITOR_LIVE_QUOTE", "1") == "1":
        try:
            return merge_live_and_cached_quote(symbol, quote)
        except Exception as exc:
            if quote:
                quote = dict(quote)
                quote["provider"] = f"{quote.get('provider')} after live quote failure: {str(exc)[:80]}"
                return quote
            if env.get("MONITOR_REQUIRE_LIVE_QUOTE", "0") == "1":
                raise
    if quote and env.get("MONITOR_LIVE_FIRST", "0") != "1":
        return quote
    if env.get("MONITOR_USE_CACHE_ONLY") == "1" and quote:
        return quote
    try:
        fetcher = getattr(pipeline, "global_stock_data_summary")
        return fetcher(symbol)
    except Exception as exc:
        if quote:
            quote = dict(quote)
            quote["provider"] = f"{quote.get('provider')} after live failure: {str(exc)[:80]}"
            return quote
        raise


def format_alert(row: dict[str, Any], quote: dict[str, Any], decision: TriggerDecision) -> str:
    return "\n".join([
        f"买点触发提醒：{decision.name} {decision.symbol}",
        "",
        f"类型：{decision.trigger_type}",
        f"当前价：${decision.close:.2f}",
        f"买入区：{row.get('buy_zone') or '-'}",
        f"突破价：{row.get('breakout_price') or '-'}",
        f"止损：{row.get('stop_loss') or '-'}",
        f"止盈：{row.get('take_profit_1') or '-'} / {row.get('take_profit_2') or '-'}",
        "",
        "建议：可按计划小仓第一笔，后续只在确认继续有效时加仓。",
        f"触发理由：{decision.reason}；量比 {safe_float(quote.get('volume_ratio'), 1.0):.2f}；20日涨幅 {safe_float(quote.get('ret_20d')):.1f}%。",
        "",
        f"原报告风控：{row.get('quality_note') or '通过'}",
    ])


def run_monitor(*, dry_run: bool = False, mock_quotes: dict[str, Any] | None = None) -> list[TriggerDecision]:
    env = pipeline.build_env()
    rows = load_monitor_rows()
    state = load_json(STATE_PATH, {"sent": {}})
    sent = state.setdefault("sent", {})
    decisions: list[TriggerDecision] = []

    for row in rows:
        symbol = normalize_symbol(str(row.get("symbol") or ""))
        if not symbol:
            continue
        try:
            quote = fetch_quote(symbol, env, mock_quotes)
            decision = evaluate_trigger(row, quote, env)
            decisions.append(decision)
            stamp = dt.datetime.now().isoformat(timespec="seconds")
            if decision.should_alert and not sent.get(decision.dedup_key):
                message = format_alert(row, quote, decision)
                if dry_run:
                    print(message)
                else:
                    pipeline.telegram_send(env, message)
                sent[decision.dedup_key] = {"sent_at": stamp, "message": message}
                append_log(f"{stamp} ALERT {decision.symbol} {decision.trigger_type} {decision.reason}")
            elif decision.should_alert:
                append_log(f"{stamp} SKIP_DUP {decision.symbol} {decision.trigger_type}")
            else:
                append_log(f"{stamp} NO_ALERT {decision.symbol} {decision.reason} blocks={'；'.join(decision.blocks)}")
        except Exception as exc:
            append_log(f"{dt.datetime.now().isoformat(timespec='seconds')} ERROR {symbol} {type(exc).__name__}: {exc}")

    if not dry_run:
        write_json(STATE_PATH, state)
    return decisions


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor historical Top10 buy triggers and send Telegram alerts.")
    parser.add_argument("--dry-run", action="store_true", help="Print alerts and write logs without sending Telegram.")
    parser.add_argument("--mock-quotes", help="JSON file with symbol quote overrides for testing.")
    args = parser.parse_args()
    mock_quotes = load_json(Path(args.mock_quotes), {}) if args.mock_quotes else None
    decisions = run_monitor(dry_run=args.dry_run, mock_quotes=mock_quotes)
    alerts = [d for d in decisions if d.should_alert]
    print(f"checked={len(decisions)} alerts={len(alerts)}")


if __name__ == "__main__":
    main()
