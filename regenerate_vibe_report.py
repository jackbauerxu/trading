import json

import run_daily_pipeline as p


def read_output_json(name, default):
    path = p.OUTPUTS / name
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


env = p.build_env()
pool = p.read_stock_pool(p.ROOT / "stock_pool.yaml")
candidates = read_output_json("candidates_top50.json", [])
trading = read_output_json("tradingagents_top20.json", [])
uzi = read_output_json("uzi_top10.json", [])
openbb_context = read_output_json("openbb_context.json", {"symbol_count": 0})
kronos_context = read_output_json("kronos_context.json", {"symbol_count": 0})
dexter_context_path = p.OUTPUTS / "dexter_context.json"
dexter_context = json.loads(dexter_context_path.read_text(encoding="utf-8")) if dexter_context_path.exists() else {"symbols": []}
candidates = p.apply_dexter_signals(candidates, dexter_context)

final_rows = p.merge_scores(candidates, trading, uzi, int(env.get("PIPELINE_UZI_TOP_N", "10")))
symbols = []
for row in final_rows[: int(env.get("VIBE_TRADING_TOP_N", "5"))]:
    symbol = p.normalize_dsa_symbol(str(row.get("symbol") or ""))
    log_path = p.WORK / f"vibe_trading_{p.safe_name(symbol)}.log"
    parsed = p.parse_last_json(log_path.read_text(encoding="utf-8", errors="ignore")) if log_path.exists() else None
    item = None
    if isinstance(parsed, dict) and isinstance(parsed.get("symbols"), list) and parsed["symbols"]:
        item = dict(parsed["symbols"][0])
    elif isinstance(parsed, dict):
        item = dict(parsed)
    if item:
        item.setdefault("symbol", symbol)
        item.setdefault("name", row.get("name", ""))
        item.setdefault("status", "ok")
        symbols.append(item)

vibe_review = {
    "status": "ok" if symbols else "fallback",
    "symbol_count": len(symbols),
    "workers": int(env.get("VIBE_TRADING_WORKERS", "2")),
    "timeout_per_symbol": int(env.get("VIBE_TRADING_TIMEOUT_PER_SYMBOL", env.get("VIBE_TRADING_TIMEOUT", "420"))),
    "symbols": symbols,
}
p.write_json(p.OUTPUTS / "vibe_trading_review.json", vibe_review)
p.write_text(p.WORK / "vibe_trading_review.log", json.dumps(vibe_review, ensure_ascii=False, indent=2))

final_rows = p.apply_vibe_trading_review(final_rows, vibe_review)[: int(env.get("PIPELINE_UZI_TOP_N", "10"))]
buy_rows = p.select_buy_list(final_rows, int(env.get("PIPELINE_BUY_TOP_N", "3")))
final_rows = [p.prepare_report_row(row) for row in final_rows]
buy_rows = [p.prepare_report_row(row) for row in buy_rows]
watch = p.update_watchlist(final_rows, buy_rows, env)
counts = {
    "universe": len(pool),
    "openbb": int(openbb_context.get("symbol_count") or 0),
    "kronos": int(kronos_context.get("symbol_count") or 0),
    "dexter": int(dexter_context.get("symbol_count") or 0),
    "vibe_trading": len(symbols),
    "screen": len(candidates),
    "research": len(trading),
    "committee": len(final_rows),
}
p.write_json(p.OUTPUTS / "final_top10.json", final_rows)
p.write_json(p.OUTPUTS / "buy_top3.json", buy_rows)
markdown = p.render_final_markdown(final_rows, buy_rows, counts, watch)
p.write_text(p.OUTPUTS / "final_top10.md", markdown)
p.write_text(p.OUTPUTS / "buy_top3.md", p.render_buy_markdown(buy_rows, counts))
if env.get("PIPELINE_SEND_TELEGRAM", "1") == "1":
    p.telegram_send(env, markdown)
print(json.dumps({"vibe_status": vibe_review["status"], "vibe_count": len(symbols), "buy_count": len(buy_rows)}, ensure_ascii=False))
