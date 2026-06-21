from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

import run_daily_pipeline as r


def test_determine_run_mode_defaults_to_formal() -> None:
    args = SimpleNamespace(run_mode=None, smoke=False, diagnostic=False)

    assert r.determine_run_mode({}, args) == "formal"


def test_determine_run_mode_prefers_cli_smoke() -> None:
    args = SimpleNamespace(run_mode=None, smoke=True, diagnostic=False)

    assert r.determine_run_mode({}, args) == "smoke"


def test_determine_run_mode_uses_env_when_cli_empty() -> None:
    args = SimpleNamespace(run_mode=None, smoke=False, diagnostic=False)

    assert r.determine_run_mode({"PIPELINE_RUN_MODE": "diagnostic"}, args) == "diagnostic"


def test_formal_status_blocks_quick_tradingagents() -> None:
    status = r.build_flow_status(
        env={"PIPELINE_SEND_TELEGRAM": "0"},
        run_mode="formal",
        counts={"screen": 2, "research": 2, "committee": 2},
        trading=[
            {"symbol": "NVDA", "ta_status": "full"},
            {"symbol": "MSFT", "ta_status": "quick"},
        ],
        uzi=[
            {"symbol": "NVDA", "status": "ok", "quality_flags": []},
            {"symbol": "MSFT", "status": "ok", "quality_flags": []},
        ],
        vibe_review={"status": "skipped"},
        final_report_written=True,
        telegram_enabled=False,
        telegram_sent=None,
    )

    assert status["run_mode"] == "formal"
    assert status["overall_status"] == "failed"
    assert status["can_publish_buy_report"] is False
    assert "tradingagents" in status["missing_layers"]
    assert any("MSFT" in reason for reason in status["blocking_reasons"])


def test_formal_status_blocks_partial_ta_stage_meta_even_when_rows_are_full() -> None:
    status = r.build_flow_status(
        env={"PIPELINE_SEND_TELEGRAM": "0"},
        run_mode="formal",
        counts={"screen": 1, "research": 1, "committee": 1},
        trading=[{"symbol": "NVDA", "ta_status": "full"}],
        uzi=[{"symbol": "NVDA", "status": "ok", "quality_flags": []}],
        vibe_review={"status": "skipped"},
        final_report_written=True,
        telegram_enabled=False,
        telegram_sent=None,
        ta_stage_meta={
            "ta_stage_status": "partial_failure",
            "ta_completed_full": 0,
            "ta_total_symbols": 1,
            "ta_failed_symbols": ["NVDA"],
        },
    )

    assert status["overall_status"] == "failed"
    assert status["can_publish_buy_report"] is False
    assert "tradingagents" in status["missing_layers"]
    assert any("partial_failure" in reason for reason in status["blocking_reasons"])


def test_formal_status_blocks_watchdog_timeout_ta_stage_meta() -> None:
    status = r.build_flow_status(
        env={"PIPELINE_SEND_TELEGRAM": "0"},
        run_mode="formal",
        counts={"screen": 1, "research": 0, "committee": 0},
        trading=[],
        uzi=[],
        vibe_review={"status": "skipped"},
        final_report_written=True,
        telegram_enabled=False,
        telegram_sent=None,
        ta_stage_meta={
            "ta_stage_status": "watchdog_timeout",
            "ta_completed_full": 0,
            "ta_total_symbols": 1,
            "ta_failed_symbols": ["MRVL"],
        },
    )

    assert status["overall_status"] == "failed"
    assert status["can_publish_buy_report"] is False
    assert "tradingagents" in status["missing_layers"]
    assert any("watchdog_timeout" in reason for reason in status["blocking_reasons"])


def test_formal_status_blocks_degraded_uzi() -> None:
    status = r.build_flow_status(
        env={"PIPELINE_SEND_TELEGRAM": "0"},
        run_mode="formal",
        counts={"screen": 1, "research": 1, "committee": 1},
        trading=[{"symbol": "NVDA", "ta_status": "full"}],
        uzi=[{"symbol": "NVDA", "status": "degraded", "quality_flags": ["UZI 投委输出质量不足"]}],
        vibe_review={"status": "skipped"},
        final_report_written=True,
        telegram_enabled=False,
        telegram_sent=None,
    )

    assert status["overall_status"] == "failed"
    assert status["can_publish_buy_report"] is False
    assert "uzi" in status["missing_layers"]
    assert "UZI 投委输出质量不足" in "；".join(status["blocking_reasons"])


def test_smoke_status_allows_skips_but_cannot_publish_buy_report() -> None:
    status = r.build_flow_status(
        env={"PIPELINE_SKIP_TRADINGAGENTS": "1", "PIPELINE_SKIP_UZI": "1", "PIPELINE_SEND_TELEGRAM": "0"},
        run_mode="smoke",
        counts={"screen": 1, "research": 1, "committee": 1},
        trading=[{"symbol": "NVDA", "ta_status": "fallback"}],
        uzi=[{"symbol": "NVDA", "status": "fallback", "quality_flags": ["UZI 未返回有效投委结果"]}],
        vibe_review={"status": "skipped"},
        final_report_written=True,
        telegram_enabled=False,
        telegram_sent=None,
    )

    assert status["run_mode"] == "smoke"
    assert status["overall_status"] == "ok"
    assert status["can_publish_buy_report"] is False
    assert "smoke 模式只证明连线" in "；".join(status["blocking_reasons"])


def test_formal_status_blocks_telegram_send_failure() -> None:
    status = r.build_flow_status(
        env={"PIPELINE_SEND_TELEGRAM": "1"},
        run_mode="formal",
        counts={"screen": 1, "research": 1, "committee": 1},
        trading=[{"symbol": "NVDA", "ta_status": "full"}],
        uzi=[{"symbol": "NVDA", "status": "ok", "quality_flags": []}],
        vibe_review={"status": "skipped"},
        final_report_written=True,
        telegram_enabled=True,
        telegram_sent=False,
    )

    assert status["overall_status"] == "failed"
    assert status["can_publish_buy_report"] is False
    assert "telegram" in status["missing_layers"]


def test_telegram_text_for_status_uses_failure_notice_when_not_publishable() -> None:
    status = {
        "run_mode": "formal",
        "overall_status": "failed",
        "can_publish_buy_report": False,
        "blocking_reasons": ["UZI 未正式完成：NVDA degraded"],
        "evidence_files": ["outputs/pipeline_status.json", "outputs/final_top10.md"],
    }

    text = r.telegram_text_for_status("# 今日 AI 选股报告\n\n## 今日买入 1 只", status)

    assert "正式流程未完成" in text
    assert "UZI 未正式完成" in text
    assert "## 今日买入 1 只" not in text


def test_telegram_text_for_status_keeps_markdown_when_publishable() -> None:
    status = {"run_mode": "formal", "overall_status": "ok", "can_publish_buy_report": True}

    text = r.telegram_text_for_status("# 今日 AI 选股报告\n\n## 今日买入 1 只", status)

    assert text.startswith("# 今日 AI 选股报告")
    assert "## 今日买入 1 只" in text


def test_telegram_send_raises_when_configuration_is_missing() -> None:
    with pytest.raises(RuntimeError, match="Telegram not configured"):
        r.telegram_send({}, "report")


def test_telegram_send_raises_when_api_rejects_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(r, "telegram_post", lambda url, payload, env: json.dumps({"ok": False, "description": "bad chat"}))

    with pytest.raises(RuntimeError, match="bad chat"):
        r.telegram_send({"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_CHAT_ID": "chat"}, "report")


def test_apply_flow_status_removes_buy_rows_when_formal_incomplete() -> None:
    top10 = [{"symbol": "NVDA", "buy_eligible": True, "quality_note": ""}]
    buy3 = [{"symbol": "NVDA", "buy_decision": "买入"}]
    status = {
        "run_mode": "formal",
        "overall_status": "failed",
        "can_publish_buy_report": False,
        "blocking_reasons": ["深度投研层未正式完成：NVDA"],
    }

    gated_top10, gated_buy3 = r.apply_flow_status_to_report(top10, buy3, status)

    assert gated_buy3 == []
    assert gated_top10[0]["buy_eligible"] is False
    assert "正式流程未完成" in gated_top10[0]["quality_note"]


def test_render_flow_status_section_marks_formal_failure() -> None:
    status = {
        "run_mode": "formal",
        "overall_status": "failed",
        "completed_layers": ["dsa"],
        "missing_layers": ["tradingagents"],
        "blocking_reasons": ["TradingAgents 未正式完成：NVDA"],
        "can_publish_buy_report": False,
    }

    lines = r.render_flow_status_section(status)
    text = "\n".join(lines)

    assert "正式流程状态：失败" in text
    assert "深度投研层未正式完成：NVDA" in text
    assert "TradingAgents" not in text
    assert "可发布买入日报：否" in text


def run_minimal_formal_main(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    *,
    send_telegram: bool,
    telegram_error: Exception | None = None,
) -> None:
    outputs = tmp_path / "outputs"
    work = tmp_path / "work"
    pool = [{"symbol": "NVDA", "name": "NVIDIA"}]
    candidates = [{"symbol": "NVDA", "name": "NVIDIA", "score": 95}]
    trading = [{"symbol": "NVDA", "ta_status": "full"}]
    uzi = [{"symbol": "NVDA", "status": "ok", "quality_flags": []}]
    final_rows = [{"symbol": "NVDA", "name": "NVIDIA", "buy_eligible": True, "quality_note": "", "buy_decision": "买入"}]
    buy3 = [{"symbol": "NVDA", "name": "NVIDIA", "buy_eligible": True, "quality_note": "", "buy_decision": "买入"}]

    monkeypatch.setattr(r, "OUTPUTS", outputs)
    monkeypatch.setattr(r, "WORK", work)
    monkeypatch.setattr(sys, "argv", ["run_daily_pipeline.py", *(["--send-telegram"] if send_telegram else [])])
    monkeypatch.setattr(r, "build_env", lambda: {"PIPELINE_SEND_TELEGRAM": "1" if send_telegram else "0"})
    monkeypatch.setattr(r, "read_stock_pool", lambda path: pool)
    monkeypatch.setattr(r, "stage_openbb_context", lambda pool_arg, env: {"status": "ok", "symbol_count": len(pool_arg)})
    monkeypatch.setattr(r, "build_enriched_stock_data", lambda *args, **kwargs: {"symbol_count": 1, "complete_count": 1})
    monkeypatch.setattr(r, "stage_kronos_context", lambda openbb_context, env: {"status": "ok", "symbol_count": 1})
    monkeypatch.setattr(r, "stage_daily_stock_analysis", lambda pool_arg, env, dsa_top, openbb_context, kronos_context: list(candidates))
    monkeypatch.setattr(r, "stage_dexter_context", lambda candidates_arg, env: {"status": "skipped", "symbol_count": 0})
    monkeypatch.setattr(r, "apply_dexter_signals", lambda candidates_arg, dexter_context: list(candidates_arg))
    monkeypatch.setattr(r, "stage_tradingagents", lambda candidates_arg, env, ta_top: list(trading))
    monkeypatch.setattr(r, "stage_uzi", lambda trading_arg, env, uzi_top: list(uzi))
    monkeypatch.setattr(r, "merge_scores", lambda candidates_arg, trading_arg, uzi_arg, uzi_top: list(final_rows))
    monkeypatch.setattr(r, "stage_vibe_trading_review", lambda final_rows_arg, env: {"status": "skipped", "symbol_count": 0, "symbols": []})
    monkeypatch.setattr(r, "apply_vibe_trading_review", lambda final_rows_arg, vibe_review: list(final_rows_arg))
    monkeypatch.setattr(r, "select_buy_list", lambda final_rows_arg, buy_top: list(buy3))
    monkeypatch.setattr(r, "prepare_report_row", lambda row: dict(row))
    monkeypatch.setattr(r, "render_final_markdown", lambda top10, buy_rows, counts, watch=None, flow_status=None: json.dumps({
        "symbols": [row["symbol"] for row in top10],
        "buy_symbols": [row["symbol"] for row in buy_rows],
        "status": flow_status,
    }, ensure_ascii=False))
    monkeypatch.setattr(r, "render_buy_markdown", lambda buy_rows, counts: json.dumps(buy_rows, ensure_ascii=False))
    monkeypatch.setattr(r, "humanize_report_chinese", lambda text: text)
    monkeypatch.setattr(r, "update_watchlist", lambda top10, buy_rows, env: {"symbols": [row["symbol"] for row in top10], "buy_symbols": [row["symbol"] for row in buy_rows]})

    def fake_telegram_send(env: dict[str, str], text: str) -> None:
        if telegram_error is not None:
            raise telegram_error

    monkeypatch.setattr(r, "telegram_send", fake_telegram_send)

    r.main()


def test_main_formal_publish_readiness_keeps_buy_rows_when_telegram_excluded(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    run_minimal_formal_main(monkeypatch, tmp_path, send_telegram=False)

    buy3 = json.loads((tmp_path / "outputs" / "buy_top3.json").read_text(encoding="utf-8"))
    status = json.loads((tmp_path / "outputs" / "pipeline_status.json").read_text(encoding="utf-8"))

    assert [row["symbol"] for row in buy3] == ["NVDA"]
    assert status["overall_status"] == "ok"
    assert status["can_publish_buy_report"] is True
    assert status["telegram_sent"] is None


def test_main_persists_successful_telegram_status_after_send(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    run_minimal_formal_main(monkeypatch, tmp_path, send_telegram=True)

    status = json.loads((tmp_path / "outputs" / "pipeline_status.json").read_text(encoding="utf-8"))

    assert status["overall_status"] == "ok"
    assert "telegram" in status["completed_layers"]
    assert status["telegram_sent"] is True


def test_main_persists_failed_telegram_status_before_reraising(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    with pytest.raises(RuntimeError, match="telegram boom"):
        run_minimal_formal_main(monkeypatch, tmp_path, send_telegram=True, telegram_error=RuntimeError("telegram boom"))

    status = json.loads((tmp_path / "outputs" / "pipeline_status.json").read_text(encoding="utf-8"))

    assert status["overall_status"] == "failed"
    assert "telegram" in status["missing_layers"]
    assert status["telegram_sent"] is False


def test_write_stage_status_adds_public_display_fields(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(r, "OUTPUTS", tmp_path)

    status = r.write_stage_status(
        run_mode="formal",
        stage="ta_in_progress",
        overall_status="failed",
        blocking_reasons=["TradingAgents 未正式完成：NVDA"],
        completed_layers=["dsa"],
        missing_layers=["tradingagents"],
    )

    assert status["public_overall_status"] == "失败"
    assert "深度投研层未正式完成：NVDA" in status["public_blocking_reasons"]
    assert status["public_completed_layers"] == "初筛层"
    assert status["public_missing_layers"] == "深度投研层"


def test_watchlist_render_public_text_and_refreshes_stale_price(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(r, "OUTPUTS", tmp_path)
    (tmp_path / "candidates_top50.json").write_text(
        '[{"symbol":"STX","close":1070.23,"ret_20d":45.94}]',
        encoding="utf-8",
    )
    (tmp_path / "watchlist_state.json").write_text(
        """
{
  "positions": {
    "STX": {
      "symbol": "STX",
      "name": "希捷",
      "first_seen": "2026-06-20",
      "seen_count": 1,
      "status": "watching",
      "last_score": 59.0,
      "last_close": "$107.02",
      "buy_zone": "$98.35-$104.13",
      "breakout_price": "$109.70",
      "stop_loss": "$92.94",
      "quality_note": "TradingAgents 未给出买入；UZI 投委分低于 60",
      "trade_trigger": "Vibe-Trading 未正式完成：fallback"
    }
  },
  "history": []
}
""",
        encoding="utf-8",
    )

    result = r.update_watchlist([], [], {})
    text = result["watch_report"]

    assert "$1070.23" in text
    assert "$107.02" not in text
    assert "TradingAgents" not in text
    assert "UZI" not in text
    assert "Vibe-Trading" not in text
    assert "深度投研层未给出买入" in text
