from __future__ import annotations

import monitor_buy_triggers as m


def base_row(**overrides):
    row = {
        "symbol": "WDC",
        "name": "西部数据",
        "action": "买入",
        "rating": "Buy",
        "buy_zone": "$62.59-$66.27",
        "breakout_price": "$69.81",
        "stop_loss": "$59.15",
        "take_profit_1": "$74.24",
        "take_profit_2": "$79.35",
        "quality_note": "",
        "uzi_score": 66.0,
        "ta_status": "full",
        "total_score": 72.0,
    }
    row.update(overrides)
    return row


def quote(**overrides):
    data = {"close": 64.9, "ret_20d": 18.0, "volume_ratio": 1.15, "provider": "test"}
    data.update(overrides)
    return data


def test_pullback_trigger_sends_when_price_and_quality_pass() -> None:
    decision = m.evaluate_trigger(base_row(), quote(), {"MONITOR_PRICE_SCALE_INVALID_SYMBOLS": ""})

    assert decision.should_alert is True
    assert decision.trigger_type == "回踩买入"
    assert "进入回踩买入区" in decision.reason


def test_price_touch_does_not_alert_when_uzi_is_below_threshold() -> None:
    decision = m.evaluate_trigger(
        base_row(uzi_score=44.8, quality_note="UZI 投委分低于 60；UZI 投委结论偏谨慎或看空"),
        quote(close=64.9),
    )

    assert decision.should_alert is False
    assert "UZI 投委分低于 60" in "；".join(decision.blocks)


def test_breakout_requires_price_above_breakout_and_not_overheated() -> None:
    decision = m.evaluate_trigger(
        base_row(),
        quote(close=70.2, ret_20d=24.0, volume_ratio=1.35),
        {"MONITOR_PRICE_SCALE_INVALID_SYMBOLS": ""},
    )

    assert decision.should_alert is True
    assert decision.trigger_type == "突破确认"


def test_breakout_does_not_alert_when_ret20_is_overheated() -> None:
    decision = m.evaluate_trigger(base_row(), quote(close=70.2, ret_20d=34.0, volume_ratio=1.35))

    assert decision.should_alert is False
    assert "20日涨幅 34.0% 仍过热" in "；".join(decision.blocks)


def test_dedup_key_includes_symbol_and_trigger_type() -> None:
    decision = m.evaluate_trigger(base_row(), quote(close=70.2, ret_20d=24.0, volume_ratio=1.35))

    assert decision.dedup_key.startswith("WDC|突破确认|")


def test_price_scale_invalid_symbol_never_alerts() -> None:
    decision = m.evaluate_trigger(
        base_row(uzi_score=80.0, rating="Buy", quality_note=""),
        quote(close=64.9, ret_20d=12.0),
        {"MONITOR_PRICE_SCALE_INVALID_SYMBOLS": "WDC"},
    )

    assert decision.should_alert is False
    assert "价格口径未通过" in "；".join(decision.blocks)


def test_price_scale_ratio_mismatch_never_alerts() -> None:
    decision = m.evaluate_trigger(
        base_row(reference_price="$681.08", buy_zone="$62.59-$66.27"),
        quote(close=64.9, ret_20d=12.0),
        {"MONITOR_PRICE_SCALE_INVALID_SYMBOLS": ""},
    )

    assert decision.should_alert is False
    assert "价格口径未通过" in "；".join(decision.blocks)


def test_trade_level_scale_mismatch_never_alerts_even_when_price_touches_zone() -> None:
    decision = m.evaluate_trigger(
        base_row(
            reference_price="$681.08",
            buy_zone="$625.91-$662.69",
            breakout_price="$69.81",
            stop_loss="$59.15",
            take_profit_1="$74.24",
            take_profit_2="$79.35",
        ),
        quote(close=640.0, ret_20d=12.0),
        {"MONITOR_PRICE_SCALE_INVALID_SYMBOLS": ""},
    )

    assert decision.should_alert is False
    assert "价位一致性未通过" in "；".join(decision.blocks)


def test_load_cached_quote_reads_symbols_list_payload(tmp_path, monkeypatch) -> None:
    out = tmp_path / "outputs"
    out.mkdir()
    (out / "enriched_stock_data.json").write_text(
        """
{
  "status": "ok",
  "symbols": [
    {"symbol": "WDC", "close": 68.108, "ret_20d": 41.3, "volume_ratio": 2.36, "provider": "cache"}
  ]
}
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(m, "OUTPUTS", out)

    data = m.load_cached_quote("WDC")

    assert data["close"] == 68.108
    assert data["provider"] == "cache"
