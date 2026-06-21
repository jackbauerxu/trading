from __future__ import annotations

from pathlib import Path

import run_daily_pipeline as r


def test_read_stock_pool_keeps_on_ticker_as_string(tmp_path) -> None:
    pool = tmp_path / "stock_pool.yaml"
    pool.write_text(
        """
groups:
  us_core:
    - {symbol: ON, name: "安森美"}
""",
        encoding="utf-8",
    )

    rows = r.read_stock_pool(pool)

    assert rows == [{"symbol": "ON", "name": "安森美", "group": "us_core"}]
    assert all(row["symbol"] != "TRUE" for row in rows)


def test_fetch_fmp_fundamentals_keeps_partial_payload_when_estimates_fail(monkeypatch) -> None:
    def fake_http_json(url: str, params: dict[str, object], timeout: int = 18):
        if "/profile/" in url:
            return [{
                "symbol": "GE",
                "companyName": "GE Aerospace",
                "industry": "Aerospace & Defense",
                "price": 210.5,
                "mktCap": 228000000000,
                "pe": 37.2,
                "priceToBookRatio": 7.4,
                "eps": 5.66,
                "lastDiv": 1.44,
            }]
        if "/income-statement/" in url:
            return [
                {"calendarYear": "2024", "revenue": 38900000000, "netIncome": 6600000000},
                {"calendarYear": "2025", "revenue": 41600000000, "netIncome": 7200000000},
            ]
        if "/ratios/" in url:
            return [
                {
                    "calendarYear": "2024",
                    "returnOnEquity": 0.184,
                    "currentRatio": 1.21,
                    "debtRatio": 0.41,
                    "freeCashFlowOperatingCashFlowRatio": 0.44,
                    "returnOnCapitalEmployed": 0.162,
                },
                {
                    "calendarYear": "2025",
                    "returnOnEquity": 0.197,
                    "currentRatio": 1.28,
                    "debtRatio": 0.39,
                    "freeCashFlowOperatingCashFlowRatio": 0.48,
                    "returnOnCapitalEmployed": 0.171,
                },
            ]
        if "/analyst-estimates/" in url:
            raise RuntimeError("plan limit")
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(r, "http_json", fake_http_json)

    data = r.fetch_fmp_fundamentals("GE", {"FMP_API_KEY": "demo"})

    assert data["name"] == "GE Aerospace"
    assert data["market_cap_raw"] == 228000000000
    assert data["pe"] == 37.2
    assert data["pb"] == 7.4
    assert data["revenue_history"] == [389.0, 416.0]
    assert data["net_profit_history"] == [66.0, 72.0]
    assert data["roe_history"] == [18.4, 19.7]


def test_fetch_pipeline_fundamentals_merges_partial_sources(monkeypatch) -> None:
    monkeypatch.setattr(r, "fetch_fmp_fundamentals", lambda symbol, env: {
        "source": "FMP",
        "name": "GE Aerospace",
        "industry": "Aerospace & Defense",
        "price": 210.5,
        "market_cap_raw": 0,
        "market_cap_yi": 0,
        "pe": 0,
        "pb": 0,
        "revenue_history": [],
        "net_profit_history": [],
        "roe_history": [],
        "financial_years": [],
        "financial_health": {},
    })
    monkeypatch.setattr(r, "fetch_alpha_overview", lambda symbol, env: {
        "source": "Alpha Vantage",
        "pb": 7.1,
        "target_price": 235.0,
        "forward_pe": 31.5,
        "eps_next_year": 6.8,
    })
    monkeypatch.setattr(r, "fetch_yahoo_fundamentals", lambda symbol, env: {
        "source": "Yahoo",
        "market_cap_raw": 228000000000,
        "market_cap_yi": 2280.0,
        "pe": 37.2,
        "revenue_history": [332.0, 356.0, 389.0, 416.0],
        "net_profit_history": [48.0, 55.0, 66.0, 72.0],
        "roe_history": [13.2, 14.8, 18.4, 19.7],
        "financial_years": ["2022", "2023", "2024", "2025"],
        "coverage_count": 19,
        "buy_rating_pct": 68.4,
        "financial_health": {"net_margin": 17.3, "current_ratio": 1.28},
    })
    monkeypatch.setattr(r, "fetch_eastmoney_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_sec_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_quote_fallback_fundamentals", lambda symbol: {})

    data = r.fetch_pipeline_fundamentals("GE", {})

    assert data["name"] == "GE Aerospace"
    assert data["market_cap_raw"] == 228000000000
    assert data["market_cap_yi"] == 2280.0
    assert data["pe"] == 37.2
    assert data["pb"] == 7.1
    assert data["target_price"] == 235.0
    assert data["forward_pe"] == 31.5
    assert data["revenue_history"] == [332.0, 356.0, 389.0, 416.0]
    assert data["net_profit_history"] == [48.0, 55.0, 66.0, 72.0]
    assert data["roe_history"] == [13.2, 14.8, 18.4, 19.7]
    assert data["coverage_count"] == 19
    assert data["buy_rating_pct"] == 68.4
    assert data["source"] == "FMP+Alpha Vantage+Yahoo"


def test_build_uzi_seed_raw_clears_financial_and_valuation_gaps_when_fundamentals_complete() -> None:
    raw = r.build_uzi_seed_raw(
        symbol="GE",
        uzi_symbol="GE",
        item={"symbol": "GE", "name": "GE Aerospace"},
        dsa_item={"close": 210.5, "pct_1d": 1.2, "ret_20d": 8.6, "volume_ratio": 1.14},
        market_row={"close": 210.5},
        fundamentals={
            "source": "FMP+Alpha Vantage+Yahoo",
            "name": "GE Aerospace",
            "industry": "Aerospace & Defense",
            "price": 210.5,
            "market_cap_raw": 228000000000,
            "market_cap_yi": 2280.0,
            "pe": 37.2,
            "pb": 7.1,
            "eps": 5.66,
            "dividend_yield": 0.68,
            "revenue_history": [332.0, 356.0, 389.0, 416.0],
            "net_profit_history": [48.0, 55.0, 66.0, 72.0],
            "roe_history": [13.2, 14.8, 18.4, 19.7],
            "financial_years": ["2022", "2023", "2024", "2025"],
            "financial_health": {"current_ratio": 1.28, "net_margin": 17.3},
            "target_price": 235.0,
            "coverage_count": 19,
            "buy_rating_pct": 68.4,
            "eps_next_year": 6.8,
            "forward_pe": 31.5,
        },
    )

    flags = r.uzi_seed_data_quality_flags(raw)

    assert "UZI 财务维度不足" not in flags
    assert "UZI 估值维度不足" not in flags


def test_fetch_pipeline_fundamentals_uses_eastmoney_and_sec_when_primary_sources_thin(monkeypatch) -> None:
    monkeypatch.setattr(r, "fetch_fmp_fundamentals", lambda symbol, env: {"source": "FMP", "name": "Micron"})
    monkeypatch.setattr(r, "fetch_alpha_overview", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_yahoo_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_eastmoney_fundamentals", lambda symbol, env: {
        "source": "Eastmoney",
        "pb": 2.8,
        "revenue_history": [198.0, 214.0, 236.0, 254.0],
        "net_profit_history": [22.0, 28.0, 33.0, 41.0],
        "roe_history": [8.6, 10.2, 11.5, 13.1],
        "financial_years": ["2022", "2023", "2024", "2025"],
        "financial_health": {"current_ratio": 2.6, "net_margin": 16.1},
    })
    monkeypatch.setattr(r, "fetch_sec_fundamentals", lambda symbol, env: {
        "source": "SEC",
        "eps": 6.25,
    })

    data = r.fetch_pipeline_fundamentals("MU", {})

    assert data["name"] == "Micron"
    assert data["pb"] == 2.8
    assert data["eps"] == 6.25
    assert data["revenue_history"] == [198.0, 214.0, 236.0, 254.0]
    assert data["net_profit_history"] == [22.0, 28.0, 33.0, 41.0]
    assert data["roe_history"] == [8.6, 10.2, 11.5, 13.1]
    assert data["source"] == "FMP+Eastmoney+SEC"


def test_derive_roe_from_sec_builds_percent_series() -> None:
    us_gaap = {
        "NetIncomeLoss": {
            "units": {
                "USD": [
                    {"form": "10-K", "end": "2024-08-31", "val": 7780000000},
                    {"form": "10-K", "end": "2025-08-31", "val": 9240000000},
                ]
            }
        },
        "StockholdersEquity": {
            "units": {
                "USD": [
                    {"form": "10-K", "end": "2024-08-31", "val": 54800000000},
                    {"form": "10-K", "end": "2025-08-31", "val": 60300000000},
                ]
            }
        },
    }

    roe = r.derive_roe_from_sec(us_gaap)

    assert roe == [14.1971, 15.3234]


def test_fetch_pipeline_fundamentals_uses_quote_fallback_and_tracks_field_sources(monkeypatch) -> None:
    monkeypatch.setattr(r, "fetch_fmp_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_alpha_overview", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_yahoo_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_eastmoney_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_sec_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_akshare_hk_valuation", lambda symbol: {})
    monkeypatch.setattr(r, "fetch_quote_fallback_fundamentals", lambda symbol: {
        "source": "QuoteFallback/TencentQuote",
        "name": "Tencent Holdings",
        "price": 457.2,
        "market_cap_yi": 41850.0,
        "market_cap_raw": 4185000000000.0,
        "pe": 18.9,
        "pb": 3.12,
        "_field_sources": {
            "price": "QuoteFallback/TencentQuote",
            "market_cap_yi": "QuoteFallback/TencentQuote",
            "market_cap_raw": "QuoteFallback/TencentQuote",
            "pe": "QuoteFallback/TencentQuote",
            "pb": "QuoteFallback/TencentQuote",
        },
    })

    data = r.fetch_pipeline_fundamentals("hk00700", {})

    assert data["name"] == "Tencent Holdings"
    assert data["price"] == 457.2
    assert data["pe"] == 18.9
    assert data["pb"] == 3.12
    assert data["source"] == "QuoteFallback/TencentQuote"
    assert data["_field_sources"]["price"] == "QuoteFallback/TencentQuote"


def test_hk_quote_fallback_price_clears_basic_uzi_gap(monkeypatch) -> None:
    monkeypatch.setattr(r, "global_hk_quote_tencent", lambda code: {
        "provider": "global/tencent",
        "name": "舜宇光学科技",
        "close": 79.55,
    })
    monkeypatch.setattr(r, "global_hk_quote_sina", lambda code: {})
    monkeypatch.setattr(r, "global_eastmoney_quote", lambda code, prefix: {})

    data = r.fetch_quote_fallback_fundamentals("hk02382")
    raw = r.build_uzi_seed_raw(
        symbol="hk02382",
        uzi_symbol="hk02382",
        item={"symbol": "hk02382", "name": "舜宇光学科技"},
        dsa_item={"close": 79.55, "ret_20d": 8.0},
        market_row={"close": 79.55},
        fundamentals=data,
    )

    assert data["price"] == 79.55
    assert data["_field_sources"]["price"] == "QuoteFallback/TencentQuote"
    assert "UZI 基础行情缺失" not in r.uzi_seed_data_quality_flags(raw)


def test_build_uzi_seed_raw_embeds_field_sources() -> None:
    raw = r.build_uzi_seed_raw(
        symbol="hk00700",
        uzi_symbol="hk00700",
        item={"symbol": "hk00700", "name": "腾讯控股"},
        dsa_item={"close": 457.2, "pct_1d": -1.8, "ret_20d": -3.52, "volume_ratio": 0.62},
        market_row={"close": 457.2},
        fundamentals={
            "source": "QuoteFallback/TencentQuote",
            "name": "Tencent Holdings",
            "price": 457.2,
            "market_cap_yi": 41850.0,
            "market_cap_raw": 4185000000000.0,
            "pe": 18.9,
            "pb": 3.12,
            "_field_sources": {
                "price": "QuoteFallback/TencentQuote",
                "market_cap_yi": "QuoteFallback/TencentQuote",
                "market_cap_raw": "QuoteFallback/TencentQuote",
                "pe": "QuoteFallback/TencentQuote",
                "pb": "QuoteFallback/TencentQuote",
            },
        },
    )

    assert raw["field_sources"]["price"] == "QuoteFallback/TencentQuote"
    assert raw["dimensions"]["0_basic"]["data"]["field_sources"]["pb"] == "QuoteFallback/TencentQuote"


def test_fetch_akshare_hk_valuation_prefers_eniu_and_baidu(monkeypatch) -> None:
    class FakeAk:
        @staticmethod
        def stock_hk_eniu_indicator(symbol, indicator):
            assert symbol == "00522"
            if indicator == "市盈率":
                return [{"日期": "2026-06-19", "市盈率": 342.3}]
            if indicator == "市净率":
                return [{"日期": "2026-06-19", "市净率": 6.8}]
            if indicator == "市值":
                return [{"日期": "2026-06-19", "总市值": 122100000000.0}]
            return []

        @staticmethod
        def stock_hk_valuation_baidu(symbol, indicator):
            assert symbol == "00522"
            if indicator == "总市值":
                return [{"date": "2026-06-19", "value": "1221亿"}]
            if indicator == "市净率":
                return [{"date": "2026-06-19", "value": 6.8}]
            return []

    monkeypatch.setattr(r, "import_akshare_module", lambda: FakeAk)

    data = r.fetch_akshare_hk_valuation("hk00522")

    assert data["source"] == "AKShareHKValuation"
    assert data["pe"] == 342.3
    assert data["pb"] == 6.8
    assert data["market_cap_raw"] == 122100000000.0
    assert data["market_cap_yi"] == 1221.0
    assert data["_field_sources"]["pb"] == "AKShareHKValuation"


def test_fetch_pipeline_fundamentals_adds_akshare_for_hk_when_valuation_missing(monkeypatch) -> None:
    monkeypatch.setattr(r, "fetch_fmp_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_alpha_overview", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_yahoo_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_eastmoney_fundamentals", lambda symbol, env: {
        "source": "Eastmoney",
        "eps": 0.61,
        "revenue_history": [260.0, 318.0],
        "net_profit_history": [3.5, 6.1],
        "roe_history": [5.2, 8.6],
    })
    monkeypatch.setattr(r, "fetch_sec_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_marketdata_valuation_fallback", lambda symbol, env, merged: {})
    monkeypatch.setattr(r, "fetch_akshare_hk_valuation", lambda symbol: {
        "source": "AKShareHKValuation",
        "market_cap_raw": 122100000000.0,
        "market_cap_yi": 1221.0,
        "pb": 6.8,
        "pe": 342.3,
        "_field_sources": {
            "market_cap_raw": "AKShareHKValuation",
            "market_cap_yi": "AKShareHKValuation",
            "pb": "AKShareHKValuation",
            "pe": "AKShareHKValuation",
        },
    })
    monkeypatch.setattr(r, "fetch_quote_fallback_fundamentals", lambda symbol: {})

    data = r.fetch_pipeline_fundamentals("hk00522", {})

    assert data["eps"] == 0.61
    assert data["pb"] == 6.8
    assert data["market_cap_raw"] == 122100000000.0
    assert data["market_cap_yi"] == 1221.0
    assert data["source"] == "Eastmoney+AKShareHKValuation"
    assert data["_field_sources"]["market_cap_yi"] == "AKShareHKValuation"


def test_build_enriched_stock_data_merges_market_candidate_and_fundamentals(monkeypatch) -> None:
    monkeypatch.setattr(r, "OUTPUTS", r.ROOT / "work" / "test_outputs_enriched")
    monkeypatch.setattr(r, "fetch_pipeline_fundamentals", lambda symbol, env: {
        "source": "FMP+SEC",
        "price": 103.1,
        "pe": 18.5,
        "pb": 4.2,
        "revenue_history": [100.0, 120.0],
        "net_profit_history": [10.0, 15.0],
        "roe_history": [12.0, 14.0],
        "_field_sources": {"pe": "FMP", "revenue_history": "SEC"},
    })
    context = r.build_enriched_stock_data(
        [{"symbol": "STX", "name": "Seagate", "group": "semis"}],
        {"PIPELINE_ENRICH_FUNDAMENTAL_SCOPE": "candidates"},
        openbb_context={
            "symbols": [{
                "symbol": "STX",
                "original_symbol": "STX",
                "provider": "tickdb/kline",
                "close": 103.1,
                "ret_20d": 10.0,
                "volume_ratio": 1.4,
                "klines": [{"date": "2026-06-17", "open": 102, "high": 104, "low": 101, "close": 103.1, "volume": 1000}],
            }]
        },
        candidates=[{"symbol": "STX", "score": 82, "close": 103.1, "ret_20d": 10.0, "volume_ratio": 1.4}],
    )

    row = context["symbols"][0]
    assert row["symbol"] == "STX"
    assert row["fundamentals"]["pe"] == 18.5
    assert row["field_sources"]["klines"] == "tickdb/kline"
    assert row["field_sources"]["score"] == "daily_stock_analysis"
    assert "financial_statement_thin" not in row["data_quality_flags"]
    assert (r.OUTPUTS / "enriched_stock_data.json").exists()


def test_openbb_snippet_does_not_auto_divide_real_high_us_prices() -> None:
    assert 'scaled_feeds = ("sina", "tiingo", "tickdb", "stock-api")' not in r.OPENBB_SNIPPET
    assert "250 <= latest <= 2500" not in r.OPENBB_SNIPPET


def test_quote_price_scale_does_not_auto_divide_real_high_us_prices() -> None:
    assert r.normalize_marketdata_quote_price("AMD", 507.29) == 507.29


def test_score_from_stock_daily_prefers_fresh_openbb_over_stale_enriched(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(r, "OUTPUTS", tmp_path)
    r.write_json(tmp_path / "enriched_stock_data.json", {
        "symbols": [{"symbol": "AMAT", "close": 61.17, "ret_20d": 30.14, "volume_ratio": 1.25}]
    })

    rows = r.score_from_stock_daily(
        tmp_path / "missing.db",
        [{"symbol": "AMAT", "name": "Applied Materials", "group": "us"}],
        {"symbols": [{"symbol": "AMAT", "original_symbol": "AMAT", "close": 568.23, "ret_20d": 30.14, "volume_ratio": 1.25}]},
    )

    assert rows[0]["close"] == 568.23


def test_pretrade_audit_blocks_price_scale_mismatch() -> None:
    audit = r.pretrade_consistency_audit(
        {"symbol": "AMAT", "close": 61.17, "buy_zone": "$56.63-$59.66", "breakout_price": "$62.70"},
        {"symbol": "AMAT", "close": 568.23},
        {"symbol": "AMAT"},
    )

    assert not audit["passed"]
    assert "价格口径未通过" in audit["gates"]


def test_pretrade_audit_passes_consistent_price_levels() -> None:
    audit = r.pretrade_consistency_audit(
        {
            "symbol": "AMAT",
            "close": 568.23,
            "buy_zone": "$526.04-$554.17",
            "breakout_price": "$582.44",
            "stop_loss": "$497.11",
            "take_profit_1": "$619.37",
            "take_profit_2": "$661.99",
        },
        {"symbol": "AMAT", "close": 568.23},
        {"symbol": "AMAT"},
    )

    assert audit["passed"]
    assert audit["gates"] == []


def test_pretrade_audit_flags_ge_mapping_when_theme_mentions_power_chain() -> None:
    audit = r.pretrade_consistency_audit(
        {"symbol": "GE", "close": 351.73, "reason": "数据中心电力链 800V DC"},
        {"symbol": "GE", "close": 351.73},
        {"symbol": "GE"},
    )

    assert not audit["passed"]
    assert "ticker 映射未确认" in audit["gates"]


def test_merge_scores_reaudits_generated_trade_levels_before_buy_eligibility(monkeypatch) -> None:
    def bad_trade_advice(total, action, risk, market=None):
        return {
            "trade_advice": "可以进入买入观察区",
            "buy_advice": "回踩买入区 $56.63-$59.66",
            "sell_advice": "跌破 $53.51 止损",
            "position_advice": "建议仓位 10%-20%",
            "price_plan": "参考价 $568.23；回踩区 $56.63-$59.66；突破确认 $62.70；止损 $53.51；止盈 $66.68 / $71.26。",
            "reference_price": "$568.23",
            "buy_zone": "$56.63-$59.66",
            "breakout_price": "$62.70",
            "stop_loss": "$53.51",
            "take_profit_1": "$66.68",
            "take_profit_2": "$71.26",
        }

    monkeypatch.setattr(r, "make_trade_advice", bad_trade_advice)

    rows = r.merge_scores(
        candidates=[{
            "symbol": "AMAT",
            "name": "Applied Materials",
            "score": 92,
            "close": 568.23,
            "ret_20d": 8.0,
            "volume_ratio": 1.1,
            "reason": "半导体设备趋势改善",
        }],
        trading=[{
            "symbol": "AMAT",
            "name": "Applied Materials",
            "action": "BUY",
            "confidence": 0.88,
            "risk": "medium",
            "ta_status": "full",
            "reason": "多头占优",
        }],
        uzi=[{
            "symbol": "AMAT",
            "name": "Applied Materials",
            "status": "ok",
            "uzi_score": 82,
            "rating": "买入",
            "reason": "财务与估值通过",
            "quality_flags": [],
        }],
        top_n=1,
    )

    row = rows[0]
    assert not row["buy_eligible"]
    assert row["trade_bucket"] == "D"
    assert "价位一致性未通过" in row["quality_gates"]
    assert not row["pretrade_audit"]["passed"]


def test_report_sanitizes_raw_agent_prices_for_any_symbol() -> None:
    row = {
        "symbol": "ARM",
        "name": "Arm",
        "total_score": 66,
        "dsa_score": 70,
        "tradingagents_score": 72,
        "uzi_score": 61,
        "raw_uzi_score": 61,
        "committee_score_source": "UZI-Skill",
        "rating": "观察",
        "action": "观察",
        "risk": "中",
        "ta_status": "full",
        "buy_eligible": False,
        "trade_bucket": "D",
        "trade_bucket_label": "D档 Watch Only",
        "trade_trigger": "旧触发：当前42.1跌破38.8",
        "quality_gates": ["UZI 投委分低于 60"],
        "quality_note": "UZI 投委分低于 60",
        "pretrade_audit": {"passed": True, "gates": [], "checks": ["价格真实性：通过"]},
        "trade_advice": "不建议新开仓",
        "buy_advice": "旧买入：40.2-42.6",
        "sell_advice": "旧卖出：36.8",
        "position_advice": "建议 0%",
        "price_plan": "参考价 $420.00；回踩区 $402.00-$426.00；突破确认 $431.00；止损 $368.00；止盈 $455.00 / $490.00。",
        "reference_price": "$420.00",
        "buy_zone": "$402.00-$426.00",
        "breakout_price": "$431.00",
        "stop_loss": "$368.00",
        "take_profit_1": "$455.00",
        "take_profit_2": "$490.00",
        "reason": "TradingAgents 原文：当前42.1，回踩40.2-42.6，跌破36.8退出。",
        "tradingagents_reason": "Executive Summary 当前42.1，不在43-45追入。",
        "uzi_reason": "估值参考 39.5。",
    }

    p_row = r.prepare_report_row(row)
    text = "\n".join(str(p_row.get(k, "")) for k in ("buy_advice", "sell_advice", "trade_trigger", "reason", "tradingagents_reason", "uzi_reason"))

    for bad in ("当前42.1", "40.2-42.6", "36.8", "43-45", "39.5"):
        assert bad not in text
    assert "$420.00" in p_row["buy_advice"]
    assert "$402.00-$426.00" in p_row["buy_advice"]
    assert "$368.00" in p_row["sell_advice"]
    assert "参考价 $420.00" in p_row["reason"]
    assert "回踩区 $402.00-$426.00" in p_row["reason"]
    assert "止损 $368.00" in p_row["reason"]


def test_risk_capped_score_keeps_raw_score_and_uses_clear_report_label() -> None:
    rows = r.merge_scores(
        candidates=[{
            "symbol": "STX",
            "name": "Seagate",
            "score": 90,
            "close": 1070.23,
        }],
        trading=[{
            "symbol": "STX",
            "name": "Seagate",
            "action": "BUY",
            "confidence": 0.8625,
            "risk": "medium",
            "ta_status": "full",
            "reason": "深度研究通过",
        }],
        uzi=[{
            "symbol": "STX",
            "name": "Seagate",
            "status": "ok",
            "uzi_score": 55,
            "rating": "观察",
            "reason": "投委偏谨慎",
            "quality_flags": ["UZI 投委分低于 60"],
        }],
        top_n=1,
    )

    row = rows[0]
    assert row["raw_total_score"] == 78.0
    assert row["risk_adjusted_score"] == 59.0
    assert row["total_score"] == 59.0

    markdown = r.render_final_markdown(rows, [], {"universe": 1, "openbb": 1, "screen": 1, "research": 1, "committee": 1})

    assert "风控后综合分：59.0" in markdown
    assert "原始综合分：78.0" in markdown
    assert not any(line.startswith("综合分：") for line in markdown.splitlines())


def test_hold_with_zero_new_position_is_worded_as_no_new_buy() -> None:
    row = {
        "symbol": "GE",
        "name": "GE Aerospace",
        "total_score": 59.0,
        "raw_total_score": 69.61,
        "risk_adjusted_score": 59.0,
        "dsa_score": 76.51,
        "tradingagents_score": 76.51,
        "uzi_score": 53.5,
        "raw_uzi_score": 53.5,
        "committee_score_source": "备用投委评分",
        "rating": "回避",
        "action": "持有",
        "risk": "中",
        "ta_status": "full",
        "buy_eligible": False,
        "trade_bucket": "D",
        "trade_bucket_label": "D档 Watch Only",
        "trade_trigger": "只观察；硬性闸门未通过。",
        "quality_gates": ["UZI 投委分低于 60"],
        "quality_note": "UZI 投委分低于 60",
        "pretrade_audit": {"passed": True, "gates": [], "checks": []},
        "trade_advice": "只观察，不建议新开仓",
        "buy_advice": "不买入",
        "sell_advice": "跌破止损退出",
        "position_advice": "建议 0% 新仓；已有仓位按原计划风控",
        "price_plan": "参考价 $200.00；回踩区 $190.00-$198.00；突破确认 $205.00；止损 $180.00；止盈 $220.00 / $240.00。",
        "reference_price": "$200.00",
        "buy_zone": "$190.00-$198.00",
        "breakout_price": "$205.00",
        "stop_loss": "$180.00",
        "take_profit_1": "$220.00",
        "take_profit_2": "$240.00",
        "reason": "硬性闸门未通过",
    }

    markdown = r.render_final_markdown([row], [], {"universe": 1, "openbb": 1, "screen": 1, "research": 1, "committee": 1})

    assert "操作：持有" in markdown
    assert "仓位建议：空仓不买，已有仓位按止损止盈管理" in markdown
    assert "仓位建议：建议 0% 新仓" not in markdown


def test_report_row_removes_tool_unavailable_error_from_stored_reasons() -> None:
    row = {
        "symbol": "GS",
        "name": "Goldman Sachs",
        "total_score": 59.0,
        "raw_total_score": 65.57,
        "risk_adjusted_score": 59.0,
        "dsa_score": 70,
        "tradingagents_score": 70,
        "uzi_score": 50,
        "raw_uzi_score": 50,
        "committee_score_source": "备用投委评分",
        "rating": "回避",
        "action": "持有",
        "risk": "中",
        "ta_status": "full",
        "buy_eligible": False,
        "trade_bucket": "D",
        "trade_bucket_label": "D档 Watch Only",
        "quality_gates": ["UZI 投委分低于 60"],
        "quality_note": "UZI 投委分低于 60",
        "pretrade_audit": {"passed": True, "gates": [], "checks": []},
        "trade_advice": "只观察，不建议新开仓",
        "buy_advice": "不买入",
        "sell_advice": "跌破止损退出",
        "position_advice": "空仓不买，已有仓位按止损止盈管理",
        "tradingagents_reason": "注意：按要求已尝试调用 get_verified_market_snapshot，但工具返回不可用错误；因此下列精确数值来自指标输出。盈利修复仍然有效。",
        "uzi_reason": "复核偏谨慎。",
        "reason": "工具返回不可用错误；盈利修复仍然有效。",
    }

    prepared = r.prepare_report_row(row)
    combined = "\n".join(str(prepared.get(key, "")) for key in ("reason", "tradingagents_reason", "uzi_reason"))

    assert "工具返回不可用错误" not in combined
    assert "不可用错误" not in combined
    assert "get_verified_market_snapshot" not in combined
    assert "盈利修复仍然有效" in combined


def test_strip_actionable_price_sentences_keeps_non_execution_metrics() -> None:
    text = "20日涨跌 12.3%，量比 1.45，PE 18.2；当前价 42.1 高于买区 40.2-42.6；跌破 36.8 退出。"
    out = r.strip_actionable_price_sentences(text)

    assert "20日涨跌 12.3%" in out
    assert "量比 1.45" in out
    assert "PE 18.2" in out
    assert "42.1" not in out
    assert "40.2-42.6" not in out
    assert "36.8" not in out


def test_dexter_runs_without_financial_datasets_key_by_dropping_financial_tool(monkeypatch, tmp_path) -> None:
    dexter_dir = tmp_path / "dexter"
    (dexter_dir / "scripts").mkdir(parents=True)
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    monkeypatch.setattr(r, "OUTPUTS", outputs)
    monkeypatch.setattr(r, "WORK", tmp_path / "work")
    captured = {}

    def fake_run(cmd, cwd, env, timeout):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["env"] = env
        return 0, """
__PIPELINE_JSON__
{"status":"ok","symbol_count":1,"symbols":[{"symbol":"WDC","status":"ok","stance":"中性","confidence":0.5,"summary":"完成辅助复核"}]}
"""

    monkeypatch.setattr(r, "run", fake_run)

    context = r.stage_dexter_context(
        [{"symbol": "WDC", "name": "Western Digital", "score": 80, "close": 681.08}],
        {
            "DEXTER_ENABLED": "1",
            "DEXTER_DIR": str(dexter_dir),
            "DEXTER_COMMAND": "bun",
            "OPENAI_API_KEY": "test",
            "DEXTER_REQUIRE_FINANCIAL_DATASETS": "1",
            "DEXTER_TOOL_ALLOWLIST": "get_financials,get_market_data,read_filings,web_search",
        },
    )

    assert context["status"] == "ok"
    assert context["symbol_count"] == 1
    assert "get_financials" not in captured["env"]["DEXTER_TOOL_ALLOWLIST"]
    assert "get_market_data" in captured["env"]["DEXTER_TOOL_ALLOWLIST"]


def test_dexter_payload_includes_shared_data_package(monkeypatch, tmp_path) -> None:
    dexter_dir = tmp_path / "dexter"
    (dexter_dir / "scripts").mkdir(parents=True)
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    r.write_json(outputs / "enriched_stock_data.json", {
        "status": "ok",
        "symbol_count": 1,
        "symbols": [{
            "symbol": "MU",
            "close": 128.5,
            "ret_20d": 9.2,
            "volume_ratio": 1.4,
            "fundamentals": {"source": "FMP+Alpha", "pe": 18.2, "revenue_history": [300, 360]},
            "klines": [{"date": "2026-06-17", "close": 128.5}],
            "field_sources": {"price": "OpenBB/FMP"},
            "data_quality_flags": [],
        }],
    })
    r.write_json(outputs / "kronos_context.json", {
        "symbols": [{"symbol": "MU", "trend": "up", "forecast_return_5d": 2.1, "confidence": 0.7}]
    })
    monkeypatch.setattr(r, "OUTPUTS", outputs)
    monkeypatch.setattr(r, "WORK", tmp_path / "work")
    captured = {}

    def fake_run(cmd, cwd, env, timeout):
        payload = r.read_json_if_exists(Path(env["PIPELINE_DEXTER_PAYLOAD_FILE"]), [])
        captured["payload"] = payload
        return 0, """
__PIPELINE_JSON__
{"status":"ok","symbol_count":1,"symbols":[{"symbol":"MU","status":"ok","stance":"中性","confidence":0.5,"summary":"完成辅助复核"}]}
"""

    monkeypatch.setattr(r, "run", fake_run)

    context = r.stage_dexter_context(
        [{"symbol": "MU", "name": "Micron", "score": 82, "close": 128.5, "external_signal": {"stance": "观察"}}],
        {"DEXTER_ENABLED": "1", "DEXTER_DIR": str(dexter_dir), "DEXTER_COMMAND": "bun", "OPENAI_API_KEY": "test"},
    )

    assert context["symbol_count"] == 1
    shared = captured["payload"][0]["shared_data"]
    assert shared["enriched"]["fundamentals"]["pe"] == 18.2
    assert shared["kronos"]["trend"] == "up"
    assert shared["candidate"]["score"] == 82
    assert shared["external_signal"]["stance"] == "观察"
    assert shared["data_quality"]["field_sources"]["price"] == "OpenBB/FMP"


def test_uzi_timeout_uses_requested_value_without_default_hard_cap() -> None:
    assert r.resolve_uzi_timeout({"PIPELINE_UZI_TIMEOUT_PER_STOCK": "600"}) == 600


def test_uzi_timeout_respects_explicit_hard_cap() -> None:
    assert r.resolve_uzi_timeout({"PIPELINE_UZI_TIMEOUT_PER_STOCK": "600", "PIPELINE_UZI_TIMEOUT_HARD_CAP": "240"}) == 240


def test_degraded_uzi_cache_is_not_reused_even_when_fresh() -> None:
    parsed = {
        "_cache_mtime": 9999999999,
        "synthesis": {
            "overall_score": 38.3,
            "verdict_detail": "基本面 56.9 · 共识 10.5",
            "agent_reviewed": False,
        },
        "panel": {"investors": [{"name": "Buffett", "score": 38}, {"name": "Lynch", "score": 39}, {"name": "Soros", "score": 37}]},
        "raw": {"dimensions": {}},
    }

    assert not r.uzi_cache_reusable(parsed, 18.0, {"symbol": "AMAT", "confidence": 0.8}, "AMAT", {})


def test_build_uzi_seed_raw_uses_market_cap_as_valuation_fallback() -> None:
    raw = r.build_uzi_seed_raw(
        symbol="RBLX",
        uzi_symbol="RBLX",
        item={"symbol": "RBLX", "name": "Roblox"},
        dsa_item={"close": 112.0, "ret_20d": 12.0},
        market_row={"close": 112.0, "market_cap_yi": 760.0},
        fundamentals={"source": "marketdata", "price": 112.0},
    )

    flags = r.uzi_seed_data_quality_flags(raw)

    assert "10_valuation" in raw["dimensions"]
    assert "UZI 估值维度不足" not in flags


def test_fetch_marketdata_valuation_fallback_uses_polygon_price_and_sec_shares(monkeypatch) -> None:
    monkeypatch.setattr(r, "fetch_polygon_quote_fundamentals", lambda symbol, env: {
        "source": "Polygon",
        "price": 103.13,
    })
    monkeypatch.setattr(r, "fetch_tiingo_quote_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_sec_valuation_fundamentals", lambda symbol, env: {
        "source": "SECValuation",
        "shares_outstanding": 210000000.0,
        "eps": 6.77,
    })

    data = r.fetch_marketdata_valuation_fallback("STX", {})

    assert data["price"] == 103.13
    assert data["shares_outstanding"] == 210000000.0
    assert data["market_cap_raw"] == 21657300000.0
    assert data["market_cap_yi"] == 216.573
    assert round(data["pe"], 2) == 15.23
    assert data["source"] == "Polygon+SECValuation"


def test_fetch_marketdata_valuation_fallback_survives_one_source_error(monkeypatch) -> None:
    def broken_tiingo(symbol, env):
        raise NameError("bad helper")

    monkeypatch.setattr(r, "fetch_polygon_quote_fundamentals", lambda symbol, env: {
        "source": "Polygon",
        "price": 103.13,
    })
    monkeypatch.setattr(r, "fetch_tiingo_quote_fundamentals", broken_tiingo)
    monkeypatch.setattr(r, "fetch_sec_valuation_fundamentals", lambda symbol, env: {
        "source": "SECValuation",
        "shares_outstanding": 210000000.0,
        "eps": 6.77,
    })

    data = r.fetch_marketdata_valuation_fallback("STX", {})

    assert data["market_cap_raw"] == 21657300000.0
    assert round(data["pe"], 2) == 15.23


def test_normalize_marketdata_quote_price_keeps_stx_high_price_scale() -> None:
    assert round(r.normalize_marketdata_quote_price("STX", 1031.34), 3) == 1031.34


def test_anchor_fundamentals_to_market_price_recalculates_10x_stale_valuation() -> None:
    data = r.anchor_fundamentals_to_market_price(
        {
            "price": 103.134,
            "eps": 8.71,
            "shares_outstanding": 217000000,
            "market_cap_raw": 22380078000,
            "market_cap_yi": 223.80078,
            "pe": 11.84,
        },
        1070.23,
        "global/sina_kline",
    )

    assert data["price"] == 1070.23
    assert round(data["pe"], 2) == 122.87
    assert round(data["market_cap_raw"], 0) == 232239910000
    assert data["_field_sources"]["price"] == "global/sina_kline"


def test_fetch_pipeline_fundamentals_uses_marketdata_valuation_fallback(monkeypatch) -> None:
    monkeypatch.setattr(r, "fetch_fmp_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_alpha_overview", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_yahoo_fundamentals", lambda symbol, env: {})
    monkeypatch.setattr(r, "fetch_eastmoney_fundamentals", lambda symbol, env: {
        "source": "Eastmoney",
        "revenue_history": [74.0, 81.0],
        "net_profit_history": [8.0, 11.0],
        "roe_history": [14.0, 18.0],
    })
    monkeypatch.setattr(r, "fetch_sec_fundamentals", lambda symbol, env: {
        "source": "SEC",
        "eps": 6.77,
    })
    monkeypatch.setattr(r, "fetch_marketdata_valuation_fallback", lambda symbol, env, merged=None: {
        "source": "Polygon+SECValuation",
        "price": 103.13,
        "shares_outstanding": 210000000.0,
        "market_cap_raw": 21657300000.0,
        "market_cap_yi": 216.573,
        "pe": 15.23,
    })
    monkeypatch.setattr(r, "fetch_quote_fallback_fundamentals", lambda symbol: {})

    data = r.fetch_pipeline_fundamentals("STX", {})
    raw = r.build_uzi_seed_raw(
        symbol="STX",
        uzi_symbol="STX",
        item={"symbol": "STX", "name": "Seagate"},
        dsa_item={"close": 103.13, "ret_20d": 12.0},
        market_row={"close": 103.13},
        fundamentals=data,
    )

    flags = r.uzi_seed_data_quality_flags(raw)

    assert data["market_cap_raw"] == 21657300000.0
    assert data["pe"] == 15.23
    assert data["source"] == "Eastmoney+SEC+Polygon+SECValuation"
    assert "UZI 估值维度不足" not in flags


def test_complete_fundamentals_refetches_when_existing_valuation_is_thin(monkeypatch) -> None:
    monkeypatch.setattr(r, "fetch_pipeline_fundamentals", lambda symbol, env: {
        "source": "Polygon+SECValuation",
        "price": 103.13,
        "shares_outstanding": 210000000.0,
        "market_cap_raw": 21657300000.0,
        "market_cap_yi": 216.573,
        "pe": 15.23,
    })
    existing = {
        "source": "Eastmoney+SEC",
        "eps": 6.77,
        "revenue_history": [74.0, 81.0],
        "net_profit_history": [8.0, 11.0],
        "roe_history": [14.0, 18.0],
    }

    data = r.complete_pipeline_fundamentals("STX", existing, {})
    raw = r.build_uzi_seed_raw(
        symbol="STX",
        uzi_symbol="STX",
        item={"symbol": "STX", "name": "Seagate"},
        dsa_item={"close": 103.13, "ret_20d": 12.0},
        market_row={"close": 103.13},
        fundamentals=data,
    )

    assert data["market_cap_raw"] == 21657300000.0
    assert data["pe"] == 15.23
    assert data["revenue_history"] == [74.0, 81.0]
    assert data["source"] == "Eastmoney+SEC+Polygon+SECValuation"
    assert "UZI 估值维度不足" not in r.uzi_seed_data_quality_flags(raw)


def test_uzi_default_template_is_flagged_even_after_agent_review() -> None:
    flags = r.uzi_quality_flags(
        {"agent_reviewed": True},
        {"investors": [{"name": "Buffett", "score": 40}, {"name": "Lynch", "score": 42}, {"name": "Soros", "score": 39}]},
        49.7,
        "基本面 56.9 · 共识 10.5",
    )

    assert "UZI 输出疑似默认低分模板" in flags


def test_stage_tradingagents_final_excludes_quick_rows_by_default(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(r, "OUTPUTS", tmp_path)
    monkeypatch.setattr(r.Path, "exists", lambda self: True)

    def fake_quick(item, note=""):
        row = {
            "symbol": item["symbol"],
            "name": item.get("name", ""),
            "dsa_score": item.get("score", 0),
            "action": "HOLD",
            "confidence": item.get("score", 0) / 100,
            "risk": "medium",
            "reason": "quick",
            "ta_status": "quick" if not note else "quick_fallback",
            "ta_note": note or "not_scheduled_full",
        }
        return row

    def fake_run(cmd, cwd, env, timeout=None):
        symbol = env["PIPELINE_TA_TICKER"]
        text = f'__PIPELINE_JSON__\n{{"action":"BUY","confidence":0.9,"risk":"low","reason":"full {symbol}"}}'
        return 0, text

    monkeypatch.setattr(r, "quick_trading_research", fake_quick)
    monkeypatch.setattr(r, "run", fake_run)
    monkeypatch.setattr(r, "write_text", lambda path, text: None)
    candidates = [{"symbol": f"T{i}", "name": f"T{i}", "score": 90 - i} for i in range(5)]

    rows = r.stage_tradingagents(candidates, {
        "TRADINGAGENTS_DIR": "/tmp/ta",
        "TRADINGAGENTS_PYTHON": "/tmp/python",
        "PIPELINE_TRADINGAGENTS_SCAN_N": "5",
        "PIPELINE_TRADINGAGENTS_FULL_N": "3",
        "PIPELINE_TRADINGAGENTS_ALLOW_INCOMPLETE_FINAL": "0",
    }, top_n=5)

    assert len(rows) == 3
    assert all(row["ta_status"] == "full" for row in rows)
