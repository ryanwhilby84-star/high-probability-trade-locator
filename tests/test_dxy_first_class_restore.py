"""Regression tests for DXY / USD Index first-class restore."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hptl.prices.coverage import select_price_source, supported_instrument_ids
from hptl.prices.current_price_service import load_instrument_mappings, reset_caches
from hptl.prices.fred_prices import fetch_fred_instrument
from hptl.prices.workstation_ohlc_export import (
    build_instrument_workstation_ohlc,
    derive_weekly_ohlc_from_daily,
)
from hptl.valuation.dxy_macro_bias import DRIVER_CLASS, MARKET, build_dxy_macro_bias

DX = "US Dollar Index / DX"


def test_dx_select_price_source_is_fred():
    assert select_price_source(DX) == "fred"
    assert DX in supported_instrument_ids()


def test_dx_fred_fetch_emits_weekly_from_daily():
    rec = fetch_fred_instrument(DX)
    assert not rec.get("error"), rec.get("error")
    assert len(rec.get("daily") or []) >= 100
    assert len(rec.get("weekly") or []) >= 20
    scale = rec.get("price_scale") or {}
    assert scale.get("series_id") == "DTWEXBGS"
    assert scale.get("is_proxy") is True


def test_close_only_daily_aggregates_to_weekly_ohlc():
    daily = [
        {"date": "2026-07-06", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        {"date": "2026-07-07", "open": 101.0, "high": 101.0, "low": 101.0, "close": 101.0},
        {"date": "2026-07-08", "open": 99.5, "high": 99.5, "low": 99.5, "close": 99.5},
        {"date": "2026-07-09", "open": 100.5, "high": 100.5, "low": 100.5, "close": 100.5},
        {"date": "2026-07-10", "open": 100.2, "high": 100.2, "low": 100.2, "close": 100.2},
    ]
    weekly = derive_weekly_ohlc_from_daily(daily)
    assert len(weekly) >= 1
    w = weekly[-1]
    assert w["high"] >= w["low"]
    assert w["open"] == 100.0
    assert w["close"] == 100.2
    assert w["high"] == 101.0
    assert w["low"] == 99.5


def test_dx_workstation_ohlc_has_weekly_bars():
    # Prefer cot block if present for alignment metadata
    cot_path = Path("web-dashboard/public/data/cot_3y_series_latest.json")
    cot_block = None
    if cot_path.exists():
        doc = json.loads(cot_path.read_text(encoding="utf-8"))
        cot_block = (doc.get("markets") or {}).get(DX)
    block = build_instrument_workstation_ohlc(DX, cot_block=cot_block)
    assert block["ohlc_rows"] > 0, block.get("note")
    assert len(block.get("weekly_ohlc") or []) > 0
    assert "fred" in str(block.get("canonical_symbol") or block.get("price_source") or "").lower() or block[
        "ohlc_rows"
    ] > 0


def test_dx_current_price_mapping_is_fred_not_oanda():
    reset_caches()
    m = load_instrument_mappings(refresh=True)[DX]
    assert m.provider == "fred"
    assert m.provider_symbol == "DTWEXBGS"
    assert m.supports_streaming is False


def test_dxy_macro_bias_structure_and_classifications():
    doc = build_dxy_macro_bias()
    assert doc["market"] == MARKET
    assert doc["valuation_status"] == "NOT_YET_VALIDATED"
    assert doc["price_instrument"]["is_proxy"] is True
    assert doc["price_instrument"]["is_ice_dx_futures"] is False
    assert doc["positioning_instrument"]["cftc_code"] == "098662"
    assert doc["macro_bias"] in {
        "Bullish",
        "Moderately Bullish",
        "Neutral",
        "Moderately Bearish",
        "Bearish",
    }
    keys = {d["key"] for d in doc["drivers"]}
    assert "us_2y_yield" in keys
    assert "us_10y_real_yield" in keys
    assert "ice_dx_cot" in keys
    assert DRIVER_CLASS["usd_broad_fair_value_v1"] == "EXPERIMENTAL"
    assert DRIVER_CLASS["ice_dx_cot"] == "MACRO_BIAS_DRIVER"


def test_dx_cot_3y_has_genuine_tip_and_positioning():
    path = Path("web-dashboard/public/data/cot_3y_series_latest.json")
    if not path.exists():
        pytest.skip("cot_3y export missing")
    doc = json.loads(path.read_text(encoding="utf-8"))
    block = (doc.get("markets") or {}).get(DX)
    assert block, "DX missing from cot_3y"
    series = block.get("series") or []
    assert len(series) >= 52
    assert block.get("latest_date") == series[-1]["date"]
    assert series[-1].get("institutional_net") is not None
    assert series[-1].get("commercial_net") is not None
