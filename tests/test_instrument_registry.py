"""OANDA instrument universe registry."""

from __future__ import annotations

import json

import pandas as pd

from hptl.confluence.build_decision_table import TARGET_MARKETS, MARKET_ALIASES
from hptl.markets.instrument_registry import (
    LEGACY_COT_MARKETS,
    all_instrument_ids,
    export_registry_json,
    get_instrument,
    load_registry,
)
from hptl.macro.macro_transmission import build_macro_transmission
from hptl.context.macro_only_context import build_macro_only_institutional_context


def test_registry_loads_expanded_universe():
    reg = load_registry()
    ids = all_instrument_ids()
    assert len(ids) > len(LEGACY_COT_MARKETS)
    assert "AUD/CAD" in ids
    assert "Bitcoin" in ids
    assert "US 10Y T-Note" in ids
    assert "Germany 30" in ids


def test_no_duplicate_ids():
    ids = all_instrument_ids()
    assert len(ids) == len(set(ids))


def test_legacy_cot_markets_includes_core_and_new_metals_softs():
    assert len(LEGACY_COT_MARKETS) == 23
    for m in ["Gold", "Silver", "Sugar", "Platinum", "Palladium", "NASDAQ / NQ"]:
        assert m in LEGACY_COT_MARKETS
    for m in LEGACY_COT_MARKETS:
        spec = get_instrument(m)
        assert spec is not None
        assert spec.has_cot_mapping is True
        assert spec.positioning_status == "cot_available"


def test_fx_pair_no_direct_cot():
    spec = get_instrument("EUR/AUD")
    assert spec is not None
    assert spec.has_cot_mapping is False
    assert spec.positioning_status == "no_direct_pair_cot"


def test_fx_proxy_required():
    spec = get_instrument("AUD/USD")
    assert spec.cot_proxy_of == "Australian Dollar / 6A"
    assert spec.positioning_status == "proxy_required"


def test_macro_transmission_all_asset_classes():
    rates = pd.Series(
        {
            "dgs2": 4.0,
            "dgs10": 4.2,
            "dgs30": 4.5,
            "fed_funds": 4.5,
            "yield_curve_10y2y": 0.2,
            "dgs2_1w_change": 0.0,
            "dgs10_1w_change": -0.05,
            "dgs30_1w_change": 0.0,
            "fed_funds_1w_change": 0.0,
            "yield_curve_10y2y_1w_change": 0.0,
            "dgs2_4w_change": 0.0,
            "dgs10_4w_change": 0.0,
            "dgs30_4w_change": 0.0,
            "fed_funds_4w_change": 0.0,
            "core_rates_complete": True,
        }
    )
    for market in ["Gold", "S&P 500 / ES", "EUR/AUD", "Bitcoin", "US 10Y T-Note"]:
        tx = build_macro_transmission(market=market, rates_row=rates, macro_audit=None)
        assert tx.get("available") is True, market
        assert tx.get("headline")


def test_macro_only_context_does_not_crash():
    spec = get_instrument("EUR/AUD")
    rates = pd.Series(
        {
            "dgs2": 4.0,
            "dgs10": 4.2,
            "dgs30": 4.5,
            "fed_funds": 4.5,
            "yield_curve_10y2y": 0.2,
            "dgs2_1w_change": 0.0,
            "dgs10_1w_change": 0.0,
            "dgs30_1w_change": 0.0,
            "fed_funds_1w_change": 0.0,
            "yield_curve_10y2y_1w_change": 0.0,
            "dgs2_4w_change": 0.0,
            "dgs10_4w_change": 0.0,
            "dgs30_4w_change": 0.0,
            "fed_funds_4w_change": 0.0,
            "core_rates_complete": True,
        }
    )
    tx = build_macro_transmission(market="EUR/AUD", rates_row=rates, macro_audit=None)
    ctx = build_macro_only_institutional_context(
        market="EUR/AUD",
        spec=spec,
        macro_transmission=tx,
        macro_signal="risk_on",
        macro_score=5.0,
    )
    assert ctx["data_mode"] == "macro_only"
    assert ctx["attention"]["priority_tier"]


def test_export_registry_json():
    path = export_registry_json()
    assert path.exists()
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["total"] == len(all_instrument_ids())


def test_target_markets_matches_registry():
    assert len(TARGET_MARKETS) == len(all_instrument_ids())
    assert "Gold" in TARGET_MARKETS
    assert MARKET_ALIASES["Gold"]  # legacy aliases preserved
