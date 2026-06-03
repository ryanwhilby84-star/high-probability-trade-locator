"""Asset-specific macro transmission engine."""

from __future__ import annotations

import pandas as pd

from hptl.macro.macro_transmission import build_macro_transmission


def _rates_row(**overrides) -> pd.Series:
    base = {
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
    base.update(overrides)
    return pd.Series(base)


def test_gold_vs_equity_headlines_differ():
    rates = _rates_row(dgs10_1w_change=-0.12, dgs2_1w_change=-0.08, fed_funds_1w_change=-0.05)
    gold = build_macro_transmission(market="Gold", rates_row=rates, macro_audit=None)
    es = build_macro_transmission(market="S&P 500 / ES", rates_row=rates, macro_audit=None)
    assert gold["available"] and es["available"]
    assert gold["headline"] != es["headline"]
    gold_drivers = {b["driver_id"] for b in gold["drivers"]}
    es_drivers = {b["driver_id"] for b in es["drivers"]}
    assert "real_yields" in gold_drivers
    assert "liquidity_curve" in es_drivers


def test_divergence_when_bull_structure_risk_off():
    rates = _rates_row(dgs10_1w_change=0.15, dgs2_1w_change=0.12, fed_funds_1w_change=0.08)
    inst = {
        "structural_regime": "structural_bullish",
        "flow_momentum": "improving",
        "macro_alignment": "headwind",
    }
    tx = build_macro_transmission(
        market="S&P 500 / ES",
        rates_row=rates,
        macro_audit=None,
        institutional_context=inst,
    )
    assert tx["macro_vs_price"]["state"] in {"ignoring_bearish_macro", "ignoring_risk_off", "covering_against_macro"}
    assert "IGNORING" in tx["macro_vs_price"]["label"] or tx["macro_vs_price"]["state"].startswith("ignoring")


def test_copper_has_china_growth_driver():
    rates = _rates_row(dgs10_1w_change=-0.05)
    tx = build_macro_transmission(market="Copper / HG", rates_row=rates, macro_audit=None)
    ids = [b["driver_id"] for b in tx["drivers"]]
    assert "china_growth" in ids
