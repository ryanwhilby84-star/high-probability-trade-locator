"""Instrument coverage audit."""

from __future__ import annotations

import pandas as pd

from hptl.markets.coverage_audit import (
    EXPECTED_DIRECT_COT,
    audit_instrument,
    classify_data_status,
    explain_attention_eligibility,
    run_coverage_audit,
)
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, get_instrument, load_registry
from hptl.macro.macro_transmission import build_macro_transmission


def test_every_registry_instrument_gets_audit_row():
    audit = run_coverage_audit([], latest_calendar_week="2026-05-19")
    assert len(audit["instruments"]) == len(load_registry())
    for row in audit["instruments"]:
        assert row["data_status"]
        assert row["instrument_id"]


def test_no_duplicate_instrument_ids_in_audit():
    audit = run_coverage_audit([])
    ids = [x["instrument_id"] for x in audit["instruments"]]
    assert len(ids) == len(set(ids))


def test_known_cot_instruments_expected_direct():
    for m in ["Gold", "Silver", "Copper / HG", "Sugar", "Platinum", "Palladium"]:
        assert m in EXPECTED_DIRECT_COT
        spec = get_instrument(m)
        assert spec.has_cot_mapping


def test_west_texas_proxy_status():
    spec = get_instrument("West Texas Oil")
    assert spec.cot_proxy_of == "Crude Oil / CL"
    row = audit_instrument(spec, cot=pd.DataFrame(), week_rec=None, latest_calendar_week="2026-05-19")
    assert row["data_status"] in {"proxy_required", "no_data", "macro_only"}


def test_attention_exclusion_reason():
    rec = {
        "market": "EUR/AUD",
        "cot_bias": "N/A",
        "institutional_context": {
            "data_mode": "macro_only",
            "attention": {"priority_tier": "low_priority", "priority_score": 5},
        },
    }
    ok, reason = explain_attention_eligibility(rec)
    assert ok is False
    assert "low" in reason


def test_macro_generic_only_flag():
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
    tx = build_macro_transmission(market="Copper / HG", rates_row=rates, macro_audit=None)
    assert "generic_rates_only" in tx or tx.get("transmission_mode") in {"generic_rates_only", "asset_specific"}


def test_legacy_count_includes_sugar_platinum():
    assert "Sugar" in LEGACY_COT_MARKETS
    assert "Platinum" in LEGACY_COT_MARKETS
    assert len(LEGACY_COT_MARKETS) == 23


def test_classify_broken_mapping():
    spec = get_instrument("Gold")
    status = classify_data_status(
        spec=spec,
        rec={"cot_bias": "N/A", "missing_reason": "no mapped raw COT row"},
        cot_rows=0,
        cot_resolved=False,
        macro_ok=True,
        macro_generic=True,
    )
    assert status == "broken_mapping"
