"""Thesis Tracker uses FX Valuation V1 for FX instruments (not V0 price percentile)."""

from hptl.thesis_tracker.alignment import evaluate_pillars
from hptl.thesis_tracker.fx_valuation_snap import apply_fx_valuation_to_snap
from hptl.thesis_tracker.opportunity import build_opportunity


def test_fx_v1_overrides_v0_on_snap():
    row = {
        "market": "Japanese Yen / 6J",
        "instrument_meta": {"asset_class": "fx"},
        "valuation_bias": "Bearish",
        "valuation_score": 8.0,
        "valuation_wired": True,
        "fx_valuation_bias": "Bullish",
        "fx_valuation_score": 8.0,
        "fx_valuation_model_type": "FX Yield Differential V1",
        "fx_valuation_condition": "Undervalued",
        "valuation_grade": "MILD",
        "valuation_model_status": "VALIDATED",
    }
    snap = apply_fx_valuation_to_snap(row, {"valuation_bias": "Bearish", "valuation_score": 8.0})
    assert snap["valuation_source"] == "FX Yield Differential V1"
    assert snap["valuation_bias"] == "Bullish"
    assert snap["valuation_condition"] == "Undervalued"
    assert snap["valuation_grade"] == "MILD"
    assert snap["valuation_model_status"] == "VALIDATED"

    pillars = evaluate_pillars(snap, direction="long")
    val = next(p for p in pillars if p["pillar"] == "valuation")
    assert val["state"] == "Bullish"
    assert val["label"] == "Valuation"
    assert val["pass"] is True


def test_fx_without_v1_suppresses_v0():
    row = {
        "market": "Japanese Yen / 6J",
        "instrument_meta": {"asset_class": "fx"},
        "valuation_bias": "Bearish",
        "valuation_score": 8.0,
        "valuation_wired": True,
    }
    snap = apply_fx_valuation_to_snap(row, {"valuation_bias": "Bearish", "valuation_score": 8.0})
    assert snap["valuation_bias"] == "UNAVAILABLE"
    assert snap.get("valuation_source") is None

    val = next(p for p in evaluate_pillars(snap, direction="long") if p["pillar"] == "valuation")
    assert val["state"] == "UNAVAILABLE"
    assert val["wired"] is False


def test_non_fx_keeps_v0():
    row = {
        "market": "S&P 500 / ES",
        "instrument_meta": {"asset_class": "index"},
        "valuation_bias": "Bearish",
        "valuation_score": 7.5,
        "valuation_wired": True,
    }
    snap = apply_fx_valuation_to_snap(row, {"valuation_bias": "Bearish", "valuation_score": 7.5, "valuation_wired": True})
    assert snap.get("valuation_source") is None
    assert snap["valuation_bias"] == "Bearish"
    assert snap["valuation_score"] == 7.5


def test_build_opportunity_fx_thesis_uses_v1_from_confluence():
    thesis = {
        "market": "Japanese Yen / 6J",
        "direction_bias": "long",
        "status": "DISCOVERED",
        "conviction_trend": "stable",
        "snapshots": [
            {
                "week": "2025-05-06",
                "cot_bias": "Bearish",
                "cot_score": 6.0,
                "valuation_bias": "Bearish",
                "valuation_score": 10.0,
                "valuation_wired": True,
            }
        ],
    }
    opp = build_opportunity(thesis)
    val = opp["summary"]["valuation"]
    assert val["label"] == "Valuation"

