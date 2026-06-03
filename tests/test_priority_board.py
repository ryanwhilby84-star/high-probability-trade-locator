"""Priority board — full universe, canonical dedup, transparent scoring."""

from __future__ import annotations

from hptl.context.macro_only_context import build_macro_only_attention
from hptl.context.priority_board import (
    BOARD_SCORE_FLOOR,
    PRIORITY_HIGH,
    aggregate_priority_markets,
    build_priority_debug,
    effective_priority_score,
)
from hptl.markets.instrument_registry import all_instrument_ids, canonical_priority_group, get_instrument
from hptl.macro.macro_transmission import build_macro_transmission
import pandas as pd


def _rates():
    return pd.Series(
        {
            "dgs2": 4.0,
            "dgs10": 4.2,
            "dgs30": 4.5,
            "fed_funds": 4.5,
            "yield_curve_10y2y": 0.2,
            "dgs2_1w_change": 0.0,
            "dgs10_1w_change": -0.08,
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


def _cot_rec(market: str, score: float = 85.0, cot_score: float = 8.0):
    return {
        "market": market,
        "cot_bias": "Bullish",
        "cot_score": cot_score,
        "macro_score": 5.0,
        "weekly_change": 5000.0,
        "data_status": "complete",
        "macro_transmission": {"available": True, "generic_rates_only": False, "asset_alignment": "mixed"},
        "institutional_context": {
            "data_mode": "cot",
            "attention": {
                "priority_tier": "high_attention",
                "priority_score": score,
                "alerts": [{"icon": "🔥", "text": "x", "kind": "flow_extreme"}],
                "dominant_narrative": market,
                "priority_headline": market,
                "tactical_readable": "Watch",
            },
        },
    }


def test_priority_debug_contains_every_instrument_with_scoring_fields():
    debug = build_priority_debug([], calendar_week="2026-05-19", top_n=6)
    assert len(debug["instruments"]) == len(all_instrument_ids())
    for row in debug["instruments"]:
        assert "final_attention_score" in row
        assert "cot_score_component" in row
        assert "duplicate_canonical_id" in row
        assert row["exclusion_reason"] is not None or row["included_in_priority_list"]


def test_non_cot_gets_exclusion_reason():
    rates = _rates()
    tx = build_macro_transmission(market="EUR/AUD", rates_row=rates, macro_audit=None)
    spec = get_instrument("EUR/AUD")
    att = build_macro_only_attention(market="EUR/AUD", spec=spec, macro_transmission=tx, macro_signal="risk_off")
    rec = {
        "market": "EUR/AUD",
        "cot_bias": "N/A",
        "macro_regime": "risk_off",
        "macro_transmission": tx,
        "institutional_context": {"data_mode": "macro_only", "attention": att, "macro_transmission": tx},
        "data_status": "macro_only",
    }
    debug = build_priority_debug([rec], calendar_week="2026-05-19", top_n=6)
    row = next(x for x in debug["instruments"] if x["instrument_id"] == "EUR/AUD")
    assert row["attention_priority"] != PRIORITY_HIGH
    if not row["included_in_priority_list"]:
        assert row["exclusion_reason"]


def test_no_duplicate_copper_on_board():
    rates = _rates()
    hg = _cot_rec("Copper / HG", score=88.0)
    copper_tx = build_macro_transmission(market="Copper", rates_row=rates, macro_audit=None)
    spec = get_instrument("Copper")
    copper_att = build_macro_only_attention(
        market="Copper", spec=spec, macro_transmission=copper_tx, macro_signal="risk_off"
    )
    copper = {
        "market": "Copper",
        "cot_bias": "N/A",
        "macro_regime": "risk_off",
        "macro_transmission": copper_tx,
        "institutional_context": {"data_mode": "macro_only", "attention": copper_att},
        "data_status": "proxy_required",
    }
    assert canonical_priority_group(spec, "Copper") == "Copper / HG"
    debug = build_priority_debug([hg, copper], calendar_week="2026-05-19", top_n=6)
    markets = [m["market"] for m in debug["priority_markets"]]
    assert markets.count("Copper / HG") + markets.count("Copper") <= 1
    copper_row = next(x for x in debug["instruments"] if x["instrument_id"] == "Copper")
    assert "Copper" not in markets
    if copper_row["rank_before_deduplication"]:
        assert "duplicate_canonical" in (copper_row["exclusion_reason"] or "")


def test_macro_proxy_does_not_replace_direct_cot_on_board():
    """Natural Gas (direct COT) should not be bumped for Copper macro-only proxy."""
    rows = [
        _cot_rec("Soybeans", 98),
        _cot_rec("S&P 500 / ES", 95),
        _cot_rec("Coffee", 94),
        _cot_rec("Wheat", 93),
        _cot_rec("Copper / HG", 88),
        _cot_rec("Natural Gas / NG", 83),
    ]
    rates = _rates()
    copper_tx = build_macro_transmission(market="Copper", rates_row=rates, macro_audit=None)
    spec = get_instrument("Copper")
    copper_att = build_macro_only_attention(
        market="Copper", spec=spec, macro_transmission=copper_tx, macro_signal="risk_off"
    )
    rows.append(
        {
            "market": "Copper",
            "cot_bias": "N/A",
            "institutional_context": {"data_mode": "macro_only", "attention": copper_att},
            "macro_transmission": copper_tx,
        }
    )
    debug = build_priority_debug(rows, calendar_week="2026-05-19", top_n=6)
    markets = [m["market"] for m in debug["priority_markets"]]
    assert "Natural Gas / NG" in markets
    assert "Copper" not in markets
    assert len(set(markets)) == len(markets)


def test_priority_board_changes_with_mock_scores():
    rec_a = _cot_rec("Gold", 99.0)
    rates = _rates()
    tx = build_macro_transmission(market="Bitcoin", rates_row=rates, macro_audit=None)
    spec = get_instrument("Bitcoin")
    att = build_macro_only_attention(market="Bitcoin", spec=spec, macro_transmission=tx, macro_signal="risk_on")
    rec_b = {
        "market": "Bitcoin",
        "cot_bias": "N/A",
        "macro_regime": "risk_on",
        "institutional_context": {"data_mode": "macro_only", "attention": att},
        "macro_transmission": {**tx, "asset_alignment": "supportive"},
        "data_status": "macro_only",
    }
    board1 = aggregate_priority_markets([rec_a], top_n=1, calendar_week="2026-05-19")
    assert board1["priority_markets"][0]["market"] == "Gold"

    rec_b["institutional_context"]["attention"]["priority_score"] = 80.0
    board2 = aggregate_priority_markets([rec_b], top_n=1, calendar_week="2026-05-19")
    assert board2["priority_markets"][0]["market"] == "Bitcoin"


def test_not_hardcoded_legacy_list():
    rates = _rates()
    rows = []
    for pair in ["EUR/AUD", "AUD/CAD", "USD/TRY"]:
        spec = get_instrument(pair)
        tx = build_macro_transmission(market=pair, rates_row=rates, macro_audit=None)
        att = build_macro_only_attention(market=pair, spec=spec, macro_transmission=tx, macro_signal="risk_off")
        rows.append(
            {
                "market": pair,
                "cot_bias": "N/A",
                "macro_regime": "risk_off",
                "macro_transmission": tx,
                "institutional_context": {"data_mode": "macro_only", "attention": att, "macro_transmission": tx},
            }
        )
    board = aggregate_priority_markets(rows, top_n=3, calendar_week="2026-05-19")
    markets = [m["market"] for m in board["priority_markets"]]
    assert "Soybeans" not in markets
    assert all(m in {"EUR/AUD", "AUD/CAD", "USD/TRY"} for m in markets)


def test_audit_sections_present():
    rows = [_cot_rec(m, 90 - i) for i, m in enumerate(["Gold", "Silver", "Coffee"])]
    debug = build_priority_debug(rows, calendar_week="2026-05-19", top_n=6)
    assert "audit" in debug
    assert len(debug["audit"]["top_30_before_deduplication"]) <= 30
    assert debug["audit"]["top_30_before_deduplication"][0]["canonical_id"]


def test_proxy_boost_effective_score_still_computed():
    rates = _rates()
    cl_rec = _cot_rec("Crude Oil / CL", 88.0)
    wti_tx = build_macro_transmission(market="West Texas Oil", rates_row=rates, macro_audit=None)
    spec = get_instrument("West Texas Oil")
    wti_att = build_macro_only_attention(
        market="West Texas Oil", spec=spec, macro_transmission=wti_tx, macro_signal="risk_off"
    )
    wti_rec = {
        "market": "West Texas Oil",
        "cot_bias": "N/A",
        "institutional_context": {"data_mode": "macro_only", "attention": wti_att},
    }
    eff = effective_priority_score(wti_rec, wti_att, {"Crude Oil / CL": cl_rec})
    assert eff >= 0
