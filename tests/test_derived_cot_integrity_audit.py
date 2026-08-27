"""Derived COT integrity — Weekly Inspector contract."""

from __future__ import annotations

import math

from hptl.cot.derived_cot_integrity_audit import (
    LOOKBACK_WEEKS,
    audit_week,
    run_derived_cot_integrity_audit,
    write_derived_cot_integrity_audit,
)
from hptl.cot.weekly_inspector_export import expand_compact_market
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS


def _finite(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def test_lookback_is_latest_plus_12():
    assert LOOKBACK_WEEKS == 13


def test_audit_week_flags_nan_percentile():
    week = {
        "date": "2026-07-21",
        "commercial": {
            "net": 1.0,
            "weekly_change": 1.0,
            "four_week_change": 1.0,
            "twelve_week_change": 1.0,
            "percentile": float("nan"),
            "percentile_change_1w": 1.0,
            "percentile_change_4w": 1.0,
            "percentile_change_12w": 1.0,
            "percentile_observation_count": 100,
            "temperature": "neutral",
            "state_label": "Neutral",
            "direction": "stable",
            "direction_arrow": "→",
        },
        "noncommercial": {
            "net": 1.0,
            "weekly_change": 1.0,
            "four_week_change": 1.0,
            "twelve_week_change": 1.0,
            "percentile": 50.0,
            "percentile_change_1w": 1.0,
            "percentile_change_4w": 1.0,
            "percentile_change_12w": 1.0,
            "percentile_observation_count": 100,
            "temperature": "neutral",
            "state_label": "Neutral",
            "direction": "stable",
            "direction_arrow": "→",
        },
        "nonreportable": {
            "net": 1.0,
            "weekly_change": 1.0,
            "four_week_change": 1.0,
            "twelve_week_change": 1.0,
            "percentile": 50.0,
            "percentile_change_1w": 1.0,
            "percentile_change_4w": 1.0,
            "percentile_change_12w": 1.0,
            "percentile_observation_count": 100,
            "temperature": "neutral",
            "state_label": "Neutral",
            "direction": "stable",
            "direction_arrow": "→",
        },
        "cross": {
            "commercial_percentile": 50.0,
            "noncommercial_percentile": 50.0,
            "nonreportable_percentile": 50.0,
            "comm_nc_spread": 0.0,
            "comm_nc_spread_change_1w": 0.0,
            "comm_nc_spread_change_4w": 0.0,
            "comm_nr_spread": 0.0,
            "relationship": "aligned",
            "flow": "stable",
        },
    }
    fails = audit_week(week, instrument_id="Gold")
    assert any("percentile" in f["field"] for f in fails)


def test_audit_week_flags_spread_mismatch():
    week = {
        "date": "2026-07-21",
        "commercial": {
            "net": 1.0,
            "weekly_change": 1.0,
            "four_week_change": 1.0,
            "twelve_week_change": 1.0,
            "percentile": 80.0,
            "percentile_change_1w": 1.0,
            "percentile_change_4w": 1.0,
            "percentile_change_12w": 1.0,
            "percentile_observation_count": 100,
            "temperature": "neutral",
            "state_label": "Neutral",
            "direction": "stable",
            "direction_arrow": "→",
        },
        "noncommercial": {
            "net": 1.0,
            "weekly_change": 1.0,
            "four_week_change": 1.0,
            "twelve_week_change": 1.0,
            "percentile": 20.0,
            "percentile_change_1w": 1.0,
            "percentile_change_4w": 1.0,
            "percentile_change_12w": 1.0,
            "percentile_observation_count": 100,
            "temperature": "neutral",
            "state_label": "Neutral",
            "direction": "stable",
            "direction_arrow": "→",
        },
        "nonreportable": {
            "net": 1.0,
            "weekly_change": 1.0,
            "four_week_change": 1.0,
            "twelve_week_change": 1.0,
            "percentile": 50.0,
            "percentile_change_1w": 1.0,
            "percentile_change_4w": 1.0,
            "percentile_change_12w": 1.0,
            "percentile_observation_count": 100,
            "temperature": "neutral",
            "state_label": "Neutral",
            "direction": "stable",
            "direction_arrow": "→",
        },
        "cross": {
            "commercial_percentile": 80.0,
            "noncommercial_percentile": 20.0,
            "nonreportable_percentile": 50.0,
            "comm_nc_spread": 10.0,  # should be 60
            "comm_nc_spread_change_1w": 0.0,
            "comm_nc_spread_change_4w": 0.0,
            "comm_nr_spread": 30.0,
            "relationship": "opposed",
            "flow": "opposition_widening",
        },
    }
    fails = audit_week(week, instrument_id="Gold")
    assert any("spread_mismatch" in f["field"] for f in fails)


def test_live_export_crude_oil_latest_complete():
    report = run_derived_cot_integrity_audit()
    crude = next(r for r in report["instruments"] if r["instrument"] == "Crude Oil / CL")
    assert crude["status"] == "PASS", crude.get("failures")[:5]
    assert crude["latest_cot_week"]
    assert crude["historical_window_length"] >= LOOKBACK_WEEKS


def test_live_export_all_26_pass():
    report = write_derived_cot_integrity_audit()
    assert report["summary"]["markets_total"] == len(LEGACY_COT_MARKETS)
    assert report["summary"]["pass_count"] == 26
    assert report["summary"]["fail_count"] == 0
    assert report["summary"]["overall_status"] == "PASS"


def test_missing_market_identity_fails():
    report = run_derived_cot_integrity_audit(
        weekly_inspector={"markets": {}},
        cot_3y={"markets": {}},
    )
    assert report["summary"]["fail_count"] == 26
    assert report["summary"]["overall_status"] == "FAIL"


def test_missing_historical_statistics_fails():
    """Empty week series with identity present still fails the lookback contract."""
    stub = {
        "markets": {
            name: {
                "available": True,
                "latest_date": "2026-07-21",
                "weeks": [],
            }
            for name in LEGACY_COT_MARKETS
        }
    }
    report = run_derived_cot_integrity_audit(weekly_inspector=stub)
    assert report["summary"]["fail_count"] == 26
    assert report["summary"]["overall_status"] == "FAIL"


def test_weekly_analysis_blocked_when_integrity_fails():
    import pytest
    from hptl.cot.analyst_intelligence_export import run_analyst_intelligence_export

    with pytest.raises(RuntimeError, match="DERIVED COT INTEGRITY FAILED"):
        run_analyst_intelligence_export(weekly_inspector={"markets": {}})


def test_expand_preserves_crude_percentiles():
    from pathlib import Path
    import json

    path = Path("web-dashboard/public/data/cot_weekly_inspector_latest.json")
    doc = json.loads(path.read_text(encoding="utf-8"))
    block = doc["markets"]["Crude Oil / CL"]
    weeks = expand_compact_market(block)["weeks"][-13:]
    for w in weeks:
        for g in ("commercial", "noncommercial", "nonreportable"):
            pct = _finite((w.get(g) or {}).get("percentile"))
            assert pct is not None
            assert 0 <= pct <= 100
