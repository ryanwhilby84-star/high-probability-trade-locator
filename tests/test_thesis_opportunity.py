"""Thesis Opportunity Engine alignment tests."""

from hptl.thesis_tracker.alignment import evaluate_pillars, alignment_summary
from hptl.thesis_tracker.opportunity import build_opportunity


def test_long_thesis_institutions_retail_location_pass():
    snap = {
        "cot_bias": "Bullish",
        "cot_score": 7.2,
        "positioning_state": "Bullish strengthening",
        "retail_net": -5000,
        "zone_focus": "Look for Demand",
    }
    pillars = evaluate_pillars(snap, direction="long")
    by = {p["pillar"]: p for p in pillars}
    assert by["institutions"]["pass"] is True
    assert by["retail"]["pass"] is True
    assert by["location"]["pass"] is True
    assert by["valuation"]["wired"] is False
    assert by["seasonality"]["wired"] is False
    align = alignment_summary(pillars)
    assert align["pass"] == 3


def test_build_opportunity_action_pay():
    thesis = {
        "market": "Canadian Dollar / 6C",
        "direction_bias": "long",
        "status": "DISCOVERED",
        "conviction_trend": "stable",
        "snapshots": [
            {
                "week": "2026-05-26",
                "cot_bias": "Bullish",
                "cot_score": 6.5,
                "retail_net": -3000,
                "zone_focus": "Demand",
            }
        ],
    }
    opp = build_opportunity(thesis)
    assert opp["alignment"]["pass"] >= 3
    assert opp["action"] in {"PAY ATTENTION", "HIGH ATTENTION", "WATCH", "NO EDGE"}
