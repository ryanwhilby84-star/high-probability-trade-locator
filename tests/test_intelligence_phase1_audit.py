"""Phase 1 research audit — PIT table + episode clustering (no threshold changes)."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.cot.intelligence_phase1_audit import (
    EPISODE_EXIT_GAP_WEEKS,
    build_market_research_table,
)
from hptl.cot.positioning_research_engine import MIN_HISTORY


def _row(d: str, c: float, nc: float, nr: float, price: float) -> dict:
    return {
        "date": d,
        "commercial_net": c,
        "institutional_net": nc,
        "institutional_long": max(nc, 0) + 10_000,
        "institutional_short": abs(min(nc, 0)) + 10_000,
        "retail_net": nr,
        "retail_long": max(nr, 0) + 5_000,
        "retail_short": abs(min(nr, 0)) + 5_000,
        "price": price,
        "open_interest": 100_000,
    }


def _series(n: int = MIN_HISTORY + 140):
    start = date(2016, 1, 5)
    rows = []
    price = 100.0
    for i in range(n):
        phase = (i % 40) / 40.0
        c = -40_000 + phase * 80_000
        nr = 20_000 - phase * 50_000
        nc = -c * 0.6
        price = price * (1.0 + ((i % 11) - 5) * 0.002)
        rows.append(_row((start + timedelta(weeks=i)).isoformat(), c, nc, nr, price))
    return rows


def test_research_table_separates_features_and_outcome_labels():
    out = build_market_research_table("Test", {"series": _series()})
    assert out["available"]
    row = out["rows"][MIN_HISTORY + 10]
    assert "features" in row and "outcome_labels" in row
    assert "commercial" in row["features"]
    assert "noncommercial" in row["features"]
    assert row["outcome_labels"]["schema"] == "outcome_label_only"
    # Labels must not be nested inside features
    assert "outcome_labels" not in row["features"]
    assert "fwd_4w" not in row["features"]


def test_extreme_episodes_collapse_consecutive_weeks():
    series = _series()
    # Force a long commercial high-extreme regime
    for i in range(-40, 0):
        series[i]["commercial_net"] = 120_000 + i
    out = build_market_research_table("Test", {"series": series})
    eps = out["independent_extreme_episodes"]["commercial"]
    # Far fewer episodes than in-zone weeks
    in_zone_weeks = sum(
        1
        for r in out["rows"]
        if r["features"]["commercial"].get("active_extreme_zone") == "high"
    )
    assert in_zone_weeks > EPISODE_EXIT_GAP_WEEKS
    assert eps < in_zone_weeks
    assert eps >= 1


def test_price_audit_flags_mixed_scale():
    series = _series()
    # Contaminate with tonne-scale spikes
    series[80]["price"] = 5500.0
    series[120]["price"] = 4.2
    out = build_market_research_table("Test", {"series": series})
    flags = out["price_audit"]["flags"]
    assert "mixed_price_scale_regimes_in_cot3y_price" in flags
    assert out["price_audit"]["trustworthy_for_outcome_labels"] is False
