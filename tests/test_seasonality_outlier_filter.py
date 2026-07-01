"""Median-band outlier filter for seasonality (Copper, Corn)."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.seasonality.seasonality_outlier_filter import (
    OUTLIER_FILTER_MARKETS,
    filter_weekly_bars_for_seasonality,
)
from hptl.seasonality.seasonality_trust import attach_trust_metadata, classify_trust


def _weekly_bars(n: int, base: float = 5.0) -> list[tuple[str, float]]:
    start = date(2018, 1, 5)
    return [(str(start + timedelta(weeks=i)), base + i * 0.01) for i in range(n)]


def test_spike_dropped_for_copper():
    bars = _weekly_bars(120, base=5.0)
    bars[10] = (bars[10][0], 5000.0)
    kept, audit = filter_weekly_bars_for_seasonality("Copper / HG", bars, compute_before_after=False)
    assert audit["applied"] is True
    assert audit["bars_dropped"] >= 1
    assert 5000.0 in audit["closes_dropped"]
    assert all(c <= 20.0 for _, c in kept)


def test_unscoped_market_unchanged():
    bars = _weekly_bars(60, base=100.0)
    bars[5] = (bars[5][0], 99999.0)
    kept, audit = filter_weekly_bars_for_seasonality("Gold", bars, compute_before_after=False)
    assert audit["applied"] is False
    assert len(kept) == len(bars)


def test_only_copper_and_corn_scoped():
    assert OUTLIER_FILTER_MARKETS == frozenset({"Copper / HG", "Corn"})


def test_unit_scale_break_blocks_grade_a_without_filter():
    grade, _ = classify_trust(
        available=True,
        years_used=12,
        avg_weeks_per_year=50,
        seasonal_3y_weeks=52,
        unit_scale_break=True,
        outlier_filter_applied=False,
    )
    assert grade != "A"


def test_grade_a_after_filter_when_sane():
    grade, notes = classify_trust(
        available=True,
        years_used=12,
        avg_weeks_per_year=50,
        seasonal_3y_weeks=52,
        unit_scale_break=True,
        max_indexed=120.0,
        max_projection=115.0,
        outlier_filter_applied=True,
    )
    assert grade == "A"
    assert "Unit-scale break detected" in notes


def test_grade_downgraded_when_indexed_still_insane():
    grade, notes = classify_trust(
        available=True,
        years_used=12,
        avg_weeks_per_year=50,
        seasonal_3y_weeks=52,
        unit_scale_break=True,
        max_indexed=600.0,
        max_projection=100.0,
        outlier_filter_applied=True,
    )
    assert grade != "A"


def test_attach_trust_metadata_with_filter_audit():
    block = {
        "available": True,
        "years_used": 12,
        "chart_series": [{"actual": 105.0, "seasonal_10y": 102.0, "seasonal_3y": 101.0}],
    }
    bars = _weekly_bars(600, base=5.0)
    audit = {
        "applied": True,
        "unit_scale_break_detected": True,
        "max_indexed_after": 110.0,
    }
    out = attach_trust_metadata(block, bars, filter_audit=audit)
    assert out.get("data_quality_warning") == "Unit-scale break detected"
    assert out.get("outlier_filter_audit") == audit
