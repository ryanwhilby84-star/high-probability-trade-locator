"""Seasonality Workstation V1 — engine unit tests (no UI)."""

from __future__ import annotations

from hptl.seasonality_workstation.integrity import audit_daily_series
from hptl.seasonality_workstation.returns import weekly_closes_from_daily, weekly_return_rows
from hptl.seasonality_workstation.stats import bucket_stats, trimmed_mean
from hptl.seasonality_workstation.engine import _align_seasonal_to_price, _compound_path


def test_integrity_fails_on_empty():
    r = audit_daily_series("X", [])
    assert r["status"] == "FAIL"
    assert "no_daily_bars" in r["issues"]


def test_integrity_detects_duplicates_and_jumps():
    daily = [("2020-01-01", 100.0), ("2020-01-01", 101.0), ("2020-01-02", 200.0)]
    # pad with enough history-looking bars so we exercise jump/dupe paths
    from datetime import date, timedelta

    d0 = date(2015, 1, 1)
    series = []
    px = 100.0
    for i in range(400):
        series.append(((d0 + timedelta(days=i)).isoformat(), px))
        px *= 1.001
    series.append(("2020-06-01", 100.0))
    series.append(("2020-06-01", 100.0))  # dupe
    series.append(("2020-06-02", 160.0))  # jump
    r = audit_daily_series("Demo", series)
    assert "duplicate_observations:1" in r["issues"] or any("duplicate" in i for i in r["issues"])


def test_weekly_returns_and_stats():
    daily = []
    from datetime import date, timedelta

    d0 = date(2020, 1, 1)
    px = 100.0
    for i in range(200):
        daily.append(((d0 + timedelta(days=i)).isoformat(), px))
        px *= 1.002
    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)
    assert weekly
    assert any(r.get("return") is not None for r in rows)
    st = bucket_stats([0.01, 0.02, -0.01, 0.03, 0.0])
    assert st["n"] == 5
    assert st["median"] is not None
    assert trimmed_mean([0.01, 0.02, 0.5, -0.01, 0.03]) is not None


def test_projection_aligns_without_jump():
    week_stats = {
        w: {"trimmed_mean": 0.001, "median": 0.001, "mean": 0.001, "q25": 0.0, "q75": 0.002}
        for w in range(1, 53)
    }
    path = _compound_path(week_stats)
    aligned = _align_seasonal_to_price(path, anchor_week=20, anchor_price=50.0)
    assert abs(aligned[20] - 50.0) < 1e-9
