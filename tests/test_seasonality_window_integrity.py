from __future__ import annotations

from datetime import date, timedelta

from hptl.seasonality_workstation.integrity import (
    audit_daily_series,
    seasonal_window_usable_years,
)


def _weekday_series(start: date, end: date, *, missing: tuple[date, date] | None = None):
    rows: list[tuple[str, float]] = []
    cur = start
    px = 100.0
    while cur <= end:
        in_missing = missing is not None and missing[0] <= cur <= missing[1]
        if cur.weekday() < 5 and not in_missing:
            rows.append((cur.isoformat(), px))
            px += 0.01
        cur += timedelta(days=1)
    return rows


def test_large_gap_outside_selected_window_does_not_kill_series_or_year():
    # W36 + 4w is a September window. A Jan/Feb defect is diagnostic only.
    daily = _weekday_series(
        date(2008, 1, 1),
        date(2026, 9, 11),
        missing=(date(2022, 1, 10), date(2022, 3, 20)),
    )

    integrity = audit_daily_series("TEST", daily)
    assert integrity["status"] == "PASS"
    assert any(w.startswith("large_gaps:") for w in integrity["warnings"])
    assert 2022 in integrity["usable_history_years"]

    usable, excluded = seasonal_window_usable_years(
        daily,
        asof_year=2026,
        anchor_week=36,
        lookback_years=15,
        horizon_weeks=4,
    )
    assert 2022 in usable
    assert not any(e["year"] == 2022 for e in excluded)


def test_large_gap_inside_selected_window_excludes_only_affected_year():
    # Same defect size, moved into the W36 -> W40 selected seasonal window.
    daily = _weekday_series(
        date(2008, 1, 1),
        date(2026, 9, 11),
        missing=(date(2022, 9, 5), date(2022, 11, 13)),
    )

    usable, excluded = seasonal_window_usable_years(
        daily,
        asof_year=2026,
        anchor_week=36,
        lookback_years=15,
        horizon_weeks=4,
    )

    assert 2022 not in usable
    hit = next(e for e in excluded if e["year"] == 2022)
    assert "large_gap_in_window" in hit["reason"]
    # Other intact years in the same lookback remain valid.
    assert 2021 in usable
    assert 2023 in usable
