from __future__ import annotations

from datetime import date, timedelta

from hptl.seasonality_workstation.seasonal_edge_scanner import (
    WindowSpec,
    _stats,
    _window_score,
    _window_return,
)


def _rows(start: date, days: int, daily_return: float) -> list[tuple[date, float]]:
    rows = []
    px = 100.0
    d = start
    for _ in range(days):
        if d.weekday() < 5:
            rows.append((d, px))
            px *= 1.0 + daily_return
        d += timedelta(days=1)
    return rows


def test_window_return_uses_trading_closes_inside_calendar_window():
    rows = _rows(date(2020, 9, 1), 45, 0.001)
    ret = _window_return(rows, date(2020, 9, 5), date(2020, 9, 30))
    assert ret is not None
    assert ret > 0


def test_perfect_15y_record_is_exceptional_when_stable():
    s15 = _stats([(y, 2.0 + (y % 3) * 0.1) for y in range(2009, 2024)])
    s10 = _stats([(y, 2.1) for y in range(2014, 2024)])
    s5 = _stats([(y, 2.2) for y in range(2019, 2024)])
    score, grade, reasons = _window_score(s15, s10, s5, 1.0)
    assert grade == "EXCEPTIONAL"
    assert score >= 82
    assert "15y_perfect_directional_record" in reasons
    assert "5y_10y_15y_direction_aligned" in reasons


def test_frequency_without_mean_median_agreement_is_not_promoted_to_strong():
    vals = [(y, 0.2) for y in range(2009, 2023)] + [(2023, -10.0)]
    s15 = _stats(vals)
    s10 = _stats(vals[-10:])
    s5 = _stats(vals[-5:])
    _score, grade, _reasons = _window_score(s15, s10, s5, 0.9)
    assert grade not in {"EXCEPTIONAL", "STRONG"}


def test_window_spec_handles_cross_year_shape():
    spec = WindowSpec(12, 15, 1, 31, "15 Dec → 31 Jan", 0, "rolling_window")
    assert spec.end_month < spec.start_month
