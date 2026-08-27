"""Weekly Roadmap — independent weekly-return seasonality tests."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from hptl.seasonality_workstation.returns import iso_week, weekly_closes_from_daily, weekly_return_rows
from hptl.seasonality_workstation.weekly_roadmap import (
    agreement_state,
    average_weekly_returns,
    build_weekly_roadmap,
    compound_weekly_path,
    direction_from_path,
    stale_price_warning,
    valid_iso_years,
)


def _synthetic_daily(
    *,
    years: int = 8,
    end: date | None = None,
    weekly_drift: float = 0.001,
) -> list[tuple[str, float]]:
    """Dense weekday closes with mild seasonal weekly drift."""
    end = end or date(2026, 7, 28)
    start = date(end.year - years, 1, 2)
    out: list[tuple[str, float]] = []
    px = 100.0
    d = start
    while d <= end:
        if d.weekday() < 5:
            y, w = iso_week(d.isoformat())
            # mild bearish mid-year (weeks 28-34), bullish early year
            seasonal = -0.002 if 28 <= w <= 34 else weekly_drift
            px *= 1.0 + seasonal / 5.0
            out.append((d.isoformat(), px))
        d += timedelta(days=1)
    return out


def test_iso_week_merges_week_53_into_52() -> None:
    # 2020-12-31 is ISO week 53 of 2020
    y, w = iso_week("2020-12-31")
    assert y == 2020
    assert w == 52


def test_approximately_52_weekly_buckets() -> None:
    daily = _synthetic_daily()
    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)
    asof_year, _ = iso_week(daily[-1][0])
    years, _ = valid_iso_years(rows, asof_year=asof_year, lookback_years=6)
    avgs = average_weekly_returns(rows, years)
    assert len(avgs) == 52
    assert set(avgs) == set(range(1, 53))


def test_weekly_return_calculation_correct() -> None:
    weekly = [("2024-01-05", 100.0), ("2024-01-12", 102.0), ("2024-01-19", 101.0)]
    rows = weekly_return_rows(weekly)
    assert rows[0]["return"] is None
    assert rows[1]["return"] == pytest.approx(0.02)
    assert rows[2]["return"] == pytest.approx(101 / 102 - 1)


def test_compound_path_and_no_monthly_interpolation() -> None:
    week_avgs = {
        w: {"week": w, "average_return": 0.01 if w <= 2 else 0.0, "sample_count": 5, "quality_flag": "ok"}
        for w in range(1, 53)
    }
    path = compound_weekly_path(week_avgs)
    assert len(path) == 52
    assert path[0] == pytest.approx(1.01)
    assert path[1] == pytest.approx(1.01 * 1.01)
    # Remaining weeks hold compound level (0 return) — not interpolated monthly steps
    assert path[51] == pytest.approx(path[1])


def test_missing_week_not_forward_filled_into_average() -> None:
    rows = [
        {"iso_year": 2020, "iso_week": 1, "return": 0.01},
        {"iso_year": 2021, "iso_week": 1, "return": 0.03},
        # week 2 missing entirely
        {"iso_year": 2020, "iso_week": 3, "return": 0.02},
    ]
    avgs = average_weekly_returns(rows, [2020, 2021])
    assert avgs[1]["average_return"] == pytest.approx(0.02)
    assert avgs[2]["sample_count"] == 0
    assert avgs[2]["average_return"] is None
    assert avgs[2]["quality_flag"] == "missing"


def test_thin_week_detection() -> None:
    rows = [{"iso_year": 2020, "iso_week": 10, "return": 0.01}]
    avgs = average_weekly_returns(rows, [2020])
    assert avgs[10]["quality_flag"] == "insufficient"


def test_valid_year_filtering_excludes_thin_and_current() -> None:
    daily = _synthetic_daily(years=6)
    rows = weekly_return_rows(weekly_closes_from_daily(daily))
    asof_year, _ = iso_week(daily[-1][0])
    years, excluded = valid_iso_years(rows, asof_year=asof_year, lookback_years=4)
    assert asof_year not in years
    assert all(y < asof_year for y in years)
    assert isinstance(excluded, list)


def test_build_weekly_roadmap_happy_path() -> None:
    daily = _synthetic_daily(years=8)
    pack = build_weekly_roadmap(daily, lookback_years=6, smooth=3)
    assert pack["available"] is True
    assert pack["method"]["version"] == "weekly_roadmap_v1"
    assert len(pack["weekly_points"]) == 52
    assert pack["current_week"] is not None
    assert pack["current_direction"] in {"Bullish", "Bearish", "Neutral"}
    assert pack["quality_status"] in {"valid", "warning"}
    assert pack["smoothing"]["stage"] == "after_compound_path"
    # points include required fields
    p = pack["weekly_points"][0]
    for k in ("week", "average_return", "cumulative_return", "sample_count", "quality_flag"):
        assert k in p


def test_quality_gate_failure_integrity() -> None:
    daily = _synthetic_daily(years=8)
    pack = build_weekly_roadmap(
        daily,
        lookback_years=6,
        integrity={"status": "FAIL", "issues": ["excessive_discontinuities:12"]},
    )
    assert pack["available"] is False
    assert pack["quality_status"] == "unavailable"
    assert any("excessive_discontinuities" in r for r in pack["quality_reasons"])


def test_agreement_states() -> None:
    assert agreement_state("Bullish", "Bullish", "Bullish", monthly_available=True, weekly_available=True) == (
        "Aligned bullish"
    )
    assert agreement_state("Bearish", "Bearish", "Bearish", monthly_available=True, weekly_available=True) == (
        "Aligned bearish"
    )
    assert agreement_state("Bullish", "Bearish", "Neutral", monthly_available=True, weekly_available=True) == (
        "Broad bullish / short-term bearish"
    )
    assert agreement_state("Bearish", "Bullish", "Neutral", monthly_available=True, weekly_available=True) == (
        "Broad bearish / short-term bullish"
    )
    assert agreement_state("Bullish", "Bullish", "Bullish", monthly_available=False, weekly_available=True) == (
        "Unavailable"
    )


def test_stale_actual_price_warning() -> None:
    warn = stale_price_warning("2026-07-01", as_of=date(2026, 7, 28), stale_after_days=5)
    assert warn["stale"] is True
    fresh = stale_price_warning("2026-07-27", as_of=date(2026, 7, 28), stale_after_days=5)
    assert fresh["stale"] is False


def test_monthly_roadmap_unchanged_and_separate_payload_keys() -> None:
    """Engine exposes weekly_roadmap without mutating seasonal_roadmap maths module."""
    import inspect

    from hptl.seasonality_workstation import seasonal_roadmap as sr
    from hptl.seasonality_workstation import weekly_roadmap as wr

    # weekly module must not import seasonal roadmap builders that could rewrite maths
    src = inspect.getsource(wr)
    assert "build_seasonal_roadmap" not in src
    assert "year_indexed_path" not in src

    # seasonal_roadmap public API still present
    assert hasattr(sr, "build_seasonal_roadmap_curve")

    daily = _synthetic_daily(years=8)
    weekly = build_weekly_roadmap(daily, lookback_years=6)
    assert "weekly_points" in weekly
    assert weekly.get("method", {}).get("name") == "avg_iso_weekly_return_compound"


def test_no_repeated_source_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Weekly roadmap consumes the provided series — does not reload price store."""
    calls = {"n": 0}

    def _boom(*_a, **_k):
        calls["n"] += 1
        raise AssertionError("must not reload prices inside weekly roadmap")

    monkeypatch.setattr(
        "hptl.seasonality_workstation.returns.load_daily_closes",
        _boom,
    )
    monkeypatch.setattr(
        "hptl.seasonality_workstation.indexed_seasonality.load_daily_closes_for_seasonality",
        _boom,
    )
    daily = _synthetic_daily(years=6)
    pack = build_weekly_roadmap(daily, lookback_years=5)
    assert pack["available"] is True
    assert calls["n"] == 0


def test_direction_from_path() -> None:
    rising = [1.0 + i * 0.01 for i in range(52)]
    falling = [1.5 - i * 0.01 for i in range(52)]
    flat = [1.0] * 52
    assert direction_from_path(rising, 20) == "Bullish"
    assert direction_from_path(falling, 20) == "Bearish"
    assert direction_from_path(flat, 20) == "Neutral"


def test_leap_year_and_holiday_short_week_still_one_bucket() -> None:
    # 2024 is leap year; Christmas week is holiday-shortened but still one ISO week close
    daily = _synthetic_daily(years=3, end=date(2024, 12, 31))
    weekly = weekly_closes_from_daily(daily)
    weeks_2024 = [iso_week(d)[1] for d, _ in weekly if iso_week(d)[0] == 2024]
    assert max(weeks_2024) <= 52
    # Christmas week present as a single bucket
    assert 52 in weeks_2024 or 51 in weeks_2024
