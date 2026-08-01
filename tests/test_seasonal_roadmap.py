"""Seasonal Roadmap v1 — indexed path average + as-of rebase."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.markets.usd_index_identity import ICE_DXY_ID
from hptl.seasonality_workstation.indexed_seasonality import (
    FREEZE_SMOOTH_WINDOW,
    load_daily_closes_for_seasonality,
)
from hptl.seasonality_workstation.seasonal_price_path import build_seasonal_price_path
from hptl.seasonality_workstation.seasonal_roadmap import (
    METHOD_VERSION,
    average_indexed_paths,
    build_seasonal_roadmap,
    build_seasonal_roadmap_curve,
    historical_horizon_stats,
    rebase_indexed_to_price,
    year_indexed_path,
)


def _synth_daily(years: list[int], *, asof: str = "2024-07-15") -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for y in years:
        px = 100.0
        d = date(y, 1, 1)
        end = date(y, 12, 31)
        i = 0
        while d <= end:
            if d.weekday() < 5:
                if i < 50:
                    px *= 1.0015
                elif i < 120:
                    px *= 0.9988
                else:
                    px *= 1.0007
                out.append((d.isoformat(), px))
                i += 1
            d += timedelta(days=1)
    asof_d = date.fromisoformat(asof)
    px = 110.0
    d = date(asof_d.year, 1, 1)
    while d <= asof_d:
        if d.weekday() < 5:
            px *= 1.0002
            out.append((d.isoformat(), px))
        d += timedelta(days=1)
    return out


def test_indexed_path_starts_at_one():
    rows = [
        (date(2020, 1, 2), 100.0),
        (date(2020, 1, 3), 105.0),
        (date(2020, 1, 6), 95.0),
    ]
    f = year_indexed_path(rows)
    assert abs(f[0] - 1.0) < 1e-12
    assert abs(f[1] - 1.05) < 1e-12
    assert abs(f[2] - 0.95) < 1e-12


def test_average_and_rebase_equations():
    g, d = average_indexed_paths({2020: [1.0, 1.1, 0.9], 2021: [1.0, 1.0, 1.1]})
    assert d == 3
    assert abs(g[0] - 1.0) < 1e-12
    assert abs(g[1] - 1.05) < 1e-12
    assert abs(g[2] - 1.0) < 1e-12
    s = rebase_indexed_to_price(g, asof_td=2, anchor_price=200.0)
    assert abs(s[1] - 200.0) < 1e-9
    assert abs(s[0] - 200.0 * (1.0 / 1.05)) < 1e-9


def test_no_centering_asof_pin():
    years = list(range(2010, 2024))
    daily = _synth_daily(years, asof="2024-07-15")
    core = build_seasonal_roadmap(daily, asof="2024-07-15", lookback_years=15, smooth=5)
    assert core["available"] is True
    assert core["method"]["version"] == METHOD_VERSION
    assert core["method"]["centering"] == "none"
    assert core["method"]["amplitude_scaling"] == "none"
    td = core["asof_trading_day"]
    assert abs(core["prices_raw"][td - 1] - core["anchor_price"]) < 1e-6
    assert abs(core["prices_smooth"][td - 1] - core["anchor_price"]) < 1e-6


def test_forecast_stats_separate_from_amplitude():
    years = list(range(2010, 2024))
    daily = _synth_daily(years, asof="2024-07-15")
    core = build_seasonal_roadmap(daily, asof="2024-07-15", lookback_years=15)
    stats = core["forecast_stats"]
    assert "4w" in stats and "48w" in stats
    assert stats["8w"]["not_from_roadmap_amplitude"] is True
    assert stats["8w"]["n"] >= 5
    # Amplitude of path must not be copied into mean forecast
    assert stats["8w"]["mean"] is not None
    assert abs(stats["8w"]["mean"]) < 0.5  # return fraction, not price amplitude
    for weeks in (4, 8, 12, 26, 48):
        row = stats[f"{weeks}w"]
        assert "mean" in row and "median" in row
        assert "bullish_frequency" in row and "bearish_frequency" in row
        assert "n" in row
        if row["n"] and row["n"] > 0:
            assert row["mean"] is not None
            assert row["median"] is not None
            assert row["bullish_frequency"] is not None
            assert row["bearish_frequency"] is not None
    # Near and long horizons must be populated on synthetic 15Y history
    # (26W/48W from mid-year as-of require crossing calendar year-end).
    assert stats["4w"]["n"] >= 5
    assert stats["8w"]["n"] >= 5
    assert stats["12w"]["n"] >= 5
    assert stats["26w"]["n"] >= 5
    assert stats["48w"]["n"] >= 5


def test_curve_has_smoothed_and_unsmoothed():
    years = list(range(2010, 2024))
    daily = _synth_daily(years, asof="2024-07-15")
    pack = build_seasonal_roadmap_curve(daily, asof="2024-07-15", lookback_years=15, smooth=5)
    assert pack["unsmoothed"]["full_year"]
    assert pack["smoothed"]["full_year"]
    assert pack["smooth_window"] == FREEZE_SMOOTH_WINDOW


def test_roadmap_differs_from_mean_return_path():
    daily, meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    if not daily:
        return
    asof = "2026-07-23"
    road = build_seasonal_roadmap(daily, asof=asof, lookback_years=15, smooth=None)
    mean_ret = build_seasonal_price_path(daily, asof=asof, lookback_years=15)
    assert road["available"] and mean_ret["available"]
    n = min(len(road["prices_raw"]), len(mean_ret["prices"]))
    # Same family of price-rebase display, but G vs cum-mean-return → not identical
    max_diff = max(abs(road["prices_raw"][i] - mean_ret["prices"][i]) for i in range(n))
    assert max_diff > 1e-6
