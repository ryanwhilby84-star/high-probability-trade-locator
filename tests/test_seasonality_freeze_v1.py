"""Verify Freeze v1.0 equations exactly — no visual tuning."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.markets.usd_index_identity import ICE_DXY_ID
from hptl.seasonality_workstation.indexed_seasonality import (
    FREEZE_SMOOTH_WINDOW,
    METHOD_VERSION,
    average_normalized_paths,
    build_freeze_v1_path,
    build_normalised_seasonal_curve,
    centre_path,
    load_daily_closes_for_seasonality,
    normalize_year_pct,
    smooth_path,
)


def test_step1_normalisation_formula():
    rows = [
        (date(2020, 1, 2), 100.0),
        (date(2020, 1, 3), 102.0),
        (date(2020, 1, 6), 99.0),
    ]
    got = normalize_year_pct(rows)
    assert got[0] == 0.0
    assert abs(got[1] - 2.0) < 1e-12
    assert abs(got[2] - (-1.0)) < 1e-12


def test_steps_2_3_4_on_two_years():
    n1 = [0.0, 1.0, 2.0, 1.0, 3.0]
    n2 = [0.0, -1.0, 1.0, 2.0, 0.0]
    raw, d = average_normalized_paths({2020: n1, 2021: n2})
    assert d == 5
    assert raw[0] == 0.0
    assert abs(raw[1] - 0.0) < 1e-12
    assert abs(raw[2] - 1.5) < 1e-12
    centered, mu = centre_path(raw)
    assert abs(sum(centered)) < 1e-9
    assert abs(mu - sum(raw) / 5) < 1e-12
    sm = smooth_path(centered, FREEZE_SMOOTH_WINDOW)
    assert len(sm) == 5


def test_rejects_non_mean_aggregation():
    daily, _ = load_daily_closes_for_seasonality(ICE_DXY_ID)
    pack = build_normalised_seasonal_curve(daily, aggregation="median")
    assert pack["available"] is False
    assert pack["reason"] == "freeze_v1_requires_mean_aggregation"


def test_ice_dxy_freeze_matches_independent_recompute():
    daily, meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    assert daily, meta
    core = build_freeze_v1_path(daily, lookback_years=15, smooth=FREEZE_SMOOTH_WINDOW)
    assert core["available"] is True
    assert core["method"]["version"] == METHOD_VERSION
    assert core["N"] == 15
    # Step 1: each year starts at 0
    for y, rows in core["year_bars"].items():
        path = normalize_year_pct(rows)
        assert abs(path[0]) < 1e-12
    # Steps 2–4 recompute
    year_norm = {y: normalize_year_pct(rows) for y, rows in core["year_bars"].items()}
    raw, d_len = average_normalized_paths(year_norm)
    centered, mu = centre_path(raw)
    smoothed = smooth_path(centered, FREEZE_SMOOTH_WINDOW)
    assert d_len == core["D"]
    assert abs(mu - core["mu"]) < 1e-9
    assert all(abs(a - b) < 1e-9 for a, b in zip(raw, core["raw"]))
    assert all(abs(a - b) < 1e-9 for a, b in zip(centered, core["centered"]))
    assert all(abs(a - b) < 1e-9 for a, b in zip(smoothed, core["smoothed"]))
    assert abs(sum(centered) / len(centered)) < 1e-9

    pack = build_normalised_seasonal_curve(daily)
    assert pack["available"] is True
    assert pack["method"]["version"] == METHOD_VERSION
    assert pack["full_year"]
    assert pack["historical"][-1]["segment"] == "today"


def test_excludes_incomplete_current_year():
    daily, _ = load_daily_closes_for_seasonality(ICE_DXY_ID)
    asof = daily[-1][0]
    asof_year = int(asof[:4])
    core = build_freeze_v1_path(daily, asof=asof)
    assert asof_year not in core["sample_years"]
