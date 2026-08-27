"""Seasonal Price Path v1 — separate product from Freeze v1.0."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.markets.usd_index_identity import ICE_DXY_ID
from hptl.seasonality_workstation.indexed_seasonality import (
    METHOD_VERSION as FREEZE_VERSION,
    build_freeze_v1_path,
    load_daily_closes_for_seasonality,
)
from hptl.seasonality_workstation.seasonal_price_path import (
    METHOD_VERSION,
    average_daily_returns,
    build_seasonal_price_path,
    build_seasonal_price_path_curve,
    cumulative_index,
    rebase_to_price,
    year_daily_returns,
)


def _synth_daily(years: list[int], *, asof: str = "2024-07-15") -> list[tuple[str, float]]:
    """Synthetic weekday closes with a known seasonal shape."""
    out: list[tuple[str, float]] = []
    for y in years:
        px = 100.0
        d = date(y, 1, 1)
        end = date(y, 12, 31)
        i = 0
        while d <= end:
            if d.weekday() < 5:
                # Mild seasonal: rise early year, dip mid, rise late
                if i < 60:
                    px *= 1.001
                elif i < 140:
                    px *= 0.9992
                else:
                    px *= 1.0008
                out.append((d.isoformat(), px))
                i += 1
            d += timedelta(days=1)
    # asof year partial
    asof_d = date.fromisoformat(asof)
    px = 105.0
    d = date(asof_d.year, 1, 1)
    i = 0
    while d <= asof_d:
        if d.weekday() < 5:
            px *= 1.0003
            out.append((d.isoformat(), px))
            i += 1
        d += timedelta(days=1)
    return out


def test_year_daily_returns_first_zero():
    rows = [
        (date(2020, 1, 2), 100.0),
        (date(2020, 1, 3), 101.0),
        (date(2020, 1, 6), 99.0),
    ]
    r = year_daily_returns(rows)
    assert r[0] == 0.0
    assert abs(r[1] - 0.01) < 1e-12
    assert abs(r[2] - (99 / 101 - 1)) < 1e-12


def test_cumsum_and_rebase_equations():
    avg = [0.0, 0.01, -0.005, 0.02]
    idx = cumulative_index(avg)
    assert abs(idx[0] - 1.0) < 1e-12
    assert abs(idx[1] - 1.01) < 1e-12
    assert abs(idx[2] - 1.01 * 0.995) < 1e-12
    assert abs(idx[3] - idx[2] * 1.02) < 1e-12

    # Mean of index is NOT forced to zero (not Freeze centering)
    assert abs(sum(idx) / len(idx)) > 0.5

    prices = rebase_to_price(idx, asof_td=2, anchor_price=200.0)
    assert abs(prices[1] - 200.0) < 1e-9
    assert abs(prices[0] - 200.0 * (idx[0] / idx[1])) < 1e-9


def test_average_returns_common_length():
    a = [0.0, 0.01, 0.02, 0.0]
    b = [0.0, -0.01, 0.0, 0.01, 0.02]
    avg, d = average_daily_returns({2020: a, 2021: b})
    assert d == 4
    assert abs(avg[1] - 0.0) < 1e-12
    assert abs(avg[2] - 0.01) < 1e-12


def test_path_not_centered_and_in_price_units():
    years = list(range(2010, 2024))
    daily = _synth_daily(years, asof="2024-07-15")
    core = build_seasonal_price_path(daily, asof="2024-07-15", lookback_years=15)
    assert core["available"] is True
    assert core["method"]["version"] == METHOD_VERSION
    assert core["method"]["centering"] == "none"
    assert core["method"]["units"] == "price"
    prices = core["prices"]
    td = core["asof_trading_day"]
    assert abs(prices[td - 1] - core["anchor_price"]) < 1e-6
    # Path wanders in price space — not a zero-mean % strip
    assert abs(sum(prices) / len(prices)) > 10


def test_curve_payload_grey_blue_segments():
    years = list(range(2010, 2024))
    daily = _synth_daily(years, asof="2024-07-15")
    pack = build_seasonal_price_path_curve(daily, asof="2024-07-15", lookback_years=15)
    assert pack["available"] is True
    assert pack["asof_price"] == pack["anchor_price"] or abs(
        pack["asof_price"] - pack["anchor_price"]
    ) < 1e-6
    segs = {p["segment"] for p in pack["full_year"]}
    assert "historical" in segs
    assert "forward" in segs or "today" in segs
    today = next(p for p in pack["full_year"] if p["segment"] == "today")
    assert abs(today["price"] - pack["asof_price"]) < 1e-6
    for p in pack["full_year"]:
        assert "price" in p
        assert "index" in p


def test_separate_from_freeze_v1():
    daily, meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    if not daily:
        return
    freeze = build_freeze_v1_path(daily, lookback_years=15)
    path = build_seasonal_price_path(daily, lookback_years=15)
    assert freeze["available"] is True
    assert path["available"] is True
    assert freeze["method"]["version"] == FREEZE_VERSION
    assert path["method"]["version"] == METHOD_VERSION
    # Different products: Freeze is centered %; path is price-rebased
    assert abs(sum(freeze["centered"])) < 1e-6 * len(freeze["centered"])
    assert path["method"]["centering"] == "none"
    td = path["asof_trading_day"]
    assert abs(path["prices"][td - 1] - path["anchor_price"]) < 1e-4
