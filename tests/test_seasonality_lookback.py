from __future__ import annotations

from datetime import date, timedelta

from hptl.seasonality_workstation.lookback import build_reliable_seasonal_lookback


def _rows(years: range, weekly_return: float = -0.01, *, missing=None):
    """Synthetic contiguous weekly closes around ISO W36 for deterministic tests."""
    missing = missing or set()
    rows = []
    close = 100.0
    for year in years:
        monday = date.fromisocalendar(year, 34, 1)
        for offset in range(0, 17):
            d = monday + timedelta(weeks=offset)
            iso = d.isocalendar()
            ret = weekly_return
            if (year, int(iso.week)) in missing:
                ret = None
            if ret is not None:
                close *= 1.0 + ret
            rows.append(
                {
                    "date": d.isoformat(),
                    "close": close,
                    "iso_year": int(iso.year),
                    "iso_week": min(int(iso.week), 52),
                    "return": ret,
                }
            )
    return rows


def test_one_observation_per_year_and_all_requested_horizons():
    years = list(range(2010, 2025))
    result = build_reliable_seasonal_lookback(
        _rows(range(2010, 2026), -0.01),
        years=years,
        anchor_week=36,
        lookback="15Y",
    )
    assert result["available"] is True
    assert set(result["horizons"]) == {"1w", "2w", "4w", "8w", "12w"}
    for horizon in result["horizons"].values():
        assert horizon["n"] == 15
        observed_years = [row["year"] for row in horizon["annual_outcomes"]]
        assert len(observed_years) == len(set(observed_years)) == 15


def test_consistently_negative_history_is_bearish_high_confidence():
    years = list(range(2010, 2025))
    result = build_reliable_seasonal_lookback(
        _rows(range(2010, 2026), -0.01),
        years=years,
        anchor_week=36,
        lookback="15Y",
    )
    overall = result["overall"]
    assert overall["direction"] == "Bearish"
    assert overall["confidence"] == "HIGH"
    assert overall["minimum_core_sample"] == 15
    assert result["horizons"]["8w"]["bearish_frequency"] == 1.0


def test_gap_rejects_affected_year_instead_of_bridging():
    years = list(range(2015, 2025))
    rows = _rows(range(2015, 2026), -0.01, missing={(2020, 39)})
    result = build_reliable_seasonal_lookback(
        rows,
        years=years,
        anchor_week=36,
        lookback="10Y",
    )
    # W39 lies inside the 4W+ horizon from W36, so that fold is discarded.
    assert result["horizons"]["4w"]["n"] == 9
    assert 2020 not in {row["year"] for row in result["horizons"]["4w"]["annual_outcomes"]}
    # 1W does not cross the missing W39 observation and remains valid.
    assert result["horizons"]["1w"]["n"] == 10


def test_incomplete_future_horizon_is_never_invented():
    rows = _rows(range(2018, 2025), -0.01)
    # Truncate the last year shortly after the anchor so long horizons are unavailable.
    rows = [
        row
        for row in rows
        if not (row["iso_year"] == 2024 and row["iso_week"] > 39)
    ]
    years = list(range(2018, 2025))
    result = build_reliable_seasonal_lookback(
        rows,
        years=years,
        anchor_week=36,
        lookback="10Y",
    )
    assert result["horizons"]["1w"]["n"] == 7
    assert result["horizons"]["8w"]["n"] == 6
    assert 2024 not in {row["year"] for row in result["horizons"]["8w"]["annual_outcomes"]}


def test_excursions_are_explicitly_close_to_close():
    years = list(range(2010, 2025))
    result = build_reliable_seasonal_lookback(
        _rows(range(2010, 2026), -0.01),
        years=years,
        anchor_week=36,
        lookback="15Y",
    )
    h8 = result["horizons"]["8w"]
    assert h8["direction"] == "Bearish"
    assert h8["median_favourable_close_excursion_pct"] > 0
    assert h8["median_adverse_close_excursion_pct"] == 0.0
    assert result["method"]["excursion_definition"] == "close_to_close_not_intrabar_high_low"
