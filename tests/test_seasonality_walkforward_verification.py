"""Regression tests for strict production-seasonality walk-forward verification."""
from __future__ import annotations

from datetime import date, timedelta

from hptl.seasonality_workstation.walkforward_verification import (
    _shape_fit,
    build_roadmap_asof,
    evaluate_anchor,
)


def _synthetic_daily(start_year: int = 2010, end_year: int = 2024) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for y in range(start_year, end_year + 1):
        d = date(y, 1, 1)
        end = date(y, 12, 31)
        px = 100.0 + (y - start_year) * 2.0
        td = 0
        while d <= end:
            if d.weekday() < 5:
                # Recurrent within-year texture with enough non-zero volatility.
                if td < 55:
                    r = 0.0020
                elif td < 125:
                    r = -0.0016
                elif td < 190:
                    r = 0.0013
                else:
                    r = -0.0007
                # Small deterministic alternation prevents zero-variance returns.
                r += 0.0003 if td % 2 == 0 else -0.0002
                px *= 1.0 + r
                out.append((d.isoformat(), px))
                td += 1
            d += timedelta(days=1)
    return out


def test_shape_fit_ignores_absolute_price_level():
    actual = [100, 102, 101, 105, 104, 108]
    seasonal = [1000, 1020, 1010, 1050, 1040, 1080]
    fit = _shape_fit(actual, seasonal)
    assert fit["available"] is True
    assert abs(fit["level_path_correlation"] - 1.0) < 1e-9
    assert abs(fit["daily_return_correlation"] - 1.0) < 1e-9
    assert fit["path_rmse_pct"] == 0.0


def test_roadmap_asof_excludes_anchor_year_from_seasonal_sample():
    daily = _synthetic_daily()
    anchor_date = "2023-08-01"
    anchor_index = next(i for i, (d, _) in enumerate(daily) if d == anchor_date)
    road = build_roadmap_asof(
        daily,
        instrument_id="Synthetic",
        anchor_index=anchor_index,
        lookback_years=15,
    )
    assert road["available"] is True
    assert road["verification_training_end"] == anchor_date
    assert all(int(y) < 2023 for y in road["sample_years"])
    assert road["method"]["version"] == "volatility_normalised_daily_texture_v4"


def test_future_mutation_cannot_change_historical_forecast():
    daily = _synthetic_daily()
    anchor_date = "2023-08-01"
    anchor_index = next(i for i, (d, _) in enumerate(daily) if d == anchor_date)

    before = evaluate_anchor(
        daily,
        instrument_id="Synthetic",
        anchor_index=anchor_index,
        lookback_years=15,
    )
    assert before["available"] is True
    assert before["no_lookahead"] is True

    # Replace every post-anchor price with an absurd path. A strict as-of model
    # must keep its forecast unchanged even though realised outcomes change.
    mutated = list(daily)
    anchor_px = mutated[anchor_index][1]
    for i in range(anchor_index + 1, len(mutated)):
        d, _ = mutated[i]
        mutated[i] = (d, anchor_px * (1.0 + 0.02 * (i - anchor_index)))

    after = evaluate_anchor(
        mutated,
        instrument_id="Synthetic",
        anchor_index=anchor_index,
        lookback_years=15,
    )
    assert after["available"] is True
    assert after["no_lookahead"] is True

    for key in ("4w", "8w", "12w"):
        assert (
            before["horizons"][key]["predicted_mean_return"]
            == after["horizons"][key]["predicted_mean_return"]
        )
        assert (
            before["horizons"][key]["predicted_median_return"]
            == after["horizons"][key]["predicted_median_return"]
        )

    # The realised future should change, proving the test actually altered only
    # the outcome side of the experiment.
    assert (
        before["horizons"]["8w"]["actual_return"]
        != after["horizons"]["8w"]["actual_return"]
    )
