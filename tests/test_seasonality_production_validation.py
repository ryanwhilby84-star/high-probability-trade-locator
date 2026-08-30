from __future__ import annotations

from hptl.seasonality_workstation.validation import (
    projected_robust_return,
    robust_forward_horizon_stats,
    robust_lookback_agreement,
    robust_weekly_leave_one_year_out,
)


def _rows(start_year: int = 2010, end_year: int = 2026):
    rows = []
    for year in range(start_year, end_year + 1):
        for week in range(1, 53):
            # Repeatable positive seasonal structure with small pullback weeks.
            ret = -0.002 if week % 5 == 0 else 0.003
            rows.append(
                {
                    "date": f"{year}-01-01",
                    "close": 100.0,
                    "iso_year": year,
                    "iso_week": week,
                    "return": ret,
                }
            )
    return rows


def _week_stats(scale: float = 1.0):
    out = {}
    for week in range(1, 53):
        ret = (-0.002 if week % 5 == 0 else 0.003) * scale
        out[str(week)] = {
            "trimmed_mean": ret,
            "median": ret,
            "n": 12,
        }
    return out


def test_forward_horizon_wraps_week_52_into_next_year():
    rows = _rows()
    stats = robust_forward_horizon_stats(
        rows,
        years=list(range(2010, 2025)),
        anchor_week=49,
        horizons=(8,),
    )
    eight = stats["8w"]
    assert eight["year_wrap"] == "supported"
    assert eight["n"] > 0
    assert eight["mean_return"] is not None


def test_projected_return_wraps_calendar_year():
    stats = _week_stats()
    wrapped = projected_robust_return(stats, anchor_week=50, horizon=8)
    assert wrapped is not None
    # Weeks used are 51,52,1,2,3,4,5,6; week 5 is the pullback.
    expected = (1.003 ** 7) * 0.998 - 1.0
    assert abs(wrapped - expected) < 1e-12


def test_lookback_agreement_uses_robust_weekly_projection():
    lookbacks = {
        "5Y": {"week_stats": _week_stats(1.0)},
        "10Y": {"week_stats": _week_stats(0.8)},
        "15Y": {"week_stats": _week_stats(0.6)},
        "20Y": {"week_stats": _week_stats(0.5)},
    }
    result = robust_lookback_agreement(lookbacks, anchor_week=35, horizon=8)
    assert result["method"] == "robust_weekly_return_projection_sign_agreement"
    assert result["score"] == 1.0
    assert result["sign_agreement"] == 1.0


def test_leave_one_year_out_uses_prior_years_only():
    rows = _rows(2010, 2025)
    years = list(range(2010, 2025))
    result = robust_weekly_leave_one_year_out(
        rows,
        years=years,
        anchor_week=35,
        lookback="15Y",
        horizon=8,
    )
    assert result["method"] == "leave_one_year_out_robust_weekly_direction"
    assert result["n"] > 0
    assert result["hit_rate"] == 1.0
    for fold in result["outcomes"]:
        assert all(train_year < fold["year"] for train_year in fold["training_years"])
