"""Validation helpers for robust weekly-return seasonality."""
from __future__ import annotations

import math
from typing import Any

from hptl.seasonality_workstation.stats import bucket_stats

LOOKBACK_YEARS: dict[str, int | None] = {
    "5Y": 5,
    "10Y": 10,
    "15Y": 15,
    "20Y": 20,
    "FULL": None,
}


def _train_week_stats(
    rows: list[dict[str, Any]], *, test_year: int, lookback_years: int | None
) -> dict[int, dict[str, Any]]:
    first_year = -10_000 if lookback_years is None else test_year - lookback_years
    buckets: dict[int, list[float]] = {w: [] for w in range(1, 53)}
    for row in rows:
        year = int(row["iso_year"])
        week = int(row["iso_week"])
        ret = row.get("return")
        if year >= test_year or year < first_year or ret is None:
            continue
        value = float(ret)
        if 1 <= week <= 52 and math.isfinite(value):
            buckets[week].append(value)
    return {week: bucket_stats(values) for week, values in buckets.items()}


def _actual_forward_returns(
    rows: list[dict[str, Any]], *, test_year: int, anchor_week: int, horizon: int
) -> list[float] | None:
    """Read exactly N subsequent weekly observations; reject gaps instead of bridging."""
    start = None
    for i, row in enumerate(rows):
        if int(row["iso_year"]) == test_year and int(row["iso_week"]) == anchor_week:
            start = i
            break
    if start is None or start + horizon >= len(rows):
        return None
    values: list[float] = []
    previous = rows[start]
    for row in rows[start + 1 : start + horizon + 1]:
        ret = row.get("return")
        if ret is None:
            return None
        # weekly_return_rows already marks a non-adjacent observation return None.
        value = float(ret)
        if not math.isfinite(value):
            return None
        values.append(value)
        previous = row
    return values if len(values) == horizon else None


def robust_forward_horizon_stats(
    rows: list[dict[str, Any]],
    *,
    years: list[int],
    anchor_week: int,
    horizons: tuple[int, ...] = (4, 8, 12),
) -> dict[str, Any]:
    """Historical as-of→horizon returns, including ISO-year wrap correctly."""
    out: dict[str, Any] = {}
    for horizon in horizons:
        samples: list[float] = []
        for year in sorted({int(y) for y in years}):
            actual = _actual_forward_returns(
                rows,
                test_year=year,
                anchor_week=anchor_week,
                horizon=horizon,
            )
            if actual is None:
                continue
            growth = 1.0
            for ret in actual:
                growth *= 1.0 + ret
            samples.append(growth - 1.0)
        st = bucket_stats(samples)
        out[f"{horizon}w"] = {
            "mean_return": st.get("mean"),
            "median_return": st.get("median"),
            "positive_frequency": st.get("positive_frequency"),
            "n": st.get("n", 0),
            "dispersion": st.get("dispersion"),
            "source": "historical_asof_to_horizon_weekly_returns",
            "year_wrap": "supported",
        }
    return out


def projected_robust_return(
    week_stats: dict[str, dict[str, Any]] | dict[int, dict[str, Any]],
    *,
    anchor_week: int,
    horizon: int = 8,
) -> float | None:
    """Compound robust seasonal week returns forward, wrapping week 52→1."""
    growth = 1.0
    for offset in range(1, horizon + 1):
        week = ((anchor_week - 1 + offset) % 52) + 1
        st = week_stats.get(str(week)) or week_stats.get(week) or {}
        ret = st.get("trimmed_mean")
        if ret is None:
            ret = st.get("median")
        if ret is None:
            return None
        growth *= 1.0 + float(ret)
    return growth - 1.0


def robust_lookback_agreement(
    lookbacks: dict[str, dict[str, Any]], *, anchor_week: int, horizon: int = 8
) -> dict[str, Any]:
    """Sign agreement across 5/10/15/20/FULL robust-return projections."""
    projections: dict[str, float] = {}
    for label in ("5Y", "10Y", "15Y", "20Y", "FULL"):
        block = lookbacks.get(label) or {}
        ret = projected_robust_return(
            block.get("week_stats") or {}, anchor_week=anchor_week, horizon=horizon
        )
        if ret is not None:
            projections[label] = ret
    if len(projections) < 2:
        return {
            "score": None,
            "forward_8w_by_lookback": {k: round(v, 6) for k, v in projections.items()},
            "sign_agreement": None,
            "method": "robust_weekly_return_projection_sign_agreement",
        }
    signs = [1 if value > 0.002 else -1 if value < -0.002 else 0 for value in projections.values()]
    nonzero = [sign for sign in signs if sign]
    if not nonzero:
        score = 0.5
        agreement = None
    else:
        positive = sum(1 for sign in nonzero if sign > 0)
        negative = sum(1 for sign in nonzero if sign < 0)
        majority = 1 if positive >= negative else -1
        agreement = sum(1 for sign in nonzero if sign == majority) / len(nonzero)
        score = agreement
    return {
        "score": round(score, 3),
        "forward_8w_by_lookback": {k: round(v, 6) for k, v in projections.items()},
        "sign_agreement": None if agreement is None else round(agreement, 3),
        "method": "robust_weekly_return_projection_sign_agreement",
    }


def robust_weekly_leave_one_year_out(
    rows: list[dict[str, Any]],
    *,
    years: list[int],
    anchor_week: int,
    lookback: str,
    horizon: int = 8,
    minimum_training_years: int = 5,
) -> dict[str, Any]:
    """Directional leave-one-year-out validation using only prior years.

    For each test year we learn robust trimmed-mean ISO-week returns from years
    strictly before that test year, compound the next ``horizon`` seasonal
    returns (including year wrap), then compare predicted and realised direction.
    Missing observations invalidate that fold rather than being bridged.
    """
    lookback_years = LOOKBACK_YEARS.get(lookback)
    outcomes: list[dict[str, Any]] = []
    for test_year in sorted({int(y) for y in years}):
        train_years = [
            int(y)
            for y in years
            if int(y) < test_year
            and (lookback_years is None or int(y) >= test_year - lookback_years)
        ]
        if len(set(train_years)) < minimum_training_years:
            continue

        stats = _train_week_stats(
            rows, test_year=test_year, lookback_years=lookback_years
        )
        predicted_growth = 1.0
        usable = True
        for offset in range(1, horizon + 1):
            week = ((anchor_week - 1 + offset) % 52) + 1
            ret = (stats.get(week) or {}).get("trimmed_mean")
            if ret is None:
                usable = False
                break
            predicted_growth *= 1.0 + float(ret)

        actual_returns = _actual_forward_returns(
            rows,
            test_year=test_year,
            anchor_week=anchor_week,
            horizon=horizon,
        )
        if not usable or actual_returns is None:
            continue

        actual_growth = 1.0
        for ret in actual_returns:
            actual_growth *= 1.0 + ret
        predicted_return = predicted_growth - 1.0
        actual_return = actual_growth - 1.0
        hit = (
            (predicted_return > 0 and actual_return > 0)
            or (predicted_return < 0 and actual_return < 0)
            or (predicted_return == 0 and actual_return == 0)
        )
        outcomes.append(
            {
                "year": test_year,
                "training_years": sorted(set(train_years)),
                "predicted_return": round(predicted_return, 8),
                "actual_return": round(actual_return, 8),
                "direction_hit": hit,
            }
        )

    n = len(outcomes)
    hits = sum(1 for row in outcomes if row["direction_hit"])
    return {
        "method": "leave_one_year_out_robust_weekly_direction",
        "lookback": lookback,
        "horizon_weeks": horizon,
        "hit_rate": None if n == 0 else round(hits / n, 6),
        "hits": hits,
        "n": n,
        "outcomes": outcomes,
    }
