"""Auditable same-calendar-week seasonal lookback.

This module is intentionally separate from the plotted seasonal roadmap.  It
answers a narrower research question: from the same ISO week in prior years,
what actually happened over the next N completed weekly closes?

Rules are fixed and deliberately conservative:
- one anchor observation per historical year;
- exact ISO-week alignment (no hand-picked neighbouring weeks);
- only genuine adjacent weekly returns supplied by ``weekly_return_rows``;
- incomplete / gapped horizons are rejected rather than bridged;
- the current/future year contributes only when the requested horizon exists;
- no selective year removal, interpolation, smoothing, or COT input.

Excursions are CLOSE-to-CLOSE excursions because the canonical input here is a
weekly close series. They must not be described as intrabar high/low MFE/MAE.
"""

from __future__ import annotations

import math
from typing import Any

from hptl.seasonality_workstation.stats import bucket_stats

HORIZONS: tuple[int, ...] = (1, 2, 4, 8, 12)
CORE_HORIZONS: tuple[int, ...] = (4, 8, 12)
DIRECTION_DEADBAND = 0.002  # 0.20%


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _median(values: list[float]) -> float | None:
    vals = sorted(v for v in values if math.isfinite(v))
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else 0.5 * (vals[mid - 1] + vals[mid])


def _annual_outcome(
    rows: list[dict[str, Any]], *, year: int, anchor_week: int, horizon: int
) -> dict[str, Any] | None:
    """Return one exact historical observation or None when the path is incomplete."""
    start = None
    for i, row in enumerate(rows):
        if int(row.get("iso_year") or -1) == int(year) and int(row.get("iso_week") or -1) == int(anchor_week):
            start = i
            break
    if start is None or start + horizon >= len(rows):
        return None

    anchor = rows[start]
    anchor_close = _finite(anchor.get("close"))
    if anchor_close is None or anchor_close <= 0:
        return None

    growth = 1.0
    cumulative: list[float] = []
    end_row = anchor
    for row in rows[start + 1 : start + horizon + 1]:
        weekly_return = _finite(row.get("return"))
        if weekly_return is None:
            return None
        growth *= 1.0 + weekly_return
        cumulative.append(growth - 1.0)
        end_row = row

    if len(cumulative) != horizon:
        return None

    terminal = cumulative[-1]
    max_up = max([0.0, *cumulative])
    max_down = min([0.0, *cumulative])
    return {
        "year": int(year),
        "anchor_date": str(anchor.get("date") or "")[:10],
        "end_date": str(end_row.get("date") or "")[:10],
        "anchor_week": int(anchor_week),
        "horizon_weeks": int(horizon),
        "return": round(terminal, 8),
        "return_pct": round(terminal * 100.0, 3),
        "max_up_close_excursion": round(max_up, 8),
        "max_up_close_excursion_pct": round(max_up * 100.0, 3),
        "max_down_close_excursion": round(max_down, 8),
        "max_down_close_excursion_pct": round(max_down * 100.0, 3),
    }


def _classify(median_return: float | None, bull: float | None, bear: float | None) -> str:
    if median_return is None:
        return "Mixed"
    if median_return > DIRECTION_DEADBAND and bull is not None and bull >= 0.55:
        return "Bullish"
    if median_return < -DIRECTION_DEADBAND and bear is not None and bear >= 0.55:
        return "Bearish"
    return "Mixed"


def _aggregate(outcomes: list[dict[str, Any]], horizon: int) -> dict[str, Any]:
    returns = [float(row["return"]) for row in outcomes]
    stats = bucket_stats(returns)
    n = int(stats.get("n") or 0)
    positive = _finite(stats.get("positive_frequency"))
    bearish = None if positive is None else 1.0 - positive
    median_return = _finite(stats.get("median"))
    direction = _classify(median_return, positive, bearish)

    up = [float(row["max_up_close_excursion"]) for row in outcomes]
    down = [float(row["max_down_close_excursion"]) for row in outcomes]
    med_up = _median(up)
    med_down = _median(down)

    if direction == "Bullish":
        favourable = med_up
        adverse = None if med_down is None else abs(med_down)
    elif direction == "Bearish":
        favourable = None if med_down is None else abs(med_down)
        adverse = med_up
    else:
        favourable = None
        adverse = None

    return {
        "weeks": int(horizon),
        "n": n,
        "mean": _finite(stats.get("mean")),
        "median": median_return,
        "mean_pct": None if stats.get("mean") is None else round(float(stats["mean"]) * 100.0, 3),
        "median_pct": None if median_return is None else round(median_return * 100.0, 3),
        "bullish_frequency": positive,
        "bearish_frequency": bearish,
        "dispersion": _finite(stats.get("dispersion")),
        "direction": direction,
        "median_up_close_excursion": med_up,
        "median_up_close_excursion_pct": None if med_up is None else round(med_up * 100.0, 3),
        "median_down_close_excursion": med_down,
        "median_down_close_excursion_pct": None if med_down is None else round(med_down * 100.0, 3),
        "median_favourable_close_excursion": favourable,
        "median_favourable_close_excursion_pct": None if favourable is None else round(favourable * 100.0, 3),
        "median_adverse_close_excursion": adverse,
        "median_adverse_close_excursion_pct": None if adverse is None else round(adverse * 100.0, 3),
        "annual_outcomes": outcomes,
        "source": "exact_same_iso_week_historical_weekly_close_outcomes",
        "year_wrap": "supported",
    }


def _overall(horizons: dict[str, dict[str, Any]]) -> dict[str, Any]:
    core = [horizons.get(f"{weeks}w") or {} for weeks in CORE_HORIZONS]
    usable = [row for row in core if int(row.get("n") or 0) > 0]
    directions = [str(row.get("direction") or "Mixed") for row in usable]
    bullish = sum(1 for d in directions if d == "Bullish")
    bearish = sum(1 for d in directions if d == "Bearish")
    if bullish >= 2:
        direction = "Bullish"
    elif bearish >= 2:
        direction = "Bearish"
    else:
        direction = "Mixed"

    core_ns = [int(row.get("n") or 0) for row in core]
    min_n = min(core_ns) if core_ns else 0
    aligned = sum(1 for d in directions if d == direction) if direction != "Mixed" else 0
    horizon_consistency = aligned / len(directions) if directions else 0.0

    directional_freqs: list[float] = []
    for row in usable:
        if direction == "Bullish":
            value = _finite(row.get("bullish_frequency"))
        elif direction == "Bearish":
            value = _finite(row.get("bearish_frequency"))
        else:
            value = None
        if value is not None:
            directional_freqs.append(value)
    avg_directional_frequency = (
        sum(directional_freqs) / len(directional_freqs) if directional_freqs else None
    )

    if direction == "Mixed" or min_n < 5:
        confidence = "LOW"
    elif min_n >= 12 and horizon_consistency >= (2 / 3) and (avg_directional_frequency or 0) >= 0.65:
        confidence = "HIGH"
    elif min_n >= 7 and horizon_consistency >= (2 / 3) and (avg_directional_frequency or 0) >= 0.58:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    reasons: list[str] = []
    if min_n < 5:
        reasons.append(f"minimum_core_sample_below_5:{min_n}")
    if direction == "Mixed":
        reasons.append("4w_8w_12w_direction_not_consistent")
    if avg_directional_frequency is not None and avg_directional_frequency < 0.58:
        reasons.append("directional_frequency_below_58pct")

    return {
        "direction": direction,
        "confidence": confidence,
        "minimum_core_sample": min_n,
        "horizon_consistency": round(horizon_consistency, 3),
        "average_directional_frequency": None
        if avg_directional_frequency is None
        else round(avg_directional_frequency, 4),
        "reasons": reasons,
    }


def build_reliable_seasonal_lookback(
    rows: list[dict[str, Any]],
    *,
    years: list[int],
    anchor_week: int,
    lookback: str,
    horizons: tuple[int, ...] = HORIZONS,
) -> dict[str, Any]:
    """Build auditable per-year seasonal outcomes and conservative summary stats."""
    unique_years = sorted({int(year) for year in years})
    if not unique_years or not (1 <= int(anchor_week) <= 52):
        return {
            "available": False,
            "lookback": lookback,
            "anchor_week": anchor_week,
            "reason": "missing_sample_years_or_anchor_week",
        }

    horizon_stats: dict[str, dict[str, Any]] = {}
    for horizon in horizons:
        outcomes = []
        for year in unique_years:
            outcome = _annual_outcome(
                rows,
                year=year,
                anchor_week=int(anchor_week),
                horizon=int(horizon),
            )
            if outcome is not None:
                outcomes.append(outcome)
        horizon_stats[f"{int(horizon)}w"] = _aggregate(outcomes, int(horizon))

    overall = _overall(horizon_stats)
    return {
        "available": any(int(row.get("n") or 0) > 0 for row in horizon_stats.values()),
        "lookback": lookback,
        "anchor_week": int(anchor_week),
        "sample_years_requested": unique_years,
        "sample_year_count_requested": len(unique_years),
        "horizons": horizon_stats,
        # Compatibility with the visible roadmap side panel.
        "forecast_stats": horizon_stats,
        "overall": overall,
        "method": {
            "version": "seasonal_same_iso_week_lookback_v1",
            "alignment": "exact_iso_week",
            "one_observation_per_year": True,
            "weekly_observation": "completed_weekly_close",
            "missing_week_policy": "reject_horizon_do_not_bridge",
            "future_data_policy": "historical_rows_only_as_supplied_by_asof_filtered_price_history",
            "year_wrap": "supported",
            "selective_year_removal": False,
            "interpolation": False,
            "smoothing": False,
            "cot_dependency": "none",
            "excursion_definition": "close_to_close_not_intrabar_high_low",
        },
    }
