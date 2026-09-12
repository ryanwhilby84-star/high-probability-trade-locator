"""Annual high/low timing analysis for Seasonality Workstation.

Finds where each completed calendar year made its smoothed weekly trough/peak,
then measures how tightly those turning points cluster in the calendar.  Price
levels are deliberately not averaged: only timing and subsequent returns are
compared across years.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import date
from typing import Any

from hptl.seasonality_workstation.returns import iso_week, weekly_closes_from_daily

SMOOTH_WEEKS = 3
WINDOW_HALF_WIDTH = 4
FORWARD_WEEKS = (4, 8, 12)
MIN_YEARS = 5


def _circular_week_distance(a: int, b: int) -> int:
    diff = abs(int(a) - int(b))
    return min(diff, 52 - diff)


def _circular_median_week(weeks: list[int]) -> int | None:
    if not weeks:
        return None
    # Week number is circular. Pick the observed week with the minimum total
    # circular distance rather than using a naive arithmetic mean around New Year.
    return min(weeks, key=lambda w: sum(_circular_week_distance(w, x) for x in weeks))


def _week_in_window(week: int, centre: int, half_width: int = WINDOW_HALF_WIDTH) -> bool:
    return _circular_week_distance(week, centre) <= half_width


def _smoothed_by_year(weekly: list[tuple[str, float]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[tuple[int, str, float]]] = defaultdict(list)
    for d, close in weekly:
        y, w = iso_week(d)
        if math.isfinite(float(close)) and float(close) > 0:
            grouped[int(y)].append((int(w), str(d)[:10], float(close)))

    out: dict[int, list[dict[str, Any]]] = {}
    for year, rows in grouped.items():
        rows = sorted(rows, key=lambda r: r[0])
        smoothed: list[dict[str, Any]] = []
        for i, (week, d, close) in enumerate(rows):
            lo = max(0, i - 1)
            hi = min(len(rows), i + 2)
            window = [r[2] for r in rows[lo:hi]]
            smooth = statistics.median(window) if window else close
            smoothed.append({"week": week, "date": d, "close": close, "smooth": smooth})
        out[year] = smoothed
    return out


def _forward_return(rows: list[dict[str, Any]], index: int, weeks: int) -> float | None:
    target = index + weeks
    if index < 0 or target >= len(rows):
        return None
    a = float(rows[index]["close"])
    b = float(rows[target]["close"])
    if a <= 0 or not math.isfinite(a) or not math.isfinite(b):
        return None
    return (b / a - 1.0) * 100.0


def _summarise_turn(
    turns: list[dict[str, Any]],
    *,
    kind: str,
    current_week: int,
) -> dict[str, Any]:
    weeks = [int(t["week"]) for t in turns]
    centre = _circular_median_week(weeks)
    if centre is None:
        return {"kind": kind, "available": False, "sample_size": 0}

    in_window = [t for t in turns if _week_in_window(int(t["week"]), centre)]
    concentration = len(in_window) / len(turns) if turns else 0.0
    distance = _circular_week_distance(current_week, centre)
    active = distance <= WINDOW_HALF_WIDTH
    approaching = not active and distance <= WINDOW_HALF_WIDTH + 4

    forward: dict[str, Any] = {}
    for horizon in FORWARD_WEEKS:
        vals = [float(t[f"forward_{horizon}w_pct"]) for t in turns if t.get(f"forward_{horizon}w_pct") is not None]
        forward[f"{horizon}w"] = {
            "n": len(vals),
            "median_pct": round(statistics.median(vals), 2) if vals else None,
            "mean_pct": round(statistics.fmean(vals), 2) if vals else None,
            "positive_frequency": round(sum(1 for v in vals if v > 0) / len(vals), 4) if vals else None,
        }

    return {
        "kind": kind,
        "available": True,
        "sample_size": len(turns),
        "median_week": centre,
        "window_start_week": ((centre - WINDOW_HALF_WIDTH - 1) % 52) + 1,
        "window_end_week": ((centre + WINDOW_HALF_WIDTH - 1) % 52) + 1,
        "window_half_width_weeks": WINDOW_HALF_WIDTH,
        "within_window_count": len(in_window),
        "concentration": round(concentration, 4),
        "current_week": current_week,
        "distance_to_median_week": distance,
        "status": "ACTIVE" if active else "APPROACHING" if approaching else "OUTSIDE",
        "forward_returns": forward,
        "turns": turns,
    }


def build_annual_turning_points(
    daily: list[tuple[str, float]],
    *,
    lookback_years: int = 15,
    asof: str | None = None,
) -> dict[str, Any]:
    """Return annual trough/peak timing and post-turn behaviour.

    Completed years only are used for the historical sample.  The current year's
    price is used solely for context (week number and 52-week range percentile).
    """
    if not daily:
        return {"available": False, "reason": "no_daily_history"}

    cutoff_date = str(asof or daily[-1][0])[:10]
    scoped = [(d, float(c)) for d, c in daily if str(d)[:10] <= cutoff_date]
    weekly = weekly_closes_from_daily(scoped)
    if not weekly:
        return {"available": False, "reason": "no_weekly_history"}

    current_year, current_week = iso_week(weekly[-1][0])
    grouped = _smoothed_by_year(weekly)
    years = [y for y in sorted(grouped) if y < current_year and y >= current_year - int(lookback_years)]

    lows: list[dict[str, Any]] = []
    highs: list[dict[str, Any]] = []
    for year in years:
        rows = grouped.get(year) or []
        if len(rows) < 40:
            continue
        low_i = min(range(len(rows)), key=lambda i: rows[i]["smooth"])
        high_i = max(range(len(rows)), key=lambda i: rows[i]["smooth"])
        for collection, idx, kind in ((lows, low_i, "LOW"), (highs, high_i, "HIGH")):
            r = rows[idx]
            item: dict[str, Any] = {
                "year": year,
                "week": int(r["week"]),
                "date": r["date"],
                "close": round(float(r["close"]), 6),
                "kind": kind,
            }
            for horizon in FORWARD_WEEKS:
                ret = _forward_return(rows, idx, horizon)
                item[f"forward_{horizon}w_pct"] = None if ret is None else round(ret, 3)
            collection.append(item)

    if len(lows) < MIN_YEARS or len(highs) < MIN_YEARS:
        return {
            "available": False,
            "reason": "insufficient_complete_years",
            "sample_size": min(len(lows), len(highs)),
        }

    recent_52 = weekly[-52:] if len(weekly) >= 52 else weekly
    current_close = float(weekly[-1][1])
    closes_52 = [float(c) for _, c in recent_52]
    low52 = min(closes_52)
    high52 = max(closes_52)
    pct52 = None if high52 <= low52 else (current_close - low52) / (high52 - low52)

    low_summary = _summarise_turn(lows, kind="LOW", current_week=current_week)
    high_summary = _summarise_turn(highs, kind="HIGH", current_week=current_week)
    return {
        "available": True,
        "engine": "annual_turning_points_v1",
        "method": "3-week median-smoothed weekly closes; completed years only",
        "lookback_years": int(lookback_years),
        "sample_years": [t["year"] for t in lows],
        "sample_size": len(lows),
        "asof": weekly[-1][0],
        "current_year": current_year,
        "current_week": current_week,
        "current_close": round(current_close, 6),
        "range_52w": {
            "low": round(low52, 6),
            "high": round(high52, 6),
            "percentile": None if pct52 is None else round(pct52, 4),
        },
        "annual_low": low_summary,
        "annual_high": high_summary,
    }
