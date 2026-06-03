"""Seasonality pillar from historical calendar-month weekly returns."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

BIAS_BULLISH = "Bullish"
BIAS_NEUTRAL = "Neutral"
BIAS_BEARISH = "Bearish"

MIN_MONTH_SAMPLES = 3
BULL_THRESHOLD_PCT = 0.15
BEAR_THRESHOLD_PCT = -0.15


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _monthly_avg_returns(weekly: list[dict[str, Any]]) -> dict[int, list[float]]:
    """Map calendar month (1–12) -> list of weekly returns tagged to that month."""
    bars: list[tuple[str, float]] = []
    for b in weekly or []:
        if not isinstance(b, dict):
            continue
        c = _num(b.get("close"))
        d = str(b.get("date") or "")[:10]
        if c is not None and d:
            bars.append((d, c))
    bars.sort(key=lambda x: x[0])
    by_month: dict[int, list[float]] = defaultdict(list)
    for i in range(1, len(bars)):
        d0, c0 = bars[i - 1]
        d1, c1 = bars[i]
        if c0 == 0:
            continue
        ret = (c1 / c0 - 1.0) * 100.0
        try:
            m = datetime.strptime(d1[:10], "%Y-%m-%d").month
        except ValueError:
            continue
        by_month[m].append(ret)
    return by_month


def compute_seasonality(
    *,
    market: str,
    weekly_bars: list[dict[str, Any]] | None = None,
    as_of_week: str | None = None,
) -> dict[str, Any]:
    by_month = _monthly_avg_returns(weekly_bars or [])
    if not by_month:
        return {
            "market": market,
            "as_of_week": as_of_week,
            "wired": False,
            "seasonality_bias": "UNAVAILABLE",
            "seasonality_score": None,
            "seasonality_reason": "No weekly price bars for seasonality.",
            "calendar_month": None,
            "pass": False,
        }

    month = None
    if as_of_week:
        try:
            month = datetime.strptime(str(as_of_week)[:10], "%Y-%m-%d").month
        except ValueError:
            month = None
    if month is None:
        last = str((weekly_bars or [{}])[-1].get("date") or "")[:10]
        try:
            month = datetime.strptime(last, "%Y-%m-%d").month
        except ValueError:
            month = datetime.now().month

    samples = by_month.get(month) or []
    if len(samples) < MIN_MONTH_SAMPLES:
        return {
            "market": market,
            "as_of_week": as_of_week,
            "wired": False,
            "seasonality_bias": "UNAVAILABLE",
            "seasonality_score": None,
            "seasonality_reason": f"Only {len(samples)} historical weeks for month {month} (need {MIN_MONTH_SAMPLES}+).",
            "calendar_month": month,
            "pass": False,
        }

    avg_ret = sum(samples) / len(samples)
    month_names = (
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    )
    mname = month_names[month - 1]

    if avg_ret >= BULL_THRESHOLD_PCT:
        bias = BIAS_BULLISH
    elif avg_ret <= BEAR_THRESHOLD_PCT:
        bias = BIAS_BEARISH
    else:
        bias = BIAS_NEUTRAL

    score = round(min(10.0, max(0.0, abs(avg_ret) / 0.35)), 1)

    reason = (
        f"Historical {mname} seasonality: avg weekly return {avg_ret:+.2f}% "
        f"across {len(samples)} sample weeks in price history."
    )

    return {
        "market": market,
        "as_of_week": as_of_week,
        "wired": True,
        "seasonality_bias": bias,
        "seasonality_score": score,
        "seasonality_reason": reason,
        "calendar_month": month,
        "month_avg_return_pct": round(avg_ret, 3),
        "month_sample_weeks": len(samples),
        "pass": False,
    }


def seasonality_pass(bias: str, direction: str) -> bool:
    if bias == "UNAVAILABLE":
        return False
    d = direction.lower()
    if d == "long":
        return bias == BIAS_BULLISH
    if d == "short":
        return bias == BIAS_BEARISH
    return bias == BIAS_NEUTRAL
