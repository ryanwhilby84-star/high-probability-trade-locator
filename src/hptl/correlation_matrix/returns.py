"""Percentage returns from closing prices.

Correlations are never computed on price levels — only on returns.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

ReturnFrequency = Literal["daily", "weekly"]


def iso_week(date: str) -> tuple[int, int]:
    dt = datetime.strptime(str(date)[:10], "%Y-%m-%d")
    cal = dt.isocalendar()
    week = int(cal.week)
    if week > 52:
        week = 52
    return int(cal.year), week


def weekly_closes_from_daily(daily: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """Last trading-day close of each ISO week. No forward fill."""
    buckets: dict[tuple[int, int], tuple[str, float]] = {}
    for d, c in daily:
        if c is None:
            continue
        try:
            px = float(c)
        except (TypeError, ValueError):
            continue
        if not (px == px) or px <= 0:
            continue
        y, w = iso_week(d)
        prev = buckets.get((y, w))
        if prev is None or d >= prev[0]:
            buckets[(y, w)] = (str(d)[:10], px)
    return [buckets[k] for k in sorted(buckets.keys())]


def percentage_returns(closes: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """r_t = P_t / P_{t-1} - 1. Skips non-finite / non-positive closes. No ffill."""
    out: list[tuple[str, float]] = []
    prev: float | None = None
    for d, c in closes:
        try:
            px = float(c)
        except (TypeError, ValueError):
            prev = None
            continue
        if not (px == px) or px <= 0:
            prev = None
            continue
        if prev is not None and prev > 0:
            r = px / prev - 1.0
            if r == r:  # finite
                out.append((str(d)[:10], r))
        prev = px
    return out


def closes_for_frequency(
    daily: list[tuple[str, float]],
    frequency: ReturnFrequency,
) -> list[tuple[str, float]]:
    if frequency == "weekly":
        return weekly_closes_from_daily(daily)
    if frequency == "daily":
        cleaned: list[tuple[str, float]] = []
        for d, c in daily:
            try:
                px = float(c)
            except (TypeError, ValueError):
                continue
            if px == px and px > 0:
                cleaned.append((str(d)[:10], px))
        cleaned.sort(key=lambda t: t[0])
        return cleaned
    raise ValueError(f"Unsupported return frequency: {frequency!r}")


def returns_for_frequency(
    daily: list[tuple[str, float]],
    frequency: ReturnFrequency,
) -> list[tuple[str, float]]:
    return percentage_returns(closes_for_frequency(daily, frequency))
