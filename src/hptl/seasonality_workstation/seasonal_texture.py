"""Volatility-normalised daily seasonal texture helpers.

The goal is to preserve recurrent day-to-day seasonal structure without adding
synthetic noise. Historical years are first expressed in units of each year's
own typical absolute daily move. The cross-year median normalised return for
each trading-day ordinal is then rescaled to the current market's recent robust
daily volatility. This keeps the path price-like while remaining entirely
data-derived.
"""
from __future__ import annotations

from datetime import date
from math import isfinite
from typing import Any

MIN_SCALE = 1e-6
RECENT_VOL_DAYS = 60


def _median(values: list[float]) -> float | None:
    xs = sorted(float(v) for v in values if isfinite(float(v)))
    if not xs:
        return None
    n = len(xs)
    m = n // 2
    return xs[m] if n % 2 else 0.5 * (xs[m - 1] + xs[m])


def _daily_returns(rows: list[tuple[date, float]]) -> list[float]:
    out: list[float] = []
    for i in range(1, len(rows)):
        p0 = float(rows[i - 1][1])
        p1 = float(rows[i][1])
        if p0 <= 0:
            continue
        r = p1 / p0 - 1.0
        if isfinite(r):
            out.append(r)
    return out


def robust_daily_scale_from_year(rows: list[tuple[date, float]]) -> float | None:
    """Median absolute close-to-close return for one historical year."""
    values = [abs(r) for r in _daily_returns(rows) if r != 0]
    med = _median(values)
    if med is None:
        return None
    return max(float(med), MIN_SCALE)


def recent_daily_scale(
    daily: list[tuple[str, float]], *, asof: str, window: int = RECENT_VOL_DAYS
) -> float | None:
    """Robust recent daily volatility used to put seasonal texture into price units."""
    rows = [(str(d)[:10], float(c)) for d, c in daily if str(d)[:10] <= asof and float(c) > 0]
    rets: list[float] = []
    for i in range(1, len(rows)):
        p0 = rows[i - 1][1]
        p1 = rows[i][1]
        if p0 <= 0:
            continue
        r = p1 / p0 - 1.0
        if isfinite(r) and r != 0:
            rets.append(abs(r))
    if not rets:
        return None
    recent = rets[-max(10, int(window)) :]
    med = _median(recent)
    if med is None:
        return None
    return max(float(med), MIN_SCALE)


def build_texture_profile(
    years: dict[int, list[tuple[date, float]]],
    *,
    target_scale: float,
) -> tuple[list[dict[str, Any]], int]:
    """Build one robust seasonal return per trading-day ordinal.

    For each year y, r[y,d] is divided by that year's median absolute daily
    return. The seasonal score for trading day d is the cross-year median of
    those normalised returns. Multiplying by ``target_scale`` converts the score
    back into a return appropriate for the current market regime.
    """
    if not years:
        return [], 0
    d_len = min(len(rows) for rows in years.values())
    if d_len < 2:
        return [], 0

    scales = {
        year: robust_daily_scale_from_year(rows)
        for year, rows in years.items()
    }
    scales = {year: scale for year, scale in scales.items() if scale is not None and scale > 0}
    if len(scales) < 5:
        return [], 0

    profile: list[dict[str, Any]] = []
    for i in range(d_len):
        if i == 0:
            profile.append(
                {
                    "trading_day": 1,
                    "n": len(scales),
                    "seasonal_return": 0.0,
                    "normalised_median": 0.0,
                    "directional_agreement": None,
                    "positive_frequency": None,
                    "typical_abs_normalised_move": None,
                    "target_daily_scale": target_scale,
                }
            )
            continue

        z_values: list[float] = []
        for year, rows in years.items():
            scale = scales.get(year)
            if scale is None or i >= len(rows):
                continue
            p0 = float(rows[i - 1][1])
            p1 = float(rows[i][1])
            if p0 <= 0:
                continue
            r = p1 / p0 - 1.0
            if isfinite(r):
                z_values.append(r / scale)

        med_z = _median(z_values)
        typical_abs = _median([abs(z) for z in z_values])
        pos = None if not z_values else sum(1 for z in z_values if z > 0) / len(z_values)
        agreement = None if pos is None else abs(2.0 * pos - 1.0)
        seasonal_return = 0.0 if med_z is None else float(med_z) * float(target_scale)
        profile.append(
            {
                "trading_day": i + 1,
                "n": len(z_values),
                "seasonal_return": seasonal_return,
                "normalised_median": med_z,
                "directional_agreement": agreement,
                "positive_frequency": pos,
                "typical_abs_normalised_move": typical_abs,
                "target_daily_scale": target_scale,
            }
        )
    return profile, d_len


def compound_texture(profile: list[dict[str, Any]]) -> list[float]:
    idx = 100.0
    out: list[float] = []
    for i, row in enumerate(profile):
        if i:
            r = float(row.get("seasonal_return") or 0.0)
            idx *= 1.0 + r
        out.append(idx)
    return out
