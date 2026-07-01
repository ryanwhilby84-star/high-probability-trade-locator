"""Median-band weekly outlier filter for seasonality (unit-scale break repair).

Scope: instruments with known Alpha Vantage $/unit mixing (Copper, Corn).
Applied before compute_seasonality_price_block() — does not alter projection math.
"""

from __future__ import annotations

import statistics
from typing import Any

from hptl.seasonality.seasonality_engine import compute_seasonality_price_block

OUTLIER_FILTER_MARKETS: frozenset[str] = frozenset({"Copper / HG", "Corn"})

MEDIAN_BAND_LO = 0.2
MEDIAN_BAND_HI = 4.0
UNIT_BREAK_RATIO = 4.0
MAX_INDEXED_GRADE_A = 500.0
MAX_PROJECTION_GRADE_A = 500.0
MIN_BARS_AFTER_FILTER = 52


def _median_close(bars: list[tuple[str, float]]) -> float | None:
    closes = [c for _, c in bars if c > 0]
    if not closes:
        return None
    return float(statistics.median(closes))


def _max_indexed_from_block(block: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return (max indexed chart value, max forward projection) from a seasonality block."""
    if not block.get("available"):
        return None, None
    max_idx = 0.0
    max_proj = 0.0
    for row in block.get("chart_series") or []:
        for key in ("actual", "seasonal_10y", "seasonal_5y", "seasonal_3y"):
            v = row.get(key)
            if isinstance(v, (int, float)):
                max_idx = max(max_idx, float(v))
        for key in ("proj_10y", "proj_5y", "proj_3y"):
            v = row.get(key)
            if isinstance(v, (int, float)):
                max_proj = max(max_proj, float(v))
    return (max_idx if max_idx > 0 else None, max_proj if max_proj > 0 else None)


def max_indexed_from_bars(market: str, bars: list[tuple[str, float]]) -> tuple[float | None, float | None]:
    """Compute seasonality block max indexed + projection without persisting (audit helper)."""
    if not bars:
        return None, None
    block = compute_seasonality_price_block(
        market,
        bars,
        price_store_key=market,
        bar_source="audit",
    )
    return _max_indexed_from_block(block)


def filter_weekly_bars_for_seasonality(
    market: str,
    bars: list[tuple[str, float]],
    *,
    compute_before_after: bool = True,
) -> tuple[list[tuple[str, float]], dict[str, Any]]:
    """Drop weekly closes outside median band for scoped instruments."""
    audit: dict[str, Any] = {
        "market": market,
        "applied": False,
        "median_close": None,
        "bars_before": len(bars),
        "bars_after": len(bars),
        "bars_dropped": 0,
        "dates_dropped": [],
        "closes_dropped": [],
        "unit_scale_break_detected": False,
        "max_indexed_before": None,
        "max_projection_before": None,
        "max_indexed_after": None,
        "max_projection_after": None,
    }

    if market not in OUTLIER_FILTER_MARKETS or not bars:
        return bars, audit

    median = _median_close(bars)
    if median is None or median <= 0:
        audit["skip_reason"] = "no positive median"
        return bars, audit

    audit["median_close"] = round(median, 6)
    raw_closes = [c for _, c in bars]
    raw_max = max(raw_closes)
    audit["unit_scale_break_detected"] = raw_max / median > UNIT_BREAK_RATIO

    lo = median * MEDIAN_BAND_LO
    hi = median * MEDIAN_BAND_HI
    kept: list[tuple[str, float]] = []
    dropped_dates: list[str] = []
    dropped_closes: list[float] = []

    for date, close in bars:
        if lo <= close <= hi:
            kept.append((date, close))
        else:
            dropped_dates.append(date)
            dropped_closes.append(round(close, 6))

    if len(kept) < MIN_BARS_AFTER_FILTER:
        audit["skip_reason"] = f"filter would leave {len(kept)} bars (< {MIN_BARS_AFTER_FILTER})"
        return bars, audit

    audit["applied"] = True
    audit["bars_after"] = len(kept)
    audit["bars_dropped"] = len(dropped_dates)
    audit["dates_dropped"] = dropped_dates
    audit["closes_dropped"] = dropped_closes
    audit["band_lo"] = round(lo, 6)
    audit["band_hi"] = round(hi, 6)

    if compute_before_after:
        audit["max_indexed_before"], audit["max_projection_before"] = max_indexed_from_bars(market, bars)
        audit["max_indexed_after"], audit["max_projection_after"] = max_indexed_from_bars(market, kept)

    return kept, audit
