"""Seasonal Roadmap v1 — final prototype (separate from Freeze and mean-return path).

Equations (exact)
-----------------
1) Full-year indexed path for each complete historical year y:
       F_{y,d} = P_{y,d} / P_{y,1}

2) Average by aligned trading day:
       G_d = (1/N) * sum_y F_{y,d}

3) Rebase to as-of price (no centering, no amplitude scaling):
       S_d = P_today * (G_d / G_{d*})

Optional display-only: centred SMA(5) on S (or G before rebase — applied to S).
Unsmoothed path always available.

Forecast statistics are computed separately from historical as-of→horizon
returns across sample years — never inferred from roadmap plotted amplitude.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from hptl.seasonality_workstation.indexed_seasonality import (
    DEFAULT_LOOKBACK_YEARS,
    FREEZE_SMOOTH_WINDOW,
    MIN_BARS_PER_YEAR,
    _date_axis_for_trading_days,
    _trading_day_index_for_asof,
    calendar_doy,
    centered_sma,
    complete_year_bars,
)

METHOD_VERSION = "seasonal_roadmap_v1"
METHOD_NAME = "avg_indexed_year_path_asof_rebase"
PRODUCT_NAME = "Seasonal Roadmap"

# Trading-day horizons ≈ 4/8/12/26/48 calendar weeks
HORIZON_WEEKS = (4, 8, 12, 26, 48)
HORIZON_TRADING_DAYS = {4: 20, 8: 40, 12: 60, 26: 130, 48: 240}


def _parse(d: str) -> date:
    return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()


def year_indexed_path(rows: list[tuple[date, float]]) -> list[float]:
    """F_{y,d} = P_{y,d} / P_{y,1}."""
    if not rows:
        return []
    base = rows[0][1]
    if base <= 0:
        return []
    return [c / base for _, c in rows]


def average_indexed_paths(year_paths: dict[int, list[float]]) -> tuple[list[float], int]:
    """G_d = mean_y F_{y,d}; D = min common trading-day length."""
    if not year_paths:
        return [], 0
    d_len = min(len(p) for p in year_paths.values())
    if d_len < 2:
        return [], 0
    n = len(year_paths)
    g = [sum(p[d] for p in year_paths.values()) / n for d in range(d_len)]
    return g, d_len


def rebase_indexed_to_price(
    g: list[float], *, asof_td: int, anchor_price: float
) -> list[float]:
    """S_d = P_today * (G_d / G_{d*})."""
    if not g or anchor_price <= 0:
        return []
    i0 = min(max(1, asof_td), len(g)) - 1
    base = g[i0]
    if base <= 0:
        return []
    scale = anchor_price / base
    return [v * scale for v in g]


def _anchor_close(daily: list[tuple[str, float]], asof: str) -> float | None:
    last = None
    for d_s, c in daily:
        if d_s > asof:
            break
        last = float(c)
    return last


def _asof_td_in_year(rows: list[tuple[date, float]], asof_md: tuple[int, int]) -> int | None:
    """1-based trading-day index of first bar on/after (month, day) in that year."""
    target_m, target_d = asof_md
    for i, (d, _) in enumerate(rows):
        if (d.month, d.day) >= (target_m, target_d):
            return i + 1
    return None


def _continuous_closes(
    year_bars: dict[int, list[tuple[date, float]]],
    daily: list[tuple[str, float]] | None,
) -> list[tuple[date, float]]:
    """Prefer full daily series so long horizons can cross calendar years."""
    if daily:
        out: list[tuple[date, float]] = []
        for d_s, c in daily:
            try:
                out.append((_parse(d_s), float(c)))
            except Exception:
                continue
        if out:
            return out
    # Fallback: stitch complete years in order (still allows cross-year hops).
    stitched: list[tuple[date, float]] = []
    for y in sorted(year_bars.keys()):
        stitched.extend(year_bars[y])
    return stitched


def historical_horizon_stats(
    year_bars: dict[int, list[tuple[date, float]]],
    *,
    asof: str,
    horizons_weeks: tuple[int, ...] = HORIZON_WEEKS,
    daily: list[tuple[str, float]] | None = None,
) -> dict[str, Any]:
    """Mean / median / bearish frequency of asof→horizon returns across years.

    Uses each historical year's price path from the same calendar position as
    asof, forward by ~weeks of trading days. Horizons may cross year-end
    (required for 26W / 48W from mid-year as-of). Independent of roadmap amplitude.
    """
    asof_d = _parse(asof)
    asof_md = (asof_d.month, asof_d.day)
    closes = _continuous_closes(year_bars, daily)
    out: dict[str, Any] = {}

    # Index continuous closes once for O(1) forward walks.
    by_date = {d: i for i, (d, _) in enumerate(closes)}

    for weeks in horizons_weeks:
        h_td = HORIZON_TRADING_DAYS[weeks]
        rets: list[float] = []
        for y, rows in year_bars.items():
            start_td = _asof_td_in_year(rows, asof_md)
            if start_td is None:
                continue
            start_date = rows[start_td - 1][0]
            i0 = by_date.get(start_date)
            if i0 is None:
                continue
            i1 = i0 + h_td
            if i1 >= len(closes):
                continue
            # Only score a year when the horizon lands after the start year
            # (or within it) using real subsequent bars — never wrap seasonally.
            p0 = closes[i0][1]
            p1 = closes[i1][1]
            if p0 <= 0:
                continue
            # Guard: start must belong to sample year y
            if closes[i0][0].year != y:
                continue
            rets.append(p1 / p0 - 1.0)

        if not rets:
            out[f"{weeks}w"] = {
                "weeks": weeks,
                "trading_days": h_td,
                "n": 0,
                "mean": None,
                "median": None,
                "mean_pct": None,
                "median_pct": None,
                "bearish_frequency": None,
                "bullish_frequency": None,
            }
            continue

        rets_sorted = sorted(rets)
        n = len(rets_sorted)
        mid = n // 2
        median = (
            rets_sorted[mid]
            if n % 2 == 1
            else 0.5 * (rets_sorted[mid - 1] + rets_sorted[mid])
        )
        mean = sum(rets) / n
        bear = sum(1 for r in rets if r < 0) / n
        bull = sum(1 for r in rets if r > 0) / n
        out[f"{weeks}w"] = {
            "weeks": weeks,
            "trading_days": h_td,
            "n": n,
            "mean": round(mean, 6),
            "median": round(median, 6),
            "mean_pct": round(mean * 100.0, 3),
            "median_pct": round(median * 100.0, 3),
            "bearish_frequency": round(bear, 4),
            "bullish_frequency": round(bull, 4),
            "source": "historical_asof_to_horizon_returns",
            "not_from_roadmap_amplitude": True,
        }
    return out


def build_seasonal_roadmap(
    daily: list[tuple[str, float]],
    *,
    asof: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    smooth: int | None = FREEZE_SMOOTH_WINDOW,
) -> dict[str, Any]:
    """Core roadmap maths. smooth=None → unsmoothed only; else also SMA(smooth)."""
    if not daily:
        return {"available": False, "reason": "no_daily_bars"}

    asof = asof or daily[-1][0]
    asof = max((d for d, _ in daily if d <= asof), default=daily[-1][0])
    anchor = _anchor_close(daily, asof)
    if anchor is None or anchor <= 0:
        return {"available": False, "reason": "no_anchor_price"}

    years = complete_year_bars(
        daily, asof=asof, lookback_years=lookback_years, min_bars=MIN_BARS_PER_YEAR
    )
    if len(years) < 5:
        return {
            "available": False,
            "reason": "insufficient_complete_years",
            "sample_size": len(years),
            "sample_years": sorted(years.keys()),
        }

    year_paths = {y: year_indexed_path(rows) for y, rows in years.items()}
    year_paths = {y: p for y, p in year_paths.items() if len(p) >= MIN_BARS_PER_YEAR}
    if len(year_paths) < 5:
        return {"available": False, "reason": "insufficient_indexed_years"}

    g, d_len = average_indexed_paths(year_paths)
    if d_len < MIN_BARS_PER_YEAR:
        return {"available": False, "reason": "insufficient_common_trading_days", "D": d_len}

    asof_td = _trading_day_index_for_asof(daily, asof)
    asof_td = min(max(1, asof_td), d_len)
    prices_raw = rebase_indexed_to_price(g, asof_td=asof_td, anchor_price=anchor)

    prices_smooth = None
    if smooth is not None and int(smooth) > 1:
        # Smooth G, then rebase — asof pins exactly; no amplitude scaling
        g_sm = centered_sma(g, int(smooth))
        prices_smooth = rebase_indexed_to_price(
            g_sm, asof_td=asof_td, anchor_price=anchor
        )

    forecast_stats = historical_horizon_stats(
        {y: years[y] for y in year_paths},
        asof=asof,
        daily=daily,
    )

    return {
        "available": True,
        "asof": asof,
        "anchor_price": anchor,
        "sample_years": sorted(year_paths.keys()),
        "sample_size": len(year_paths),
        "D": d_len,
        "asof_trading_day": asof_td,
        "G": g,
        "prices_raw": prices_raw,
        "prices_smooth": prices_smooth,
        "smooth_window": int(smooth) if smooth and int(smooth) > 1 else None,
        "year_bars": {y: years[y] for y in year_paths},
        "forecast_stats": forecast_stats,
        "method": {
            "version": METHOD_VERSION,
            "name": METHOD_NAME,
            "product": PRODUCT_NAME,
            "lookback_years": lookback_years,
            "alignment": "trading_day_of_year",
            "index": "F_y,d = P_y,d / P_y,1",
            "aggregation": "arithmetic_mean_of_indexed_paths",
            "centering": "none",
            "amplitude_scaling": "none",
            "rebase": "S_d = P_today * (G_d / G_d_star)",
            "smooth": int(smooth) if smooth and int(smooth) > 1 else None,
            "smooth_optional": True,
            "filters_forbidden": ["fourier", "stl", "hp", "selective_year_removal"],
            "excludes_incomplete_current_year": True,
            "units": "price",
            "forecast_stats_source": "historical_asof_to_horizon_returns",
        },
    }


def _pack_full_year(
    prices: list[float],
    *,
    g: list[float],
    daily: list[tuple[str, float]],
    asof: str,
    asof_td: int,
    d_len: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    asof_d = _parse(asof)
    date_axis = _date_axis_for_trading_days(daily, asof=asof, d_len=d_len)

    def _next_trading_date(d0: date) -> date:
        nxt = d0 + timedelta(days=1)
        while nxt.weekday() >= 5:
            nxt += timedelta(days=1)
        return nxt

    full_year: list[dict[str, Any]] = []
    for i, px in enumerate(prices):
        td = i + 1
        dt = date_axis[i] if i < len(date_axis) else (asof_d + timedelta(days=i)).isoformat()
        if td < asof_td:
            segment = "historical"
        elif td == asof_td:
            segment = "today"
            dt = asof
        else:
            segment = "forward"
        full_year.append(
            {
                "trading_day": td,
                "doy": calendar_doy(_parse(dt)) if len(dt) >= 10 else td,
                "date": dt,
                "price": round(px, 6),
                "index": round(g[i], 8),
                "segment": segment,
            }
        )

    historical = [p for p in full_year if p["segment"] in ("historical", "today")]
    asof_price = prices[asof_td - 1]
    forward: list[dict[str, Any]] = []
    cursor = asof_d
    for offset, i in enumerate(range(asof_td - 1, d_len)):
        px = prices[i]
        if offset == 0:
            dt = asof
        else:
            cursor = _next_trading_date(cursor)
            dt = cursor.isoformat()
        forward.append(
            {
                "trading_day": i + 1,
                "doy": calendar_doy(_parse(dt)),
                "date": dt,
                "price": round(px, 6),
                "index": round(g[i], 8),
                "segment": "today" if offset == 0 else "forward",
                "offset_trading_days": offset,
                "cumulative_return": round(px / asof_price - 1.0, 6) if asof_price else 0.0,
            }
        )
    return full_year, historical, forward


def build_seasonal_roadmap_curve(
    daily: list[tuple[str, float]],
    *,
    asof: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    smooth: int | None = FREEZE_SMOOTH_WINDOW,
) -> dict[str, Any]:
    """Workstation payload: raw + optional SMA(5), separate forecast_stats."""
    core = build_seasonal_roadmap(
        daily, asof=asof, lookback_years=lookback_years, smooth=smooth
    )
    if not core.get("available"):
        return core

    asof = core["asof"]
    d_len = core["D"]
    asof_td = core["asof_trading_day"]
    g = core["G"]
    raw = core["prices_raw"]
    sm = core["prices_smooth"]

    full_raw, hist_raw, fwd_raw = _pack_full_year(
        raw, g=g, daily=daily, asof=asof, asof_td=asof_td, d_len=d_len
    )
    full_sm = hist_sm = fwd_sm = None
    if sm is not None:
        full_sm, hist_sm, fwd_sm = _pack_full_year(
            sm, g=g, daily=daily, asof=asof, asof_td=asof_td, d_len=d_len
        )

    return {
        "available": True,
        "method": core["method"],
        "asof": asof,
        "asof_doy": calendar_doy(_parse(asof)),
        "asof_trading_day": asof_td,
        "anchor_price": round(core["anchor_price"], 6),
        "asof_price": round(raw[asof_td - 1], 6),
        "sample_years": core["sample_years"],
        "sample_size": core["sample_size"],
        "D": d_len,
        "smooth_window": core["smooth_window"],
        "default_smooth": True if core["smooth_window"] else False,
        "unsmoothed": {
            "full_year": full_raw,
            "historical": hist_raw,
            "forward": fwd_raw,
        },
        "smoothed": (
            {
                "full_year": full_sm,
                "historical": hist_sm,
                "forward": fwd_sm,
            }
            if full_sm is not None
            else None
        ),
        # Convenience default (smoothed if present, else raw)
        "full_year": full_sm if full_sm is not None else full_raw,
        "historical": hist_sm if hist_sm is not None else hist_raw,
        "forward": fwd_sm if fwd_sm is not None else fwd_raw,
        "forecast_stats": core["forecast_stats"],
        "product_note": (
            "Seasonal Roadmap v1 — average of full-year indexed paths F=P_d/P_1, "
            "rebased to as-of price. No centering, no amplitude scaling. "
            "Forecast stats from historical asof→horizon returns only."
        ),
    }
