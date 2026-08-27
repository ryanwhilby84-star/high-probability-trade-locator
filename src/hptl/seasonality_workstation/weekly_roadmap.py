"""Weekly Roadmap — independent weekly-return seasonality path.

Preserves Seasonal Roadmap (trading-day indexed “Monthly Roadmap”) unchanged.
This module never imports or mutates seasonal_roadmap maths.

Alignment method (selected)
---------------------------
ISO calendar week (1–52), with week 53 merged into week 52.

Audit vs sequential trading-week-from-year-start:
* ISO weeks align the same economic calendar week across years (e.g. mid-July).
* Sequential trading weeks shift when year-start holidays differ, misaligning
  July/August across years for FX/futures.
* ISO is already the workstation’s weekly bucketing primitive (`returns.iso_week`).

Week-53 rule: merge into week 52 (implemented in ``returns.iso_week``).
Leap years / holiday-shortened weeks: ISO week still gets one close (last
session in that week); no artificial fill. Missing weeks are omitted from
the average (not forward-filled).

Maths
-----
1. Weekly closes from daily (one load).
2. Week-over-week returns.
3. For each ISO week w∈[1,52], average returns across valid historical years.
4. Compound: C_0=1; C_w = C_{w-1}·(1+r̄_w).
5. Optional light centered SMA on the cumulative index *after* compounding.
6. Rebase cumulative index to as-of price for chart overlay.
"""
from __future__ import annotations

import math
import time
from datetime import date, datetime
from typing import Any

from hptl.seasonality_workstation.indexed_seasonality import centered_sma
from hptl.seasonality_workstation.models import MIN_WEEKS_PER_YEAR, MIN_YEARS_FOR_PASS
from hptl.seasonality_workstation.returns import iso_week, weekly_closes_from_daily, weekly_return_rows

WEEKLY_ROADMAP_VERSION = "weekly_roadmap_v1"
WEEKLY_SMOOTH_WINDOW = 3  # light display smoothing only (odd)
MIN_SAMPLES_PER_WEEK = 3
THIN_SAMPLES_PER_WEEK = 5
DIRECTION_EPS = 0.0015  # ~15 bps cumulative slope threshold


def _parse(d: str) -> date | None:
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def alignment_method_doc() -> dict[str, Any]:
    return {
        "selected": "iso_calendar_week",
        "week_53_rule": "merge_into_week_52",
        "alternatives_audited": [
            {
                "method": "iso_calendar_week",
                "pros": [
                    "aligns equivalent calendar weeks across years",
                    "deterministic; already used by workstation weekly returns",
                    "no look-ahead",
                ],
                "cons": ["week-53 years need an explicit merge/exclude rule"],
            },
            {
                "method": "sequential_trading_week_from_year_start",
                "pros": ["exactly 52 buckets when truncating"],
                "cons": [
                    "year-start holidays shift mid-year weeks across years",
                    "worse July/August cross-year alignment for seasonal timing",
                ],
            },
        ],
        "selection_reason": (
            "ISO weeks produce more consistent cross-year comparison for "
            "futures/FX/commodities without look-ahead or artificial filling."
        ),
    }


def valid_iso_years(
    rows: list[dict[str, Any]],
    *,
    asof_year: int,
    lookback_years: int,
    min_weeks: int = MIN_WEEKS_PER_YEAR,
) -> tuple[list[int], list[dict[str, Any]]]:
    """Years fully before asof ISO year with enough weekly return samples."""
    counts: dict[int, int] = {}
    for r in rows:
        y = int(r["iso_year"])
        if y >= asof_year:
            continue
        if r.get("return") is None:
            continue
        if not math.isfinite(float(r["return"])):
            continue
        w = int(r["iso_week"])
        if 1 <= w <= 52:
            counts[y] = counts.get(y, 0) + 1

    cutoff = asof_year - int(lookback_years)
    valid: list[int] = []
    excluded: list[dict[str, Any]] = []
    for y in sorted(counts):
        if y < cutoff:
            excluded.append({"year": y, "reason": "outside_lookback", "weeks": counts[y]})
            continue
        if counts[y] < min_weeks:
            excluded.append({"year": y, "reason": "thin_year", "weeks": counts[y]})
            continue
        valid.append(y)
    # Also note requested years in window with zero rows
    for y in range(cutoff, asof_year):
        if y not in counts and y not in {e["year"] for e in excluded}:
            excluded.append({"year": y, "reason": "missing_year", "weeks": 0})
    return valid, excluded


def average_weekly_returns(
    rows: list[dict[str, Any]],
    years: list[int],
) -> dict[int, dict[str, Any]]:
    """Average genuine weekly returns per ISO week. No forward-fill."""
    year_set = set(years)
    buckets: dict[int, list[float]] = {w: [] for w in range(1, 53)}
    for r in rows:
        if int(r["iso_year"]) not in year_set:
            continue
        ret = r.get("return")
        w = int(r["iso_week"])
        if ret is None or w < 1 or w > 52:
            continue
        if not math.isfinite(float(ret)):
            continue
        buckets[w].append(float(ret))

    out: dict[int, dict[str, Any]] = {}
    for w in range(1, 53):
        samples = buckets[w]
        n = len(samples)
        if n == 0:
            out[w] = {
                "week": w,
                "average_return": None,
                "sample_count": 0,
                "quality_flag": "missing",
            }
            continue
        avg = sum(samples) / n
        if n < MIN_SAMPLES_PER_WEEK:
            flag = "insufficient"
        elif n < THIN_SAMPLES_PER_WEEK:
            flag = "thin"
        else:
            flag = "ok"
        out[w] = {
            "week": w,
            "average_return": avg,
            "sample_count": n,
            "quality_flag": flag,
        }
    return out


def compound_weekly_path(
    week_avgs: dict[int, dict[str, Any]],
) -> list[float]:
    """Compound averaged weekly returns into cumulative index (length 52).

    Missing/insufficient weeks contribute 0 return (hold level) — not a fill of
    inventing a return from neighbours.
    """
    path = [1.0] * 52
    level = 1.0
    for w in range(1, 53):
        st = week_avgs.get(w) or {}
        r = st.get("average_return")
        if r is None or not math.isfinite(float(r)):
            r = 0.0
        level *= 1.0 + float(r)
        path[w - 1] = level
    return path


def direction_from_path(path: list[float], current_week: int) -> str:
    """Bullish / Bearish / Neutral from local cumulative slope around current week."""
    if not path or current_week < 1:
        return "Neutral"
    i = min(max(current_week, 1), 52) - 1
    j0 = max(0, i - 2)
    j1 = min(51, i + 2)
    if j1 <= j0:
        return "Neutral"
    slope = (path[j1] - path[j0]) / max(1, j1 - j0)
    if slope > DIRECTION_EPS:
        return "Bullish"
    if slope < -DIRECTION_EPS:
        return "Bearish"
    return "Neutral"


def recent_price_direction(daily: list[tuple[str, float]], *, lookback_bars: int = 15) -> str:
    if len(daily) < 5:
        return "Neutral"
    window = daily[-lookback_bars:]
    a = window[0][1]
    b = window[-1][1]
    if not a or a <= 0:
        return "Neutral"
    ret = b / a - 1.0
    if ret > 0.005:
        return "Bullish"
    if ret < -0.005:
        return "Bearish"
    return "Neutral"


def agreement_state(
    monthly_direction: str,
    weekly_direction: str,
    price_direction: str,
    *,
    monthly_available: bool,
    weekly_available: bool,
) -> str:
    if not monthly_available or not weekly_available:
        return "Unavailable"
    m = (monthly_direction or "Neutral").capitalize()
    w = (weekly_direction or "Neutral").capitalize()
    if m.startswith("Bull") and w.startswith("Bull"):
        return "Aligned bullish"
    if m.startswith("Bear") and w.startswith("Bear"):
        return "Aligned bearish"
    if m.startswith("Bull") and w.startswith("Bear"):
        return "Broad bullish / short-term bearish"
    if m.startswith("Bear") and w.startswith("Bull"):
        return "Broad bearish / short-term bullish"
    if m == "Neutral" and w == "Neutral":
        if price_direction == "Neutral":
            return "Neutral"
        return "Mixed"
    return "Mixed"


def monthly_direction_from_roadmap(seasonal_roadmap: dict[str, Any] | None) -> tuple[str, bool]:
    """Read-only direction from existing Seasonal Roadmap forecast_stats (12w)."""
    if not seasonal_roadmap or not seasonal_roadmap.get("available"):
        return "Neutral", False
    stats = (seasonal_roadmap.get("forecast_stats") or {}).get("12w") or {}
    mean = stats.get("mean")
    if mean is None or not math.isfinite(float(mean)):
        return "Neutral", True
    m = float(mean)
    if m > 0.002:
        return "Bullish", True
    if m < -0.002:
        return "Bearish", True
    return "Neutral", True


def stale_price_warning(
    latest_price_date: str | None,
    *,
    as_of: date | None = None,
    stale_after_days: int = 5,
) -> dict[str, Any]:
    as_of = as_of or date.today()
    d = _parse(latest_price_date or "")
    if d is None:
        return {
            "stale": True,
            "latest_price_date": latest_price_date,
            "as_of_calendar_date": as_of.isoformat(),
            "reason": "missing_latest_price_date",
        }
    lag = (as_of - d).days
    stale = lag > stale_after_days
    return {
        "stale": stale,
        "latest_price_date": d.isoformat(),
        "as_of_calendar_date": as_of.isoformat(),
        "lag_calendar_days": lag,
        "reason": "price_lag_exceeds_threshold" if stale else None,
    }


def build_weekly_roadmap(
    daily: list[tuple[str, float]],
    *,
    asof: str | None = None,
    lookback_years: int = 15,
    smooth: int = WEEKLY_SMOOTH_WINDOW,
    integrity: dict[str, Any] | None = None,
    seasonal_roadmap: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build Weekly Roadmap from a once-loaded daily close series.

    ``daily`` must already be loaded by the caller — this function never reloads
    price files.
    """
    t0 = time.perf_counter()
    method_doc = alignment_method_doc()

    if not daily:
        return _unavailable(
            ["missing_source_data"],
            method_doc=method_doc,
            elapsed_ms=0.0,
            lookback_years=lookback_years,
        )

    # Integrity gate reuse — do not weaken.
    if integrity and integrity.get("status") == "FAIL":
        reasons = list(integrity.get("issues") or ["integrity_failed"])
        return _unavailable(
            reasons,
            method_doc=method_doc,
            elapsed_ms=(time.perf_counter() - t0) * 1000.0,
            lookback_years=lookback_years,
            integrity_status="unavailable",
        )

    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)
    if not weekly or not rows:
        return _unavailable(
            ["insufficient_weekly_coverage"],
            method_doc=method_doc,
            elapsed_ms=(time.perf_counter() - t0) * 1000.0,
            lookback_years=lookback_years,
        )

    asof_date = str(asof or weekly[-1][0])[:10]
    asof_year, current_week = iso_week(asof_date)
    anchor_price = float(weekly[-1][1])
    # Prefer weekly close on asof week if present
    for d, c in reversed(weekly):
        if iso_week(d) == (asof_year, current_week):
            anchor_price = float(c)
            break

    valid_years, excluded = valid_iso_years(
        rows, asof_year=asof_year, lookback_years=lookback_years
    )
    quality_reasons: list[str] = []
    if len(valid_years) < MIN_YEARS_FOR_PASS:
        quality_reasons.append(f"thin_years:{len(valid_years)}<{MIN_YEARS_FOR_PASS}")

    week_avgs = average_weekly_returns(rows, valid_years)
    missing = sum(1 for w in range(1, 53) if (week_avgs[w].get("sample_count") or 0) == 0)
    thin = sum(1 for w in range(1, 53) if (week_avgs[w].get("quality_flag") in {"thin", "insufficient"}))
    if missing > 8:
        quality_reasons.append(f"insufficient_weekly_coverage:missing_weeks={missing}")
    if thin > 20:
        quality_reasons.append(f"elevated_thin_weeks:{thin}")

    if quality_reasons:
        status = "unavailable" if any("thin_years" in r or "insufficient_weekly" in r for r in quality_reasons) else "warning"
    else:
        status = "valid"
    if integrity and integrity.get("warnings"):
        if status == "valid":
            status = "warning"
        quality_reasons.extend(f"integrity_warning:{w}" for w in (integrity.get("warnings") or [])[:6])

    if status == "unavailable":
        return _unavailable(
            quality_reasons,
            method_doc=method_doc,
            elapsed_ms=(time.perf_counter() - t0) * 1000.0,
            lookback_years=lookback_years,
            valid_years=valid_years,
            excluded_years=excluded,
            integrity_status=status,
            asof_date=asof_date,
            current_week=current_week,
        )

    cum_raw = compound_weekly_path(week_avgs)
    cum_smooth = centered_sma(cum_raw, smooth) if smooth and smooth > 1 else list(cum_raw)
    # Rebase so current week pins to 1.0 on index, then to price
    cw = min(max(current_week, 1), 52)
    pin_raw = cum_raw[cw - 1] or 1.0
    pin_sm = cum_smooth[cw - 1] or 1.0

    def _points(cum: list[float], pin: float) -> list[dict[str, Any]]:
        pts: list[dict[str, Any]] = []
        for w in range(1, 53):
            st = week_avgs[w]
            idx = cum[w - 1] / pin
            pts.append(
                {
                    "week": w,
                    "average_return": None
                    if st["average_return"] is None
                    else round(float(st["average_return"]), 8),
                    "cumulative_return": round(idx - 1.0, 8),
                    "cumulative_index": round(idx, 8),
                    "price": round(anchor_price * idx, 8),
                    "sample_count": st["sample_count"],
                    "quality_flag": st["quality_flag"],
                    "segment": "historical" if w < cw else ("today" if w == cw else "forward"),
                }
            )
        return pts

    points_raw = _points(cum_raw, pin_raw)
    points_smooth = _points(cum_smooth, pin_sm)
    direction = direction_from_path(cum_smooth if smooth else cum_raw, cw)
    price_dir = recent_price_direction(daily)
    monthly_dir, monthly_ok = monthly_direction_from_roadmap(seasonal_roadmap)
    agree = agreement_state(
        monthly_dir,
        direction,
        price_dir,
        monthly_available=monthly_ok,
        weekly_available=True,
    )

    stale = stale_price_warning(daily[-1][0] if daily else None)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return {
        "available": True,
        "method": {
            "version": WEEKLY_ROADMAP_VERSION,
            "name": "avg_iso_weekly_return_compound",
            "alignment": method_doc,
            "lookback_years": lookback_years,
            "note": (
                "Genuine weekly returns averaged by ISO week, then compounded. "
                "Not monthly interpolation."
            ),
        },
        "as_of_date": asof_date,
        "lookback_years": lookback_years,
        "valid_years": valid_years,
        "excluded_years": excluded,
        "requested_years": list(range(asof_year - lookback_years, asof_year)),
        "valid_year_count": len(valid_years),
        "weekly_points": points_smooth if smooth and smooth > 1 else points_raw,
        "unsmoothed": {"weekly_points": points_raw},
        "smoothed": {"weekly_points": points_smooth} if smooth and smooth > 1 else None,
        "current_week": cw,
        "current_direction": direction,
        "quality_status": status,
        "quality_reasons": quality_reasons,
        "smoothing": {
            "applied": bool(smooth and smooth > 1),
            "window": smooth if smooth and smooth > 1 else None,
            "stage": "after_compound_path",
        },
        "anchor_price": anchor_price,
        "missing_week_count": missing,
        "thin_week_count": thin,
        "comparison": {
            "monthly_roadmap_direction": monthly_dir,
            "weekly_roadmap_direction": direction,
            "actual_price_direction": price_dir,
            "seasonal_agreement": agree,
            "monthly_label": "Monthly Roadmap",  # UI name for seasonal_roadmap product
            "summary_lines": _summary_lines(monthly_dir, direction, price_dir, agree, stale),
        },
        "actual_price": {
            "latest_price_date": daily[-1][0] if daily else None,
            "seasonality_as_of_date": asof_date,
            "stale": stale,
        },
        "calculation_ms": round(elapsed_ms, 3),
    }


def _summary_lines(
    monthly_dir: str,
    weekly_dir: str,
    price_dir: str,
    agree: str,
    stale: dict[str, Any],
) -> list[str]:
    price_phrase = {
        "Bullish": "Moving higher",
        "Bearish": "Moving lower",
        "Neutral": "Sideways / mixed",
    }.get(price_dir, "Unavailable")
    weekly_phrase = weekly_dir
    if monthly_dir.startswith("Bear") and weekly_dir.startswith("Bear"):
        weekly_phrase = "Bearish acceleration" if price_dir == "Bearish" else "Bearish"
    if monthly_dir.startswith("Bull") and weekly_dir.startswith("Bull"):
        weekly_phrase = "Bullish acceleration" if price_dir == "Bullish" else "Bullish"
    lines = [
        f"Monthly Roadmap: {monthly_dir}",
        f"Weekly Roadmap: {weekly_phrase}",
        f"Actual price: {price_phrase}",
        f"Seasonal agreement: {agree}",
    ]
    if stale.get("stale"):
        lines.append(
            f"Price warning: latest close {stale.get('latest_price_date')} "
            f"lags calendar {stale.get('as_of_calendar_date')} "
            f"({stale.get('lag_calendar_days')}d)"
        )
    return lines


def _unavailable(
    reasons: list[str],
    *,
    method_doc: dict[str, Any],
    elapsed_ms: float,
    lookback_years: int,
    valid_years: list[int] | None = None,
    excluded_years: list[dict[str, Any]] | None = None,
    integrity_status: str = "unavailable",
    asof_date: str | None = None,
    current_week: int | None = None,
) -> dict[str, Any]:
    return {
        "available": False,
        "method": {
            "version": WEEKLY_ROADMAP_VERSION,
            "name": "avg_iso_weekly_return_compound",
            "alignment": method_doc,
            "lookback_years": lookback_years,
        },
        "as_of_date": asof_date,
        "lookback_years": lookback_years,
        "valid_years": valid_years or [],
        "excluded_years": excluded_years or [],
        "valid_year_count": len(valid_years or []),
        "weekly_points": [],
        "current_week": current_week,
        "current_direction": "Neutral",
        "quality_status": integrity_status,
        "quality_reasons": reasons,
        "smoothing": {"applied": False, "window": None, "stage": None},
        "comparison": {
            "monthly_roadmap_direction": "Unavailable",
            "weekly_roadmap_direction": "Unavailable",
            "actual_price_direction": "Unavailable",
            "seasonal_agreement": "Unavailable",
            "summary_lines": ["Weekly Roadmap unavailable"] + [f"· {r}" for r in reasons],
        },
        "calculation_ms": round(elapsed_ms, 3),
    }
