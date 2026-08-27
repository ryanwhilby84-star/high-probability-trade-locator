"""Price-series integrity audit for Seasonality Workstation.

Fails loudly — never silently compute seasonality from poor data.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from hptl.seasonality_workstation.models import (
    MAX_GAP_DAYS,
    MAX_SINGLE_DAY_RETURN,
    MIN_WEEKS_PER_YEAR,
    MIN_YEARS_FOR_PASS,
)


def _parse(d: str) -> datetime | None:
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d")
    except ValueError:
        return None


def audit_daily_series(
    instrument_id: str,
    daily: list[tuple[str, float]],
    *,
    source: str | None = None,
) -> dict[str, Any]:
    """Audit a (date, close) daily series. status: PASS | FAIL."""
    issues: list[str] = []
    warnings: list[str] = []

    if not daily:
        return {
            "instrument_id": instrument_id,
            "status": "FAIL",
            "issues": ["no_daily_bars"],
            "warnings": [],
            "source": source,
            "bar_count": 0,
            "available_history_years": 0.0,
            "first_date": None,
            "last_date": None,
        }

    dates = [d for d, _ in daily]
    closes = [c for _, c in daily]

    # Duplicates
    seen: set[str] = set()
    dupes = 0
    for d in dates:
        if d in seen:
            dupes += 1
        seen.add(d)
    if dupes:
        issues.append(f"duplicate_observations:{dupes}")

    # Ordering
    if dates != sorted(dates):
        issues.append("unsorted_or_out_of_order")

    # Non-positive / non-finite
    bad_px = sum(1 for c in closes if c is None or c <= 0 or c != c)
    if bad_px:
        issues.append(f"invalid_closes:{bad_px}")

    # Gaps
    gaps: list[dict[str, Any]] = []
    for i in range(1, len(dates)):
        a = _parse(dates[i - 1])
        b = _parse(dates[i])
        if not a or not b:
            continue
        delta = (b - a).days
        if delta > MAX_GAP_DAYS:
            gaps.append({"from": dates[i - 1], "to": dates[i], "days": delta})
    large_gaps = [g for g in gaps if g["days"] > 45]
    if large_gaps:
        issues.append(f"large_gaps:{len(large_gaps)}")
    elif len(gaps) > 12:
        warnings.append(f"elevated_gap_count:{len(gaps)}")

    # Discontinuities (possible rolls / bad ticks)
    jumps = 0
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        cur = closes[i]
        if prev and prev > 0 and cur and cur > 0:
            ret = abs(cur / prev - 1.0)
            if ret >= MAX_SINGLE_DAY_RETURN:
                jumps += 1
    if jumps > 8:
        issues.append(f"excessive_discontinuities:{jumps}")
    elif jumps > 0:
        warnings.append(f"discontinuities:{jumps}")

    first = dates[0]
    last = dates[-1]
    a = _parse(first)
    b = _parse(last)
    years = ((b - a).days / 365.25) if a and b else 0.0

    # ISO week coverage by calendar year (exclude incomplete current year)
    from hptl.seasonality_workstation.returns import weekly_closes_from_daily, iso_week

    weekly = weekly_closes_from_daily(daily)
    by_year: dict[int, set[int]] = {}
    for d, _ in weekly:
        y, w = iso_week(d)
        by_year.setdefault(y, set()).add(w)
    current_year = b.year if b else None
    hist_years = sorted(y for y in by_year if current_year is None or y < current_year)
    thin_years = [y for y in hist_years if len(by_year[y]) < MIN_WEEKS_PER_YEAR]
    usable_years = [y for y in hist_years if y not in thin_years]

    if years < MIN_YEARS_FOR_PASS:
        issues.append(f"insufficient_history:{years:.1f}y<{MIN_YEARS_FOR_PASS}y")
    if len(usable_years) < MIN_YEARS_FOR_PASS:
        issues.append(f"insufficient_usable_years:{len(usable_years)}<{MIN_YEARS_FOR_PASS}")
    if thin_years:
        warnings.append(f"thin_years:{thin_years[:6]}")

    status = "FAIL" if issues else "PASS"
    return {
        "instrument_id": instrument_id,
        "status": status,
        "issues": issues,
        "warnings": warnings,
        "source": source,
        "bar_count": len(daily),
        "weekly_bar_count": len(weekly),
        "available_history_years": round(years, 2),
        "first_date": first,
        "last_date": last,
        "gap_count": len(gaps),
        "largest_gaps": gaps[:5],
        "discontinuity_count": jumps,
        "usable_history_years": usable_years,
        "usable_year_count": len(usable_years),
        "thin_years": thin_years,
        "data_quality": (
            "HIGH"
            if status == "PASS" and not warnings and years >= 15
            else "MEDIUM"
            if status == "PASS"
            else "FAIL"
        ),
    }
