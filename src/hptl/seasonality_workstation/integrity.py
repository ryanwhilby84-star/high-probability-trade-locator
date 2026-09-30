"""Price-series integrity audit for Seasonality Workstation.

Global integrity checks protect against structurally unusable data. Local historical
coverage defects (large gaps / thin calendar years) are reported as diagnostics and
must be evaluated against the selected seasonal window rather than globally blocking
an otherwise valid seasonality calculation.
"""

from __future__ import annotations

from datetime import datetime
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


def _required_forward_weeks(
    base_year: int,
    anchor_week: int,
    horizon_weeks: int,
) -> set[tuple[int, int]]:
    """ISO year/week keys required after an anchor for one historical sample."""
    required: set[tuple[int, int]] = set()
    y = int(base_year)
    w = int(anchor_week)
    for _ in range(int(horizon_weeks)):
        w += 1
        if w > 52:
            w = 1
            y += 1
        required.add((y, w))
    return required


def seasonal_window_usable_years(
    daily: list[tuple[str, float]],
    *,
    asof_year: int,
    anchor_week: int,
    lookback_years: int,
    horizon_weeks: int,
) -> tuple[list[int], list[dict[str, Any]]]:
    """Return historical years usable for the exact selected forward window.

    A defect elsewhere in a calendar year does not disqualify that year. A year is
    excluded only when the selected forward seasonal window itself lacks weekly data
    or is crossed by a large source-data gap.
    """
    from hptl.seasonality_workstation.returns import iso_week, weekly_closes_from_daily, weekly_return_rows

    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)
    available_returns = {
        (int(r["iso_year"]), int(r["iso_week"]))
        for r in rows
        if r.get("return") is not None
    }

    gap_weeks: set[tuple[int, int]] = set()
    for i in range(1, len(daily)):
        a = _parse(daily[i - 1][0])
        b = _parse(daily[i][0])
        if not a or not b:
            continue
        if (b - a).days <= MAX_GAP_DAYS:
            continue
        # Mark every ISO week touched by the missing interval. This prevents the
        # first bar after a long gap from masquerading as a valid weekly return.
        cur = a
        while cur <= b:
            gap_weeks.add(iso_week(cur.strftime("%Y-%m-%d")))
            cur = cur.fromordinal(cur.toordinal() + 1)

    cutoff = int(asof_year) - int(lookback_years)
    usable: list[int] = []
    excluded: list[dict[str, Any]] = []
    for y in range(cutoff, int(asof_year)):
        required = _required_forward_weeks(y, anchor_week, horizon_weeks)
        missing = sorted(required - available_returns)
        crossed_gaps = sorted(required & gap_weeks)
        reasons: list[str] = []
        if missing:
            reasons.append("missing_window_weeks")
        if crossed_gaps:
            reasons.append("large_gap_in_window")
        if reasons:
            excluded.append(
                {
                    "year": y,
                    "reason": "+".join(reasons),
                    "missing_weeks": missing,
                    "gap_weeks": crossed_gaps,
                }
            )
        else:
            usable.append(y)
    return usable, excluded


def audit_daily_series(
    instrument_id: str,
    daily: list[tuple[str, float]],
    *,
    source: str | None = None,
) -> dict[str, Any]:
    """Audit a (date, close) daily series. status: PASS | FAIL.

    Only structural defects that make the whole series unsafe are global blockers.
    Local coverage defects stay visible as warnings and are scoped to the selected
    seasonal window by ``seasonal_window_usable_years``.
    """
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

    seen: set[str] = set()
    dupes = 0
    for d in dates:
        if d in seen:
            dupes += 1
        seen.add(d)
    if dupes:
        issues.append(f"duplicate_observations:{dupes}")

    if dates != sorted(dates):
        issues.append("unsorted_or_out_of_order")

    bad_px = sum(1 for c in closes if c is None or c <= 0 or c != c)
    if bad_px:
        issues.append(f"invalid_closes:{bad_px}")

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
        warnings.append(f"large_gaps:{len(large_gaps)}")
    elif len(gaps) > 12:
        warnings.append(f"elevated_gap_count:{len(gaps)}")

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

    from hptl.seasonality_workstation.returns import weekly_closes_from_daily, iso_week

    weekly = weekly_closes_from_daily(daily)
    by_year: dict[int, set[int]] = {}
    for d, _ in weekly:
        y, w = iso_week(d)
        by_year.setdefault(y, set()).add(w)
    current_year = b.year if b else None
    hist_years = sorted(y for y in by_year if current_year is None or y < current_year)
    thin_years = [y for y in hist_years if len(by_year[y]) < MIN_WEEKS_PER_YEAR]

    # Do not globally remove a historical year because coverage is thin somewhere
    # else in that year. Exact-window eligibility is decided later.
    usable_years = list(hist_years)

    if years < MIN_YEARS_FOR_PASS:
        issues.append(f"insufficient_history:{years:.1f}y<{MIN_YEARS_FOR_PASS}y")
    if len(hist_years) < MIN_YEARS_FOR_PASS:
        issues.append(f"insufficient_history_years:{len(hist_years)}<{MIN_YEARS_FOR_PASS}")
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
