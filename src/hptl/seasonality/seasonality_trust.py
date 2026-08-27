"""Seasonality trust grading — shared by export, audit, and confluence gating."""

from __future__ import annotations

from typing import Any

from hptl.seasonality.seasonality_price_bars import history_quality

MIN_WEEKS_PER_YEAR_A = 35
MIN_YEARS_A = 10
MIN_SEASONAL_WEEKS_A = 40
MIN_YEARS_B = 3
MIN_SEASONAL_WEEKS_B = 12
PLATEAU_REJECT_RUN = 10
MAX_INDEXED_GRADE_A = 500.0
MAX_PROJECTION_GRADE_A = 500.0
UNIT_SCALE_WARNING = "Unit-scale break detected"


def _longest_identical_run(values: list[float], tol: float = 0.02) -> int:
    if not values:
        return 0
    best = run = 1
    for i in range(1, len(values)):
        if abs(values[i] - values[i - 1]) <= tol:
            run += 1
            best = max(best, run)
        else:
            run = 1
    return best


def _seasonal_weeks(chart_series: list[dict[str, Any]], key: str = "seasonal_3y") -> int:
    return sum(1 for r in chart_series if r.get(key) is not None)


def _indexed_sanity_ok(
    *,
    max_indexed: float | None,
    max_projection: float | None,
) -> bool:
    if max_indexed is not None and max_indexed > MAX_INDEXED_GRADE_A:
        return False
    if max_projection is not None and max_projection > MAX_PROJECTION_GRADE_A:
        return False
    return True


def classify_trust(
    *,
    available: bool,
    years_used: int,
    avg_weeks_per_year: float,
    seasonal_3y_weeks: int,
    max_flat_run: int = 0,
    reason: str | None = None,
    unit_scale_break: bool = False,
    max_indexed: float | None = None,
    max_projection: float | None = None,
    outlier_filter_applied: bool = False,
) -> tuple[str, str]:
    if not available:
        return "C", reason or "Seasonality unavailable"

    sanity_ok = _indexed_sanity_ok(max_indexed=max_indexed, max_projection=max_projection)
    grade_a_eligible = (
        years_used >= MIN_YEARS_A
        and avg_weeks_per_year >= MIN_WEEKS_PER_YEAR_A
        and seasonal_3y_weeks >= MIN_SEASONAL_WEEKS_A
        and max_flat_run < PLATEAU_REJECT_RUN
        and sanity_ok
    )
    if unit_scale_break and not outlier_filter_applied:
        grade_a_eligible = False

    if grade_a_eligible:
        notes = "Production-ready: dense curve, 10Y+ history, weekly-grade bars"
        if unit_scale_break and outlier_filter_applied:
            notes = f"{UNIT_SCALE_WARNING}; repaired via median-band filter. {notes}"
        return "A", notes

    if years_used < MIN_YEARS_B or seasonal_3y_weeks < MIN_SEASONAL_WEEKS_B:
        detail = []
        if years_used < MIN_YEARS_B:
            detail.append(f"only {years_used}Y history")
        if seasonal_3y_weeks < MIN_SEASONAL_WEEKS_B:
            detail.append(f"seasonal curve {seasonal_3y_weeks}/52 weeks")
        return "C", "; ".join(detail) or "Insufficient for trustworthy seasonality"

    if avg_weeks_per_year < 10 and seasonal_3y_weeks < 15:
        return (
            "C",
            f"very sparse bars (~{avg_weeks_per_year:.0f} ISO wk/yr); "
            f"seasonal curve {seasonal_3y_weeks}/52 weeks",
        )

    detail = []
    if years_used < MIN_YEARS_A:
        detail.append(f"history {years_used}Y (target {MIN_YEARS_A}Y)")
    if avg_weeks_per_year < MIN_WEEKS_PER_YEAR_A:
        detail.append(f"bar density ~{avg_weeks_per_year:.0f} ISO wk/yr (target {MIN_WEEKS_PER_YEAR_A}+)")
    if seasonal_3y_weeks < MIN_SEASONAL_WEEKS_A:
        detail.append(f"seasonal gaps {52 - seasonal_3y_weeks} weeks on 3Y curve")
    return "B", "; ".join(detail) or "Usable but below production target"


def trust_score(
    *,
    years_used: int,
    avg_weeks_per_year: float,
    seasonal_3y_weeks: int,
    grade: str,
) -> float:
    curve = min(seasonal_3y_weeks / 52.0, 1.0) * 35.0
    history = min(years_used / 10.0, 1.0) * 35.0
    density = min(avg_weeks_per_year / MIN_WEEKS_PER_YEAR_A, 1.0) * 30.0
    raw = curve + history + density
    if grade == "C":
        return round(min(raw * 0.45, 45.0), 2)
    if grade == "B":
        return round(50.0 + raw * 0.45, 2)
    return round(100.0 + raw * 0.3, 2)


def attach_trust_metadata(
    block: dict[str, Any],
    bars: list[tuple[str, float]],
    *,
    filter_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Add trust_grade, trust_score, trust_notes to a seasonality price block."""
    if not block.get("available"):
        block["trust_grade"] = "C"
        block["trust_score"] = 0.0
        block["trust_notes"] = block.get("reason") or "Unavailable"
        block["confluence_eligible"] = False
        return block

    _, avg_wpy, _ = history_quality(bars)
    chart = block.get("chart_series") or []
    s3_weeks = _seasonal_weeks(chart)
    s3_vals = [float(r["seasonal_3y"]) for r in chart if r.get("seasonal_3y") is not None]
    max_flat = _longest_identical_run(s3_vals)
    years_used = int(block.get("years_used") or block.get("years_of_history") or 0)

    max_indexed = 0.0
    max_projection = 0.0
    for row in chart:
        for key in ("actual", "seasonal_10y", "seasonal_5y", "seasonal_3y"):
            v = row.get(key)
            if isinstance(v, (int, float)):
                max_indexed = max(max_indexed, float(v))
        for key in ("proj_10y", "proj_5y", "proj_3y"):
            v = row.get(key)
            if isinstance(v, (int, float)):
                max_projection = max(max_projection, float(v))

    fa = filter_audit or {}
    unit_break = bool(fa.get("unit_scale_break_detected"))
    filter_applied = bool(fa.get("applied"))

    grade, notes = classify_trust(
        available=True,
        years_used=years_used,
        avg_weeks_per_year=avg_wpy,
        seasonal_3y_weeks=s3_weeks,
        max_flat_run=max_flat,
        unit_scale_break=unit_break,
        max_indexed=max_indexed if max_indexed > 0 else None,
        max_projection=max_projection if max_projection > 0 else None,
        outlier_filter_applied=filter_applied,
    )
    if unit_break and not filter_applied and grade == "A":
        grade, notes = "B", f"{UNIT_SCALE_WARNING}; outlier filter not applied. {notes}"
    elif unit_break and filter_applied and not _indexed_sanity_ok(
        max_indexed=max_indexed if max_indexed > 0 else None,
        max_projection=max_projection if max_projection > 0 else None,
    ):
        grade, notes = "B", f"{UNIT_SCALE_WARNING}; filtered series still exceeds indexed sanity gate. {notes}"
    block["trust_grade"] = grade
    block["trust_notes"] = notes
    block["trust_score"] = trust_score(
        years_used=years_used,
        avg_weeks_per_year=avg_wpy,
        seasonal_3y_weeks=s3_weeks,
        grade=grade,
    )
    block["confluence_eligible"] = grade == "A"
    block["avg_weeks_per_year"] = round(avg_wpy, 1)
    block["seasonal_3y_weeks"] = s3_weeks
    if fa:
        block["outlier_filter_audit"] = fa
        if unit_break:
            block["data_quality_warning"] = UNIT_SCALE_WARNING
    return block
