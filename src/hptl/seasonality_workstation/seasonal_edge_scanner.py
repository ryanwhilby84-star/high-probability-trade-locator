"""Cross-market seasonal edge scanner.

Scans recurring calendar months and forward seasonal windows across the tracked
legacy market universe. This is intentionally independent of the existing
Seasonal Roadmap presentation: it asks a different question — how consistently
did this exact calendar window move in one direction over 5Y/10Y/15Y samples?
"""

from __future__ import annotations

import calendar
import math
import statistics
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Iterable

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS
from hptl.seasonality_workstation.returns import load_daily_closes

LOOKBACKS = (5, 10, 15)
START_OFFSETS = (0, 7, 14, 21, 28)
WINDOW_LENGTHS = (28, 35, 42, 49, 56, 63, 70)
MIN_SAMPLE_15Y = 12


@dataclass(frozen=True)
class WindowSpec:
    start_month: int
    start_day: int
    end_month: int
    end_day: int
    label: str
    days_until_start: int
    kind: str


def _safe_date(year: int, month: int, day: int) -> date:
    return date(year, month, min(day, calendar.monthrange(year, month)[1]))


def _target_dates(year: int, spec: WindowSpec) -> tuple[date, date]:
    start = _safe_date(year, spec.start_month, spec.start_day)
    end_year = year
    if (spec.end_month, spec.end_day) < (spec.start_month, spec.start_day):
        end_year += 1
    end = _safe_date(end_year, spec.end_month, spec.end_day)
    return start, end


def _group_daily(daily: Iterable[tuple[str, float]]) -> list[tuple[date, float]]:
    out: list[tuple[date, float]] = []
    for d, close in daily:
        try:
            dt = date.fromisoformat(str(d)[:10])
            px = float(close)
        except Exception:
            continue
        if math.isfinite(px) and px > 0:
            out.append((dt, px))
    return sorted(out, key=lambda x: x[0])


def _window_return(rows: list[tuple[date, float]], start: date, end: date) -> float | None:
    start_px = None
    end_px = None
    # exact calendar windows, nearest actual trading closes inside the interval
    for d, px in rows:
        if d < start:
            continue
        if d > end:
            break
        if start_px is None:
            start_px = px
        end_px = px
    if start_px is None or end_px is None or start_px <= 0:
        return None
    return (end_px / start_px - 1.0) * 100.0


def _returns_for_years(rows: list[tuple[date, float]], spec: WindowSpec, years: list[int]) -> list[tuple[int, float]]:
    out: list[tuple[int, float]] = []
    for y in years:
        start, end = _target_dates(y, spec)
        r = _window_return(rows, start, end)
        if r is not None and math.isfinite(r):
            out.append((y, r))
    return out


def _stats(values: list[tuple[int, float]]) -> dict[str, Any]:
    xs = [v for _, v in values]
    n = len(xs)
    if not xs:
        return {
            "n": 0,
            "bullish": 0,
            "bearish": 0,
            "bullish_frequency": None,
            "bearish_frequency": None,
            "direction": "Mixed",
            "directional_frequency": None,
            "mean_pct": None,
            "median_pct": None,
            "best_pct": None,
            "worst_pct": None,
        }
    bull = sum(1 for x in xs if x > 0)
    bear = sum(1 for x in xs if x < 0)
    bf = bull / n
    sf = bear / n
    direction = "Bullish" if bf > sf else "Bearish" if sf > bf else "Mixed"
    freq = max(bf, sf)
    return {
        "n": n,
        "bullish": bull,
        "bearish": bear,
        "bullish_frequency": round(bf, 4),
        "bearish_frequency": round(sf, 4),
        "direction": direction,
        "directional_frequency": round(freq, 4),
        "mean_pct": round(statistics.fmean(xs), 3),
        "median_pct": round(statistics.median(xs), 3),
        "best_pct": round(max(xs), 3),
        "worst_pct": round(min(xs), 3),
        "years": [y for y, _ in values],
        "returns": [{"year": y, "return_pct": round(v, 3)} for y, v in values],
    }


def _lookback_years(asof: date, years: int) -> list[int]:
    # Only completed historical occurrences. Current year is excluded from the test.
    return list(range(asof.year - years, asof.year))


def _window_score(stats15: dict[str, Any], stats10: dict[str, Any], stats5: dict[str, Any], neighbour_freq: float | None) -> tuple[float, str, list[str]]:
    reasons: list[str] = []
    if stats15.get("n", 0) < MIN_SAMPLE_15Y:
        return 0.0, "INSUFFICIENT", ["fewer_than_12_valid_15y_observations"]

    direction = stats15.get("direction")
    if direction not in {"Bullish", "Bearish"}:
        return 0.0, "WEAK", ["no_dominant_direction"]

    freq15 = float(stats15.get("directional_frequency") or 0.0)
    freq10 = float(stats10.get("directional_frequency") or 0.0)
    freq5 = float(stats5.get("directional_frequency") or 0.0)
    median = float(stats15.get("median_pct") or 0.0)
    mean = float(stats15.get("mean_pct") or 0.0)

    same_sign = (median > 0 and mean > 0 and direction == "Bullish") or (
        median < 0 and mean < 0 and direction == "Bearish"
    )
    stable_direction = stats10.get("direction") == direction and stats5.get("direction") == direction

    score = 0.0
    score += min(45.0, max(0.0, (freq15 - 0.5) / 0.5 * 45.0))
    score += min(15.0, max(0.0, (freq10 - 0.5) / 0.5 * 15.0))
    score += min(10.0, max(0.0, (freq5 - 0.5) / 0.5 * 10.0))
    score += min(15.0, abs(median) * 3.0)
    score += 8.0 if same_sign else 0.0
    score += 5.0 if stable_direction else 0.0
    if neighbour_freq is not None:
        score += min(2.0, max(0.0, (neighbour_freq - 0.5) / 0.5 * 2.0))

    if freq15 >= 0.933:
        reasons.append("15y_frequency_93pct_plus")
    if freq15 >= 0.999:
        reasons.append("15y_perfect_directional_record")
    if stable_direction:
        reasons.append("5y_10y_15y_direction_aligned")
    if same_sign:
        reasons.append("mean_median_agree")
    if abs(median) >= 1.0:
        reasons.append("meaningful_median_move")
    if neighbour_freq is not None and neighbour_freq >= 0.8:
        reasons.append("neighbour_window_stable")

    if score >= 82 and freq15 >= 0.867 and stable_direction and same_sign:
        grade = "EXCEPTIONAL" if freq15 >= 0.933 else "STRONG"
    elif score >= 68 and freq15 >= 0.8 and same_sign:
        grade = "STRONG"
    elif score >= 55 and freq15 >= 0.733:
        grade = "DEVELOPING"
    else:
        grade = "WEAK"
    return round(score, 1), grade, reasons


def _monthly_specs(asof: date) -> list[WindowSpec]:
    specs: list[WindowSpec] = []
    for offset in (0, 1):
        month = ((asof.month - 1 + offset) % 12) + 1
        y = asof.year + ((asof.month - 1 + offset) // 12)
        start = date(y, month, 1)
        end = date(y, month, calendar.monthrange(y, month)[1])
        specs.append(
            WindowSpec(
                start.month,
                start.day,
                end.month,
                end.day,
                calendar.month_name[month],
                max(0, (start - asof).days),
                "calendar_month",
            )
        )
    return specs


def _rolling_specs(asof: date) -> list[WindowSpec]:
    specs: list[WindowSpec] = []
    for offset in START_OFFSETS:
        start = asof + timedelta(days=offset)
        for length in WINDOW_LENGTHS:
            end = start + timedelta(days=length)
            specs.append(
                WindowSpec(
                    start.month,
                    start.day,
                    end.month,
                    end.day,
                    f"{start.strftime('%d %b')} → {end.strftime('%d %b')}",
                    offset,
                    "rolling_window",
                )
            )
    return specs


def _neighbour_frequency(rows: list[tuple[date, float]], spec: WindowSpec, asof: date, direction: str) -> float | None:
    # Robustness guard against one magic start/end date. Shift the complete window
    # one week earlier/later and require the dominant direction to survive nearby.
    freqs: list[float] = []
    nominal_start = date(2000, spec.start_month, spec.start_day)
    nominal_end_year = 2001 if (spec.end_month, spec.end_day) < (spec.start_month, spec.start_day) else 2000
    nominal_end = _safe_date(nominal_end_year, spec.end_month, spec.end_day)
    duration = (nominal_end - nominal_start).days
    for shift in (-7, 7):
        anchor = asof + timedelta(days=spec.days_until_start + shift)
        shifted_end = anchor + timedelta(days=duration)
        shifted = WindowSpec(anchor.month, anchor.day, shifted_end.month, shifted_end.day, "neighbour", 0, spec.kind)
        s = _stats(_returns_for_years(rows, shifted, _lookback_years(asof, 15)))
        if s.get("direction") == direction and s.get("directional_frequency") is not None:
            freqs.append(float(s["directional_frequency"]))
        else:
            freqs.append(0.0)
    return round(sum(freqs) / len(freqs), 4) if freqs else None


def scan_instrument(instrument_id: str, *, asof: date | None = None) -> dict[str, Any]:
    asof = asof or date.today()
    daily, source, error = load_daily_closes(instrument_id)
    if error or not daily:
        return {"instrument_id": instrument_id, "status": "unavailable", "error": error or "no_daily_history"}
    rows = _group_daily(daily)
    if len(rows) < 500:
        return {"instrument_id": instrument_id, "status": "unavailable", "error": "insufficient_history"}

    candidates: list[dict[str, Any]] = []
    for spec in [*_monthly_specs(asof), *_rolling_specs(asof)]:
        lb: dict[str, Any] = {}
        for years in LOOKBACKS:
            lb[f"{years}Y"] = _stats(_returns_for_years(rows, spec, _lookback_years(asof, years)))
        s15, s10, s5 = lb["15Y"], lb["10Y"], lb["5Y"]
        neighbour = _neighbour_frequency(rows, spec, asof, s15.get("direction")) if spec.kind == "rolling_window" else None
        score, grade, reasons = _window_score(s15, s10, s5, neighbour)
        if grade in {"WEAK", "INSUFFICIENT"}:
            continue
        direction = s15.get("direction")
        candidates.append({
            "instrument_id": instrument_id,
            "kind": spec.kind,
            "window": spec.label,
            "days_until_start": spec.days_until_start,
            "direction": direction,
            "grade": grade,
            "edge_score": score,
            "lookbacks": lb,
            "neighbour_directional_frequency": neighbour,
            "reasons": reasons,
            "thesis": (
                f"{direction} seasonal window: {s15.get('bullish') if direction == 'Bullish' else s15.get('bearish')}/"
                f"{s15.get('n')} historical occurrences in the dominant direction; median {s15.get('median_pct'):+.2f}% "
                f"with 5Y/10Y/15Y {'alignment' if s5.get('direction') == direction and s10.get('direction') == direction else 'mixed stability'}."
            ),
        })

    # Remove near-duplicate rolling windows: keep the highest score for same direction
    # and start offset bucket, while retaining monthly signals separately.
    monthly = [c for c in candidates if c["kind"] == "calendar_month"]
    rolling = sorted((c for c in candidates if c["kind"] == "rolling_window"), key=lambda c: c["edge_score"], reverse=True)
    kept: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for c in rolling:
        key = (c["direction"], int(c["days_until_start"] // 7))
        if key in seen:
            continue
        seen.add(key)
        kept.append(c)
        if len(kept) >= 5:
            break

    edges = sorted([*monthly, *kept], key=lambda c: (c["grade"] == "EXCEPTIONAL", c["edge_score"]), reverse=True)
    return {
        "instrument_id": instrument_id,
        "status": "ok",
        "source": source,
        "latest_price_date": rows[-1][0].isoformat(),
        "asof_calendar_date": asof.isoformat(),
        "edges": edges,
    }


def build_seasonal_edge_scan(*, instruments: Iterable[str] | None = None, asof: date | None = None) -> dict[str, Any]:
    asof = asof or date.today()
    universe = list(instruments or LEGACY_COT_MARKETS)
    results = [scan_instrument(i, asof=asof) for i in universe]
    edges = [edge for r in results if r.get("status") == "ok" for edge in r.get("edges", [])]
    edges.sort(key=lambda e: (e["grade"] == "EXCEPTIONAL", e["edge_score"]), reverse=True)
    alerts = [e for e in edges if e.get("days_until_start", 99) <= 14 and e.get("grade") in {"STRONG", "EXCEPTIONAL"}]
    return {
        "status": "ok",
        "engine": "seasonal_edge_scanner_v1",
        "asof": asof.isoformat(),
        "instrument_count": len(universe),
        "available_instruments": sum(1 for r in results if r.get("status") == "ok"),
        "edge_count": len(edges),
        "alert_count": len(alerts),
        "alerts": alerts[:20],
        "top_edges": edges[:40],
        "instrument_results": results,
        "methodology": {
            "lookbacks": ["5Y", "10Y", "15Y"],
            "monthly_windows": "current_and_next_calendar_month",
            "rolling_windows": "starts_0_to_28_days_ahead_in_7d_steps; lengths_28_to_70_days",
            "robustness": "mean_median_sign_agreement + 5Y/10Y/15Y direction alignment + +/-7d neighbour-window stability",
            "warning": "Historical seasonality is descriptive, not causal. High hit rates are ranked only after robustness checks to reduce calendar-window overfitting.",
        },
    }
