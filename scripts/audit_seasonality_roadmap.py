#!/usr/bin/env python3
"""Forensic audit of Seasonality Workstation roadmap math.

This script does not tune or alter seasonality. It independently reconstructs
sample-year selection, the indexed annual roadmap, the as-of rebase, and the
4/8/12/26/48-week historical return statistics from the same canonical daily
price input. It then compares those independent calculations with the production
Seasonal Roadmap payload and writes a transparent per-year ledger.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.indexed_seasonality import MIN_BARS_PER_YEAR
from hptl.seasonality_workstation.returns import load_daily_closes
from hptl.seasonality_workstation.seasonal_roadmap import (
    HORIZON_TRADING_DAYS,
    HORIZON_WEEKS,
    build_seasonal_roadmap,
)

LOOKBACKS = {"5Y": 5, "10Y": 10, "15Y": 15, "20Y": 20}


def _d(value: str) -> date:
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def _sample_years(
    daily: list[tuple[str, float]], *, asof: str, lookback_years: int
) -> dict[int, list[tuple[date, float]]]:
    """Independent reproduction of complete historical calendar-year selection."""
    asof_date = _d(asof)
    first_year = asof_date.year - int(lookback_years)
    last_year = asof_date.year - 1
    buckets: dict[int, list[tuple[date, float]]] = {}
    for d_s, close in daily:
        d = _d(d_s)
        if first_year <= d.year <= last_year and float(close) > 0:
            buckets.setdefault(d.year, []).append((d, float(close)))
    return {
        y: sorted(rows, key=lambda x: x[0])
        for y, rows in sorted(buckets.items())
        if len(rows) >= MIN_BARS_PER_YEAR
    }


def _current_asof_td(daily: list[tuple[str, float]], asof: str) -> int:
    year = _d(asof).year
    return sum(1 for d_s, _ in daily if _d(d_s).year == year and d_s <= asof)


def _historical_calendar_anchor(
    rows: list[tuple[date, float]], *, month: int, day: int
) -> tuple[int, date, float] | None:
    for i, (d, close) in enumerate(rows):
        if (d.month, d.day) >= (month, day):
            return i, d, close
    return None


def _mean_path(years: dict[int, list[tuple[date, float]]]) -> tuple[list[float], int]:
    indexed: dict[int, list[float]] = {}
    for year, rows in years.items():
        base = rows[0][1]
        indexed[year] = [close / base for _, close in rows]
    common = min((len(v) for v in indexed.values()), default=0)
    if not common:
        return [], 0
    return [
        sum(path[i] for path in indexed.values()) / len(indexed)
        for i in range(common)
    ], common


def _rebase(path: list[float], *, asof_td: int, anchor: float) -> list[float]:
    base = path[asof_td - 1]
    scale = anchor / base
    return [value * scale for value in path]


def _median(values: list[float]) -> float:
    return float(statistics.median(values))


def _close_enough(a: Any, b: Any, tol: float = 1e-6) -> bool:
    if a is None or b is None:
        return a is b
    return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=tol)


def audit(instrument: str, asof: str | None, lookback_years: int) -> dict[str, Any]:
    daily, source, load_error = load_daily_closes(instrument)
    if load_error or not daily:
        return {
            "passed": False,
            "instrument": instrument,
            "error": load_error or "no_daily_bars",
            "source": source,
        }

    daily = sorted((str(d)[:10], float(c)) for d, c in daily if float(c) > 0)
    requested_asof = asof or daily[-1][0]
    resolved_asof = max((d for d, _ in daily if d <= requested_asof), default="")
    if not resolved_asof:
        return {"passed": False, "instrument": instrument, "error": "no_asof_bar"}

    production = build_seasonal_roadmap(
        daily,
        asof=resolved_asof,
        lookback_years=lookback_years,
        smooth=5,
    )
    if not production.get("available"):
        return {
            "passed": False,
            "instrument": instrument,
            "source": source,
            "asof": resolved_asof,
            "error": production.get("reason", "roadmap_unavailable"),
        }

    years = _sample_years(daily, asof=resolved_asof, lookback_years=lookback_years)
    asof_date = _d(resolved_asof)
    asof_td = _current_asof_td(daily, resolved_asof)
    mean_path, common_days = _mean_path(years)
    asof_td = min(max(1, asof_td), common_days)
    anchor = next(close for d, close in reversed(daily) if d <= resolved_asof)
    independent_prices = _rebase(mean_path, asof_td=asof_td, anchor=anchor)

    continuous = [(_d(d), close) for d, close in daily]
    date_index = {d: i for i, (d, _) in enumerate(continuous)}
    ledgers: list[dict[str, Any]] = []
    returns_by_horizon: dict[int, list[float]] = {w: [] for w in HORIZON_WEEKS}

    for year, rows in years.items():
        ordinal_idx = min(asof_td, len(rows)) - 1
        ordinal_date, ordinal_close = rows[ordinal_idx]
        cal_anchor = _historical_calendar_anchor(
            rows, month=asof_date.month, day=asof_date.day
        )
        row: dict[str, Any] = {
            "year": year,
            "bars": len(rows),
            "first_date": rows[0][0].isoformat(),
            "first_close": rows[0][1],
            "last_date": rows[-1][0].isoformat(),
            "last_close": rows[-1][1],
            "roadmap_ordinal_anchor": {
                "trading_day": asof_td,
                "date": ordinal_date.isoformat(),
                "close": ordinal_close,
            },
            "stats_calendar_anchor": None,
            "anchor_alignment_drift_days": None,
            "horizons": {},
        }
        if cal_anchor is not None:
            _, cal_date, cal_close = cal_anchor
            row["stats_calendar_anchor"] = {
                "target_month_day": f"{asof_date.month:02d}-{asof_date.day:02d}",
                "actual_date": cal_date.isoformat(),
                "close": cal_close,
            }
            row["anchor_alignment_drift_days"] = (ordinal_date - cal_date).days
            i0 = date_index.get(cal_date)
            if i0 is not None:
                for weeks in HORIZON_WEEKS:
                    hop = HORIZON_TRADING_DAYS[weeks]
                    i1 = i0 + hop
                    if i1 >= len(continuous):
                        continue
                    end_date, end_close = continuous[i1]
                    ret = end_close / cal_close - 1.0
                    returns_by_horizon[weeks].append(ret)
                    row["horizons"][f"{weeks}w"] = {
                        "trading_days": hop,
                        "start_date": cal_date.isoformat(),
                        "start_close": cal_close,
                        "end_date": end_date.isoformat(),
                        "end_close": end_close,
                        "return": round(ret, 8),
                        "return_pct": round(ret * 100.0, 4),
                    }
        ledgers.append(row)

    independent_stats: dict[str, Any] = {}
    stats_checks: dict[str, Any] = {}
    for weeks in HORIZON_WEEKS:
        vals = returns_by_horizon[weeks]
        key = f"{weeks}w"
        if vals:
            calc = {
                "n": len(vals),
                "mean": round(sum(vals) / len(vals), 6),
                "median": round(_median(vals), 6),
                "bearish_frequency": round(sum(v < 0 for v in vals) / len(vals), 4),
                "bullish_frequency": round(sum(v > 0 for v in vals) / len(vals), 4),
            }
        else:
            calc = {
                "n": 0,
                "mean": None,
                "median": None,
                "bearish_frequency": None,
                "bullish_frequency": None,
            }
        independent_stats[key] = calc
        prod = production["forecast_stats"].get(key, {})
        fields = ("n", "mean", "median", "bearish_frequency", "bullish_frequency")
        field_checks = {
            field: _close_enough(calc.get(field), prod.get(field)) for field in fields
        }
        stats_checks[key] = {
            "passed": all(field_checks.values()),
            "fields": field_checks,
            "production": {field: prod.get(field) for field in fields},
            "independent": calc,
        }

    curve_errors = [
        abs(a - b)
        for a, b in zip(independent_prices, production.get("prices_raw") or [])
    ]
    curve_max_abs_error = max(curve_errors, default=float("inf"))
    sample_years_match = sorted(years) == list(production.get("sample_years") or [])
    sample_size_match = len(years) == int(production.get("sample_size") or 0)
    asof_td_match = asof_td == int(production.get("asof_trading_day") or 0)
    common_days_match = common_days == int(production.get("D") or 0)
    anchor_match = _close_enough(anchor, production.get("anchor_price"))
    curve_match = curve_max_abs_error <= 1e-9
    all_stats_match = all(v["passed"] for v in stats_checks.values())

    drifts = [
        int(row["anchor_alignment_drift_days"])
        for row in ledgers
        if row["anchor_alignment_drift_days"] is not None
    ]
    alignment_warning = any(v != 0 for v in drifts)

    checks = {
        "sample_years_match": sample_years_match,
        "sample_size_match": sample_size_match,
        "asof_trading_day_match": asof_td_match,
        "common_trading_days_match": common_days_match,
        "anchor_price_match": anchor_match,
        "raw_roadmap_curve_match": curve_match,
        "forecast_stats_match": all_stats_match,
    }
    passed = all(checks.values())

    return {
        "passed": passed,
        "instrument": instrument,
        "source": source,
        "asof": resolved_asof,
        "lookback_years": lookback_years,
        "production_method": production.get("method"),
        "checks": checks,
        "curve_max_abs_error": curve_max_abs_error,
        "sample_years": sorted(years),
        "sample_size": len(years),
        "common_trading_days": common_days,
        "asof_trading_day": asof_td,
        "anchor_price": anchor,
        "alignment_audit": {
            "roadmap_alignment": "ordinal trading-day-of-year",
            "forecast_stats_alignment": "same calendar month/day, first open bar on/after",
            "warning": alignment_warning,
            "note": (
                "The plotted roadmap and horizon statistics use different historical "
                "alignment rules. Non-zero drift is exposed rather than hidden."
            ),
            "drift_days_by_year": {
                str(row["year"]): row["anchor_alignment_drift_days"] for row in ledgers
            },
        },
        "forecast_stats_comparison": stats_checks,
        "independent_forecast_stats": independent_stats,
        "year_ledger": ledgers,
    }


def _print_summary(report: dict[str, Any], output_path: Path) -> None:
    print("\nSEASONALITY ROADMAP FORENSIC AUDIT")
    print("=" * 38)
    print(f"Instrument : {report.get('instrument')}")
    print(f"Source     : {report.get('source')}")
    print(f"As-of      : {report.get('asof')}")
    print(f"Lookback   : {report.get('lookback_years')}Y")
    print(f"Years      : {report.get('sample_years')}")
    print(f"Anchor     : {report.get('anchor_price')}")
    for name, ok in (report.get("checks") or {}).items():
        print(f"{'PASS' if ok else 'FAIL':4}  {name}")
    alignment = report.get("alignment_audit") or {}
    if alignment.get("warning"):
        print("WARN  roadmap/statistics alignment drift exists; inspect year_ledger")
    print(f"\nOverall    : {'PASS' if report.get('passed') else 'FAIL'}")
    print(f"Audit JSON : {output_path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Forensic audit of a Seasonality Roadmap")
    parser.add_argument("--instrument", default="Soybeans")
    parser.add_argument("--asof", default=None, help="YYYY-MM-DD; defaults to latest available bar")
    parser.add_argument("--lookback", choices=sorted(LOOKBACKS), default="15Y")
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)

    report = audit(args.instrument, args.asof, LOOKBACKS[args.lookback])
    safe_instrument = "".join(ch.lower() if ch.isalnum() else "_" for ch in args.instrument).strip("_")
    asof_label = report.get("asof") or args.asof or "latest"
    output = Path(args.output) if args.output else (
        ROOT / "data" / "audits" / "seasonality" /
        f"{safe_instrument}_{asof_label}_{args.lookback.lower()}_roadmap_audit.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    _print_summary(report, output)
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
