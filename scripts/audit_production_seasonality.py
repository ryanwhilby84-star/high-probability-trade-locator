#!/usr/bin/env python3
"""Audit the production robust weekly-return seasonality roadmap.

Designed for the final pre-commit check, including the Soybeans 2026-08-24
reference case. It runs the return model on data truncated at the requested
as-of date, computes a leave-one-year-out directional validation, applies the
same production reliability gate, and writes a compact audit JSON.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.engine import (  # noqa: E402
    _lookback_agreement,
    compute_lookback_block,
)
from hptl.seasonality_workstation.integrity import audit_daily_series  # noqa: E402
from hptl.seasonality_workstation.production_roadmap import (  # noqa: E402
    METHOD_VERSION,
    build_production_roadmap,
)
from hptl.seasonality_workstation.returns import (  # noqa: E402
    iso_week,
    load_daily_closes,
    weekly_closes_from_daily,
    weekly_return_rows,
)
from hptl.seasonality_workstation.stats import bucket_stats  # noqa: E402

LOOKBACKS: dict[str, int | None] = {
    "5Y": 5,
    "10Y": 10,
    "15Y": 15,
    "20Y": 20,
    "FULL": None,
}


def _clean(block: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in block.items() if not k.startswith("_")}


def _train_week_stats(
    rows: list[dict[str, Any]], *, test_year: int, lookback_years: int | None
) -> dict[int, dict[str, Any]]:
    first_year = -10_000 if lookback_years is None else test_year - lookback_years
    buckets: dict[int, list[float]] = {w: [] for w in range(1, 53)}
    for row in rows:
        year = int(row["iso_year"])
        week = int(row["iso_week"])
        ret = row.get("return")
        if year >= test_year or year < first_year or ret is None:
            continue
        if 1 <= week <= 52 and math.isfinite(float(ret)):
            buckets[week].append(float(ret))
    return {w: bucket_stats(buckets[w]) for w in range(1, 53)}


def _next_h_actual_returns(
    rows: list[dict[str, Any]], *, test_year: int, anchor_week: int, horizon: int
) -> list[float] | None:
    start = None
    for i, row in enumerate(rows):
        if int(row["iso_year"]) == test_year and int(row["iso_week"]) == anchor_week:
            start = i
            break
    if start is None or start + horizon >= len(rows):
        return None
    actual: list[float] = []
    for row in rows[start + 1 : start + horizon + 1]:
        ret = row.get("return")
        if ret is None or not math.isfinite(float(ret)):
            return None
        actual.append(float(ret))
    return actual if len(actual) == horizon else None


def leave_one_year_out_hit_rate(
    rows: list[dict[str, Any]],
    *,
    years: list[int],
    anchor_week: int,
    lookback_years: int | None,
    horizon: int = 8,
) -> dict[str, Any]:
    outcomes: list[dict[str, Any]] = []
    for test_year in sorted(years):
        train_years = [
            y
            for y in years
            if y < test_year and (lookback_years is None or y >= test_year - lookback_years)
        ]
        if len(train_years) < 5:
            continue
        stats = _train_week_stats(rows, test_year=test_year, lookback_years=lookback_years)
        predicted = 1.0
        usable_prediction = True
        for offset in range(1, horizon + 1):
            week = ((anchor_week - 1 + offset) % 52) + 1
            ret = (stats.get(week) or {}).get("trimmed_mean")
            if ret is None:
                usable_prediction = False
                break
            predicted *= 1.0 + float(ret)
        actual_returns = _next_h_actual_returns(
            rows, test_year=test_year, anchor_week=anchor_week, horizon=horizon
        )
        if not usable_prediction or not actual_returns:
            continue
        actual = 1.0
        for ret in actual_returns:
            actual *= 1.0 + ret
        pred_ret = predicted - 1.0
        actual_ret = actual - 1.0
        hit = (pred_ret > 0 and actual_ret > 0) or (pred_ret < 0 and actual_ret < 0)
        outcomes.append(
            {
                "year": test_year,
                "train_years": train_years,
                "predicted_return": round(pred_ret, 6),
                "actual_return": round(actual_ret, 6),
                "direction_hit": hit,
            }
        )
    n = len(outcomes)
    hits = sum(1 for row in outcomes if row["direction_hit"])
    return {
        "hit_rate": None if n == 0 else round(hits / n, 6),
        "n": n,
        "hits": hits,
        "horizon_weeks": horizon,
        "method": "leave_one_year_out_robust_weekly_direction",
        "outcomes": outcomes,
    }


def audit(instrument: str, *, asof: str | None, lookback: str) -> dict[str, Any]:
    daily, source, load_error = load_daily_closes(instrument)
    if load_error or not daily:
        return {"passed": False, "instrument": instrument, "error": load_error or "no_daily_data"}
    daily = sorted((str(d)[:10], float(c)) for d, c in daily if float(c) > 0)
    requested = asof or daily[-1][0]
    daily = [(d, c) for d, c in daily if d <= requested]
    if not daily:
        return {"passed": False, "instrument": instrument, "error": "no_data_at_asof"}

    resolved_asof = daily[-1][0]
    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)
    anchor_date, anchor_price = weekly[-1]
    asof_year, anchor_week = iso_week(anchor_date)
    integrity = audit_daily_series(instrument, daily, source=source)
    usable = list(integrity.get("usable_history_years") or [])

    blocks: dict[str, dict[str, Any]] = {}
    raw_blocks: dict[str, dict[str, Any]] = {}
    for label in LOOKBACKS:
        block = compute_lookback_block(
            rows,
            weekly,
            lookback=label,
            asof_year=asof_year,
            usable_years=usable,
            anchor_week=anchor_week,
            anchor_price=anchor_price,
            anchor_date=anchor_date,
        )
        raw_blocks[label] = block
        blocks[label] = _clean(block)

    agreement = _lookback_agreement(
        {label: block["_trimmed_path_raw"] for label, block in raw_blocks.items()},
        anchor_week,
    )
    selected = blocks[lookback]
    wf = leave_one_year_out_hit_rate(
        rows,
        years=selected.get("sample_years") or [],
        anchor_week=anchor_week,
        lookback_years=LOOKBACKS[lookback],
    )
    research = {
        "status": "ok",
        "selected_lookback": lookback,
        "lookbacks": blocks,
        "anchor": {
            "date": anchor_date,
            "price": anchor_price,
            "iso_year": asof_year,
            "iso_week": anchor_week,
        },
        "integrity": integrity,
        "lookback_agreement": agreement,
        "walk_forward": wf,
    }
    roadmap = build_production_roadmap(research)
    points = ((roadmap.get("unsmoothed") or {}).get("full_year") or [])
    today = next((p for p in points if p.get("segment") == "today"), None)

    checks = {
        "integrity_pass": integrity.get("status") == "PASS",
        "method_is_robust_weekly_returns_v2": (roadmap.get("method") or {}).get("version") == METHOD_VERSION,
        "exactly_52_weekly_observations": len(points) == 52,
        "no_payload_smoothing": roadmap.get("smoothed") is None and roadmap.get("smooth_window") is None,
        "anchor_is_exact": today is not None and abs(float(today["price"]) - float(anchor_price)) <= 1e-9,
        "cot_not_a_dependency": (roadmap.get("method") or {}).get("cot_dependency") == "none",
        "missing_week_rule_declared": (roadmap.get("method") or {}).get("missing_week_rule") == "do_not_bridge_missing_week_returns",
        "walk_forward_has_observations": int(wf.get("n") or 0) > 0,
        "reliability_verdict_present": bool((roadmap.get("reliability") or {}).get("verdict")),
    }
    passed = all(checks.values())
    return {
        "passed": passed,
        "instrument": instrument,
        "source": source,
        "requested_asof": requested,
        "resolved_asof": resolved_asof,
        "lookback": lookback,
        "anchor_week": anchor_week,
        "anchor_price": anchor_price,
        "sample_years": selected.get("sample_years"),
        "sample_size": selected.get("sample_size"),
        "checks": checks,
        "lookback_agreement": agreement,
        "walk_forward": wf,
        "forecast_stats": roadmap.get("forecast_stats"),
        "reliability": roadmap.get("reliability"),
        "method": roadmap.get("method"),
        "first_12_forward_points": [p for p in points if p.get("segment") in {"today", "forward"}][:13],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit robust production seasonality roadmap")
    parser.add_argument("--instrument", default="Soybeans")
    parser.add_argument("--asof", default=None, help="YYYY-MM-DD; defaults to latest available bar")
    parser.add_argument("--lookback", choices=tuple(LOOKBACKS), default="15Y")
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)

    report = audit(args.instrument, asof=args.asof, lookback=args.lookback)
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in args.instrument).strip("_")
    asof_label = report.get("resolved_asof") or args.asof or "latest"
    output = Path(args.output) if args.output else ROOT / "data" / "audits" / "seasonality" / f"{safe}_{asof_label}_{args.lookback.lower()}_production_audit.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print("PRODUCTION SEASONALITY AUDIT")
    print("=" * 30)
    print(f"Instrument : {report.get('instrument')}")
    print(f"As-of      : {report.get('resolved_asof')}")
    print(f"Lookback   : {report.get('lookback')}")
    for name, ok in (report.get("checks") or {}).items():
        print(f"{'PASS' if ok else 'FAIL':4}  {name}")
    reliability = report.get("reliability") or {}
    print(f"Reliability: {reliability.get('verdict')} · score={reliability.get('score')}")
    if reliability.get("reasons"):
        print("Reasons    : " + "; ".join(reliability["reasons"]))
    print(f"Overall    : {'PASS' if report.get('passed') else 'FAIL'}")
    print(f"Audit JSON : {output}")
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
