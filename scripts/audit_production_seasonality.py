#!/usr/bin/env python3
"""Audit the production robust DAILY-return seasonality roadmap."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.engine import compute_lookback_block  # noqa: E402
from hptl.seasonality_workstation.integrity import audit_daily_series  # noqa: E402
from hptl.seasonality_workstation.production_roadmap import METHOD_VERSION, build_production_roadmap  # noqa: E402
from hptl.seasonality_workstation.returns import iso_week, load_daily_closes, weekly_closes_from_daily, weekly_return_rows  # noqa: E402
from hptl.seasonality_workstation.validation import (  # noqa: E402
    LOOKBACK_YEARS,
    robust_forward_horizon_stats,
    robust_lookback_agreement,
    robust_weekly_leave_one_year_out,
)

LOOKBACKS = LOOKBACK_YEARS


def _clean(block: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in block.items() if not k.startswith("_")}


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
        clean = _clean(block)
        clean["forward_horizons"] = robust_forward_horizon_stats(
            rows,
            years=list(clean.get("sample_years") or []),
            anchor_week=anchor_week,
            horizons=(4, 8, 12),
        )
        blocks[label] = clean

    agreement = robust_lookback_agreement(blocks, anchor_week=anchor_week, horizon=8)
    selected = blocks[lookback]
    wf = robust_weekly_leave_one_year_out(
        rows,
        years=list(selected.get("sample_years") or []),
        anchor_week=anchor_week,
        lookback=lookback,
        horizon=8,
    )
    research = {
        "status": "ok",
        "instrument_id": instrument,
        "selected_lookback": lookback,
        "lookbacks": blocks,
        "anchor": {"date": anchor_date, "price": anchor_price, "iso_year": asof_year, "iso_week": anchor_week},
        "integrity": integrity,
        "lookback_agreement": agreement,
        "walk_forward": wf,
        "_daily_closes": daily,
    }
    roadmap = build_production_roadmap(research)
    points = ((roadmap.get("unsmoothed") or {}).get("full_year") or [])
    today = next((p for p in points if p.get("segment") == "today"), None)
    daily_returns = [p.get("trimmed_mean_return") for p in points if p.get("trimmed_mean_return") is not None]

    checks = {
        "integrity_pass": integrity.get("status") == "PASS",
        "method_is_robust_daily_returns_v3": (roadmap.get("method") or {}).get("version") == METHOD_VERSION,
        "daily_observation_count_gt_180": len(points) > 180,
        "daily_observation_count_gt_weekly_52": len(points) > 52,
        "daily_path_has_up_moves": any(float(r) > 0 for r in daily_returns),
        "daily_path_has_down_moves": any(float(r) < 0 for r in daily_returns),
        "no_payload_smoothing": roadmap.get("smoothed") is None and roadmap.get("smooth_window") is None,
        "anchor_is_exact": today is not None and abs(float(today["price"]) - float(anchor_price)) <= 1e-9,
        "cot_not_a_dependency": (roadmap.get("method") or {}).get("cot_dependency") == "none",
        "daily_return_aggregation_declared": (roadmap.get("method") or {}).get("aggregation") == "10pct_trimmed_mean_daily_close_to_close_return",
        "year_wrap_horizon_stats": all(
            ((selected.get("forward_horizons") or {}).get(h) or {}).get("year_wrap") == "supported"
            for h in ("4w", "8w", "12w")
        ),
        "walk_forward_is_robust_model": wf.get("method") == "leave_one_year_out_robust_weekly_direction",
        "walk_forward_has_observations": int(wf.get("n") or 0) > 0,
        "lookback_agreement_is_robust_model": agreement.get("method") == "robust_weekly_return_projection_sign_agreement",
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
        "sample_years": roadmap.get("sample_years"),
        "sample_size": roadmap.get("sample_size"),
        "observation_count": len(points),
        "checks": checks,
        "lookback_agreement": agreement,
        "walk_forward": wf,
        "forecast_stats": roadmap.get("forecast_stats"),
        "reliability": roadmap.get("reliability"),
        "method": roadmap.get("method"),
        "first_20_forward_points": [p for p in points if p.get("segment") in {"today", "forward"}][:20],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit robust daily production seasonality roadmap")
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

    print("PRODUCTION DAILY SEASONALITY AUDIT")
    print("=" * 34)
    print(f"Instrument : {report.get('instrument')}")
    print(f"As-of      : {report.get('resolved_asof')}")
    print(f"Lookback   : {report.get('lookback')}")
    print(f"Points     : {report.get('observation_count')}")
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
