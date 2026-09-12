#!/usr/bin/env python
"""Build/export Seasonal Edge Scanner payload with full-universe integrity gating.

The audit runs against the exact 26-market scanner universe. Known unsafe source
bindings are repaired through explicit futures foundations before publication;
anything still failing remains withheld rather than being silently downgraded.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.seasonality_workstation.seasonal_edge_scanner import build_seasonal_edge_scan
from hptl.seasonality_workstation.universe_audit import (
    audit_seasonality_instrument,
    audit_seasonality_universe,
)


def _repair_one(instrument_id: str) -> dict:
    """Attempt only deterministic, direction-correct repairs we explicitly own."""
    before = audit_seasonality_instrument(instrument_id)
    result = {
        "instrument_id": instrument_id,
        "attempted": False,
        "before": before,
        "after": before,
        "status": "not_needed" if before.get("status") == "PASS" else "no_known_repair",
    }
    if before.get("status") == "PASS":
        return result

    repair: Callable[[], object] | None = None
    repair_name: str | None = None

    if instrument_id == "Corn":
        from hptl.prices.corn_foundation_backfill import run_corn_foundation_backfill

        repair = lambda: run_corn_foundation_backfill(execute=True)
        repair_name = "yahoo:ZC=F"
    elif instrument_id in {
        "Coffee",
        "Cocoa",
        "Cotton",
        "Japanese Yen / 6J",
        "Swiss Franc / 6S",
        "Canadian Dollar / 6C",
        "Copper / HG",
    }:
        from hptl.prices.softs_futures_backfill import promote_soft_futures

        repair = lambda iid=instrument_id: promote_soft_futures(iid)
        repair_name = "direction_correct_yahoo_futures"
    elif instrument_id == "US Dollar Index / DX":
        from hptl.prices.ice_dx_futures_backfill import promote_ice_dx_futures

        repair = lambda: promote_ice_dx_futures([instrument_id])
        repair_name = "yahoo:DX-Y.NYB"

    if repair is None:
        return result

    result["attempted"] = True
    result["repair_name"] = repair_name
    try:
        result["repair"] = repair()
    except Exception as exc:  # noqa: BLE001
        result["status"] = "repair_failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    after = audit_seasonality_instrument(instrument_id)
    result["after"] = after
    result["status"] = "repaired" if after.get("status") == "PASS" else "still_failed"
    return result


def _repair_failed_instruments(pre_audit: dict, *, enabled: bool) -> list[dict]:
    if not enabled:
        return [
            {
                "instrument_id": iid,
                "attempted": False,
                "status": "repair_disabled",
                "before": (pre_audit.get("instruments") or {}).get(iid),
            }
            for iid in pre_audit.get("failed") or []
        ]
    return [_repair_one(iid) for iid in (pre_audit.get("failed") or [])]


def _integrity_gate(payload: dict, audit_payload: dict) -> dict:
    audits = audit_payload.get("instruments") or {}
    failed = set(audit_payload.get("failed") or [])

    for result in payload.get("instrument_results", []):
        instrument_id = result.get("instrument_id")
        if not instrument_id:
            continue
        audit = audits.get(instrument_id) or {
            "instrument_id": instrument_id,
            "status": "FAIL",
            "issues": ["missing_universe_audit"],
            "warnings": [],
            "scanner_eligible": False,
        }
        result["integrity"] = audit
        result["series_fingerprint"] = audit.get("series_fingerprint")
        result["scanner_scope_fingerprint"] = audit.get("scanner_scope_fingerprint")
        if audit.get("status") != "PASS":
            failed.add(instrument_id)
            result["status"] = "integrity_failed"
            result["edges"] = []
            result["error"] = ";".join(audit.get("issues", [])) or "integrity_failed"

    all_edges = [
        edge
        for result in payload.get("instrument_results", [])
        if result.get("status") == "ok"
        for edge in result.get("edges", [])
        if edge.get("instrument_id") not in failed
    ]
    all_edges.sort(
        key=lambda e: (e.get("grade") == "EXCEPTIONAL", e.get("edge_score", 0)),
        reverse=True,
    )
    payload["top_edges"] = all_edges[:40]
    payload["alerts"] = [
        e
        for e in all_edges
        if e.get("days_until_start", 99) <= 14
        and e.get("grade") in {"STRONG", "EXCEPTIONAL"}
    ][:20]
    payload["edge_count"] = len(all_edges)
    payload["alert_count"] = len(payload["alerts"])
    payload["available_instruments"] = sum(
        1 for r in payload.get("instrument_results", []) if r.get("status") == "ok"
    )
    payload["integrity_gate"] = {
        "engine": audit_payload.get("engine"),
        "policy": "FAIL instruments cannot publish seasonal edges or alerts",
        "universe": audit_payload.get("universe") or [],
        "passed": audit_payload.get("passed") or [],
        "failed": sorted(failed),
        "pass_count": audit_payload.get("pass_count", 0),
        "fail_count": len(failed),
        "warning_count": audit_payload.get("warning_count", 0),
    }
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build seasonal edge scan with data audit")
    parser.add_argument(
        "--no-repair",
        action="store_true",
        help="Audit only; do not attempt known deterministic futures-source repairs.",
    )
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Run/export the full seasonality audit without rebuilding scanner edges.",
    )
    args = parser.parse_args(argv)

    # First pass identifies the exact failures before any mutation. Repair only the
    # source families for which we have explicit, direction-correct foundations.
    pre_audit = audit_seasonality_universe(today=date.today())
    repairs = _repair_failed_instruments(pre_audit, enabled=not args.no_repair)

    # Publication is always based on a fresh post-repair audit of the whole universe.
    audit_payload = audit_seasonality_universe(today=date.today())
    audit_payload["pre_repair_status"] = {
        "pass_count": pre_audit.get("pass_count"),
        "fail_count": pre_audit.get("fail_count"),
        "failed": pre_audit.get("failed") or [],
    }
    audit_payload["repairs"] = repairs

    data_dir = PROJECT_ROOT / "web-dashboard" / "public" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    audit_out = data_dir / "seasonality_data_audit_latest.json"
    audit_out.write_text(json.dumps(audit_payload, indent=2), encoding="utf-8")

    payload = None
    out = data_dir / "seasonal_edge_scan_latest.json"
    if not args.audit_only:
        payload = build_seasonal_edge_scan(asof=date.today())
        payload = _integrity_gate(payload, audit_payload)
        payload["audit_output"] = str(audit_out)
        payload["repairs"] = repairs
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    repaired = [r["instrument_id"] for r in repairs if r.get("status") == "repaired"]
    repair_failures = [
        {
            "instrument_id": r.get("instrument_id"),
            "status": r.get("status"),
            "error": r.get("error"),
        }
        for r in repairs
        if r.get("attempted") and r.get("status") != "repaired"
    ]
    summary = {
        "status": payload.get("status") if payload else audit_payload.get("status"),
        "output": str(out) if payload else None,
        "audit_output": str(audit_out),
        "instrument_count": audit_payload.get("instrument_count"),
        "integrity_pass": audit_payload.get("pass_count"),
        "integrity_fail": audit_payload.get("fail_count"),
        "integrity_warnings": audit_payload.get("warning_count"),
        "failed_instruments": audit_payload.get("failed"),
        "repaired_instruments": repaired,
        "repair_failures": repair_failures,
        "available_instruments": payload.get("available_instruments") if payload else None,
        "edge_count": payload.get("edge_count") if payload else None,
        "alert_count": payload.get("alert_count") if payload else None,
    }
    print(json.dumps(summary))

    # Audit failure does not crash dev startup; failed instruments are withheld.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
