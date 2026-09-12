#!/usr/bin/env python
"""Build/export Seasonal Edge Scanner payload with full-universe integrity gating.

Before scanning, Corn gets a targeted repair attempt when its canonical source is
invalid or its series fails structural integrity.  This prevents the known Alpha
Vantage CORN ETF / CBOT futures scale mix from ever publishing a seasonal edge.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.seasonality_workstation.seasonal_edge_scanner import build_seasonal_edge_scan
from hptl.seasonality_workstation.universe_audit import (
    audit_seasonality_instrument,
    audit_seasonality_universe,
)


def _repair_corn_if_needed(*, enabled: bool = True) -> dict:
    before = audit_seasonality_instrument("Corn")
    result = {
        "instrument_id": "Corn",
        "attempted": False,
        "before": before,
        "after": before,
        "status": "not_needed" if before.get("status") == "PASS" else "not_attempted",
    }
    if before.get("status") == "PASS" or not enabled:
        return result

    result["attempted"] = True
    try:
        from hptl.prices.corn_foundation_backfill import run_corn_foundation_backfill

        repair = run_corn_foundation_backfill(execute=True)
        result["repair"] = repair
    except Exception as exc:  # noqa: BLE001
        result["status"] = "repair_failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    after = audit_seasonality_instrument("Corn")
    result["after"] = after
    result["status"] = "repaired" if after.get("status") == "PASS" else "still_failed"
    return result


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
        help="Do not attempt the targeted Yahoo ZC=F Corn foundation repair.",
    )
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Run/export the full seasonality audit without rebuilding scanner edges.",
    )
    args = parser.parse_args(argv)

    repairs = [_repair_corn_if_needed(enabled=not args.no_repair)]
    audit_payload = audit_seasonality_universe(today=date.today())
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

    summary = {
        "status": payload.get("status") if payload else audit_payload.get("status"),
        "output": str(out) if payload else None,
        "audit_output": str(audit_out),
        "instrument_count": audit_payload.get("instrument_count"),
        "integrity_pass": audit_payload.get("pass_count"),
        "integrity_fail": audit_payload.get("fail_count"),
        "integrity_warnings": audit_payload.get("warning_count"),
        "failed_instruments": audit_payload.get("failed"),
        "corn_repair": repairs[0].get("status"),
        "available_instruments": payload.get("available_instruments") if payload else None,
        "edge_count": payload.get("edge_count") if payload else None,
        "alert_count": payload.get("alert_count") if payload else None,
    }
    print(json.dumps(summary))

    # Audit failure does not crash dev startup; failed instruments are withheld.
    # The terminal summary makes failures explicit for repair.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
