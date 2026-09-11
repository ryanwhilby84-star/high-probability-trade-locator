#!/usr/bin/env python
"""Build/export Seasonal Edge Scanner payload, gated by the canonical integrity audit."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.seasonality_workstation.integrity import audit_daily_series
from hptl.seasonality_workstation.returns import load_daily_closes
from hptl.seasonality_workstation.seasonal_edge_scanner import build_seasonal_edge_scan


def _integrity_gate(payload: dict) -> tuple[dict, dict]:
    audits: dict[str, dict] = {}
    failed: set[str] = set()

    for result in payload.get("instrument_results", []):
        instrument_id = result.get("instrument_id")
        if not instrument_id:
            continue
        daily, source, error = load_daily_closes(instrument_id)
        if error or not daily:
            audit = {
                "instrument_id": instrument_id,
                "status": "FAIL",
                "issues": [error or "no_daily_history"],
                "warnings": [],
                "source": source,
            }
        else:
            audit = audit_daily_series(instrument_id, daily, source=source)
        audits[instrument_id] = audit
        result["integrity"] = audit
        if audit.get("status") != "PASS":
            failed.add(instrument_id)
            result["status"] = "integrity_failed"
            result["edges"] = []
            result["error"] = ";".join(audit.get("issues", [])) or "integrity_failed"

    def allowed(edge: dict) -> bool:
        return edge.get("instrument_id") not in failed

    all_edges = [
        edge
        for result in payload.get("instrument_results", [])
        if result.get("status") == "ok"
        for edge in result.get("edges", [])
        if allowed(edge)
    ]
    all_edges.sort(key=lambda e: (e.get("grade") == "EXCEPTIONAL", e.get("edge_score", 0)), reverse=True)
    payload["top_edges"] = all_edges[:40]
    payload["alerts"] = [
        e for e in all_edges
        if e.get("days_until_start", 99) <= 14 and e.get("grade") in {"STRONG", "EXCEPTIONAL"}
    ][:20]
    payload["edge_count"] = len(all_edges)
    payload["alert_count"] = len(payload["alerts"])
    payload["available_instruments"] = sum(1 for r in payload.get("instrument_results", []) if r.get("status") == "ok")
    payload["integrity_gate"] = {
        "policy": "FAIL instruments cannot publish seasonal edges or alerts",
        "passed": sorted(k for k, v in audits.items() if v.get("status") == "PASS"),
        "failed": sorted(failed),
        "pass_count": sum(1 for v in audits.values() if v.get("status") == "PASS"),
        "fail_count": len(failed),
    }
    audit_payload = {
        "status": "ok",
        "asof": payload.get("asof"),
        "instrument_count": len(audits),
        "pass_count": payload["integrity_gate"]["pass_count"],
        "fail_count": len(failed),
        "instruments": audits,
    }
    return payload, audit_payload


def main() -> int:
    payload = build_seasonal_edge_scan(asof=date.today())
    payload, audit_payload = _integrity_gate(payload)

    data_dir = PROJECT_ROOT / "web-dashboard" / "public" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out = data_dir / "seasonal_edge_scan_latest.json"
    audit_out = data_dir / "seasonality_data_audit_latest.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    audit_out.write_text(json.dumps(audit_payload, indent=2), encoding="utf-8")

    print(json.dumps({
        "status": payload.get("status"),
        "output": str(out),
        "audit_output": str(audit_out),
        "instrument_count": payload.get("instrument_count"),
        "integrity_pass": audit_payload.get("pass_count"),
        "integrity_fail": audit_payload.get("fail_count"),
        "available_instruments": payload.get("available_instruments"),
        "edge_count": payload.get("edge_count"),
        "alert_count": payload.get("alert_count"),
    }))
    return 0 if payload.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
