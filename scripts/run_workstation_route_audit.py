#!/usr/bin/env python3
"""End-to-end workstation route audit for all LEGACY_COT_MARKETS."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.workstation_route_payload import audit_all_workstation_routes  # noqa: E402

OUT_JSON = ROOT / "data" / "audits" / "workstation_route_audit.json"
OUT_MD = ROOT / "data" / "audits" / "workstation_route_audit.md"


def _md(report: dict) -> str:
    s = report["summary"]
    lines = [
        "# Workstation Route Audit",
        "",
        f"Generated: `{report['generated_at']}`",
        "",
        "## Summary",
        "",
        f"- HTTP 200 / valid workstation: **{s['http_200_ok']}**",
        f"- HTTP 422 integrity errors: **{s['http_422_integrity']}**",
        f"- HTTP 500: **{s['http_500']}**",
        f"- Blank renders: **{s['blank_renders']}**",
        f"- Overall: **{s['overall_status']}**",
        "",
        "## Per instrument",
        "",
    ]
    for row in report["instruments"]:
        lines.append(f"### {row['instrument']} — **{row['final_result']}**")
        lines.append("")
        lines.append(f"- HTTP status: `{row['http_status']}`")
        lines.append(f"- Response status: `{row['response_status']}`")
        lines.append(f"- Report date: `{row['report_date']}`")
        lines.append(f"- Payload valid: `{row['payload_valid']}`")
        missing = row.get("missing_fields") or []
        lines.append(f"- Missing fields: `{len(missing)}`")
        if missing[:5]:
            lines.append(f"- Sample missing: {', '.join(missing[:5])}")
        lines.append("")
    lines.extend(
        [
            "## Required final state",
            "",
            f"HTTP 200 / valid workstation: {s['http_200_ok']}",
            f"HTTP 409 or 422 integrity errors: {s['http_422_integrity']}",
            f"HTTP 500: {s['http_500']}",
            f"Blank renders: {s['blank_renders']}",
            "",
            f"OVERALL STATUS: {s['overall_status']}",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    report = audit_all_workstation_routes()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(_md(report), encoding="utf-8")
    s = report["summary"]
    print(f"HTTP 200 / valid workstation: {s['http_200_ok']}")
    print(f"HTTP 422 integrity errors: {s['http_422_integrity']}")
    print(f"HTTP 500: {s['http_500']}")
    print(f"Blank renders: {s['blank_renders']}")
    print(f"Report: {OUT_MD}")
    print(f"OVERALL STATUS: {s['overall_status']}")
    return 0 if s["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
