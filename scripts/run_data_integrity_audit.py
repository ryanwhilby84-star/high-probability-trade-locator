#!/usr/bin/env python3
"""Data integrity audit — symbol, source, bars, valuation/seasonality provenance."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.prices.data_integrity import build_integrity_report


def _valuation_label(row: dict) -> str:
    if row["integrity"] == "FAIL":
        return "UNAVAILABLE"
    return "AVAILABLE" if row.get("valuation_available") else "UNAVAILABLE"


def _seasonality_label(row: dict) -> str:
    if row["integrity"] == "FAIL":
        return "UNAVAILABLE"
    return "AVAILABLE" if row.get("seasonality_available") else "UNAVAILABLE"


def main() -> int:
    report = build_integrity_report()
    rows = report["rows"]
    summary = report["summary"]

    out_path = ROOT / "data" / "data_integrity_audit.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Data Integrity Audit — {summary['total']} tradeable instruments")
    print(f"PASS: {summary['pass']} | FAIL: {summary['fail']} | Score-eligible: {summary['score_eligible']}")
    print(f"Report: {out_path}")
    print()
    print(
        f"{'Instrument':<22} {'Source':<16} {'Symbol':<14} {'Daily':>6} {'Weekly':>7} "
        f"{'Valuation':<12} {'Seasonality':<12} Integrity"
    )
    print("-" * 115)
    for r in rows:
        sym = r.get("actual_symbol") or r.get("expected_symbol") or "—"
        src = r.get("actual_source") or r.get("expected_source") or "none"
        print(
            f"{r['instrument']:<22} "
            f"{str(src):<16} "
            f"{str(sym):<14} "
            f"{r['daily_bars']:>6} "
            f"{r['weekly_bars']:>7} "
            f"{_valuation_label(r):<12} "
            f"{_seasonality_label(r):<12} "
            f"{r['integrity']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
