#!/usr/bin/env python3
"""Derived COT integrity gate — Weekly Inspector contract for all LEGACY markets."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.derived_cot_integrity_audit import run_derived_cot_integrity_gate


def main() -> int:
    gate = run_derived_cot_integrity_gate()
    print(f"PASS: {gate['pass_count']}")
    print(f"FAIL: {gate['fail_count']}")
    print(f"Report: {gate['report_md']}")
    if not gate["passed"]:
        print("")
        print("DERIVED COT INTEGRITY FAILED")
        for name in gate["failing_instruments"]:
            print(f"  - {name}")
        print("")
        print(f"OVERALL STATUS: {gate['overall_status']}")
        return 1
    print("")
    print(f"OVERALL STATUS: {gate['overall_status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
