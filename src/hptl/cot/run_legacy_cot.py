"""HTPL Legacy COT reset — reconciliation, latest DB, audit, deliverable report."""
from __future__ import annotations

import argparse
import sys

from hptl.cot.legacy_cot import REPORT_DELIVERABLE, run_legacy_cot_reset


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Legacy COT reconciliation + latest + audit JSON")
    parser.add_argument("--year", type=int, default=None, help="CFTC legacy year (default: current UTC year)")
    parser.add_argument("--weeks", type=int, default=13, help="Weeks of history per instrument")
    args = parser.parse_args(argv)
    result = run_legacy_cot_reset(year=args.year, weeks=args.weeks)
    for label, path in result["paths"].items():
        print(f"Wrote {label}: {path}")
    print(f"Wrote report: {REPORT_DELIVERABLE}")
    counts = result["reconciliation"]["status_counts"]
    print(f"Status: PASS={counts.get('PASS')} FAIL={counts.get('FAIL')} REVIEW={counts.get('NEEDS_MANUAL_REVIEW')}")
    print(f"Regression tests_passed={result['report']['tests_passed']}")
    return 0 if result["report"]["tests_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
