"""HTPL Legacy COT reset — reconciliation, latest DB, audit, deliverable report."""
from __future__ import annotations

import argparse
import sys

from hptl.cot.legacy_cot import (
    HISTORY_YEARS,
    REPORT_DELIVERABLE,
    WEEKS_HISTORY,
    default_history_years,
    run_legacy_cot_reset,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Legacy COT reconciliation + latest + audit JSON")
    parser.add_argument("--year", type=int, default=None, help="CFTC legacy year (default: current UTC year)")
    parser.add_argument(
        "--weeks",
        type=int,
        default=WEEKS_HISTORY,
        help=f"Weeks of history per instrument (default {WEEKS_HISTORY} = 10Y)",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help=f"Comma-separated CFTC annual years (default: last {HISTORY_YEARS} calendar years).",
    )
    args = parser.parse_args(argv)
    years = (
        [int(y.strip()) for y in args.years.split(",") if y.strip()]
        if args.years
        else default_history_years()
    )
    result = run_legacy_cot_reset(year=args.year, weeks=args.weeks, years=years)
    for label, path in result["paths"].items():
        print(f"Wrote {label}: {path}")
    print(f"Wrote report: {REPORT_DELIVERABLE}")
    counts = result["reconciliation"]["status_counts"]
    print(f"Status: PASS={counts.get('PASS')} FAIL={counts.get('FAIL')} REVIEW={counts.get('NEEDS_MANUAL_REVIEW')}")
    print(f"Regression tests_passed={result['report']['tests_passed']}")
    return 0 if result["report"]["tests_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
