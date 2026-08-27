#!/usr/bin/env python3
"""Universal Price ↔ COT alignment audit + gate for LEGACY_COT_MARKETS."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.prices.price_cot_alignment_audit import run_price_cot_alignment_gate


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--no-live-provider",
        action="store_true",
        help="Skip live OANDA/Yahoo provider fetches (still audits store→weekly→workstation→COT).",
    )
    args = ap.parse_args()
    gate = run_price_cot_alignment_gate(live_provider=not args.no_live_provider)
    print(f"PASS: {gate['pass_count']}  FAIL: {gate['fail_count']}")
    print(f"Report: {gate['report_md']}")
    if not gate["passed"]:
        print("")
        print("PRICE / COT ALIGNMENT FAILED")
        for name in gate["failing_instruments"]:
            print(f"  - {name}")
        print("")
        print("OVERALL STATUS")
        print("FAIL")
        return 1
    print("")
    print("OVERALL STATUS")
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
