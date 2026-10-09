#!/usr/bin/env python3
"""Repair Corn/softs foundations, rebuild workstation candles, then run the gate.

This does not rerun COT downloads, valuations or the full dashboard pipeline.
Use --rebuild-only after a successful price repair to avoid another download.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.prices.corn_foundation_backfill import run_corn_foundation_backfill
from hptl.prices.softs_futures_backfill import promote_soft_futures
from hptl.prices.workstation_ohlc_export import run as rebuild_workstation
from hptl.prices.price_cot_alignment_audit import run_price_cot_alignment_gate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild-only", action="store_true")
    args = parser.parse_args()
    if not args.rebuild_only:
        try:
            result = run_corn_foundation_backfill(execute=True)
            if result.get("status") != "promoted":
                raise RuntimeError(f"Corn promotion failed: {result}")
            print(f"Corn refreshed through {result.get('latest_date') or 'provider latest'}")
            for market in ("Cocoa", "Cotton"):
                result = promote_soft_futures(market)
                print(f"{market} refreshed through {result.get('corrected_latest_date')}")
        except Exception as exc:
            print(f"PRICE REPAIR FAILED: {exc}", file=sys.stderr)
            print("Some histories may have refreshed; workstation export was not rebuilt.", file=sys.stderr)
            return 1
    rebuild_workstation()
    gate = run_price_cot_alignment_gate(live_provider=True)
    print(f"PASS: {gate['pass_count']}  FAIL: {gate['fail_count']}")
    print(f"Report: {gate['report_md']}")
    if not gate["passed"]:
        print("PRICE / COT ALIGNMENT FAILED")
        for market in gate["failing_instruments"]:
            print(f"  - {market}")
        return 1
    print("PRICE / COT ALIGNMENT PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
