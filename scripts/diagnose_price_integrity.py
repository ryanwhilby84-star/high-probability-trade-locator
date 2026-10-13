#!/usr/bin/env python3
"""Read-only, targeted price-health diagnostic. Does not fetch, overwrite or publish data.

Usage:
  PYTHONPATH=src python scripts/diagnose_price_integrity.py --instrument Soybeans --instrument Sugar
  PYTHONPATH=src python scripts/diagnose_price_integrity.py --all-supported
"""
from __future__ import annotations
import argparse
import json
from collections import Counter
from hptl.prices.coverage import load_price_coverage, supported_instrument_ids
from hptl.prices.price_health import audit_record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", action="append", default=[])
    parser.add_argument("--all-supported", action="store_true")
    args = parser.parse_args()
    if not args.instrument and not args.all_supported:
        parser.error("Specify --instrument (repeatable) or --all-supported")
    ids = list(dict.fromkeys(args.instrument + (supported_instrument_ids(load_price_coverage()) if args.all_supported else [])))
    rows = [audit_record(iid) for iid in ids]
    print(json.dumps({
        "summary": dict(Counter(row["status"] for row in rows)),
        "rows": rows,
    }, indent=2, default=str))
    # Diagnostic deliberately exits successfully: it must never be used as a release gate.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
