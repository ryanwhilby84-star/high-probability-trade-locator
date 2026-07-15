#!/usr/bin/env python3
"""Run the HPTL price integrity audit."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.prices.price_integrity_audit import TARGET_INSTRUMENTS, write_price_integrity_audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit HPTL price freshness and reference mismatches.")
    parser.add_argument("--instrument", action="append", default=[], help="Instrument id to audit; repeatable.")
    parser.add_argument("--no-fetch", action="store_true", help="Do not fetch live references; use local exports only.")
    args = parser.parse_args()

    instruments = args.instrument or TARGET_INSTRUMENTS
    path = write_price_integrity_audit(instruments, fetch_live=not args.no_fetch)
    print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
