"""Refresh canonical price store for all coverage-supported instruments."""

from __future__ import annotations

import argparse
import sys

from hptl.price_config import PriceApiConfigError, validate_price_api_keys
from hptl.prices.coverage import load_price_coverage, supported_instrument_ids
from hptl.prices.price_store import CANONICAL_PATH, write_instrument_record, write_price_store
from hptl.prices.unified_adapter import UnifiedPriceAdapter


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh HTPL canonical price store (OANDA + Alpha Vantage)")
    parser.add_argument("--limit", type=int, default=0, help="Max instruments (0 = all supported)")
    parser.add_argument("--instrument", type=str, default="", help="Refresh single instrument id")
    parser.add_argument("--skip-validation", action="store_true")
    args = parser.parse_args()

    try:
        if not args.skip_validation:
            validate_price_api_keys(probe_live=True)
    except (PriceApiConfigError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    coverage = load_price_coverage()
    ids = [args.instrument.strip()] if args.instrument.strip() else supported_instrument_ids(coverage)
    if args.limit > 0:
        ids = ids[: args.limit]

    adapter = UnifiedPriceAdapter(coverage)
    records: dict = {}

    def progress(i: int, total: int, iid: str, rec: dict) -> None:
        status = "ok" if (rec.get("daily") or rec.get("weekly")) and not rec.get("error") else "fail"
        print(f"[{i}/{total}] {iid}: {status}", flush=True)

    for iid in ids:
        rec = adapter.fetch(iid)
        records[iid] = rec
        src = adapter.source_for(iid) or "none"
        write_instrument_record(rec, fetched_via=src)
        progress(len(records), len(ids), iid, rec)

    path = write_price_store(
        records,
        coverage_generated_at=coverage.get("generated_at"),
    )
    s = records
    ok = sum(1 for r in s.values() if r.get("daily") and not r.get("error"))
    print(f"\nStored {len(records)} instruments ({ok} with daily bars)")
    print(f"Canonical: {path}")
    print(f"Dashboard: web-dashboard/public/data/prices_latest.json")


if __name__ == "__main__":
    main()
