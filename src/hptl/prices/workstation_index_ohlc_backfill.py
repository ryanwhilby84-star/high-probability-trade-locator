"""CLI: backfill workstation index OHLC cache (OANDA NAS100_USD, etc.)."""

from __future__ import annotations

import argparse
import json
import sys

from hptl.prices.workstation_index_ohlc_history import (
    WORKSTATION_INDEX_SOURCES,
    run_backfill,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill workstation index OHLC history cache.")
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instruments",
        help="Instrument id (repeatable). Default: all configured index proxies.",
    )
    parser.add_argument("--window-start", default="2017-01-03", help="Earliest date (YYYY-MM-DD).")
    args = parser.parse_args(argv)

    ids = args.instruments or ["NASDAQ / NQ"]
    for iid in ids:
        if iid not in WORKSTATION_INDEX_SOURCES:
            print(f"ERROR: no workstation index source for {iid!r}", file=sys.stderr)
            print("Configured:", ", ".join(WORKSTATION_INDEX_SOURCES.keys()), file=sys.stderr)
            return 1

    results = run_backfill(ids, window_start=args.window_start)
    print(json.dumps(results, indent=2, ensure_ascii=False))
    if not any(r.get("daily_rows", 0) > 100 for r in results):
        print("WARNING: backfill returned few rows — check OANDA_API_KEY / network.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
