"""Backfill and promote price history for remaining COT coverage FAIL instruments."""

from __future__ import annotations

import argparse
import logging
import sys

from hptl.config import get_oanda_api_key
from hptl.prices.cot_fail_backfill import run_all


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill COT FAIL instrument price history")
    parser.add_argument("--years", type=int, default=10)
    parser.add_argument("--no-promote", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not get_oanda_api_key():
        print("ERROR: OANDA_API_KEY not set (required for OANDA backfill leg)", file=sys.stderr)
        return 1

    result = run_all(years=args.years, promote=not args.no_promote)
    print("OANDA backfill ok:", result["oanda"].get("backfill_completed_successfully"))
    for row in result["oanda"].get("pairs") or []:
        print(
            f"  {row.get('instrument'):28} status={row.get('status')} "
            f"bars={row.get('total_bars_after_merge')} "
            f"{row.get('earliest_date')}..{row.get('latest_date')}"
        )
    print("FRED backfill:")
    for row in result["fred"]:
        print(
            f"  {row.get('instrument'):28} status={row.get('status')} "
            f"bars={row.get('total_daily_bars')} series={row.get('series_id')}"
        )
    print("Promoted:", result["promotion"].get("count"))
    for row in result["promotion"].get("promoted") or []:
        print(
            f"  {row.get('instrument'):28} total={row.get('total_daily_bars')} "
            f"{row.get('earliest_date')}..{row.get('latest_date')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
