"""Metal daily OHLC backfill (Gold/Silver) via OANDA XAU_USD / XAG_USD.

Usage:
    python -m hptl.prices.backfill_metal_daily --dry-run
    python -m hptl.prices.backfill_metal_daily --execute --promote
    python -m hptl.prices.backfill_metal_daily --execute --promote --instrument Gold
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from hptl.config import get_oanda_api_key
from hptl.prices.fx_daily_backfill import (
    RECOMMENDED_CHUNK_DAYS,
    STAGING_DIR,
    BackfillPair,
    backfill_pair,
    run_backfill,
)
from hptl.prices.fx_oanda_backfill_feasibility_audit import OANDA_MAX_COUNT, _iso_from, _probe_candles
from hptl.prices.promote_price_backfill import promote_staging_backfill

logger = logging.getLogger(__name__)

METAL_PAIRS: tuple[BackfillPair, ...] = (
    ("Gold", "XAU_USD", "Gold"),
    ("Silver", "XAG_USD", "Silver"),
)


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")


def _plan_pair(display: str, oanda: str, store_key: str, *, years: int) -> dict:
    start = date.today() - timedelta(days=years * 366)
    bars, meta = _probe_candles(oanda, from_time=_iso_from(start), count=OANDA_MAX_COUNT)
    return {
        "pair": display,
        "oanda_symbol": oanda,
        "store_key": store_key,
        "probe_bars": len(bars),
        "probe_earliest": bars[0]["date"] if bars else None,
        "probe_latest": bars[-1]["date"] if bars else None,
        "probe_error": meta.get("error"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Metal daily backfill via OANDA (Gold/Silver)")
    parser.add_argument("--years", type=int, default=10)
    parser.add_argument("--instrument", type=str, default="", help="Single instrument id (e.g. Gold)")
    parser.add_argument("--execute", action="store_true", help="Fetch OANDA and write staging")
    parser.add_argument("--promote", action="store_true", help="Promote staging into prices_latest.json")
    args = parser.parse_args(argv)

    pairs = METAL_PAIRS
    if args.instrument.strip():
        iid = args.instrument.strip()
        matched = [p for p in METAL_PAIRS if p[2] == iid or p[0] == iid]
        if not matched:
            print(f"ERROR: unknown metal instrument {iid!r}", file=sys.stderr)
            return 1
        pairs = tuple(matched)

    if not args.execute:
        print(f"Metal daily backfill plan years={args.years} dry_run=True")
        for display, oanda, store_key in pairs:
            plan = _plan_pair(display, oanda, store_key, years=args.years)
            print(
                f"  {plan['pair']:8} probe={plan['probe_bars']:4} "
                f"range={plan['probe_earliest']}..{plan['probe_latest']}"
            )
            if plan["probe_error"]:
                print(f"           warn: {plan['probe_error']}")
        print(f"\nStaging: {STAGING_DIR}/")
        print("Execute: python -m hptl.prices.backfill_metal_daily --execute --promote")
        return 0

    if not get_oanda_api_key():
        print("ERROR: OANDA_API_KEY not set", file=sys.stderr)
        return 1

    _configure_logging()
    summary = run_backfill(pairs=pairs, years=args.years, chunk_size=RECOMMENDED_CHUNK_DAYS)
    ok = summary.get("backfill_completed_successfully")
    print(f"Backfill completed: {ok}")

    if args.promote:
        keys = [p[2] for p in pairs]
        result = promote_staging_backfill(keys)
        for row in result.get("promoted") or []:
            print(
                f"Promoted {row['instrument']}: +{row['bars_added']} bars, "
                f"total={row['total_daily_bars']} "
                f"({row['earliest_date']} .. {row['latest_date']})"
            )

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
