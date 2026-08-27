"""FX daily OHLC backfill — dry-run probe or staging execute.

Usage:
    python -m hptl.prices.backfill_fx_daily --source oanda --years 10 --dry-run
    python -m hptl.prices.backfill_fx_daily --source oanda --years 10 --execute

Writes staging to data/processed/prices/backfill/ — does not promote to production.
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
    SUMMARY_PATH,
    run_backfill,
    validate_staging_coverage,
)
from hptl.prices.fx_oanda_backfill_feasibility_audit import (
    OANDA_MAX_COUNT,
    TEST_PAIRS,
    _iso_from,
    _probe_candles,
    _stored_daily_stats,
)
from hptl.prices.price_store import load_price_store
from hptl.seasonality import fx_seasonality_coverage_audit

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def _plan_pair(display: str, oanda: str, store_key: str, *, years: int) -> dict:
    instruments = load_price_store().get("instruments") or {}
    stored = _stored_daily_stats(store_key, instruments)
    start = date.today() - timedelta(days=years * 366)
    bars, meta = _probe_candles(oanda, from_time=_iso_from(start), count=OANDA_MAX_COUNT)
    return {
        "pair": display,
        "oanda_symbol": oanda,
        "store_key": store_key,
        "stored_bars": stored["stored_daily_bar_count"],
        "probe_bars": len(bars),
        "probe_earliest": bars[0]["date"] if bars else None,
        "probe_latest": bars[-1]["date"] if bars else None,
        "probe_error": meta.get("error"),
        "recommended_chunks": max(1, (len(bars) // RECOMMENDED_CHUNK_DAYS) + 1) if bars else 0,
    }


def _print_final_report(summary: dict, staging_validation: dict) -> None:
    totals = summary.get("totals") or {}
    print()
    print("=" * 60)
    print("FX DAILY BACKFILL — FINAL REPORT")
    print("=" * 60)
    ok = summary.get("backfill_completed_successfully")
    print(f"1. Backfill completed successfully? {'yes' if ok else 'no'}")
    print()
    print("2. Bars added per pair:")
    for row in summary.get("pairs") or []:
        print(
            f"   {row.get('display_symbol', '?'):8} "
            f"+{int(row.get('bars_added') or 0):5} bars  "
            f"total={int(row.get('total_bars_after_merge') or 0):5}  "
            f"status={row.get('status')}"
        )
    print()
    print("3. Earliest available date after merge:")
    for row in summary.get("pairs") or []:
        print(
            f"   {row.get('display_symbol', '?'):8} "
            f"{row.get('earliest_date') or '—'} .. {row.get('latest_date') or '—'}"
        )
    print()
    print("4. Years of coverage after merge:")
    for row in summary.get("pairs") or []:
        print(
            f"   {row.get('display_symbol', '?'):8} "
            f"{float(row.get('years_of_coverage') or 0):.2f} years"
        )
    print()
    can_10y = staging_validation.get("can_10y_pairs") or []
    print(f"5. Pairs qualifying for Seasonality V2 (staging): {', '.join(can_10y) or 'none'}")
    v2_ready = staging_validation.get("can_10y_count", 0) >= len(TEST_PAIRS)
    print(
        f"6. FX Seasonality V2 production testing? "
        f"{'Ready for staging validation / promotion gate' if v2_ready else 'Not yet — insufficient staging depth'}"
    )
    print()
    print(f"Summary: {SUMMARY_PATH}")
    print(f"Staging:  {STAGING_DIR}")
    print("Production store NOT modified — promotion requires explicit confirmation.")
    print("=" * 60)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FX daily backfill (dry-run by default)")
    parser.add_argument("--source", default="oanda", choices=["oanda"])
    parser.add_argument("--years", type=int, default=10)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Fetch OANDA history and write staging files",
    )
    args = parser.parse_args(argv)

    if not get_oanda_api_key():
        print("ERROR: OANDA_API_KEY not set", file=sys.stderr)
        return 1

    dry_run = not args.execute

    if dry_run:
        print(f"FX daily backfill plan source={args.source} years={args.years} dry_run=True")
        print(f"Staging target: {STAGING_DIR}/")
        print(f"Chunk size: {RECOMMENDED_CHUNK_DAYS} bars; OANDA max {OANDA_MAX_COUNT}/request")
        print()
        for display, oanda, store_key in TEST_PAIRS:
            plan = _plan_pair(display, oanda, store_key, years=args.years)
            print(
                f"  {plan['pair']:8} stored={plan['stored_bars']:4} "
                f"probe={plan['probe_bars']:4} "
                f"range={plan['probe_earliest']}..{plan['probe_latest']} "
                f"chunks~{plan['recommended_chunks']}"
            )
            if plan["probe_error"]:
                print(f"           warn: {plan['probe_error']}")
        print("\nDry-run complete — no files written.")
        print("Execute: python -m hptl.prices.backfill_fx_daily --source oanda --years 10 --execute")
        return 0

    _configure_logging()
    print(f"FX daily backfill EXECUTE source={args.source} years={args.years}")
    print(f"Writing to staging: {STAGING_DIR}/")
    print()

    summary = run_backfill(years=args.years, chunk_size=RECOMMENDED_CHUNK_DAYS)

    print("\nRunning production FX seasonality coverage audit (baseline — unchanged store)...")
    fx_seasonality_coverage_audit.run()

    staging_validation = validate_staging_coverage()
    _print_final_report(summary, staging_validation)
    return 0 if summary.get("backfill_completed_successfully") else 1


if __name__ == "__main__":
    sys.exit(main())
