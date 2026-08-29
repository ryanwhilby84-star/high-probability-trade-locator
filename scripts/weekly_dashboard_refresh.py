#!/usr/bin/env python3
"""Weekly dashboard refresh — COT/master, prices, pillars, confluence, chart series, validation."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")


def _refresh_cot_market_prices() -> int:
    """Refresh the price store for the direct COT universe before alignment checks.

    The weekly alignment gate compares the newest COT report with workstation
    weekly candles. Running the gate against last week's price store creates a
    false wall of alignment failures even when COT itself is current. Keep this
    targeted to LEGACY_COT_MARKETS rather than refreshing the full 100+ market
    registry.
    """
    from hptl.markets.instrument_registry import LEGACY_COT_MARKETS
    from hptl.prices.run_price_refresh import main as price_refresh_main

    price_args: list[str] = ["--skip-validation"]
    for instrument_id in LEGACY_COT_MARKETS:
        price_args.extend(["--instrument", instrument_id])

    print(f"Refreshing price inputs for {len(LEGACY_COT_MARKETS)} COT markets before alignment gate...")
    return int(price_refresh_main(price_args) or 0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Weekly dashboard refresh (no full confluence enrichment).")
    parser.add_argument("--force-cot", action="store_true", help="Force CFTC re-download even if local week is current.")
    parser.add_argument("--skip-cot-pull", action="store_true", help="Skip live CFTC pull; refresh exports from local master only.")
    parser.add_argument(
        "--skip-price-refresh",
        action="store_true",
        help="Skip the targeted live price refresh for direct COT markets.",
    )
    args = parser.parse_args(argv)

    if not args.skip_price_refresh:
        price_rc = _refresh_cot_market_prices()
        if price_rc != 0:
            print(
                f"WARNING: targeted COT price refresh exited {price_rc}; "
                "continuing so the alignment audit can report the exact remaining failures.",
                file=sys.stderr,
            )

    from hptl.dashboard.weekly_refresh import print_weekly_report, run_weekly_refresh

    report = run_weekly_refresh(force_cot=args.force_cot, skip_cot_pull=args.skip_cot_pull)
    print_weekly_report(report)
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
