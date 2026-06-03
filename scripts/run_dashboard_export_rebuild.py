#!/usr/bin/env python3
"""Rebuild dashboard JSON exports: prices → valuation → seasonality → confluence."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild dashboard export pipeline.")
    parser.add_argument("--skip-confluence", action="store_true", help="Skip confluence rebuild (slow).")
    args = parser.parse_args(argv)

    from hptl.prices.price_store import PUBLIC_PATH, rebuild_price_store_from_disk

    price_path = rebuild_price_store_from_disk()
    store = __import__("json").loads(price_path.read_text(encoding="utf-8"))
    print(
        f"1/4 prices_latest.json — {store['summary']['instruments_total']} instruments, "
        f"{store['summary']['with_daily_bars']} with bars → {PUBLIC_PATH}"
    )

    from hptl.valuation.export import build_valuation_latest, write_valuation_exports

    val = build_valuation_latest()
    write_valuation_exports(val)
    print(
        f"2/4 valuation_latest.json — wired {val['summary']['wired_count']}/"
        f"{val['summary']['total_instruments']}"
    )

    from hptl.seasonality.export import build_seasonality_latest, write_seasonality_exports

    sea = build_seasonality_latest()
    write_seasonality_exports(sea)
    print(
        f"3/4 seasonality_latest.json — wired {sea['summary']['wired_count']}/"
        f"{sea['summary']['total_instruments']}"
    )

    if args.skip_confluence:
        print("4/6 confluence_history_latest.json — skipped")
        print("5/6 thesis_tracker_latest.json — skipped (requires confluence)")
        return 0

    from hptl.confluence.build_decision_table import run as run_confluence

    out = run_confluence()
    print(f"4/6 confluence_history_latest.json → {out}")

    from hptl.thesis_tracker.run_thesis_seed import main as run_thesis_seed

    rc = run_thesis_seed(["--reset", "--weeks=13"])
    if rc != 0:
        return rc
    print("5/6 thesis_tracker_latest.json — re-seeded from confluence")

    from hptl.thesis_tracker.opportunity_distribution_report import write_scanner_latest

    write_scanner_latest()
    print("6/6 scanner_latest.json — refreshed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
