#!/usr/bin/env python3
"""One-command HPTL data pipeline refresh — prices → COT → exports → dist sync."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")


def _step(label: str) -> None:
    print(f"\n--- {label} ---")


def refresh_prices(*, live: bool, skip_validation: bool) -> None:
    if live:
        from hptl.prices.run_price_refresh import main as run_price_refresh

        argv: list[str] = []
        if skip_validation:
            argv.append("--skip-validation")
        run_price_refresh()
        return

    from hptl.prices.price_store import PUBLIC_PATH, rebuild_price_store_from_disk

    path = rebuild_price_store_from_disk()
    print(f"Rebuilt price store from disk -> {path}")
    print(f"Dashboard export: {PUBLIC_PATH}")


def refresh_valuation_history(*, markets: list[str] | None, max_weeks: int | None) -> None:
    from hptl.valuation.instrument_valuation_history_viz_export import (
        export_instrument_valuation_history,
        write_export,
    )

    doc = export_instrument_valuation_history(markets=markets or None, max_weeks=max_weeks)
    _, pub = write_export(doc)
    print(f"instrument_valuation_history_latest.json — {len(doc.get('instruments') or {})} markets → {pub}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refresh the full HPTL workstation data pipeline in dependency order.",
    )
    parser.add_argument("--skip-prices", action="store_true", help="Skip price store refresh/rebuild.")
    parser.add_argument(
        "--live-prices",
        action="store_true",
        help="Fetch live prices via OANDA/Alpha Vantage (default: rebuild from on-disk records).",
    )
    parser.add_argument("--skip-cot-pull", action="store_true", help="Skip live CFTC pull; use local master CSV.")
    parser.add_argument("--force-cot", action="store_true", help="Force CFTC re-download.")
    parser.add_argument(
        "--skip-valuation-history",
        action="store_true",
        help="Skip instrument_valuation_history_latest.json (slow viz export).",
    )
    parser.add_argument("--valuation-history-weeks", type=int, default=None, help="Limit weeks for valuation history.")
    parser.add_argument("--verify", action="store_true", help="Run freshness verification after refresh.")
    parser.add_argument("--json-report", type=str, default="", help="Write verification JSON to this path.")
    args = parser.parse_args(argv)

    errors: list[str] = []

    if not args.skip_prices:
        _step("1/8 Market prices")
        try:
            refresh_prices(live=args.live_prices, skip_validation=args.live_prices is False)
        except Exception as exc:
            errors.append(f"price refresh: {exc}")
            print(f"ERROR: {exc}", file=sys.stderr)

    _step("2/8 COT + master + pillar exports + confluence + workstation exports")
    from hptl.dashboard.weekly_refresh import print_weekly_report, run_weekly_refresh

    report = run_weekly_refresh(force_cot=args.force_cot, skip_cot_pull=args.skip_cot_pull)
    print_weekly_report(report)
    if report.errors:
        errors.extend(report.errors)

    if not args.skip_valuation_history:
        _step("3/8 Instrument valuation history (viz export)")
        try:
            refresh_valuation_history(markets=None, max_weeks=args.valuation_history_weeks)
        except Exception as exc:
            errors.append(f"valuation history: {exc}")
            print(f"ERROR: {exc}", file=sys.stderr)
    else:
        _step("3/8 Instrument valuation history — skipped")

    _step("4/8 Verification")
    from hptl.dashboard.pipeline_freshness import build_pipeline_freshness_report, print_freshness_report

    freshness = build_pipeline_freshness_report()
    if args.json_report:
        out = Path(args.json_report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(freshness.as_dict(), indent=2), encoding="utf-8")
        print(f"Wrote report → {out}")

    rc = print_freshness_report(freshness)

    if errors or not report.passed:
        return 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
