#!/usr/bin/env python3
"""Weekly dashboard refresh — COT/master, pillars, confluence, chart series, validation."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Weekly dashboard refresh (no full confluence enrichment).")
    parser.add_argument("--force-cot", action="store_true", help="Force CFTC re-download even if local week is current.")
    parser.add_argument("--skip-cot-pull", action="store_true", help="Skip live CFTC pull; refresh exports from local master only.")
    args = parser.parse_args(argv)

    from hptl.dashboard.weekly_refresh import print_weekly_report, run_weekly_refresh

    report = run_weekly_refresh(force_cot=args.force_cot, skip_cot_pull=args.skip_cot_pull)
    print_weekly_report(report)
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
