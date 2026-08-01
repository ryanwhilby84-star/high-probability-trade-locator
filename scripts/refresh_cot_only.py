#!/usr/bin/env python3
"""COT-only refresh — live CFTC download → process → INCREMENTAL dashboard publish.

Calls the smallest set of existing functions to get the newest CFTC week onto the
dashboard and then exits. It does NOT call ``run_full_pipeline`` and it does NOT
rebuild the full confluence history (the slow stage that reprocesses all
historical commodity data and prints ``COCOA DEBUG`` records for 30+ minutes).

Stages (in order):
    1. baseline local COT week
    2. run_workbook_export()            -> live download + parse CFTC ZIPs
    3. run_backfill_master()            -> merge into tracked master CSV
    4. refresh_legacy_cot_if_stale()    -> legacy_cot_latest.json (only if behind)
    5. catch_up_confluence_export()     -> INCREMENTAL confluence publish (+dist)
    6. cot_3y_series_export.run()       -> cot_3y_series_latest.json (chart source)
    7. sync_dist_exports()              -> final public -> dist publish

No FX macro history, no valuation, no seasonality, no scanner enrichment.

Usage:
    python scripts/refresh_cot_only.py
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# COT-only: never trigger live environment feeds; never let the stage watchdog
# kill the (legitimately slow-ish) incremental confluence build.
os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
os.environ.setdefault("HPTL_SKIP_VALUATION", "1")


def _iso(ts) -> str:
    import pandas as pd

    if ts is None or pd.isna(ts):
        return "—"
    return str(ts)[:10]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="COT-only dashboard refresh (live CFTC download + incremental publish; no FX/valuation).",
    )
    parser.parse_args(argv)

    from hptl.cot.report_dates import get_latest_local_report_date

    errors: list[str] = []
    files_written: list[str] = []

    local_before = _iso(get_latest_local_report_date())
    print("=" * 72)
    print("HPTL COT-ONLY REFRESH (live CFTC download -> incremental publish)")
    print("=" * 72)
    print(f"[1/7] local COT week before: {local_before}")

    # --- 2. Live download + parse latest CFTC data ---------------------------
    print("[2/7] downloading + parsing latest CFTC ZIPs (workbook export)…")
    try:
        from hptl.cot.workbook_export import run_workbook_export

        wb = run_workbook_export()
        files_written.append(str(getattr(wb, "workbook_path", "")))
        files_written.append(str(getattr(wb, "processed_csv_path", "")))
    except Exception as exc:  # noqa: BLE001 — report and stop
        print(f"FATAL: workbook export (download/parse) failed: {exc}")
        return 1

    # --- 3. Merge tracked master CSV -----------------------------------------
    print("[3/7] merging tracked master CSV…")
    try:
        from hptl.confluence.cot_tracked_backfill import run_backfill_master

        master_path = run_backfill_master(ensure_years_from=2025)
        files_written.append(str(master_path))
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL: tracked master merge failed: {exc}")
        return 1

    local_after = _iso(get_latest_local_report_date())
    print(f"      local COT week after download+merge: {local_after}")

    # --- 4. Legacy positioning (only if it trails master) --------------------
    print("[4/7] refreshing legacy COT bundle if stale…")
    try:
        from hptl.dashboard.weekly_refresh import refresh_legacy_cot_if_stale

        legacy_week = refresh_legacy_cot_if_stale()
        print(f"      legacy_cot_latest.json week: {legacy_week}")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"legacy COT refresh failed: {exc}")

    # --- 5. INCREMENTAL confluence publish (NOT full history rebuild) --------
    print("[5/7] publishing confluence (incremental — new weeks only)…")
    confluence_after = "—"
    try:
        from hptl.confluence.export_from_masters import catch_up_confluence_export

        catch = catch_up_confluence_export(
            cot_feed_meta={"latest_cftc_report_date": local_after, "cot_data_stale": False},
        )
        confluence_after = catch.confluence_after
        if catch.export_path:
            files_written.append(catch.export_path)
        print(
            f"      confluence {catch.confluence_before} -> {catch.confluence_after} "
            f"(weeks built: {', '.join(catch.weeks_built) or 'none'}; records {catch.records_exported})"
        )
        if catch.error:
            errors.append(f"confluence catch-up: {catch.error}")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"confluence catch-up failed: {exc}")

    # --- 6. Positioning chart series -----------------------------------------
    print("[6/7] rebuilding cot_3y_series_latest.json (chart source)…")
    try:
        from hptl.cot.cot_3y_series_export import run as run_cot_3y

        cot3_path = run_cot_3y()
        files_written.append(str(cot3_path))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"cot_3y series export failed: {exc}")

    # --- 7. Final public -> dist publish -------------------------------------
    print("[7/7] syncing public -> dist…")
    try:
        from hptl.confluence.dashboard_export import sync_dist_exports

        sync_dist_exports()
    except Exception as exc:  # noqa: BLE001
        errors.append(f"dist sync failed: {exc}")

    advanced = local_after != "—" and local_after > local_before
    files = [f for f in files_written if f and f != "None"]

    print("")
    print("| COT-only refresh summary | Value |")
    print("| --- | --- |")
    print(f"| local COT week (before) | {local_before} |")
    print(f"| CFTC week downloaded / processed | {local_after} |")
    print(f"| dashboard confluence week (after) | {confluence_after} |")
    print(f"| week advanced | {'yes' if advanced else 'no'} |")
    print(f"| files written | {len(files)} |")
    for path in files:
        print(f"|   -> {path} |")
    if errors:
        print(f"| errors | {len(errors)} |")
        for err in errors:
            print(f"|   - {err} |")
    status = "PASS" if not errors else "FAIL"
    print(f"| final status | {status} |")
    print("=" * 72)

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
