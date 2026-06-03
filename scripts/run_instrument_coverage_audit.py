#!/usr/bin/env python3
"""Run instrument coverage audit from confluence export or live records."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hptl.markets.coverage_audit import run_coverage_audit, write_coverage_audit


def main() -> None:
    parser = argparse.ArgumentParser(description="HPTL instrument coverage audit")
    parser.add_argument(
        "--confluence",
        type=Path,
        default=Path("web-dashboard/public/data/confluence_history_latest.json"),
        help="Path to confluence JSON (uses latest calendar week slice)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/instrument_coverage_audit.json"),
        help="Output JSON path",
    )
    parser.add_argument("--print-table", action="store_true", help="Print summary table to stdout")
    args = parser.parse_args()

    records: list = []
    latest_week = ""
    if args.confluence.exists():
        doc = json.loads(args.confluence.read_text(encoding="utf-8"))
        records = doc.get("records") or []
        dates = sorted({str(r.get("date") or "") for r in records if r.get("date")})
        latest_week = dates[-1] if dates else ""

    audit = run_coverage_audit(records, latest_calendar_week=latest_week)
    path = write_coverage_audit(audit, path=args.out)

    if args.print_table:
        print(f"Audit week: {latest_week}")
        print(f"Written: {path}")
        s = audit["summary"]
        print(
            f"total={s['total']} complete={s['complete']} macro_only={s['macro_only']} "
            f"proxy={s['proxy_required']} broken={s['broken_mapping']} no_data={s['no_data']}"
        )
        print("\nBroken / mapping issues:")
        for row in audit["instruments"]:
            if row["data_status"] in {"broken_mapping", "cot_mapping_missing", "no_data"}:
                print(f"  {row['instrument_id']:28} {row['data_status']:20} {row.get('expected_cot_note') or ''}")
        print("\nPriority board:")
        dbg = audit.get("priority_markets_debug") or {}
        for m in dbg.get("priority_markets") or []:
            print(f"  * {m.get('market')} ({m.get('priority_tier')}) score={m.get('priority_score')}")
    else:
        print(path)


if __name__ == "__main__":
    main()
