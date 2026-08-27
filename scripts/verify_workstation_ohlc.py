"""Verify workstation OHLC/COT alignment for key instruments."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OHLC_PATH = ROOT / "data" / "processed" / "workstation_ohlc_latest.json"
INSTRUMENTS = ["Sugar", "Gold", "Australian Dollar / 6A", "NASDAQ / NQ"]


def iso_week(date_str: str) -> str:
    return pd.Timestamp(str(date_str)[:10]).strftime("%G-W%V")


def main() -> int:
    ohlc_doc = json.loads(OHLC_PATH.read_text(encoding="utf-8"))
    cur_week = pd.Timestamp.now("UTC").normalize().strftime("%G-W%V")
    print(f"Current ISO week: {cur_week}\n")

    failures = 0
    for name in INSTRUMENTS:
        block = ohlc_doc["instruments"].get(name, {})
        audit = block.get("tail_alignment_audit") or {}
        matched = audit.get("final_12_matched") or []
        cot_last = str(block.get("cot_last_date") or "")[:10]
        ohlc_last = block.get("ohlc_last_date")
        issues: list[str] = []

        partial = [
            b
            for b in block.get("weekly_ohlc", [])
            if iso_week(b["date"]) >= cur_week
        ]
        if partial:
            issues.append(f"partial_week_in_export({len(partial)})")

        after_cot = [b for b in block.get("weekly_ohlc", []) if b["date"] > cot_last]
        if after_cot:
            issues.append(f"ohlc_after_cot({len(after_cot)})")

        prev_ohlc = None
        for m in matched:
            if m.get("matched"):
                if m.get("ohlc_date") == prev_ohlc and prev_ohlc is not None:
                    issues.append(f"stale_reuse@{m.get('cot_date')}")
                prev_ohlc = m.get("ohlc_date")

        print("=" * 60)
        print(name)
        print(f"  cot_last={cot_last} ohlc_last={ohlc_last} rejected={block.get('rejected_ohlc_rows', 0)}")
        print("  last 5 matched:")
        for m in matched[-5:]:
            print(
                f"    {m['cot_date']} -> ohlc={m.get('ohlc_date')} "
                f"matched={m['matched']} close={m.get('close')}"
            )
        if issues:
            print(f"  RESULT: FAIL — {', '.join(issues)}")
            failures += 1
        else:
            print("  RESULT: PASS")

    print()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
