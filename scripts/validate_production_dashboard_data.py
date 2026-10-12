#!/usr/bin/env python3
"""Fail closed when essential static dashboard data is missing or malformed."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "web-dashboard" / "public" / "data"
REQUIRED = {
    "confluence_history_latest.json": ("records",),
    "cot_weekly_inspector_latest.json": ("markets",),
    "cot_commercial_attention_latest.json": ("attention_board",),
    "instrument_registry.json": (),
}

def main() -> int:
    failures = []
    for name, keys in REQUIRED.items():
        path = DATA / name
        if not path.is_file():
            failures.append(f"{name}: missing")
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("expected JSON object")
            if name == "confluence_history_latest.json":
                if not payload.get("latest_cot_report_date"):
                    failures.append(f"{name}: latest_cot_report_date missing")
            for key in keys:
                value = payload.get(key)
                if not isinstance(value, (list, dict)) or not value:
                    failures.append(f"{name}: {key} is absent or empty")
        except (ValueError, OSError) as exc:
            failures.append(f"{name}: {exc}")
    if failures:
        print("PRODUCTION DATA GATE FAILED:")
        for failure in failures:
            print(" -", failure)
        return 1
    print("Production data gate passed.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
