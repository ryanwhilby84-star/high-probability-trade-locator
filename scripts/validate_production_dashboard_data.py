#!/usr/bin/env python3
"""Fail closed when essential production dashboard data is missing or malformed."""
from pathlib import Path
import sys

from package_dashboard_data_release import validate

ROOT = Path(__file__).resolve().parents[1]

def main() -> int:
    errors, files = validate(ROOT / "web-dashboard" / "public" / "data", max_cot_age_days=21)
    if errors:
        print("PRODUCTION DATA GATE FAILED:")
        for error in errors:
            print(" -", error)
        return 1
    print(f"Production data gate passed ({len(files)} essential files).")
    return 0

if __name__ == "__main__":
    sys.exit(main())
