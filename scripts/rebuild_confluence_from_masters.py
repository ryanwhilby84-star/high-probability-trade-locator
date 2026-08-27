#!/usr/bin/env python3
"""Fast confluence catch-up: audit local drivers → valuation → incremental confluence export."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")


def main() -> int:
    from hptl.confluence.export_from_masters import print_summary, run_master_rebuild

    result = run_master_rebuild()
    print_summary(result)
    return 1 if result.error else 0


if __name__ == "__main__":
    raise SystemExit(main())
