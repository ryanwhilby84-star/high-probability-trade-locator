#!/usr/bin/env python3
"""Export seasonality_workstation_latest.json for platform consumers."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.export import PUBLIC, run_seasonality_workstation_export  # noqa: E402


def main() -> int:
    payload = run_seasonality_workstation_export()
    s = payload.get("summary") or {}
    print(
        f"Seasonality Workstation export — ok={s.get('markets_ok')} "
        f"fail={s.get('markets_fail')} total={s.get('markets_total')}"
    )
    print(f"wrote {PUBLIC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
