#!/usr/bin/env python3
"""Run Seasonality Workstation V1 multi-instrument engine audit."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.export import (  # noqa: E402
    AUDIT_MD,
    run_seasonality_workstation_audit,
)


def main() -> int:
    report = run_seasonality_workstation_audit()
    s = report.get("summary") or {}
    print(f"Seasonality Workstation audit — ok={s.get('ok')} fail={s.get('fail')} total={s.get('total')}")
    print(f"wrote {AUDIT_MD}")
    return 0 if s.get("fail", 1) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
