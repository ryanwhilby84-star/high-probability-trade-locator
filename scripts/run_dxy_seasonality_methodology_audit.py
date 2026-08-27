#!/usr/bin/env python3
"""Run DXY seasonality methodology audit (research only — no production UI changes)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.dxy_methodology_audit import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
