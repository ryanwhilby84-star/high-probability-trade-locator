#!/usr/bin/env python3
"""Run Natural Gas Institutional Valuation V1 export."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.energy_ng_valuation_export import main

if __name__ == "__main__":
    raise SystemExit(main())
