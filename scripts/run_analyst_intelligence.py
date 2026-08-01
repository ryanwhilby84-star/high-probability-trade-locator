#!/usr/bin/env python3
"""Build cot_analyst_intelligence_latest.json from the trajectory reasoning engine."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.analyst_intelligence_export import main

if __name__ == "__main__":
    raise SystemExit(main())
