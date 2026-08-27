#!/usr/bin/env python3
"""Export point-in-time institutional valuation history for UI workstation overlay."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.instrument_valuation_history_viz_export import main

if __name__ == "__main__":
    raise SystemExit(main())
