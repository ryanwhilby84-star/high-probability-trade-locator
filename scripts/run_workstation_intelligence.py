"""Precompute COT Workstation Intelligence V2.

Usage:
    python scripts/run_workstation_intelligence.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from hptl.cot.workstation_intelligence_export import main

if __name__ == "__main__":
    raise SystemExit(main())
