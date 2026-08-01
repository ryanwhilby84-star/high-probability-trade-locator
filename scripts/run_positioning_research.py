"""CLI: run COT positioning research export (full cot3y universe by default)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.positioning_research_export import main

if __name__ == "__main__":
    raise SystemExit(main())
