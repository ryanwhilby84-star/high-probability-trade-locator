"""Phase 3A-PUBLISH — regenerate valuation_latest.json from live engines."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.export import write_valuation_exports

if __name__ == "__main__":
    paths = write_valuation_exports()
    print(json.dumps({k: str(v) for k, v in paths.items()}, indent=2))
