"""Build Natural Gas Valuation Workstation historical artifacts (research only).

Usage:
  python scripts/build_ng_valuation_workstation.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.ng_valuation_workstation_history import (  # noqa: E402
    run_ng_valuation_workstation_build,
)


def main() -> int:
    t0 = time.perf_counter()
    payload = run_ng_valuation_workstation_build(write=True)
    elapsed = round(time.perf_counter() - t0, 2)
    cov = payload.get("coverage") or {}
    verd = payload.get("verdict") or {}
    print("NG Valuation Workstation build")
    print(f"  coverage: {cov.get('first_week')} → {cov.get('last_week')} n={cov.get('n_weeks')}")
    print(f"  walkforward_fv: {cov.get('n_walkforward_fair_values')}")
    print(f"  frozen_fv: {cov.get('n_frozen_fair_values')}")
    print(f"  verdict: {verd.get('verdict')}")
    print(f"  {verd.get('reason')}")
    print(f"  runtime_sec={elapsed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
