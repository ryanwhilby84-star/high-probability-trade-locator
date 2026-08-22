"""CLI: Gold structural / Keynesian valuation research (research only).

Reuses the Natural Gas research workflow for Gold equilibrium models.
Does not modify published valuation engines or weaken the standalone Tier-1 gate.

Usage:
  python scripts/run_gold_structural_valuation_research.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_structural_valuation_research import (  # noqa: E402
    run_gold_structural_valuation_research,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold structural / Keynesian valuation research (research only)."
    )
    parser.add_argument("--as-of-week", default=None)
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_structural_valuation_research(as_of_week=args.as_of_week)
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Structural Valuation Research (research only)")
    print(f"  ok={payload.get('ok')} runtime_sec={elapsed}")
    if not payload.get("ok"):
        print(f"  error={payload.get('error')}")
        return 1

    verdict = payload.get("verdict") or {}
    print(f"  verdict={verdict.get('verdict')}")
    print(f"  strongest={verdict.get('strongest_candidate')}")
    print("  Ranking:")
    for row in payload.get("ranking") or []:
        u = row.get("valuation_usefulness") or {}
        print(
            f"    #{row.get('rank')} {row.get('id')}: score={row.get('structural_score')} "
            f"decision={row.get('decision')} OOS_R2={row.get('oos_r2')} "
            f"spread13={u.get('spread_pp')} signs_ok={row.get('signs_ok')} "
            f"flip={row.get('coef_sign_flip')}"
        )
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
