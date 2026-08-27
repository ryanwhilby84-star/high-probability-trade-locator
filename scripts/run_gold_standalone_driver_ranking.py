"""CLI: Gold standalone driver ranking gate (research only).

Ranks Tier-1 candidates one-at-a-time before any combination testing.

Usage:
  python scripts/run_gold_standalone_driver_ranking.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_standalone_driver_ranking import (  # noqa: E402
    run_gold_standalone_driver_ranking,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold Tier-1 standalone ranking (rank before combine)."
    )
    parser.add_argument("--as-of-week", default=None)
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_standalone_driver_ranking(as_of_week=args.as_of_week)
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Standalone Driver Ranking (research only)")
    print(f"  ok={payload.get('ok')} runtime_sec={elapsed}")
    if not payload.get("ok"):
        print(f"  error={payload.get('error')}")
        return 1

    print("  Ranking table:")
    for row in payload.get("ranking_table") or []:
        print(
            f"    #{row.get('rank')} {row.get('label')}: "
            f"score={row.get('standalone_score')} keep={row.get('keep')} "
            f"OOS_R2={row.get('oos_r2')} sign_ok={row.get('signs_ok')} "
            f"flip={row.get('coef_sign_flip')}"
        )
    print(f"  winners={payload.get('winners')}")
    print(f"  maybe={payload.get('maybe')}")
    print(f"  rejected={payload.get('rejected')}")
    best = payload.get("best_combination") or {}
    print(f"  best_combo={best.get('features')} signs_ok={best.get('signs_ok')} "
          f"oos_r2={best.get('oos_r2')}")
    rec = payload.get("recommendation") or {}
    print(f"  status={rec.get('status')}")
    print(f"  narrative={rec.get('narrative')}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
