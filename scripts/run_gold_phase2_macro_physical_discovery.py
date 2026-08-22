"""CLI: Gold Valuation Phase 2 — Macro + Physical Discovery (research only).

Usage:
  python scripts/run_gold_phase2_macro_physical_discovery.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_phase2_macro_physical_discovery import (  # noqa: E402
    run_gold_phase2_discovery,
    write_phase2_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold Phase 2 transform-aware macro+physical discovery (research only)."
    )
    parser.add_argument("--as-of-week", default=None)
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_phase2_discovery(as_of_week=args.as_of_week)
    paths = write_phase2_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Phase 2 Macro+Physical Discovery (research only)")
    print(f"  ok={payload.get('ok')} runtime_sec={elapsed}")
    print(f"  economic_status={payload.get('economic_status')}")
    print(f"  suitable_for_v2={payload.get('suitable_for_gold_v2')}")
    print(f"  retained={payload.get('variables_retained')}")
    print(f"  equation={payload.get('final_equation')}")
    best = payload.get("best_combined_model") or {}
    pub = payload.get("published_on_phase2_sample") or {}
    print(
        f"  published: RMSE={pub.get('oos_rmse')} MAE={pub.get('oos_mae')} R2={pub.get('oos_r2')}"
    )
    print(
        f"  phase2: RMSE={best.get('oos_rmse')} MAE={best.get('oos_mae')} R2={best.get('oos_r2')}"
    )
    for row in payload.get("driver_ranking") or []:
        print(
            f"  {row.get('driver')}: {row.get('recommendation')} "
            f"sign={row.get('sign')} stable={row.get('stable')} "
            f"indep={row.get('independent')} tf={row.get('best_transform')}"
        )
    print(f"  {payload.get('plain_english')}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    print(f"  runtime_sec={elapsed}")
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
