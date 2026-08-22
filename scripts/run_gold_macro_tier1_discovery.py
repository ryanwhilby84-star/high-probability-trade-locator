"""CLI: Gold Valuation — Tier 1 Macro Discovery (research only).

Does not modify published Gold valuation or any pipeline.

Usage:
  python scripts/run_gold_macro_tier1_discovery.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_macro_tier1_discovery import (  # noqa: E402
    run_gold_macro_tier1_discovery,
    write_tier1_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold Tier-1 macro discovery research (walk-forward, VIF, DM)."
    )
    parser.add_argument("--as-of-week", default=None)
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_macro_tier1_discovery(as_of_week=args.as_of_week)
    paths = write_tier1_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Macro Tier 1 Discovery (research only)")
    print(f"  ok={payload.get('ok')} runtime_sec={elapsed}")
    if not payload.get("ok"):
        print(f"  error={payload.get('error')}")
        return 1

    dollar = payload.get("dollar_selection") or {}
    print(f"  dollar: {dollar.get('decision')}")
    pub = payload.get("published_on_tier1_sample") or {}
    full = payload.get("kitchen_sink_model") or payload.get("full_combined_model") or {}
    best = payload.get("best_combined_model") or {}
    print(f"  economic_status: {payload.get('economic_status')}")
    print(
        f"  published: OOS_RMSE={pub.get('oos_rmse')} MAE={pub.get('oos_mae')} R2={pub.get('oos_r2')}"
    )
    print(
        f"  kitchen_sink: OOS_RMSE={full.get('oos_rmse')} MAE={full.get('oos_mae')} R2={full.get('oos_r2')}"
    )
    print(
        f"  economics_constrained: OOS_RMSE={best.get('oos_rmse')} MAE={best.get('oos_mae')} R2={best.get('oos_r2')}"
    )
    print(f"  retained_status_quo: {payload.get('variables_retained')}")
    print(f"  rejected: {payload.get('variables_rejected')}")
    print(f"  kitchen_eq: {payload.get('kitchen_sink_equation')}")
    print(f"  status_quo_eq: {payload.get('final_equation')}")
    dm = payload.get("diebold_mariano_vs_published") or {}
    print(
        f"  DM_kitchen_vs_published: p={dm.get('p_value_one_sided')} "
        f"mean_diff={dm.get('mean_loss_diff')}"
    )
    for row in payload.get("ranked_contribution_table") or []:
        print(
            f"  {row.get('feature')}: {row.get('recommendation')} "
            f"coef={row.get('coefficient')} p={row.get('p_value')} "
            f"VIF={row.get('vif')} OOS%={row.get('oos_contribution_rmse_pct')}"
        )
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    print(f"  runtime_sec={elapsed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
