"""CLI: Natural Gas Valuation — Macro Validation Phase 5 (US 10Y Real Yield).

Research-only. Does not modify published fair value or weekly COT.

Usage:
  python scripts/run_ng_driver_validation_phase5_real_yield.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.ng_driver_validation_phase5_real_yield import (  # noqa: E402
    run_phase5_real_yield_validation,
    write_phase5_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 5: prove whether US 10Y Real Yield deserves promotion from "
            "Experimental to Validated (vs Storage+Production YoY v2)."
        )
    )
    parser.add_argument("--as-of-week", default=None)
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_phase5_real_yield_validation(as_of_week=args.as_of_week)
    paths = write_phase5_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("NG Macro Validation Phase 5 (US 10Y Real Yield)")
    print(f"  ok={payload.get('ok')} runtime_sec={elapsed}")
    if not payload.get("ok"):
        print(f"  error={payload.get('error')}")
        return 1

    ds = payload.get("real_yield_dataset") or {}
    print(
        f"  dataset: {ds.get('symbol')} n={(ds.get('history_available') or {}).get('n_observations')} "
        f"tip={ds.get('current_observation_date')} value={ds.get('latest_value')}"
    )
    a = payload.get("storage_only_model") or {}
    b = payload.get("v2_storage_production_yoy_model") or {}
    print(
        f"  storage_only: OOS_RMSE={a.get('oos_rmse')} OOS_MAE={a.get('oos_mae')} OOS_R2={a.get('oos_r2')}"
    )
    print(
        f"  v2_prod_yoy: OOS_RMSE={b.get('oos_rmse')} OOS_MAE={b.get('oos_mae')} OOS_R2={b.get('oos_r2')}"
    )
    for c in payload.get("candidates") or []:
        if not c.get("ok"):
            print(f"  {c.get('transform_id')}: FAIL ({c.get('reason')})")
            continue
        d = c.get("decision") or {}
        sp = c.get("candidate_storage_prod_yoy_real_yield") or {}
        print(
            f"  {c.get('transform_id')}: decision={d.get('recommendation')} "
            f"ΔRMSE%_vs_v2={d.get('oos_rmse_improvement_pct_vs_v2')} "
            f"OOS_RMSE={sp.get('oos_rmse')} signs_ok={sp.get('signs_ok')}"
        )
    print(f"  REAL_YIELD_RECOMMENDATION: {payload.get('real_yield_recommendation')}")
    print(f"  {payload.get('plain_english')}")
    print(
        f"  published_model={payload.get('published_model_id')} "
        f"unchanged={payload.get('published_model_unchanged')}"
    )
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    print(f"  runtime_sec={elapsed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
