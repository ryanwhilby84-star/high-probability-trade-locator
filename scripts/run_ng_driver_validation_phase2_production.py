"""CLI: Natural Gas Valuation — Driver Validation Phase 2 (Production).

Research-only. Does not modify published fair value or weekly COT workflow.

Usage (repo root):
  python scripts/run_ng_driver_validation_phase2_production.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.ng_driver_validation_phase2_production import (  # noqa: E402
    run_phase2_production_validation,
    write_phase2_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 2: prove whether US Dry Gas Production deserves promotion "
            "from Experimental to Validated (Storage-only vs Storage+Production)."
        )
    )
    parser.add_argument(
        "--as-of-week",
        default=None,
        help="Optional ISO date cutoff for the weekly panel (YYYY-MM-DD).",
    )
    args = parser.parse_args(argv)

    payload = run_phase2_production_validation(as_of_week=args.as_of_week)
    paths = write_phase2_outputs(payload)

    print("NG Driver Validation Phase 2 (Production)")
    print(f"  ok={payload.get('ok')}")
    if not payload.get("ok"):
        print(f"  error={payload.get('error')}")
        for label, path in paths.items():
            print(f"  wrote {label}: {path}")
        return 1

    math_doc = payload.get("current_valuation_math") or {}
    print(f"  fair_value_equation: {math_doc.get('fair_value_equation')}")
    print(f"  validated: {math_doc.get('validated_drivers')}")
    storage = payload.get("storage_only_model") or {}
    print(
        f"  storage_only: OOS_RMSE={storage.get('oos_rmse')} "
        f"OOS_MAE={storage.get('oos_mae')} OOS_R2={storage.get('oos_r2')}"
    )
    for c in payload.get("candidates") or []:
        if not c.get("ok"):
            print(f"  {c.get('transform_id')}: FAIL ({c.get('reason')})")
            continue
        d = c.get("decision") or {}
        sp = c.get("storage_plus_production") or {}
        print(
            f"  {c.get('transform_id')}: decision={d.get('recommendation')} "
            f"ΔRMSE%={d.get('oos_rmse_improvement_pct_vs_storage')} "
            f"OOS_RMSE={sp.get('oos_rmse')} signs_ok={sp.get('signs_ok')} "
            f"leaky={c.get('leaky')}"
        )
    print(f"  PRODUCTION_RECOMMENDATION: {payload.get('production_recommendation')}")
    print(f"  {payload.get('plain_english')}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
