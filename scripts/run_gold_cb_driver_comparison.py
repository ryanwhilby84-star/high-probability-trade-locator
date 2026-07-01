#!/usr/bin/env python3
"""Compare monthly CB level vs rolling 12-month for Gold production driver spec."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_cb_driver_comparison import run_cb_driver_comparison, write_comparison_artifacts


def main() -> int:
    print("=== Gold CB driver comparison: monthly level vs rolling 12m ===\n")
    report = run_cb_driver_comparison()
    if report.get("status") != "ok":
        print(f"ERROR: {report.get('error')}")
        return 1

    json_path, md_path = write_comparison_artifacts(report)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}\n")

    rec = report.get("recommendation") or {}
    level = report.get("monthly_level", {}).get("full_sample", {})
    roll12 = report.get("rolling_12m", {}).get("full_sample", {})
    lcb = level.get("cb_coefficient") or {}
    rcb = roll12.get("cb_coefficient") or {}

    print("Full sample:")
    print(f"  Monthly level  adj R2={level.get('adj_r_squared')}  CB beta={lcb.get('beta')}  p={lcb.get('p_value')}  sign={'OK' if lcb.get('sign_passed') else 'FAIL'}")
    print(f"  Rolling 12m    adj R2={roll12.get('adj_r_squared')}  CB beta={rcb.get('beta')}  p={rcb.get('p_value')}  sign={'OK' if rcb.get('sign_passed') else 'FAIL'}")

    for label, spec in [("Monthly level", report["monthly_level"]), ("Rolling 12m", report["rolling_12m"])]:
        wf = spec.get("walk_forward_oos") or {}
        ho = spec.get("holdout_oos") or {}
        rc = spec.get("rolling_cb_coefficient") or {}
        print(f"\n{label}:")
        print(f"  Holdout OOS RMSE(log)={(ho.get('oos_metrics') or {}).get('rmse_log')}")
        print(f"  Walk-forward OOS RMSE(log)={(wf.get('oos_metrics') or {}).get('rmse_log')}")
        print(f"  Rolling CB beta: {rc.get('pct_positive')}% positive, std={rc.get('beta_std')}")

    print(f"\nRECOMMENDATION: {rec.get('recommended_production_cb_driver')}")
    print(f"  {rec.get('rationale')}")
    print(f"\n  Note: {rec.get('note')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
