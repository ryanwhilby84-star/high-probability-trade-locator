#!/usr/bin/env python3
"""Stage breakeven inflation (T10YIE) as additive Gold research driver."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.util.console_safe import configure_stdout_utf8, safe_print as print

configure_stdout_utf8()

from hptl.valuation.gold_breakeven_inflation_research import (
    run_gold_breakeven_inflation_research,
    write_research_artifacts,
)


def main() -> int:
    print("=== Gold breakeven inflation (T10YIE) research ===\n")
    report = run_gold_breakeven_inflation_research()
    if report.get("status") != "ok":
        print(f"ERROR: {report.get('error')}")
        return 1

    json_path, md_path, diag_path = write_research_artifacts(report)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    if diag_path:
        print(f"Wrote {diag_path}")
    print()

    checks = report.get("checks") or {}
    prod = report.get("production_baseline") or {}
    res = report.get("research_model") or {}
    ry_p = prod.get("real_yield_coefficient") or {}
    ry_r = res.get("real_yield_coefficient") or {}
    be_r = res.get("breakeven_coefficient") or {}

    print("Production baseline (4-feature):")
    print(
        f"  adj R2={prod.get('adj_r_squared')}  real_yield beta={ry_p.get('beta')}  "
        f"sign={'OK' if ry_p.get('sign_passed') else 'FAIL'}  publish={prod.get('publish_decision')}"
    )

    print("\nResearch model (+ breakeven_10y):")
    print(f"  adj R2={res.get('adj_r_squared')}  publish={res.get('publish_decision')}")
    print(
        f"  real_yield beta={ry_r.get('beta')}  p={ry_r.get('p_value')}  "
        f"sign={'OK' if ry_r.get('sign_passed') else 'FAIL'}"
    )
    print(
        f"  breakeven_10y beta={be_r.get('beta')}  p={be_r.get('p_value')}  "
        f"sign={'OK' if be_r.get('sign_passed') else 'FAIL'}"
    )

    print("\nGate checklist:")
    print(f"  real_yield sign restored: {'YES' if checks.get('real_yield_sign_restored') else 'NO'}")
    print(f"  breakeven positive & significant: {'YES' if checks.get('breakeven_statistically_significant') and checks.get('breakeven_sign_positive') else 'NO'}")
    print(f"  all sign gates pass: {'YES' if checks.get('all_sign_gates_pass') else 'NO'}")
    print(f"  Gold publishable: {'YES' if checks.get('gold_publishable') else 'NO'}")

    verdict = report.get("verdict") or {}
    print(f"\nDECISION: {verdict.get('decision')}")
    print(f"  {verdict.get('summary')}")
    for b in res.get("blockers") or []:
        print(f"  blocker: {b}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
