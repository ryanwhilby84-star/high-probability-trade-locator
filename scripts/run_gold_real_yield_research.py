#!/usr/bin/env python3
"""Research real_yield specifications for Gold production model."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.util.console_safe import configure_stdout_utf8, safe_print as print

configure_stdout_utf8()

from hptl.valuation.gold_real_yield_research import (
    run_gold_real_yield_research,
    write_research_artifacts,
)


def main() -> int:
    print("=== Gold real_yield specification research ===\n")
    report = run_gold_real_yield_research()
    if report.get("status") != "ok":
        print(f"ERROR: {report.get('error')}")
        return 1

    json_path, md_path = write_research_artifacts(report)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}\n")

    baseline = next(
        (v for v in report.get("variants", []) if v["variant_id"] == "baseline_dfii10_level"),
        None,
    )
    if baseline:
        ry = baseline.get("real_yield_coefficient") or {}
        print(
            f"Baseline DFII10: adj R2={baseline.get('adj_r_squared')}  "
            f"RY beta={ry.get('beta')}  p={ry.get('p_value')}  "
            f"sign={'OK' if ry.get('sign_passed') else 'FAIL'}"
        )

    print(f"\nVariants fitted: {report.get('variants_fitted')}")
    print(f"Real-yield sign pass: {report.get('variants_real_yield_sign_pass')}")
    print(f"Full publish pass: {report.get('variants_full_publish_pass')}")

    rec = report.get("recommendation") or {}
    print(f"\nDECISION: {rec.get('decision')}")
    print(f"  {rec.get('rationale')}")
    for note in rec.get("notes") or []:
        print(f"  - {note}")

    mc = report.get("multicollinearity_diagnostic") or {}
    print(f"\nMulticollinearity: partial corr = {mc.get('partial_corr_real_yield_log_price')}")
    for row in mc.get("vif") or []:
        print(f"  VIF {row['feature']}: {row['vif']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
