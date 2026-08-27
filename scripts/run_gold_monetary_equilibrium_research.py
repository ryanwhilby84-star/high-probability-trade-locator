"""CLI: Gold monetary equilibrium + ECM research (research only).

Usage:
  python scripts/run_gold_monetary_equilibrium_research.py
  python scripts/run_gold_monetary_equilibrium_research.py --start 1975-01-01
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_monetary_equilibrium_research import (  # noqa: E402
    run_gold_monetary_equilibrium_research,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold monetary equilibrium + ECM research (research only)."
    )
    parser.add_argument("--start", default="1975-01-01")
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_monetary_equilibrium_research(
        start=args.start, force_refresh=args.force_refresh
    )
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Monetary Equilibrium Research (research only)")
    print(f"  ok={payload.get('ok')} runtime_sec={elapsed}")
    if not payload.get("ok"):
        print(f"  error={payload.get('error')}")
        return 1

    panel = payload.get("panel") or {}
    print(f"  panel={panel.get('n_weeks')} weeks {panel.get('start')}..{panel.get('end')}")
    print(f"  sources={panel.get('source_counts')}")
    verdict = payload.get("verdict") or {}
    print(f"  verdict={verdict.get('verdict')}")
    print(f"  strongest={verdict.get('strongest_candidate')}")
    print("  Ranking:")
    for row in payload.get("ranking") or []:
        if row.get("is_baseline"):
            continue
        sp = (row.get("valuation_spread_13w") or {}).get("spread_pp")
        ecm = row.get("ecm_expanding") or {}
        print(
            f"    #{row.get('rank')} {row.get('id')}: class={row.get('classification')} "
            f"price={row.get('price_model_score')} val={row.get('valuation_score')} "
            f"OOS_R2={row.get('oos_r2')} spread13={sp} "
            f"lambda={ecm.get('lambda_mean')} flip={row.get('coef_sign_flip')}"
        )
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
