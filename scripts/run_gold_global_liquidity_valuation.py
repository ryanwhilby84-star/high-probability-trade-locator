"""CLI: Gold Valuation V3 — Global Liquidity & Real-Yield Fair Value (research only).

Usage:
  python scripts/run_gold_global_liquidity_valuation.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_global_liquidity_valuation import (  # noqa: E402
    run_gold_global_liquidity_valuation,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold Valuation V3 — Global Liquidity & Real Yield (research only)."
    )
    parser.add_argument("--start", default="2000-01-01")
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_global_liquidity_valuation(start=args.start)
    if not payload.get("ok"):
        print(f"ERROR: {payload.get('error')}")
        return 1
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Valuation V3 — Global Liquidity (research only)")
    print(f"  runtime_sec={elapsed}")
    panel = payload.get("panel") or {}
    print(
        f"  core={panel.get('n_core')} "
        f"{panel.get('core_start')}..{panel.get('core_end')}"
    )
    print(f"  best={payload.get('best_model_id')}")
    v = payload.get("verdict") or {}
    print(f"  verdict={v.get('verdict')}")
    tip = payload.get("tip") or {}
    if tip:
        print("  Tip card:")
        for label, usd in (tip.get("drivers_usd") or {}).items():
            print(f"    {label}: {usd}")
        print(f"    Net: {tip.get('net_contribution_usd')}")
        print(f"    Fair Value: {tip.get('fair_value')}")
        print(f"    Price: {tip.get('market_price')}")
        print(f"    {tip.get('premium_discount')}: {tip.get('deviation_pct')}%")
        print(f"    Bucket: {tip.get('bucket')}")
    print(f"  spread13={(payload.get('spread_13w') or {}).get('spread_pp')}")
    print(f"  spread52={(payload.get('spread_52w') or {}).get('spread_pp')}")
    print(f"  spread104={(payload.get('spread_104w') or {}).get('spread_pp')}")
    narrative = (v.get("narrative") or "").encode("ascii", "replace").decode("ascii")
    print(f"  narrative={narrative}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
