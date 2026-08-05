"""CLI: Gold V5 — Supply/Demand Market-Clearing Engine (research only).

Usage:
  python scripts/run_gold_market_clearing_valuation.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_market_clearing_valuation import (  # noqa: E402
    run_gold_market_clearing_valuation,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold V5 market-clearing valuation (research only)."
    )
    parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_market_clearing_valuation()
    if not payload.get("ok"):
        print(f"ERROR: {payload.get('error')}")
        return 1
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold V5 — Market Clearing (research only)")
    print(f"  runtime_sec={elapsed}")
    panel = payload.get("panel") or {}
    print(
        f"  quarters={panel.get('n_quarters')} "
        f"{panel.get('start')}..{panel.get('end')}"
    )
    print(f"  best_stage={payload.get('best_stage')}")
    v = payload.get("verdict") or {}
    print(f"  verdict={v.get('verdict')}")
    tip = payload.get("tip") or {}
    if tip:
        print("  Tip card:")
        for k in [
            "jewellery_or_fabrication",
            "technology",
            "bar_coin",
            "etf_investment",
            "investment_aggregate",
            "central_bank",
            "mine_supply",
            "recycling_supply",
            "net_imbalance_tonnes",
            "implied_dlog_price",
            "fair_value",
            "market_price",
        ]:
            if tip.get(k) is not None:
                print(f"    {k}: {tip.get(k)}")
        print(f"    {tip.get('premium_discount')}: {tip.get('deviation_pct')}%")
        print(f"    Bucket: {tip.get('bucket')}")
    print(f"  spread13={(payload.get('spread_13w') or {}).get('spread_pp')}")
    print(f"  leakage={payload.get('price_identity_leakage')}")
    narrative = (v.get("narrative") or "").encode("ascii", "replace").decode("ascii")
    print(f"  narrative={narrative}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
