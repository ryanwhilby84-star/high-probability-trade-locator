"""CLI: Gold Macro Fair Value V2 (research only).

Usage:
  python scripts/run_gold_macro_fair_value.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_macro_fair_value import (  # noqa: E402
    run_gold_macro_fair_value,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gold Macro Fair Value V2 (research only).")
    parser.add_argument("--start", default="2003-01-01")
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_macro_fair_value(start=args.start)
    if not payload.get("ok"):
        print(f"ERROR: {payload.get('error')}")
        return 1
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold Macro Fair Value V2 (research only)")
    print(f"  runtime_sec={elapsed}")
    panel = payload.get("panel") or {}
    print(
        f"  aligned={panel.get('aligned_n')} "
        f"{panel.get('aligned_start')}..{panel.get('aligned_end')}"
    )
    v = payload.get("verdict") or {}
    print(f"  verdict={v.get('verdict')}")
    tip = payload.get("tip") or {}
    if tip:
        print("  Tip card:")
        for label, usd in (tip.get("drivers_usd") or {}).items():
            print(f"    {label}: {usd}")
        print(f"    Net Effect: {tip.get('net_macro_effect_usd')}")
        print(f"    Fair Value: {tip.get('fair_value')}")
        print(f"    Price: {tip.get('market_price')}")
        print(f"    {tip.get('premium_discount')}: {tip.get('deviation_pct')}%")
        print(f"    Bucket: {tip.get('bucket')}")
    print(f"  spread13={(payload.get('spread_13w') or {}).get('spread_pp')}")
    narrative = (v.get("narrative") or "").encode("ascii", "replace").decode("ascii")
    print(f"  narrative={narrative}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
