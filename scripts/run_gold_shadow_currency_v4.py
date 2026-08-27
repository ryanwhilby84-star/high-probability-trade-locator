"""CLI: Gold V4 — Fixed-Form Shadow Currency Engine (research only).

Usage:
  python scripts/run_gold_shadow_currency_v4.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_shadow_currency_v4 import (  # noqa: E402
    run_gold_shadow_currency_v4,
    write_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gold V4 Fixed-Form Shadow Currency Engine (research only)."
    )
    parser.add_argument("--start", default="2000-01-01")
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    payload = run_gold_shadow_currency_v4(start=args.start)
    if not payload.get("ok"):
        print(f"ERROR: {payload.get('error')}")
        return 1
    paths = write_outputs(payload)
    elapsed = round(time.perf_counter() - t0, 2)

    print("Gold V4 — Shadow Currency (research only)")
    print(f"  runtime_sec={elapsed}")
    panel = payload.get("panel") or {}
    print(f"  panel={panel.get('n')} {panel.get('start')}..{panel.get('end')}")
    params = payload.get("parameters") or {}
    print(f"  k={params.get('current_k')}  beta={params.get('current_beta')}")
    v = payload.get("verdict") or {}
    print(f"  verdict={v.get('verdict')}")
    tip = payload.get("tip") or {}
    if tip:
        print("  Tip card:")
        print(f"    monetary_value/oz={tip.get('monetary_value_per_ounce')}")
        print(f"    yield_factor={tip.get('yield_factor')}")
        print(f"    dxy_factor={tip.get('dxy_factor')}")
        print(f"    Fair Value: {tip.get('fair_value')}")
        print(f"    Price: {tip.get('market_price')}")
        print(f"    {tip.get('premium_discount')}: {tip.get('deviation_pct')}%")
        print(f"    Bucket: {tip.get('bucket')}")
    print(f"  spread13={(payload.get('spread_13w') or {}).get('spread_pp')}")
    print(f"  spread52={(payload.get('spread_52w') or {}).get('spread_pp')}")
    print(f"  spread104={(payload.get('spread_104w') or {}).get('spread_pp')}")
    print(f"  bounds_pass={(payload.get('boundary_tests') or {}).get('all_pass')}")
    narrative = (v.get("narrative") or "").encode("ascii", "replace").decode("ascii")
    print(f"  narrative={narrative}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
