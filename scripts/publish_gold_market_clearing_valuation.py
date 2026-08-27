"""Publish Gold market-clearing valuation to dashboard JSON (NG-style).

Usage:
  python scripts/publish_gold_market_clearing_valuation.py
  python scripts/publish_gold_market_clearing_valuation.py --no-rerun
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.valuation.gold_market_clearing_export import (  # noqa: E402
    write_gold_valuation_exports,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish Gold market-clearing valuation for the HPTL dashboard."
    )
    parser.add_argument(
        "--no-rerun",
        action="store_true",
        help="Reuse existing audit ranking/history instead of re-running the engine",
    )
    args = parser.parse_args(argv)

    paths = write_gold_valuation_exports(rerun=not args.no_rerun)
    doc = json.loads(paths["data"].read_text(encoding="utf-8"))
    inst = doc.get("instrument") or {}
    print("Gold market-clearing dashboard publish")
    print(f"  model={inst.get('model_id')}")
    print(f"  spot={inst.get('spot_price')} fair={inst.get('fair_value')}")
    print(f"  deviation_pct={inst.get('deviation_pct')} bucket={inst.get('valuation_bucket')}")
    print(f"  imbalance_t={inst.get('net_imbalance_tonnes')}")
    print(f"  history_n={len(inst.get('history') or [])}")
    print(f"  quarters={inst.get('panel_quarters')}")
    for k, p in paths.items():
        print(f"  wrote {k}: {p}")
    return 0 if inst.get("wired") else 1


if __name__ == "__main__":
    raise SystemExit(main())
