"""CLI: COT Intelligence Phase 1 — research table + data audit (no production intelligence)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.intelligence_phase1_audit import run_phase1_audit, write_phase1_outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 1 COT intelligence audit: point-in-time research table, "
            "price-quality flags, episode clustering, turning-point inventory. "
            "Does not modify EX/DIV thresholds or workstation behaviour."
        )
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        help="Optional market id (repeatable). Default: full cot3y universe.",
    )
    args = parser.parse_args(argv)

    payload = run_phase1_audit(markets=args.markets)
    paths = write_phase1_outputs(payload)
    summary = payload["audit"]["summary"]
    print("Phase 1 audit complete")
    print(
        f"  markets_built={summary['markets_built']} "
        f"trustworthy_price={summary['markets_trustworthy_price_for_outcomes']}"
    )
    print(f"  events={summary['aggregate_event_counts']}")
    print(f"  independent_extreme_episodes={summary['aggregate_independent_extreme_episodes']}")
    flagged = summary["markets_with_price_flags"]
    print(f"  price_flagged_markets={len(flagged)}")
    for item in flagged[:12]:
        print(f"    - {item['market']}: {item.get('flags')} / ohlc={item.get('ohlc_flags')}")
    if len(flagged) > 12:
        print(f"    ... +{len(flagged) - 12} more")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
