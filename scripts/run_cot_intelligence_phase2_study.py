"""CLI: COT Intelligence Phase 2 — turning-point validation & outcome study."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.intelligence_phase2_turning_points import (
    run_phase2_study,
    write_phase2_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 2: validate turning-point definitions A–D (positioning first, "
            "then price outcomes on trustworthy markets). No production changes."
        )
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        help="Optional market id (repeatable). Default: full cot3y universe.",
    )
    args = parser.parse_args(argv)

    payload = run_phase2_study(markets=args.markets)
    paths = write_phase2_outputs(payload)
    audit = payload["audit"]
    counts = audit["inventory_counts"]
    rec = audit["recommendations"]
    print("Phase 2 turning-point study complete")
    print(f"  independent_events={counts['independent_total']}")
    print(f"  by_definition={counts['by_definition']}")
    print(f"  price_eligible={counts['price_eligible']}")
    print(f"  promote={rec.get('promote')} research_only={rec.get('research_only')} reject={rec.get('reject')}")
    for d, block in (rec.get("by_definition") or {}).items():
        pos = block.get("positioning_snapshot") or {}
        print(
            f"  {d} [{block.get('verdict')}]: "
            f"n={pos.get('n')} align4={pos.get('pct_aligned_4w')} "
            f"false_turn={pos.get('false_turn_rate')}"
        )
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
