"""CLI: COT Intelligence Phase 3 — multi-group configuration discovery."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.intelligence_phase3_configurations import (
    run_phase3_discovery,
    write_phase3_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 3: discover interpretable multi-group positioning configurations "
            "around TP-B / divergence / extreme onsets. No production score or UI."
        )
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        help="Optional market id (repeatable). Default: full cot3y universe.",
    )
    args = parser.parse_args(argv)

    payload = run_phase3_discovery(markets=args.markets)
    paths = write_phase3_outputs(payload)
    audit = payload["audit"]
    s = audit["summary"]
    print("Phase 3 configuration discovery complete")
    print(f"  samples={audit['inventory_counts']}")
    print(
        f"  families candidate={s['candidate']} weak={s['weak']} reject={s['reject']}"
    )
    print(f"  phase4_shortlist={len(audit.get('phase4_candidates') or [])}")
    for row in (audit.get("family_table_top") or [])[:8]:
        if row["verdict"] == "candidate":
            print(f"  CAND n={row['n']} mkts={row['n_markets']}: {row['human'][:90]}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
