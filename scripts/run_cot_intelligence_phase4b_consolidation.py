"""CLI: COT Intelligence Phase 4B — structural consolidation & sample expansion."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.intelligence_phase4b_consolidation import (
    run_phase4b,
    write_phase4b_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 4B: consolidate PROMISING Phase-3 families into broader "
            "positioning archetypes; rebuild samples; fresh OOS validation. "
            "No UI, no score, no return-based merging."
        )
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        help="Optional market id (repeatable). Default: full cot3y universe.",
    )
    args = parser.parse_args(argv)

    payload = run_phase4b(markets=args.markets)
    paths = write_phase4b_outputs(payload)
    audit = payload["audit"]
    ev = audit["executive_verdict"]
    print("Phase 4B structural consolidation complete")
    print(f"  archetypes={audit['archetype_freeze']['ids']}")
    print(
        f"  VALIDATED={ev['n_validated']} PROMISING={ev['n_promising']} "
        f"FAILED={ev['n_failed']}"
    )
    print(f"  fragmentation_solved={ev['fragmentation_solved']}")
    print(f"  phase5_justified={ev['phase5_live_matching_justified']}")
    print(f"  verdict: {ev['answer']}")
    for v in audit["validation"]:
        c = v["classification"]
        o4 = (v.get("oos_outcomes") or {}).get("fwd_4w") or {}
        inv = v.get("inventory") or {}
        print(
            f"  {v['archetype_id']} {c['classification']:20s} "
            f"indep_n={inv.get('n_independent')} oos_n={o4.get('n')} "
            f"%+4W={o4.get('pct_positive')}"
        )
    print(f"  TP-B+DIV C: {ev['tpb_div_summary']['commercial']}")
    print(f"  TP-B+DIV NC: {ev['tpb_div_summary']['noncommercial']}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
