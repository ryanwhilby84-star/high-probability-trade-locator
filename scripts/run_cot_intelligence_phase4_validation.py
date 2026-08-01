"""CLI: COT Intelligence Phase 4 — chronological walk-forward validation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.intelligence_phase4_validation import (
    run_phase4_validation,
    write_phase4_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 4: falsify frozen Phase-3 candidate families via chronological "
            "walk-forward. No retuning, no UI, no intelligence score."
        )
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        help="Optional market id (repeatable). Default: full cot3y universe.",
    )
    args = parser.parse_args(argv)

    payload = run_phase4_validation(markets=args.markets)
    paths = write_phase4_outputs(payload)
    audit = payload["audit"]
    ev = audit["executive_verdict"]
    print("Phase 4 walk-forward validation complete")
    print(f"  folds={[f['fold_id'] for f in audit['walk_forward']['folds']]}")
    print(
        f"  VALIDATED={ev['n_validated']} PROMISING={ev['n_promising']} "
        f"FAILED={ev['n_failed']}"
    )
    print(f"  verdict: {ev['answer']}")
    print(f"  guidance: {ev['guidance']}")
    ic = audit["interactions"].get("verdict_commercial_tpB_div") or {}
    inc = audit["interactions"].get("verdict_nc_tpB_div") or {}
    print(
        f"  interaction Commercial TP-B+DIV: {ic.get('classification')} — {ic.get('reason')}"
    )
    print(f"  interaction NC TP-B+DIV: {inc.get('classification')} — {inc.get('reason')}")
    for fam in audit["families"]:
        c = fam["classification"]
        o4 = (fam.get("oos_outcomes") or {}).get("fwd_4w") or {}
        print(
            f"  {fam['candidate_id']} {c['classification']:20s} "
            f"n={o4.get('n')} %+4W={o4.get('pct_positive')} "
            f"| {(fam.get('family_human') or '')[:70]}"
        )
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
