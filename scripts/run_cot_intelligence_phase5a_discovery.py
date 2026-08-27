"""CLI: COT Intelligence Phase 5A — price-anchored behaviour discovery."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.intelligence_phase5a import run_phase5a, write_phase5a_outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 5A: detect major rallies/sell-offs then discover recurring "
            "pre-move COT behaviour. Research only — no UI, no live alerts."
        )
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        help="Optional trustworthy market id (repeatable).",
    )
    args = parser.parse_args(argv)

    payload = run_phase5a(markets=args.markets)
    paths = write_phase5a_outputs(payload)
    s = payload["summary"]
    print("Phase 5A discovery complete")
    print(f"  markets={s['n_markets']}")
    print(
        f"  independent rallies={s['n_independent_rallies']} "
        f"selloffs={s['n_independent_selloffs']}"
    )
    print(
        f"  rally_clusters={s['rally_cluster_meta']} "
        f"selloff_clusters={s['selloff_cluster_meta']}"
    )
    print(f"  distinctive_control_contrasts={s['n_distinctive_control_contrasts']}")
    for label, path in paths.items():
        print(f"  wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
