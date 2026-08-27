"""Run Commercial-led COT Attention Engine V1 and export dashboard JSON.

Usage:
    python scripts/run_commercial_attention_engine.py
    python scripts/run_commercial_attention_engine.py --as-of 2026-07-14
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from hptl.cot.commercial_attention_export import run_commercial_attention_export  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Commercial COT Attention Engine V1")
    parser.add_argument("--as-of", default=None, help="COT report date YYYY-MM-DD (default: latest)")
    args = parser.parse_args()
    payload = run_commercial_attention_export(as_of=args.as_of)
    summary = payload.get("summary") or {}
    board = payload.get("attention_board") or []
    print(
        f"Commercial attention V1 — week {payload.get('source_week')} | "
        f"scanned={summary.get('instruments_scanned')} eligible={summary.get('eligible')} "
        f"events={summary.get('with_events')} HIGH={summary.get('high_attention')}"
    )
    for i, row in enumerate(board[:15], start=1):
        print(
            f"  {i:2d}. {row['instrument']} [{row['attention_label']}] "
            f"pts={row['evidence_points']} :: {', '.join(row.get('events') or [])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
