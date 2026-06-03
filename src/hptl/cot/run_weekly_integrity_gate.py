"""CLI entry for the weekly COT integrity gate (background validation, not a dashboard)."""
from __future__ import annotations

import argparse
import sys

from hptl.cot.weekly_integrity_gate import run_weekly_integrity_gate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Weekly COT integrity gate (source truth + lineage)")
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download official CFTC deacot zip for source-truth validation",
    )
    parser.add_argument("--no-thesis-seed", action="store_true", help="Skip thesis snapshot refresh")
    parser.add_argument("--no-republish", action="store_true", help="Do not rebuild confluence after quarantine")
    args = parser.parse_args(argv)

    result = run_weekly_integrity_gate(
        force_download=args.force_download,
        seed_thesis=not args.no_thesis_seed,
        republish_on_quarantine=not args.no_republish,
        skip_deliverable_markdown=True,
    )
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
