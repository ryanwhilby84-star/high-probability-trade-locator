"""Export audited COT trader-group JSON for the dashboard integrity layer."""
from __future__ import annotations

import argparse
import sys

from hptl.cot.cot_groups_integrity import WEEKS_HISTORY, run_cot_groups_integrity


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build cot_groups_latest.json and cot_group_audit_latest.json")
    parser.add_argument("--weeks", type=int, default=WEEKS_HISTORY, help="Weeks of history per instrument")
    args = parser.parse_args(argv)
    paths = run_cot_groups_integrity(weeks=args.weeks)
    for label, path in paths.items():
        print(f"Wrote {label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
