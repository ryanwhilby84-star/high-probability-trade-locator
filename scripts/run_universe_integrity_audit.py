#!/usr/bin/env python3
"""Run the Phase 1–7 universe integrity audit for all LEGACY_COT_MARKETS.

Exit codes:
  0 — gate open (all PASS, warnings allowed)
  1 — gate closed (one or more FAIL)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.universe_integrity_audit import (  # noqa: E402
    OUT_MD,
    render_markdown,
    write_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--strict-warnings",
        action="store_true",
        help="Treat WARN as FAIL for CI gate",
    )
    args = parser.parse_args()

    from hptl.markets.universe_integrity_audit import run_universe_integrity_audit

    report = run_universe_integrity_audit(seed=args.seed)
    write_report(report)
    md = render_markdown(report)
    try:
        print(md)
    except UnicodeEncodeError:
        print(md.encode("ascii", errors="replace").decode("ascii"))
    print(f"\nWrote {OUT_MD}")

    summary = report["summary"]
    print(
        f"Gate: {'OPEN' if summary.get('gate_open') else 'CLOSED'} | "
        f"PASS={summary['passed']} WARN={summary['warnings']} FAIL={summary['failed']}"
    )
    if summary["failed"] or report.get("universe_issues"):
        return 1
    if args.strict_warnings and summary["warnings"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
