#!/usr/bin/env python3
"""Verify HPTL workstation data pipeline freshness per instrument."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report per-instrument data pipeline freshness.")
    parser.add_argument("--all", action="store_true", help="Include non-COT valuation markets.")
    parser.add_argument("--show-passing", action="store_true", help="Print passing instruments too.")
    parser.add_argument("--json", type=str, default="", help="Write JSON report to path.")
    args = parser.parse_args(argv)

    from hptl.dashboard.pipeline_freshness import build_pipeline_freshness_report, print_freshness_report

    report = build_pipeline_freshness_report(include_non_cot=args.all)
    if args.json:
        out = Path(args.json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report.as_dict(), indent=2), encoding="utf-8")
        print(f"Wrote {out}")

    return print_freshness_report(report, show_passing=args.show_passing)


if __name__ == "__main__":
    raise SystemExit(main())
