"""Build COT proof dashboard export — dashboard vs raw Legacy CFTC."""
from __future__ import annotations

import argparse
import sys

from hptl.cot.cot_proof import build_cot_proof, write_cot_proof_exports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build cot_proof_latest.json for dashboard verification")
    parser.add_argument("--year", type=int, default=None, help="CFTC legacy year (default: current UTC year)")
    parser.add_argument("--no-download", action="store_true", help="Use cached legacy zip only")
    args = parser.parse_args(argv)

    payload = build_cot_proof(year=args.year, download=not args.no_download)
    paths = write_cot_proof_exports(payload)
    summary = payload["summary"]
    print(f"Wrote {paths['proof']}")
    print(f"Wrote {paths['public']}")
    print(
        f"Checked={summary['total_instruments_checked']} "
        f"PASS={summary['pass_count']} FAIL={summary['fail_count']} "
        f"REVIEW={summary['needs_review_count']}"
    )
    if summary["failed_instruments"]:
        print("Failed:", ", ".join(summary["failed_instruments"][:12]))
        if len(summary["failed_instruments"]) > 12:
            print(f"  ... +{len(summary['failed_instruments']) - 12} more")
    return 0 if summary["all_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
