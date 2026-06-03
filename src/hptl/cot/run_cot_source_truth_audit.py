"""Run independent CFTC Legacy Futures Only vs dashboard source-of-truth audit."""
from __future__ import annotations

import argparse
import sys

from hptl.cot.cot_source_truth_audit import build_cot_source_truth_audit, write_cot_source_truth_exports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit dashboard vs official CFTC Legacy Futures Only (fresh download)"
    )
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Use latest cached official zip in data/raw (not recommended for truth audit)",
    )
    args = parser.parse_args(argv)

    payload = build_cot_source_truth_audit(year=args.year, force_download=not args.no_download)
    paths = write_cot_source_truth_exports(payload)
    s = payload["summary"]
    print(f"Wrote {paths['audit']}")
    print(f"Wrote {paths['public']}")
    print(f"Wrote {paths['deliverable']}")
    print(
        f"Checked={s['total_instruments_checked']} PASS={s['pass_count']} "
        f"FAIL={s['fail_count']} REVIEW={s['needs_manual_review_count']}"
    )
    cl = payload["instruments"].get("Crude Oil / CL", {})
    print(f"Crude Oil / CL: {cl.get('status')} code={cl.get('selected_cftc_code')} reasons={cl.get('failure_reasons')}")
    nq = payload["instruments"].get("NASDAQ / NQ", {})
    print(f"NASDAQ / NQ: {nq.get('status')} code={nq.get('selected_cftc_code')}")
    return 0 if s["all_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
