"""Run full COT data lineage audit across HTPL layers."""
from __future__ import annotations

import argparse
import sys

import json

from hptl.cot.data_lineage_audit import (
    DATA_OUT,
    PUBLIC_OUT,
    build_data_lineage_audit,
    write_data_lineage_exports,
)


def print_pass_fail_table(payload: dict[str, Any]) -> None:
    """Final remediation table — no narrative."""
    print("Instrument | PASS/FAIL")
    for iid in sorted((payload.get("instruments") or {}).keys()):
        st = payload["instruments"][iid].get("overall_status", "FAIL")
        print(f"{iid} | {st}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build cot_data_lineage_latest.json")
    parser.add_argument("--table-only", action="store_true", help="Print pass/fail table only.")
    parser.add_argument("--quiet", action="store_true", help="Skip writing deliverable markdown.")
    args = parser.parse_args(argv)
    payload = build_data_lineage_audit()
    if not args.quiet:
        write_data_lineage_exports(payload)
    else:
        for path in (DATA_OUT, PUBLIC_OUT):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if args.table_only:
        print_pass_fail_table(payload)
    else:
        s = payload["summary"]
        print(f"PASS={s['pass_count']} FAIL={s['fail_count']}")
        print_pass_fail_table(payload)
    return 0 if payload["summary"]["all_layers_identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
