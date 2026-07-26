#!/usr/bin/env python3
"""CLI for Vite workstation route transport.

Stdout: single JSON object (the route payload).
Stderr: diagnostics only.
Exit codes:
  0 — status == ok
  3 — status == integrity_error (derived-COT / data contract)
  1 — unexpected builder/process failure
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.cot.workstation_route_payload import build_workstation_route_payload  # noqa: E402

EXIT_OK = 0
EXIT_PROCESS = 1
EXIT_INTEGRITY = 3


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or not str(args[0]).strip():
        print("usage: build_workstation_route_payload.py <instrument_id>", file=sys.stderr)
        return EXIT_PROCESS

    instrument = str(args[0]).strip()
    try:
        body, _http_status = build_workstation_route_payload(instrument)
    except Exception as exc:  # noqa: BLE001
        print(f"builder_exception:{type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_PROCESS

    status = body.get("status")
    try:
        sys.stdout.write(json.dumps(body, allow_nan=False))
        sys.stdout.write("\n")
        sys.stdout.flush()
    except (TypeError, ValueError) as exc:
        print(f"json_encode_error:{exc}", file=sys.stderr)
        return EXIT_PROCESS

    if status == "ok":
        return EXIT_OK
    if status == "integrity_error":
        print(
            f"derived_cot_integrity_error instrument={instrument!r} "
            f"report_date={body.get('report_date')!r} "
            f"missing={body.get('missing_fields')!r}",
            file=sys.stderr,
        )
        return EXIT_INTEGRITY

    print(f"unexpected_status:{status!r}", file=sys.stderr)
    return EXIT_PROCESS


if __name__ == "__main__":
    raise SystemExit(main())
