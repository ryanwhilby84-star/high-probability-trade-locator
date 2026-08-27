#!/usr/bin/env python3
"""CLI: build Correlation Matrix Workstation payload as JSON on stdout.

Usage:
  python scripts/build_correlation_matrix_payload.py [frequency] [lookback]

Exit codes:
  0 — ok
  3 — validation / request failure
  1 — crash
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.correlation_matrix.service import build_correlation_matrix_payload  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    frequency = args[0] if args else "daily"
    lookback = args[1] if len(args) > 1 else "60"
    try:
        payload = build_correlation_matrix_payload(
            frequency=frequency,
            lookback=int(lookback),
        )
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {
                    "status": "error",
                    "engine": "correlation_matrix_v1",
                    "error": "builder_crash",
                    "message": str(exc)[:400],
                }
            ),
            flush=True,
        )
        return 1
    print(json.dumps(payload), flush=True)
    return 0 if payload.get("status") == "ok" else 3


if __name__ == "__main__":
    raise SystemExit(main())
