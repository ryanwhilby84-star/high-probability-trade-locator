#!/usr/bin/env python3
"""CLI: build Seasonality Workstation route payload as JSON on stdout.

Exit codes:
  0 — ok
  3 — integrity / research failure
  1 — crash
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.seasonality_workstation.payload import build_seasonality_workstation_payload  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print(json.dumps({"status": "error", "error": "missing_instrument_id"}), flush=True)
        return 3
    instrument = args[0]
    lookback = args[1] if len(args) > 1 else "10Y"
    try:
        payload = build_seasonality_workstation_payload(instrument, lookback=lookback)
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {
                    "status": "error",
                    "instrument_id": instrument,
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
