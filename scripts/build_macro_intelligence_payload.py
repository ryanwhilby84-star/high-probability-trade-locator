#!/usr/bin/env python3
"""CLI: Phase 5 Macro Intelligence payload.

Usage:
  python scripts/build_macro_intelligence_payload.py
      reads JSON from stdin: {"instrument_id": "Gold"}

  python scripts/build_macro_intelligence_payload.py Gold

Exit: 0 ok, 3 validation error, 1 crash
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.macro_intelligence.service import (  # noqa: E402
    build_macro_intelligence_payload,
    build_macro_intelligence_payload_from_json,
)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        if args:
            # Positional instrument id or JSON file path
            if args[0].endswith(".json") and Path(args[0]).is_file():
                raw = Path(args[0]).read_text(encoding="utf-8")
                payload = build_macro_intelligence_payload_from_json(raw)
            else:
                payload = build_macro_intelligence_payload(instrument_id=args[0])
        else:
            raw = sys.stdin.read()
            if not raw.strip():
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "engine": "macro_intelligence_v5",
                            "error": "empty_request",
                            "message": "JSON request or instrument_id required.",
                        }
                    ),
                    flush=True,
                )
                return 3
            payload = build_macro_intelligence_payload_from_json(raw)
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {
                    "status": "error",
                    "engine": "macro_intelligence_v5",
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
