#!/usr/bin/env python3
"""CLI: Phase 2A trade-basket analysis.

Usage:
  python scripts/build_trade_basket_payload.py
      reads JSON request from stdin

  python scripts/build_trade_basket_payload.py path/to/request.json

Exit: 0 ok, 3 validation/analysis error, 1 crash
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.trade_basket.service import build_trade_basket_payload_from_json  # noqa: E402
from hptl.portfolio_intelligence.service import (  # noqa: E402
    enrich_basket_with_portfolio_intelligence,
)
from hptl.trade_basket.currency_exposure import (  # noqa: E402
    enrich_basket_with_currency_exposure,
)
from hptl.trade_basket.portfolio_thesis import (  # noqa: E402
    enrich_basket_with_portfolio_thesis,
)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        if args:
            raw = Path(args[0]).read_text(encoding="utf-8")
        else:
            raw = sys.stdin.read()
        if not raw.strip():
            print(
                json.dumps(
                    {
                        "status": "error",
                        "engine": "trade_basket_v2a",
                        "error": "empty_request",
                        "message": "JSON request required on stdin or as file argument.",
                    }
                ),
                flush=True,
            )
            return 3
        payload = build_trade_basket_payload_from_json(raw)
        # Phase 3 enrichment — Phase 2A fields remain intact.
        payload = enrich_basket_with_portfolio_intelligence(payload)
        # Phase 4 FX currency exposure — pair correlations already use pair IDs.
        payload = enrich_basket_with_currency_exposure(payload)
        # Phase 4.5 thesis summary — presentation only from existing fields.
        payload = enrich_basket_with_portfolio_thesis(payload)
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {
                    "status": "error",
                    "engine": "trade_basket_v2a",
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
