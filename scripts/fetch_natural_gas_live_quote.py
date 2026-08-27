"""Fetch a fresh OANDA REST quote for Natural Gas (NATGAS_USD).

Used by the Vite /api/ng-live-price polling fallback when the WebSocket
Current Price Service (:8787) is unavailable.

Does not write files, rebuild valuation, or touch COT.

Usage:
  python scripts/fetch_natural_gas_live_quote.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

MARKET = "Natural Gas / NG"
SYMBOL = "NATGAS_USD"


def main() -> int:
    from hptl.oanda.oanda_prices import fetch_pricing

    now = datetime.now(timezone.utc)
    try:
        snaps = fetch_pricing([SYMBOL])
    except Exception as exc:  # noqa: BLE001 - surface as JSON for the HTTP layer
        print(
            json.dumps(
                {
                    "ok": False,
                    "instrument_id": MARKET,
                    "provider_symbol": SYMBOL,
                    "error": str(exc),
                    "fetched_at": now.isoformat(),
                }
            )
        )
        return 1

    snap = snaps.get(SYMBOL) or {}
    mid = snap.get("mid")
    bid = snap.get("bid")
    ask = snap.get("ask")
    as_of = snap.get("as_of") or now.isoformat()
    age_s = None
    try:
        ts = datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_s = max(0.0, (now - ts.astimezone(timezone.utc)).total_seconds())
    except ValueError:
        age_s = 0.0

    ok = mid is not None
    print(
        json.dumps(
            {
                "ok": ok,
                "instrument_id": MARKET,
                "provider_symbol": SYMBOL,
                "provider": "oanda",
                "mid": mid,
                "bid": bid,
                "ask": ask,
                "as_of": as_of,
                "age_seconds": round(age_s, 3) if age_s is not None else None,
                "source": "oanda_rest_poll",
                "update_mode": "POLLING",
                "fetched_at": now.isoformat(),
            }
        )
    )
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
