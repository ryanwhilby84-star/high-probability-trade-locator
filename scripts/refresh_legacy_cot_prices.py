#!/usr/bin/env python3
"""Refresh canonical price histories for LEGACY_COT_MARKETS only.

Usage:
  python scripts/refresh_legacy_cot_prices.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS  # noqa: E402
from hptl.markets.usd_index_identity import ICE_DXY_ID  # noqa: E402
from hptl.prices.corn_foundation_backfill import run_corn_foundation_backfill  # noqa: E402
from hptl.prices.coverage import select_price_source  # noqa: E402
from hptl.prices.ice_dx_futures_backfill import promote_ice_dx_futures  # noqa: E402
from hptl.prices.price_store import (  # noqa: E402
    load_instrument_record_internal,
    write_price_store_merged,
)
from hptl.prices.run_price_refresh import refresh_instrument_record  # noqa: E402
from hptl.prices.softs_futures_backfill import SOFTS_YAHOO, promote_soft_futures  # noqa: E402
from hptl.prices.unified_adapter import UnifiedPriceAdapter  # noqa: E402

OUT = ROOT / "data" / "audits" / "legacy_cot_price_refresh.json"


def _latest(mid: str) -> str | None:
    rec = load_instrument_record_internal(mid) or {}
    daily = rec.get("daily") or []
    if not daily:
        return None
    return str(daily[-1].get("date") or "")[:10]


def main() -> int:
    adapter = UnifiedPriceAdapter()
    rows: list[dict[str, Any]] = []
    records: dict[str, Any] = {}

    # 1) Yahoo continuous futures promotions (includes Copper / HG, softs, 6J)
    for mid in SOFTS_YAHOO:
        if mid not in LEGACY_COT_MARKETS:
            continue
        before = _latest(mid)
        try:
            promo = promote_soft_futures(mid)
            rows.append(
                {
                    "instrument": mid,
                    "action": "yahoo_promote",
                    "ok": True,
                    "before_latest": before,
                    "after_latest": promo.get("corrected_latest_date"),
                    "source": promo.get("corrected_source"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            rows.append(
                {
                    "instrument": mid,
                    "action": "yahoo_promote",
                    "ok": False,
                    "error": str(exc)[:300],
                    "before_latest": before,
                }
            )

    # 2) Corn ZC=F
    before = _latest("Corn")
    try:
        promo = run_corn_foundation_backfill(execute=True)
        rows.append(
            {
                "instrument": "Corn",
                "action": "yahoo_promote",
                "ok": promo.get("status") == "promoted",
                "before_latest": before,
                "after_latest": _latest("Corn"),
                "detail": promo,
            }
        )
    except Exception as exc:  # noqa: BLE001
        rows.append(
            {
                "instrument": "Corn",
                "action": "yahoo_promote",
                "ok": False,
                "error": str(exc)[:300],
                "before_latest": before,
            }
        )

    # 3) ICE DX
    before = _latest(ICE_DXY_ID) or _latest("US Dollar Index / DX")
    try:
        promo = promote_ice_dx_futures()
        rows.append(
            {
                "instrument": "US Dollar Index / DX",
                "action": "yahoo_promote_ice_dx",
                "ok": True,
                "before_latest": before,
                "after_latest": _latest(ICE_DXY_ID) or _latest("US Dollar Index / DX"),
                "detail": promo if isinstance(promo, list) else promo,
            }
        )
    except Exception as exc:  # noqa: BLE001
        rows.append(
            {
                "instrument": "US Dollar Index / DX",
                "action": "yahoo_promote_ice_dx",
                "ok": False,
                "error": str(exc)[:300],
                "before_latest": before,
            }
        )

    # 4) Remaining via unified adapter (OANDA / AV)
    done = set(SOFTS_YAHOO) | {"Corn", "US Dollar Index / DX"}
    for mid in LEGACY_COT_MARKETS:
        if mid in done:
            continue
        before = _latest(mid)
        src = select_price_source(mid)
        try:
            fetched = adapter.fetch(mid)
            via = str(fetched.get("_fetched_via") or src or "unified")
            rec = refresh_instrument_record(mid, fetched, fetched_via=via)
            records[mid] = rec
            after = _latest(mid)
            rows.append(
                {
                    "instrument": mid,
                    "action": "adapter_refresh",
                    "ok": bool((rec or {}).get("daily")) and after is not None,
                    "before_latest": before,
                    "after_latest": after,
                    "source": via,
                    "error": (rec or {}).get("error"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            rows.append(
                {
                    "instrument": mid,
                    "action": "adapter_refresh",
                    "ok": False,
                    "before_latest": before,
                    "error": str(exc)[:300],
                    "source": src,
                }
            )

    if records:
        write_price_store_merged(records)

    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": "LEGACY_COT_MARKETS",
        "universe_count": len(LEGACY_COT_MARKETS),
        "rows": rows,
        "ok_count": sum(1 for r in rows if r.get("ok")),
        "fail_count": sum(1 for r in rows if not r.get("ok")),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    print(json.dumps({"ok_count": doc["ok_count"], "fail_count": doc["fail_count"]}, indent=2))
    for r in rows:
        print(
            f"{'OK' if r.get('ok') else 'FAIL':4} {r['instrument'][:28]:28} "
            f"{r.get('before_latest')} -> {r.get('after_latest')} ({r.get('action')})"
            + (f" err={r.get('error')}" if r.get("error") else "")
        )
    return 0 if doc["fail_count"] == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
