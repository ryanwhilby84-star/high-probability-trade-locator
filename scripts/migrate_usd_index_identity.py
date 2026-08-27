#!/usr/bin/env python3
"""Migrate FRED DTWEXBGS off DX onto Broad USD; bind ICE DX futures to DX/DXY.

Never silently substitutes FRED for ICE or ICE for FRED.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import (  # noqa: E402
    BROAD_USD_ID,
    DX_COT_ID,
    FRED_BROAD_SERIES,
    ICE_DXY_ID,
)
from hptl.prices.fred_prices import fetch_fred_instrument  # noqa: E402
from hptl.prices.ice_dx_futures_backfill import promote_ice_dx_futures  # noqa: E402
from hptl.prices.price_store import (  # noqa: E402
    load_all_instrument_records,
    load_instrument_record_internal,
    write_instrument_record,
    write_price_store,
)


def migrate_fred_to_broad() -> dict:
    """Copy existing DX FRED bars to Broad USD id, or refetch FRED if missing."""
    dx = load_instrument_record_internal(DX_COT_ID) or {}
    scale = dx.get("price_scale") or {}
    daily = dx.get("daily") or []

    if scale.get("series_id") == FRED_BROAD_SERIES and daily:
        rec = {
            "instrument_id": BROAD_USD_ID,
            "price": dx.get("price"),
            "daily": daily,
            "weekly": dx.get("weekly") or [],
            "range_52w": dx.get("range_52w"),
            "history": dx.get("history"),
            "error": None,
            "price_scale": {
                "source": "fred",
                "series_id": FRED_BROAD_SERIES,
                "is_fallback": False,
                "is_proxy": False,
                "is_fred_broad": True,
                "instrument_label": BROAD_USD_ID,
                "canonical_note": (
                    "FRED Nominal Broad U.S. Dollar Index (DTWEXBGS). "
                    "Not ICE DX futures."
                ),
            },
        }
        write_instrument_record(rec, fetched_via="fred", historical_via=f"fred:{FRED_BROAD_SERIES}")
        records = load_all_instrument_records() or {}
        records[BROAD_USD_ID] = rec
        write_price_store(records)
        return {
            "status": "migrated_from_dx_fred",
            "bars": len(daily),
            "latest": daily[-1]["date"] if daily else None,
        }

    # Already migrated or DX already ICE — refresh Broad from FRED directly
    fetched = fetch_fred_instrument(BROAD_USD_ID, observation_start="2006-01-01")
    if fetched.get("error") or not fetched.get("daily"):
        return {"status": "failed", "error": fetched.get("error") or "no_bars"}
    write_instrument_record(fetched, fetched_via="fred", historical_via=f"fred:{FRED_BROAD_SERIES}")
    records = load_all_instrument_records() or {}
    records[BROAD_USD_ID] = fetched
    write_price_store(records)
    return {
        "status": "fetched_fred",
        "bars": len(fetched["daily"]),
        "latest": fetched["daily"][-1]["date"],
    }


def main() -> int:
    report = {
        "migrated_at": datetime.now(timezone.utc).isoformat(),
        "broad_usd": None,
        "ice_dx": None,
    }
    report["broad_usd"] = migrate_fred_to_broad()
    report["ice_dx"] = promote_ice_dx_futures()

    # Sanity: Broad ≠ ICE levels
    broad = load_instrument_record_internal(BROAD_USD_ID) or {}
    ice = load_instrument_record_internal(ICE_DXY_ID) or {}
    b_close = (broad.get("daily") or [{}])[-1].get("close")
    i_close = (ice.get("daily") or [{}])[-1].get("close")
    report["level_check"] = {
        "broad_latest_close": b_close,
        "ice_latest_close": i_close,
        "distinct": (
            b_close is not None
            and i_close is not None
            and abs(float(b_close) - float(i_close)) > 1.0
        ),
    }
    if not report["level_check"]["distinct"]:
        print("ERROR: Broad and ICE closes not distinct — aborting identity claim", file=sys.stderr)
        print(json.dumps(report, indent=2))
        return 1

    out = ROOT / "data" / "audits" / "usd_index_identity_migration.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
