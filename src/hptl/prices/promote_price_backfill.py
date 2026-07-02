"""Promote staged OANDA backfill files into the canonical price store."""

from __future__ import annotations

from typing import Any

from hptl.prices.coverage import load_price_coverage
from hptl.prices.fx_daily_backfill import _load_staging_record, merge_daily_bars
from hptl.prices.models import build_history_meta, compute_range_52w
from hptl.prices.price_store import (
    load_all_instrument_records,
    load_instrument_record,
    load_price_store,
    write_instrument_record,
    write_price_store,
)
from hptl.seasonality.seasonality_v2 import normalize_daily_bars


def promote_staging_backfill(store_keys: list[str]) -> dict[str, Any]:
    """Merge staging daily bars into production records and rewrite prices_latest.json."""
    records = load_all_instrument_records()
    if not records:
        doc = load_price_store()
        for iid, blk in (doc.get("instruments") or {}).items():
            records[iid] = {
                "instrument_id": iid,
                "price": blk.get("price"),
                "daily": blk.get("daily") or [],
                "weekly": blk.get("weekly") or [],
                "range_52w": blk.get("range_52w"),
                "history": blk.get("history"),
                "error": blk.get("error"),
                "price_scale": blk.get("price_scale"),
            }

    promoted: list[dict[str, Any]] = []
    for key in store_keys:
        staging = _load_staging_record(key)
        if not staging or not staging.get("daily"):
            continue
        prod = records.get(key) or load_instrument_record(key) or {
            "instrument_id": key,
            "daily": [],
            "weekly": [],
        }
        merged, added = merge_daily_bars(prod.get("daily") or [], staging.get("daily") or [])
        daily = normalize_daily_bars(merged)
        range_52w = compute_range_52w(daily)
        weekly = prod.get("weekly") or staging.get("weekly") or []
        rec = {
            **prod,
            "instrument_id": key,
            "daily": daily,
            "weekly": weekly,
            "range_52w": range_52w,
            "history": build_history_meta(daily, weekly, range_52w),
            "error": None,
        }
        write_instrument_record(rec, fetched_via="oanda_backfill", historical_via="oanda_backfill")
        records[key] = rec
        promoted.append(
            {
                "instrument": key,
                "bars_added": added,
                "total_daily_bars": len(daily),
                "earliest_date": daily[0]["date"] if daily else None,
                "latest_date": daily[-1]["date"] if daily else None,
            }
        )

    if promoted:
        cov = load_price_coverage()
        write_price_store(records, coverage_generated_at=cov.get("generated_at"))

    return {"promoted": promoted, "count": len(promoted)}
