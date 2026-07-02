"""FRED-primary price foundation for soft commodities (Cocoa, Coffee, Cotton).

Alpha Vantage commodity feeds for softs are monthly and Cocoa was incorrectly mapped to
the COTTON function. These instruments use IMF global price series from FRED as the
canonical daily store (monthly observations stored as OHLC with identical OHLC).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from hptl.prices.cot_fail_backfill import backfill_fred_instrument, fred_series_to_daily_bars
from hptl.prices.models import build_history_meta, compute_range_52w
from hptl.prices.price_store import (
    load_all_instrument_records,
    load_instrument_record,
    write_instrument_record,
    write_price_store,
)
from hptl.prices.promote_price_backfill import promote_staging_backfill
from hptl.seasonality.seasonality_v2 import normalize_daily_bars

SOFTS_INSTRUMENTS: tuple[str, ...] = ("Cocoa", "Coffee", "Cotton")

# instrument_id -> (fred_series_id, human-readable note)
FRED_PRIMARY_SOFTS: dict[str, tuple[str, str]] = {
    "Cocoa": (
        "PCOCOUSDM",
        "IMF global cocoa price (monthly). Not ICE CC futures — canonical macro benchmark.",
    ),
    "Coffee": (
        "PCOFFOTMUSDM",
        "IMF global Arabica coffee price (monthly). Not ICE KC futures — canonical macro benchmark.",
    ),
    "Cotton": (
        "PCOTTINDUSDM",
        "IMF global cotton price (monthly). Not ICE CT futures — canonical macro benchmark.",
    ),
}

FRED_OBS_START = "1990-01-01"


def backfill_softs_fred(*, observation_start: str = FRED_OBS_START) -> list[dict[str, Any]]:
    """Stage + promote FRED primary history for Cocoa, Coffee, Cotton."""
    results: list[dict[str, Any]] = []
    for instrument_id, (series_id, note) in FRED_PRIMARY_SOFTS.items():
        row = backfill_fred_instrument(instrument_id, series_id, observation_start=observation_start)
        row["note"] = note
        results.append(row)
    promotion = promote_staging_backfill(list(FRED_PRIMARY_SOFTS.keys()))
    return results + [{"promotion": promotion}]


def merge_av_with_fred_primary(
    instrument_id: str,
    *,
    fred_series_id: str,
    note: str,
    observation_start: str = FRED_OBS_START,
) -> dict[str, Any]:
    """Write production record: FRED history + any existing non-proxy daily bars merged by date."""
    fred_daily = normalize_daily_bars(fred_series_to_daily_bars(fred_series_id, observation_start=observation_start))
    existing = load_instrument_record(instrument_id) or {}
    existing_daily = existing.get("daily") or []

    by_date: dict[str, dict[str, Any]] = {}
    for bar in fred_daily:
        by_date[str(bar["date"])[:10]] = {**bar, "_source": "fred"}
    for bar in existing_daily:
        d = str(bar.get("date") or "")[:10]
        if not d:
            continue
        scale = (existing.get("price_scale") or {}).get("source")
        if scale == "fred":
            continue
        by_date[d] = {**bar, "_source": scale or "alpha_vantage"}

    daily = normalize_daily_bars(
        [
            {
                "date": bar["date"],
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
                "volume": bar.get("volume"),
            }
            for bar in by_date.values()
        ]
    )
    range_52w = compute_range_52w(daily)
    weekly = existing.get("weekly") or []
    rec = {
        "instrument_id": instrument_id,
        "price": {"mid": daily[-1]["close"], "as_of": daily[-1]["date"]} if daily else None,
        "daily": daily,
        "weekly": weekly,
        "range_52w": range_52w,
        "history": build_history_meta(daily, weekly, range_52w),
        "error": None,
        "price_scale": {
            "source": "fred",
            "series_id": fred_series_id,
            "is_proxy": False,
            "proxy_note": None,
            "canonical_note": note,
        },
    }
    write_instrument_record(rec, fetched_via="fred_primary", historical_via="fred_primary")
    return {
        "instrument": instrument_id,
        "fred_series": fred_series_id,
        "total_daily_bars": len(daily),
        "earliest_date": daily[0]["date"] if daily else None,
        "latest_date": daily[-1]["date"] if daily else None,
        "note": note,
    }


def promote_softs_canonical_prices(*, observation_start: str = FRED_OBS_START) -> list[dict[str, Any]]:
    """Promote FRED-primary canonical prices for all softs into production store."""
    out: list[dict[str, Any]] = []
    for instrument_id, (series_id, note) in FRED_PRIMARY_SOFTS.items():
        out.append(
            merge_av_with_fred_primary(
                instrument_id,
                fred_series_id=series_id,
                note=note,
                observation_start=observation_start,
            )
        )
    records = load_all_instrument_records()
    if records:
        write_price_store(records)
    return out


def softs_fred_meta(instrument_id: str) -> dict[str, Any] | None:
    if instrument_id not in FRED_PRIMARY_SOFTS:
        return None
    series_id, note = FRED_PRIMARY_SOFTS[instrument_id]
    return {
        "canonical_source": "fred",
        "canonical_symbol": f"fred:{series_id}",
        "proxy": False,
        "proxy_explanation": None,
        "confidence": "medium",
        "note": note,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
