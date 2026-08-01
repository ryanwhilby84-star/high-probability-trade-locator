"""FRED daily price fetch for instruments without OANDA / Alpha Vantage coverage."""

from __future__ import annotations

from typing import Any

from hptl.prices.cot_fail_backfill import FRED_COT_FAIL_SERIES, fred_series_to_daily_bars
from hptl.prices.models import InstrumentPriceRecord, OhlcBar, build_history_meta, compute_range_52w
from hptl.prices.price_store import load_instrument_record_internal
from hptl.seasonality.seasonality_v2 import normalize_daily_bars

FRED_INSTRUMENT_SERIES: dict[str, str] = dict(FRED_COT_FAIL_SERIES)


def fred_series_for(instrument_id: str) -> str | None:
    return FRED_INSTRUMENT_SERIES.get(instrument_id)


def fetch_fred_instrument(instrument_id: str, *, observation_start: str = "2016-01-01") -> InstrumentPriceRecord:
    """Fetch FRED observations; merge with stored production bars when present."""
    series_id = fred_series_for(instrument_id)
    if not series_id:
        return {
            "instrument_id": instrument_id,
            "price": None,
            "daily": [],
            "weekly": [],
            "range_52w": None,
            "history": None,
            "error": "unknown_fred_instrument",
        }

    incoming = normalize_daily_bars(fred_series_to_daily_bars(series_id, observation_start=observation_start))
    if not incoming:
        return {
            "instrument_id": instrument_id,
            "price": None,
            "daily": [],
            "weekly": [],
            "range_52w": None,
            "history": None,
            "error": f"no_fred_bars:{series_id}",
        }

    existing = load_instrument_record_internal(instrument_id) or {}
    daily = _merge_fred_with_existing(existing.get("daily") or [], incoming, instrument_id)
    from hptl.prices.workstation_ohlc_export import derive_weekly_ohlc_from_daily

    weekly_raw = derive_weekly_ohlc_from_daily(
        [{**b, "source": f"fred:{series_id}"} for b in daily]
    )
    weekly = [
        {
            "date": b["date"],
            "open": b["open"],
            "high": b["high"],
            "low": b["low"],
            "close": b["close"],
            "volume": None,
        }
        for b in weekly_raw
    ]
    price = {"mid": daily[-1]["close"], "as_of": daily[-1]["date"]} if daily else None
    range_52w = compute_range_52w(daily)
    history = build_history_meta(daily, weekly, range_52w) if daily else None

    scale_meta: dict[str, Any] = {
        "source": "fred",
        "series_id": series_id,
        "is_fallback": False,
        "is_proxy": False,
    }
    if instrument_id == "Broad US Dollar Index — DTWEXBGS":
        scale_meta["is_fred_broad"] = True
        scale_meta["fallback_note"] = (
            "FRED Nominal Broad U.S. Dollar Index (DTWEXBGS). "
            "Not ICE DX / Dixie futures — never substitute for DXY seasonality."
        )
        scale_meta["instrument_label"] = "Broad US Dollar Index — DTWEXBGS"
        scale_meta["canonical_note"] = scale_meta["fallback_note"]

    return {
        "instrument_id": instrument_id,
        "price": price,
        "daily": daily,
        "weekly": weekly,
        "range_52w": range_52w,
        "history": history,
        "error": None,
        "price_scale": scale_meta,
    }


def _merge_fred_with_existing(
    existing: list[OhlcBar],
    incoming: list[OhlcBar],
    instrument_id: str,
) -> list[OhlcBar]:
    """Preserve stored history; extend/replace tail from FRED. Scale FRED to stored anchor on overlap."""
    if not existing:
        return incoming
    if not incoming:
        return existing

    by_date: dict[str, OhlcBar] = {str(b["date"]): dict(b) for b in existing}
    overlap = sorted(set(by_date) & {str(b["date"]) for b in incoming})
    scale = 1.0
    if overlap and instrument_id == "Broad US Dollar Index — DTWEXBGS":
        anchor = overlap[-1]
        stored_close = float(by_date[anchor]["close"])
        fred_close = float(next(b["close"] for b in incoming if b["date"] == anchor))
        if fred_close and fred_close > 0:
            scale = stored_close / fred_close

    for bar in incoming:
        d = str(bar["date"])
        close = float(bar["close"]) * scale
        if d in by_date:
            continue
        by_date[d] = {
            "date": d,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": None,
        }

    merged = [by_date[k] for k in sorted(by_date)]
    return normalize_daily_bars(merged)
