"""Daily close history extraction for Macro Hub correlation prep."""

from __future__ import annotations

from typing import Any

import pandas as pd

from hptl.macro.fred_client import FredUnavailable, get_series_df, last_source
from hptl.macro_hub.config import CORRELATION_WINDOWS_DAYS, FRED_OBS_START_5Y, HISTORY_WINDOWS_DAYS
from hptl.macro_hub.freshness import freshness_status
from hptl.prices.price_store import load_instrument_record


def _daily_closes_from_bars(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for bar in bars or []:
        if not isinstance(bar, dict):
            continue
        dt = bar.get("date")
        close = bar.get("close")
        if dt is None or close is None:
            continue
        try:
            out.append({"date": str(dt)[:10], "close": float(close)})
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: x["date"])
    return out


def _window_slice(closes: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if not closes or days <= 0:
        return []
    return closes[-days:]


def _history_block(closes: list[dict[str, Any]]) -> dict[str, Any]:
    windows: dict[str, Any] = {}
    for label, days in HISTORY_WINDOWS_DAYS.items():
        slice_ = _window_slice(closes, days)
        windows[label] = {
            "bar_count": len(slice_),
            "from": slice_[0]["date"] if slice_ else None,
            "to": slice_[-1]["date"] if slice_ else None,
            "closes": slice_,
        }
    return {
        "daily_all": closes,
        "windows": windows,
        "correlation_prep": {
            "windows_days": list(CORRELATION_WINDOWS_DAYS),
            "ready": any(w["bar_count"] >= min(CORRELATION_WINDOWS_DAYS) for w in windows.values()),
        },
    }


def price_block_from_store(
    instrument_id: str,
    *,
    label: str,
    stale_after_days: int = 5,
) -> dict[str, Any]:
    rec = load_instrument_record(instrument_id)
    if not rec:
        return _empty_price_block(instrument_id, label, source="price_store", reason="instrument_not_in_store")

    closes = _daily_closes_from_bars(rec.get("daily") or [])
    latest = closes[-1] if closes else None
    snap = rec.get("price") or {}
    latest_price = latest["close"] if latest else snap.get("mid")
    latest_date = latest["date"] if latest else (str(snap.get("as_of") or "")[:10] or None)

    freshness = freshness_status(latest_date, stale_after_days=stale_after_days)
    return {
        "instrument_id": instrument_id,
        "label": label,
        "latest_price": latest_price,
        "latest_date": latest_date,
        "source": "price_store",
        "freshness": freshness,
        "history": _history_block(closes) if closes else None,
        "error": rec.get("error"),
    }


def fred_series_block(
    series_id: str,
    *,
    label: str,
    obs_start: str = FRED_OBS_START_5Y,
    stale_after_days: int = 7,
    allow_live: bool | None = None,
) -> dict[str, Any]:
    try:
        df = get_series_df(series_id, obs_start, allow_live=allow_live)
    except FredUnavailable as exc:
        return {
            "series_id": series_id,
            "label": label,
            "latest_value": None,
            "latest_date": None,
            "source": "fred",
            "fetch_mode": last_source(series_id, obs_start) or "unavailable",
            "freshness": {"status": "missing", "as_of": None, "age_days": None},
            "history": None,
            "error": str(exc),
        }

    closes: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        try:
            closes.append({"date": pd.Timestamp(row["date"]).strftime("%Y-%m-%d"), "close": float(row["value"])})
        except (TypeError, ValueError):
            continue

    latest = closes[-1] if closes else None
    latest_date = latest["date"] if latest else None
    return {
        "series_id": series_id,
        "label": label,
        "latest_value": latest["close"] if latest else None,
        "latest_date": latest_date,
        "source": "fred",
        "fetch_mode": last_source(series_id, obs_start) or "unknown",
        "freshness": freshness_status(latest_date, stale_after_days=stale_after_days),
        "history": _history_block(closes) if closes else None,
        "error": None,
    }


def _empty_price_block(
    instrument_id: str,
    label: str,
    *,
    source: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "instrument_id": instrument_id,
        "label": label,
        "latest_price": None,
        "latest_date": None,
        "source": source,
        "freshness": {"status": "missing", "as_of": None, "age_days": None},
        "history": None,
        "error": reason,
    }
