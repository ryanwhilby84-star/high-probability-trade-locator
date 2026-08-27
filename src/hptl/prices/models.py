"""Canonical price store shapes (source-agnostic)."""

from __future__ import annotations

from typing import Any, TypedDict


class OhlcBar(TypedDict, total=False):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None


class PriceSnapshot(TypedDict, total=False):
    mid: float | None
    bid: float | None
    ask: float | None
    as_of: str


class Range52w(TypedDict, total=False):
    high: float
    low: float
    as_of: str
    start_date: str
    end_date: str


class PriceHistoryMeta(TypedDict, total=False):
    range_52w: Range52w
    daily_from: str | None
    daily_to: str | None
    weekly_from: str | None
    weekly_to: str | None
    bar_count_daily: int
    bar_count_weekly: int


class InstrumentPriceRecord(TypedDict, total=False):
    instrument_id: str
    price: PriceSnapshot | None
    daily: list[OhlcBar]
    weekly: list[OhlcBar]
    forming_daily: OhlcBar | None
    forming_weekly: OhlcBar | None
    range_52w: Range52w | None
    history: PriceHistoryMeta | None
    error: str | None
    price_scale: dict[str, Any] | None


# Research-grade history targets. OANDA permits up to 5,000 candles per request;
# these targets give the workstation enough history for COT analogue/lookback work
# while remaining within one request per granularity.
DAILY_BAR_TARGET = 5000
WEEKLY_BAR_TARGET = 1000
WEEKLY_LOOKBACK_DAYS = WEEKLY_BAR_TARGET * 7


def compute_range_52w(daily: list[OhlcBar]) -> Range52w | None:
    if not daily:
        return None
    window = daily[-252:] if len(daily) > 252 else daily
    highs = [b["high"] for b in window if b.get("high") is not None]
    lows = [b["low"] for b in window if b.get("low") is not None]
    if not highs or not lows:
        return None
    return {
        "high": max(highs),
        "low": min(lows),
        "as_of": window[-1].get("date"),
        "start_date": window[0].get("date"),
        "end_date": window[-1].get("date"),
    }


def build_history_meta(
    daily: list[OhlcBar],
    weekly: list[OhlcBar],
    range_52w: Range52w | None,
) -> PriceHistoryMeta:
    return {
        "range_52w": range_52w or {},
        "daily_from": daily[0]["date"] if daily else None,
        "daily_to": daily[-1]["date"] if daily else None,
        "weekly_from": weekly[0]["date"] if weekly else None,
        "weekly_to": weekly[-1]["date"] if weekly else None,
        "bar_count_daily": len(daily),
        "bar_count_weekly": len(weekly),
    }


def bars_to_public(bars: list[OhlcBar]) -> list[dict[str, Any]]:
    return [
        {
            "date": b.get("date"),
            "open": b.get("open"),
            "high": b.get("high"),
            "low": b.get("low"),
            "close": b.get("close"),
            "volume": b.get("volume"),
        }
        for b in bars
    ]


def record_to_public(rec: InstrumentPriceRecord) -> dict[str, Any]:
    """Dashboard-facing record — no data-source fields."""
    forming_d = rec.get("forming_daily")
    forming_w = rec.get("forming_weekly")
    return {
        "instrument_id": rec.get("instrument_id"),
        "price": rec.get("price"),
        "daily": bars_to_public(rec.get("daily") or []),
        "weekly": bars_to_public(rec.get("weekly") or []),
        "forming_daily": bars_to_public([forming_d])[0] if forming_d else None,
        "forming_weekly": bars_to_public([forming_w])[0] if forming_w else None,
        "range_52w": rec.get("range_52w"),
        "history": rec.get("history"),
        "error": rec.get("error"),
        "price_scale": rec.get("price_scale"),
    }
