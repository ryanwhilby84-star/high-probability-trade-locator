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
    """Compute trailing 52-week high/low from normalized daily bars."""
    if not daily:
        return None
    recent = daily[-260:]
    highs = [float(b["high"]) for b in recent if b.get("high") is not None]
    lows = [float(b["low"]) for b in recent if b.get("low") is not None]
    if not highs or not lows:
        return None
    return {
        "high": max(highs),
        "low": min(lows),
        "as_of": recent[-1]["date"],
        "start_date": recent[0]["date"],
        "end_date": recent[-1]["date"],
    }


def build_history_meta(
    daily: list[OhlcBar], weekly: list[OhlcBar], range_52w: Range52w | None
) -> PriceHistoryMeta:
    return {
        "range_52w": range_52w,
        "daily_from": daily[0]["date"] if daily else None,
        "daily_to": daily[-1]["date"] if daily else None,
        "weekly_from": weekly[0]["date"] if weekly else None,
        "weekly_to": weekly[-1]["date"] if weekly else None,
        "bar_count_daily": len(daily),
        "bar_count_weekly": len(weekly),
    }
