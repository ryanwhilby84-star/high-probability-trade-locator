"""OANDA v20 pricing and candle fetch (normalized OHLC)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from hptl.oanda.oanda_client import OandaApiError, api_get, resolve_account_id
from hptl.prices.models import DAILY_BAR_TARGET, WEEKLY_BAR_TARGET, OhlcBar, PriceSnapshot

_GRANULARITY = {"D": "D", "W": "W", "daily": "D", "weekly": "W"}


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _candle_to_bar(c: dict[str, Any]) -> OhlcBar | None:
    mid = c.get("mid") or c.get("bid") or c.get("ask") or {}
    t = str(c.get("time") or "")[:10]
    if not t:
        return None
    o = _float(mid.get("o"))
    h = _float(mid.get("h"))
    l = _float(mid.get("l"))
    cl = _float(mid.get("c"))
    if o is None or h is None or l is None or cl is None:
        return None
    return {
        "date": t,
        "open": o,
        "high": h,
        "low": l,
        "close": cl,
        "volume": _float(c.get("volume")),
    }


def _parse_candles(payload: dict[str, Any]) -> tuple[list[OhlcBar], OhlcBar | None]:
    """Return completed bars plus the latest incomplete forming bar (if any)."""
    out: list[OhlcBar] = []
    forming: OhlcBar | None = None
    for c in payload.get("candles") or []:
        bar = _candle_to_bar(c)
        if bar is None:
            continue
        if c.get("complete", True):
            out.append(bar)
        else:
            # Keep the newest incomplete tip for live/forming display only.
            forming = bar
    out.sort(key=lambda b: b["date"])
    return out, forming


def fetch_candles(
    instrument: str,
    *,
    granularity: str = "D",
    count: int = DAILY_BAR_TARGET,
) -> list[OhlcBar]:
    g = _GRANULARITY.get(granularity, granularity)
    doc = api_get(
        f"/v3/instruments/{instrument}/candles",
        params={
            "granularity": g,
            "count": str(count),
            "price": "M",
        },
    )
    complete, _forming = _parse_candles(doc)
    return complete


def fetch_candles_with_forming(
    instrument: str,
    *,
    granularity: str = "D",
    count: int = DAILY_BAR_TARGET,
) -> tuple[list[OhlcBar], OhlcBar | None]:
    g = _GRANULARITY.get(granularity, granularity)
    doc = api_get(
        f"/v3/instruments/{instrument}/candles",
        params={
            "granularity": g,
            "count": str(count),
            "price": "M",
        },
    )
    return _parse_candles(doc)


def fetch_pricing(instruments: list[str]) -> dict[str, PriceSnapshot]:
    if not instruments:
        return {}
    aid = resolve_account_id()
    doc = api_get(
        f"/v3/accounts/{aid}/pricing",
        params={"instruments": ",".join(instruments)},
    )
    now = datetime.now(timezone.utc).isoformat()
    out: dict[str, PriceSnapshot] = {}
    for row in doc.get("prices") or []:
        name = str(row.get("instrument") or "").strip()
        if not name:
            continue
        bids = row.get("bids") or []
        asks = row.get("asks") or []
        bid = _float(bids[0].get("price")) if bids else None
        ask = _float(asks[0].get("price")) if asks else None
        mid = None
        if bid is not None and ask is not None:
            mid = (bid + ask) / 2.0
        elif bid is not None:
            mid = bid
        elif ask is not None:
            mid = ask
        out[name] = {
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "as_of": str(row.get("time") or now)[:32],
        }
    return out


def fetch_instrument_prices(
    symbol: str,
) -> tuple[PriceSnapshot | None, list[OhlcBar], list[OhlcBar], OhlcBar | None, OhlcBar | None]:
    """Current price + completed daily/weekly candles + forming (incomplete) tips."""
    try:
        pricing = fetch_pricing([symbol])
        price = pricing.get(symbol)
        daily, forming_daily = fetch_candles_with_forming(
            symbol, granularity="D", count=DAILY_BAR_TARGET
        )
        weekly, forming_weekly = fetch_candles_with_forming(
            symbol, granularity="W", count=WEEKLY_BAR_TARGET
        )
        return price, daily, weekly, forming_daily, forming_weekly
    except OandaApiError:
        raise
