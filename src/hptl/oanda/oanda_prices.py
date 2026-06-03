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


def _parse_candles(payload: dict[str, Any]) -> list[OhlcBar]:
    out: list[OhlcBar] = []
    for c in payload.get("candles") or []:
        if not c.get("complete", True):
            continue
        mid = c.get("mid") or c.get("bid") or c.get("ask") or {}
        t = str(c.get("time") or "")[:10]
        if not t:
            continue
        o = _float(mid.get("o"))
        h = _float(mid.get("h"))
        l = _float(mid.get("l"))
        cl = _float(mid.get("c"))
        if o is None or h is None or l is None or cl is None:
            continue
        out.append(
            {
                "date": t,
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": _float(c.get("volume")),
            }
        )
    out.sort(key=lambda b: b["date"])
    return out


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


def fetch_instrument_prices(symbol: str) -> tuple[PriceSnapshot | None, list[OhlcBar], list[OhlcBar]]:
    """Current price + daily + weekly candles for one OANDA instrument."""
    try:
        pricing = fetch_pricing([symbol])
        price = pricing.get(symbol)
        daily = fetch_candles(symbol, granularity="D", count=DAILY_BAR_TARGET)
        weekly = fetch_candles(symbol, granularity="W", count=WEEKLY_BAR_TARGET)
        return price, daily, weekly
    except OandaApiError:
        raise
