"""Small dependency-free Yahoo chart quote adapter.

This is a secondary current-price provider for instruments the configured OANDA
account cannot supply.  It deliberately uses the chart endpoint rather than the
crumb/cookie quote endpoint and fails closed: provider/network errors simply
return no quote so Current Price Service can use its trusted fallback chain.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.request import Request, urlopen

from hptl.prices.models import PriceSnapshot

_YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1m&includePrePost=true"
_USER_AGENT = "Mozilla/5.0 HPTL-Current-Price/1.0"


def _fetch_one(symbol: str, timeout: float = 8.0) -> tuple[str, PriceSnapshot | None]:
    url = _YAHOO_CHART.format(symbol=quote(symbol, safe=""))
    req = Request(url, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
    try:
        with urlopen(req, timeout=timeout) as resp:
            doc = json.loads(resp.read().decode("utf-8"))
        result = ((doc.get("chart") or {}).get("result") or [None])[0] or {}
        meta = result.get("meta") or {}
        price = meta.get("regularMarketPrice")
        ts = meta.get("regularMarketTime")
        if price is None:
            closes = (((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or [])
            timestamps = result.get("timestamp") or []
            for idx in range(len(closes) - 1, -1, -1):
                if closes[idx] is not None:
                    price = closes[idx]
                    if idx < len(timestamps):
                        ts = timestamps[idx]
                    break
        if price is None:
            return symbol, None
        as_of = (
            datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
            if ts is not None
            else datetime.now(timezone.utc).isoformat()
        )
        return symbol, {"mid": float(price), "bid": None, "ask": None, "as_of": as_of}
    except Exception:  # noqa: BLE001 - secondary provider must fail closed
        return symbol, None


def fetch_yahoo_quotes(symbols: list[str]) -> dict[str, PriceSnapshot]:
    """Fetch current-ish regular market prices for unique Yahoo symbols."""
    unique = sorted({s for s in symbols if s})
    if not unique:
        return {}
    out: dict[str, PriceSnapshot] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(unique))) as pool:
        futures = [pool.submit(_fetch_one, symbol) for symbol in unique]
        for future in as_completed(futures):
            symbol, snap = future.result()
            if snap is not None:
                out[symbol] = snap
    return out
