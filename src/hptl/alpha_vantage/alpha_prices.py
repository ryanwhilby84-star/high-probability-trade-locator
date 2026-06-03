"""Alpha Vantage price + OHLC fetch (normalized)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from hptl.alpha_vantage.client import AlphaVantageApiError, _get
from hptl.alpha_vantage.mappings import AlphaVantageMapping, resolve_alpha_mapping
from hptl.markets.instrument_registry import InstrumentSpec
from hptl.prices.models import DAILY_BAR_TARGET, WEEKLY_BAR_TARGET, OhlcBar, PriceSnapshot

import os


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_av_daily_series(doc: dict[str, Any]) -> list[OhlcBar]:
    """TIME_SERIES_* or FX daily keyed objects."""
    series_key = next((k for k in doc if "Time Series" in k), None)
    if not series_key:
        return []
    series = doc[series_key]
    if not isinstance(series, dict):
        return []
    out: list[OhlcBar] = []
    for date_str, row in series.items():
        if not isinstance(row, dict):
            continue
        o = _float(row.get("1. open") or row.get("1. Open") or row.get("open"))
        h = _float(row.get("2. high") or row.get("2. High") or row.get("high"))
        l = _float(row.get("3. low") or row.get("3. Low") or row.get("low"))
        c = _float(row.get("4. close") or row.get("4. Close") or row.get("close"))
        if o is None or c is None:
            continue
        out.append(
            {
                "date": date_str[:10],
                "open": o,
                "high": h if h is not None else o,
                "low": l if l is not None else o,
                "close": c,
                "volume": _float(row.get("5. volume") or row.get("6. volume")),
            }
        )
    out.sort(key=lambda b: b["date"])
    return out[-DAILY_BAR_TARGET:]


def _parse_commodity_data(doc: dict[str, Any]) -> list[OhlcBar]:
    data = doc.get("data")
    if not isinstance(data, list):
        return []
    out: list[OhlcBar] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        d = str(row.get("date") or "")[:10]
        o = _float(row.get("open"))
        h = _float(row.get("high"))
        l = _float(row.get("low"))
        c = _float(row.get("close") or row.get("value"))
        if not d or c is None:
            continue
        out.append(
            {
                "date": d,
                "open": o if o is not None else c,
                "high": h if h is not None else c,
                "low": l if l is not None else c,
                "close": c,
                "volume": None,
            }
        )
    out.sort(key=lambda b: b["date"])
    return out[-DAILY_BAR_TARGET:]


def _resample_weekly(daily: list[OhlcBar]) -> list[OhlcBar]:
    if not daily:
        return []
    buckets: dict[str, list[OhlcBar]] = {}
    for bar in daily:
        d = bar["date"]
        # ISO week key
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            wk = dt.strftime("%G-W%V")
        except ValueError:
            wk = d[:7]
        buckets.setdefault(wk, []).append(bar)
    weekly: list[OhlcBar] = []
    for wk in sorted(buckets.keys()):
        chunk = buckets[wk]
        weekly.append(
            {
                "date": chunk[-1]["date"],
                "open": chunk[0]["open"],
                "high": max(b["high"] for b in chunk),
                "low": min(b["low"] for b in chunk),
                "close": chunk[-1]["close"],
                "volume": None,
            }
        )
    return weekly[-52:]


def _fetch_fx_daily(mapping: AlphaVantageMapping) -> list[OhlcBar]:
    fc = mapping.params.get("from_currency", "")
    tc = mapping.params.get("to_currency", "")
    doc = _get("FX_DAILY", from_symbol=fc, to_symbol=tc)  # AV uses from_symbol / to_symbol
    return _parse_av_daily_series(doc)


def _fetch_fx_weekly(mapping: AlphaVantageMapping) -> list[OhlcBar]:
    fc = mapping.params.get("from_currency", "")
    tc = mapping.params.get("to_currency", "")
    try:
        doc = _get("FX_WEEKLY", from_symbol=fc, to_symbol=tc)
        bars = _parse_av_daily_series(doc)
        if bars:
            return bars[-52:]
    except AlphaVantageApiError:
        pass
    return _resample_weekly(_fetch_fx_daily(mapping))


def _fetch_commodity_daily(mapping: AlphaVantageMapping) -> list[OhlcBar]:
    doc = _get(mapping.function, **mapping.params)
    return _parse_commodity_data(doc)


def _fetch_equity_daily(mapping: AlphaVantageMapping, *, outputsize: str = "compact") -> list[OhlcBar]:
    """Free-tier AV allows compact (100 bars); full requires premium and often rate-limits."""
    sym = mapping.params.get("symbol", mapping.symbol)
    doc = _get("TIME_SERIES_DAILY", symbol=sym, outputsize=outputsize)
    return _parse_av_daily_series(doc)


def _fetch_equity_weekly(mapping: AlphaVantageMapping) -> list[OhlcBar]:
    sym = mapping.params.get("symbol", mapping.symbol)
    for outputsize in ("compact", "full"):
        try:
            doc = _get("TIME_SERIES_WEEKLY", symbol=sym, outputsize=outputsize)
            bars = _parse_av_daily_series(doc)
            if bars:
                return bars[-WEEKLY_BAR_TARGET:]
        except AlphaVantageApiError:
            continue
    return []


def _fetch_crypto_daily(mapping: AlphaVantageMapping) -> list[OhlcBar]:
    sym = mapping.params.get("symbol", "BTC")
    market = mapping.params.get("market", "USD")
    doc = _get("DIGITAL_CURRENCY_DAILY", symbol=sym, market=market)
    series_key = next((k for k in doc if "Time Series" in k), None)
    if not series_key:
        return []
    return _parse_av_daily_series({series_key: doc[series_key]})


def _fetch_treasury_daily(mapping: AlphaVantageMapping) -> list[OhlcBar]:
    doc = _get(mapping.function, **mapping.params)
    data = doc.get("data")
    if not isinstance(data, list):
        return []
    out: list[OhlcBar] = []
    for row in data:
        d = str(row.get("date") or "")[:10]
        v = _float(row.get("value"))
        if d and v is not None:
            out.append({"date": d, "open": v, "high": v, "low": v, "close": v, "volume": None})
    out.sort(key=lambda b: b["date"])
    return out[-DAILY_BAR_TARGET:]


def _fetch_spot_price(mapping: AlphaVantageMapping, daily: list[OhlcBar]) -> PriceSnapshot | None:
    now = datetime.now(timezone.utc).isoformat()
    if daily:
        last = daily[-1]
        return {"mid": last["close"], "bid": None, "ask": None, "as_of": last["date"]}
    if mapping.category == "fx":
        try:
            doc = _get(
                "CURRENCY_EXCHANGE_RATE",
                from_currency=mapping.params.get("from_currency", ""),
                to_currency=mapping.params.get("to_currency", ""),
            )
            rate = doc.get("Realtime Currency Exchange Rate") or {}
            mid = _float(rate.get("5. Exchange Rate"))
            if mid is not None:
                return {"mid": mid, "bid": None, "ask": None, "as_of": str(rate.get("6. Last Refreshed") or now)[:32]}
        except AlphaVantageApiError:
            return None
    if mapping.category in {"index", "rates"} and mapping.params.get("symbol"):
        try:
            doc = _get("GLOBAL_QUOTE", symbol=mapping.params["symbol"])
            q = doc.get("Global Quote") or {}
            mid = _float(q.get("05. price"))
            if mid is not None:
                return {"mid": mid, "bid": None, "ask": None, "as_of": str(q.get("07. latest trading day") or now)[:32]}
        except AlphaVantageApiError:
            return None
    return None


def fetch_instrument_prices(spec: InstrumentSpec) -> tuple[PriceSnapshot | None, list[OhlcBar], list[OhlcBar]]:
    mapping = resolve_alpha_mapping(spec)
    if mapping is None:
        raise AlphaVantageApiError(f"No Alpha Vantage mapping for {spec.id}", function="")

    delay = float(os.getenv("ALPHA_VANTAGE_PRICE_DELAY_SEC", "12"))
    if delay > 0:
        import time

        time.sleep(delay)

    if mapping.category == "fx":
        daily = _fetch_fx_daily(mapping)
        weekly = _fetch_fx_weekly(mapping)
    elif mapping.category == "commodity":
        daily = _fetch_commodity_daily(mapping)
        weekly = _resample_weekly(daily)
    elif mapping.category == "crypto":
        daily = _fetch_crypto_daily(mapping)
        weekly = _resample_weekly(daily)
    elif mapping.category == "index":
        weekly = _fetch_equity_weekly(mapping)
        daily: list[OhlcBar] = []
        try:
            daily = _fetch_equity_daily(mapping)
        except AlphaVantageApiError:
            daily = []
        if weekly and not daily:
            daily = list(weekly)
        elif not weekly and daily:
            weekly = _resample_weekly(daily)
    elif mapping.category == "rates":
        daily = _fetch_treasury_daily(mapping)
        weekly = _resample_weekly(daily)
    else:
        daily = []
        weekly = []

    price = _fetch_spot_price(mapping, daily)
    return price, daily, weekly
