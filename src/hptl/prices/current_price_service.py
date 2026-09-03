"""Canonical Current Price Service.

OANDA is the preferred provider. Instruments unavailable on the configured
OANDA account are resolved through explicit alternate-provider mappings instead
of being left unmapped. All callers continue to use this module as the single
current-price interface.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from hptl.config import PROCESSED_DIR, get_oanda_api_key
from hptl.markets.instrument_registry import all_instrument_ids, load_registry
from hptl.oanda.oanda_client import OandaApiError
from hptl.oanda.oanda_prices import fetch_pricing
from hptl.prices.alternate_provider_mappings import (
    ALTERNATE_PROVIDER_MAPPINGS,
    PROVIDER_FRED,
    PROVIDER_YAHOO,
)
from hptl.prices.models import PriceSnapshot
from hptl.prices.oanda_instrument_discovery import (
    PROVIDER_OANDA,
    _candidate_symbols,
    _currency_from_symbol,
    load_discovery,
)

CURRENT_PRICE_STALE_SECONDS = 60
STATUS_LIVE = "LIVE"
STATUS_STALE = "STALE"
STATUS_FALLBACK = "FALLBACK"
STATUS_UNAVAILABLE = "UNAVAILABLE"


@dataclass
class InstrumentMapping:
    internal_key: str
    display_name: str
    provider: str | None
    provider_symbol: str | None
    asset_type: str | None
    currency: str | None
    price_precision: int | None
    supports_streaming: bool
    tradeable: bool = True

    @property
    def is_mapped(self) -> bool:
        return bool(self.provider and self.provider_symbol)


@dataclass
class CurrentPrice:
    internal_key: str
    display_name: str
    provider: str | None
    provider_symbol: str | None
    asset_type: str | None
    currency: str | None
    price_precision: int | None
    timestamp: str | None
    bid: float | None
    ask: float | None
    mid: float | None
    status: str
    age_seconds: float | None
    tradeable: bool
    fallback_close: float | None = None
    fallback_source: str | None = None
    note: str | None = None

    @property
    def current_price(self) -> float | None:
        if self.mid is not None and self.status in (STATUS_LIVE, STATUS_STALE):
            return self.mid
        return self.fallback_close

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["current_price"] = self.current_price
        return d


QuoteSource = Callable[[list[str]], dict[str, PriceSnapshot]]
_quote_source: QuoteSource | None = None


def set_quote_source(source: QuoteSource | None) -> None:
    global _quote_source
    _quote_source = source


def _default_quote_source(symbols: list[str]) -> dict[str, PriceSnapshot]:
    if not symbols or not get_oanda_api_key():
        return {}
    try:
        return fetch_pricing(symbols)
    except OandaApiError:
        return {}


def _active_quote_source() -> QuoteSource:
    return _quote_source or _default_quote_source


_MAPPING_CACHE: dict[str, InstrumentMapping] | None = None


def _mapping_from_discovery(row: dict[str, Any]) -> InstrumentMapping:
    return InstrumentMapping(
        internal_key=row.get("internal_key"),
        display_name=row.get("display_name") or row.get("internal_key"),
        provider=row.get("provider"),
        provider_symbol=row.get("provider_symbol"),
        asset_type=row.get("asset_type"),
        currency=row.get("currency"),
        price_precision=row.get("price_precision"),
        supports_streaming=bool(row.get("supports_streaming")),
        tradeable=bool(row.get("tradeable", True)),
    )


def _mapping_from_registry() -> dict[str, InstrumentMapping]:
    reg = load_registry()
    out: dict[str, InstrumentMapping] = {}
    for iid in all_instrument_ids(tradeable_only=True):
        spec = reg.get(iid)
        if not spec:
            continue
        candidates = _candidate_symbols(spec, reg)
        symbol = candidates[0] if candidates else None
        out[iid] = InstrumentMapping(
            internal_key=iid,
            display_name=spec.display_name or iid,
            provider=PROVIDER_OANDA if symbol else None,
            provider_symbol=symbol,
            asset_type=spec.asset_class,
            currency=_currency_from_symbol(symbol),
            price_precision=None,
            supports_streaming=bool(symbol),
            tradeable=bool(spec.tradeable),
        )
    return out


def _overlay_fred_mappings(mappings: dict[str, InstrumentMapping]) -> dict[str, InstrumentMapping]:
    try:
        from hptl.prices.fred_prices import FRED_INSTRUMENT_SERIES
    except Exception:
        return mappings
    out = dict(mappings)
    for iid, series_id in FRED_INSTRUMENT_SERIES.items():
        existing = out.get(iid)
        if existing and existing.is_mapped and existing.provider == PROVIDER_OANDA:
            continue
        out[iid] = InstrumentMapping(
            internal_key=iid,
            display_name=existing.display_name if existing else iid,
            provider=PROVIDER_FRED,
            provider_symbol=series_id,
            asset_type=existing.asset_type if existing else "macro",
            currency=existing.currency if existing and existing.currency else "USD",
            price_precision=4,
            supports_streaming=False,
            tradeable=existing.tradeable if existing else True,
        )
    return out


def _overlay_alternate_mappings(mappings: dict[str, InstrumentMapping]) -> dict[str, InstrumentMapping]:
    """Fill every known OANDA gap with an explicit provider mapping.

    A verified OANDA mapping always wins. Yahoo mappings intentionally supersede
    old FRED commodity fallbacks because they provide an actual market quote.
    """
    out = dict(mappings)
    for iid, alt in ALTERNATE_PROVIDER_MAPPINGS.items():
        existing = out.get(iid)
        if existing and existing.is_mapped and existing.provider == PROVIDER_OANDA:
            continue
        provider = str(alt["provider"])
        out[iid] = InstrumentMapping(
            internal_key=iid,
            display_name=existing.display_name if existing else iid,
            provider=provider,
            provider_symbol=str(alt["provider_symbol"]),
            asset_type=existing.asset_type if existing else "macro",
            currency=alt.get("currency") or (existing.currency if existing else None),
            price_precision=alt.get("price_precision"),
            supports_streaming=False,
            tradeable=existing.tradeable if existing else True,
        )
    return out


def load_instrument_mappings(*, refresh: bool = False) -> dict[str, InstrumentMapping]:
    global _MAPPING_CACHE
    if _MAPPING_CACHE is not None and not refresh:
        return _MAPPING_CACHE
    doc = load_discovery()
    if doc and isinstance(doc.get("instruments"), dict):
        mappings = {key: _mapping_from_discovery(row) for key, row in doc["instruments"].items()}
    else:
        mappings = _mapping_from_registry()
    mappings = _overlay_fred_mappings(mappings)
    _MAPPING_CACHE = _overlay_alternate_mappings(mappings)
    return _MAPPING_CACHE


def mapping_source() -> str:
    return "discovery+alternate_providers" if load_discovery() else "registry_fallback+alternate_providers"


_PRICE_STORE_CACHE: dict[str, Any] | None = None
_WS_OHLC_CACHE: dict[str, Any] | None = None


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _price_store() -> dict[str, Any]:
    global _PRICE_STORE_CACHE
    if _PRICE_STORE_CACHE is None:
        try:
            from hptl.prices.price_store import load_price_store
            _PRICE_STORE_CACHE = load_price_store() or {}
        except Exception:
            _PRICE_STORE_CACHE = {}
    return _PRICE_STORE_CACHE


def _ws_ohlc() -> dict[str, Any]:
    global _WS_OHLC_CACHE
    if _WS_OHLC_CACHE is None:
        import json
        path = PROCESSED_DIR / "workstation_ohlc_latest.json"
        if path.is_file():
            try:
                _WS_OHLC_CACHE = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                _WS_OHLC_CACHE = {}
        else:
            _WS_OHLC_CACHE = {}
    return _WS_OHLC_CACHE


def _trusted_close(internal_key: str) -> tuple[float | None, str | None]:
    rec = (_price_store().get("instruments") or {}).get(internal_key) or {}
    price = rec.get("price") or {}
    px = _num(price.get("mid")) or _num(price.get("bid")) or _num(price.get("ask"))
    if px is not None:
        return px, "price_store.snapshot"
    daily = rec.get("daily") or []
    if daily:
        px = _num(daily[-1].get("close"))
        if px is not None:
            return px, "price_store.daily_close"
    block = (_ws_ohlc().get("instruments") or {}).get(internal_key) or {}
    weekly = block.get("weekly_ohlc") or []
    if weekly:
        px = _num(weekly[-1].get("close"))
        if px is not None:
            return px, "workstation_ohlc.weekly_close"
    return None, None


def latest_trusted_close(internal_key: str) -> tuple[float | None, str | None]:
    return _trusted_close(internal_key)


def reset_caches() -> None:
    global _MAPPING_CACHE, _PRICE_STORE_CACHE, _WS_OHLC_CACHE
    _MAPPING_CACHE = None
    _PRICE_STORE_CACHE = None
    _WS_OHLC_CACHE = None


def _parse_age_seconds(as_of: str | None) -> float | None:
    if not as_of:
        return None
    text = str(as_of).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            head = text.split("+")[0].split(".")[0]
            dt = datetime.fromisoformat(head).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())


def _build_current_price(mapping: InstrumentMapping, snap: PriceSnapshot | None, *, allow_fallback: bool) -> CurrentPrice:
    bid = _num((snap or {}).get("bid"))
    ask = _num((snap or {}).get("ask"))
    mid = _num((snap or {}).get("mid"))
    if mid is None and bid is not None and ask is not None:
        mid = (bid + ask) / 2.0
    as_of = (snap or {}).get("as_of")
    age = _parse_age_seconds(as_of)
    fallback_close: float | None = None
    fallback_source: str | None = None
    note: str | None = None

    if mid is not None:
        status = STATUS_LIVE if age is not None and age <= CURRENT_PRICE_STALE_SECONDS else STATUS_STALE
        if status == STATUS_STALE:
            note = "provider quote older than stale threshold" if age is not None else "quote age unknown"
    else:
        status = STATUS_UNAVAILABLE
        note = "no provider quote"

    if status in (STATUS_UNAVAILABLE, STATUS_STALE) and allow_fallback:
        fallback_close, fallback_source = _trusted_close(mapping.internal_key)
        if status == STATUS_UNAVAILABLE and fallback_close is not None:
            status = STATUS_FALLBACK
            note = "no provider quote; using latest trusted close"

    return CurrentPrice(
        internal_key=mapping.internal_key,
        display_name=mapping.display_name,
        provider=mapping.provider,
        provider_symbol=mapping.provider_symbol,
        asset_type=mapping.asset_type,
        currency=mapping.currency,
        price_precision=mapping.price_precision,
        timestamp=as_of,
        bid=bid,
        ask=ask,
        mid=mid,
        status=status,
        age_seconds=age,
        tradeable=mapping.tradeable,
        fallback_close=fallback_close,
        fallback_source=fallback_source,
        note=note,
    )


def _fetch_fred_quotes(mappings: list[InstrumentMapping]) -> dict[str, PriceSnapshot]:
    """Fetch latest FRED observations by internal key, fail-closed per series."""
    out: dict[str, PriceSnapshot] = {}
    try:
        from hptl.macro import fred_client
    except Exception:
        return out
    for mapping in mappings:
        if not mapping.provider_symbol:
            continue
        try:
            df = fred_client.get_series_df(mapping.provider_symbol, "2025-01-01")
            if df is None or df.empty:
                continue
            row = df.iloc[-1]
            value = _num(row.get("value"))
            if value is None:
                continue
            dt = datetime.fromisoformat(str(row.get("date"))[:10]).replace(tzinfo=timezone.utc)
            out[mapping.provider_symbol] = {"mid": value, "bid": None, "ask": None, "as_of": dt.isoformat()}
        except Exception:
            continue
    return out


def get_current_prices(keys: list[str] | None = None, *, fetch: bool = True, allow_fallback: bool = True) -> dict[str, CurrentPrice]:
    mappings = load_instrument_mappings()
    registry_keys = list(all_instrument_ids())
    selected = list(dict.fromkeys([*registry_keys, *mappings.keys()])) if keys is None else list(keys)

    oanda_symbols = sorted({
        mappings[k].provider_symbol for k in selected
        if k in mappings and mappings[k].is_mapped and mappings[k].provider == PROVIDER_OANDA and mappings[k].provider_symbol
    })
    yahoo_symbols = sorted({
        mappings[k].provider_symbol for k in selected
        if k in mappings and mappings[k].is_mapped and mappings[k].provider == PROVIDER_YAHOO and mappings[k].provider_symbol
    })
    fred_mappings = [
        mappings[k] for k in selected
        if k in mappings and mappings[k].is_mapped and mappings[k].provider == PROVIDER_FRED
    ]

    quotes: dict[str, PriceSnapshot] = {}
    if fetch and oanda_symbols:
        quotes.update(_active_quote_source()(oanda_symbols) or {})
    if fetch and yahoo_symbols:
        try:
            from hptl.prices.yahoo_quotes import fetch_yahoo_quotes
            quotes.update(fetch_yahoo_quotes(yahoo_symbols))
        except Exception:
            pass
    if fetch and fred_mappings:
        quotes.update(_fetch_fred_quotes(fred_mappings))

    out: dict[str, CurrentPrice] = {}
    for key in selected:
        mapping = mappings.get(key)
        if mapping is None:
            mapping = InstrumentMapping(key, key, None, None, None, None, None, False)
        snap = quotes.get(mapping.provider_symbol or "") if mapping.is_mapped else None
        out[key] = _build_current_price(mapping, snap, allow_fallback=allow_fallback)
    return out


def get_current_price(key: str, *, fetch: bool = True, allow_fallback: bool = True) -> CurrentPrice | None:
    return get_current_prices([key], fetch=fetch, allow_fallback=allow_fallback).get(key)
