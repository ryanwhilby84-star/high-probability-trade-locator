"""Canonical Current Price Service.

Single shared source of the *current* market price for the whole HPTL platform
(dashboard, valuation, scanner, alerts, future features). No other module should
fetch live prices independently — they call :func:`get_current_price` /
:func:`get_current_prices` here.

Design notes
------------
* Mappings come from OANDA instrument discovery
  (``data/config/current_price_instruments.json``). If that file is absent the
  service degrades to a registry-derived mapping so it still works before the
  first discovery run.
* Prices keep FULL floating-point precision. ``mid = (bid + ask) / 2``. We never
  round here; ``price_precision`` is carried through for the display layer only.
* ``status`` is derived from quote *age*, never from file existence:
      LIVE       — live quote, age <= CURRENT_PRICE_STALE_SECONDS
      STALE      — live quote present but too old (or age unknown)
      FALLBACK   — no usable live quote; latest trusted close substituted
      UNAVAILABLE— no live quote and no trusted close
* The quote source is pluggable via :func:`set_quote_source`. Phase 1 uses OANDA
  REST batch pricing; Phase 2 will inject the persistent streaming cache without
  changing any caller.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from hptl.config import PROCESSED_DIR, get_oanda_api_key
from hptl.markets.instrument_registry import all_instrument_ids, load_registry
from hptl.oanda.oanda_client import OandaApiError
from hptl.oanda.oanda_prices import fetch_pricing
from hptl.prices.models import PriceSnapshot
from hptl.prices.oanda_instrument_discovery import (
    PROVIDER_OANDA,
    _candidate_symbols,
    _currency_from_symbol,
    load_discovery,
)

# Quote is considered stale beyond this age. Mirrors the frontend threshold
# (LIVE_QUOTE_STALE_MS = 60_000) so backend and UI agree on LIVE vs STALE.
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
        """The price callers should use: live mid, else trusted fallback close."""
        if self.mid is not None and self.status in (STATUS_LIVE, STATUS_STALE):
            return self.mid
        return self.fallback_close

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["current_price"] = self.current_price
        return d


# --------------------------------------------------------------------------- #
# Pluggable quote source (Phase 2 stream injects here)
# --------------------------------------------------------------------------- #

QuoteSource = Callable[[list[str]], dict[str, PriceSnapshot]]
_quote_source: QuoteSource | None = None


def set_quote_source(source: QuoteSource | None) -> None:
    """Override the live-quote source (e.g. the streaming cache in Phase 2)."""
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


# --------------------------------------------------------------------------- #
# Mappings
# --------------------------------------------------------------------------- #

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
    """Offline fallback mapping used before a discovery run has been performed.

    Provider symbols are best-effort candidates (underscore-normalised) and are
    NOT verified against the account — discovery must be run for authoritative
    mappings. Used so the service and validation script still function offline.
    """
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


def load_instrument_mappings(*, refresh: bool = False) -> dict[str, InstrumentMapping]:
    global _MAPPING_CACHE
    if _MAPPING_CACHE is not None and not refresh:
        return _MAPPING_CACHE

    doc = load_discovery()
    if doc and isinstance(doc.get("instruments"), dict):
        _MAPPING_CACHE = {
            key: _mapping_from_discovery(row)
            for key, row in doc["instruments"].items()
        }
    else:
        _MAPPING_CACHE = _mapping_from_registry()
    return _MAPPING_CACHE


def mapping_source() -> str:
    """Report whether authoritative discovery mappings or the fallback are in use."""
    return "discovery" if load_discovery() else "registry_fallback"


# --------------------------------------------------------------------------- #
# Trusted fallback closes
# --------------------------------------------------------------------------- #

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
        except Exception:  # noqa: BLE001 - store is optional for the service
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
    """Latest trusted close for FALLBACK: price store snapshot/daily, then weekly OHLC."""
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
    """Public accessor for the latest trusted close (value, source) for an instrument."""
    return _trusted_close(internal_key)


def reset_caches() -> None:
    """Drop cached mappings / stores (call after a fresh discovery or refresh)."""
    global _MAPPING_CACHE, _PRICE_STORE_CACHE, _WS_OHLC_CACHE
    _MAPPING_CACHE = None
    _PRICE_STORE_CACHE = None
    _WS_OHLC_CACHE = None


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def _parse_age_seconds(as_of: str | None) -> float | None:
    if not as_of:
        return None
    text = str(as_of).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        # Tolerate nanosecond precision (OANDA) by trimming fractional digits.
        try:
            head = text.split("+")[0].split(".")[0]
            dt = datetime.fromisoformat(head).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())


def _build_current_price(
    mapping: InstrumentMapping,
    snap: PriceSnapshot | None,
    *,
    allow_fallback: bool,
) -> CurrentPrice:
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
        if age is not None and age <= CURRENT_PRICE_STALE_SECONDS:
            status = STATUS_LIVE
        else:
            status = STATUS_STALE
            note = "live quote older than stale threshold" if age is not None else "quote age unknown"
    else:
        status = STATUS_UNAVAILABLE
        note = "no live quote"

    if status in (STATUS_UNAVAILABLE, STATUS_STALE) and allow_fallback:
        fallback_close, fallback_source = _trusted_close(mapping.internal_key)
        if status == STATUS_UNAVAILABLE and fallback_close is not None:
            status = STATUS_FALLBACK
            note = "no live quote; using latest trusted close"

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


def get_current_prices(
    keys: list[str] | None = None,
    *,
    fetch: bool = True,
    allow_fallback: bool = True,
) -> dict[str, CurrentPrice]:
    """Current price for each requested instrument (all mapped instruments if None)."""
    mappings = load_instrument_mappings()
    selected = keys if keys is not None else list(mappings.keys())

    symbols = sorted(
        {
            mappings[k].provider_symbol
            for k in selected
            if k in mappings and mappings[k].is_mapped and mappings[k].provider_symbol
        }
    )

    quotes: dict[str, PriceSnapshot] = {}
    if fetch and symbols:
        quotes = _active_quote_source()(symbols) or {}

    out: dict[str, CurrentPrice] = {}
    for key in selected:
        mapping = mappings.get(key)
        if mapping is None:
            mapping = InstrumentMapping(
                internal_key=key,
                display_name=key,
                provider=None,
                provider_symbol=None,
                asset_type=None,
                currency=None,
                price_precision=None,
                supports_streaming=False,
            )
        snap = quotes.get(mapping.provider_symbol or "") if mapping.is_mapped else None
        out[key] = _build_current_price(mapping, snap, allow_fallback=allow_fallback)
    return out


def get_current_price(
    key: str,
    *,
    fetch: bool = True,
    allow_fallback: bool = True,
) -> CurrentPrice | None:
    """Current price for one instrument (None if the key is unknown)."""
    result = get_current_prices([key], fetch=fetch, allow_fallback=allow_fallback)
    return result.get(key)
