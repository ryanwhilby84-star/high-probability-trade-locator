"""Attach FX yield-differential valuation fields to confluence records (LEGACY V1).

.. warning::
    **LEGACY confluence attach only** — not the dashboard valuation pillar.
    Prefer ``hptl.pillars.confluence_attach`` + ``valuation_latest.json`` (V3)
    for pillar fields on new work.

Mirrors ``hptl.pillars.confluence_attach`` — returns flat engine scalars plus a
nested ``fx_valuation`` audit block for any market that resolves to a supported
FX pair, and ``{}`` otherwise (so non-FX rows are untouched).
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from hptl.fx.currency_map import COT_CURRENCY_SOURCES
from hptl.fx.fx_valuation import resolve_pair_currencies
from hptl.fx.fx_institutional_valuation import value_fx_pair_institutional
from hptl.prices.price_store import load_price_store

# Canonical USD pair id -> legacy COT market id holding its spot (e.g. "EUR/USD" -> "Euro FX / 6E").
_COT_MARKET_FOR_PAIR: dict[str, str] = {
    str(spec.get("quote")): str(spec.get("market")) for spec in COT_CURRENCY_SOURCES.values()
}
# Currency -> (canonical USD pair, orientation pair id) for cross synthesis.
_USD_PAIR_FOR_CCY: dict[str, str] = {
    code: str(spec.get("quote")) for code, spec in COT_CURRENCY_SOURCES.items()
}


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


@lru_cache(maxsize=1)
def _price_instruments() -> dict[str, Any]:
    return load_price_store().get("instruments") or {}


def _record_for_pair(pair_id: str) -> dict[str, Any]:
    """Price record for a canonical pair, falling back to its COT-major instrument."""
    instruments = _price_instruments()
    rec = instruments.get(pair_id)
    if rec:
        return rec
    cot_market = _COT_MARKET_FOR_PAIR.get(pair_id)
    if cot_market:
        return instruments.get(cot_market) or {}
    return {}


def _spot_from_record(rec: dict[str, Any]) -> float | None:
    price = rec.get("price") or {}
    spot = None
    if isinstance(price, dict):
        spot = _num(price.get("mid")) or _num(price.get("bid")) or _num(price.get("ask"))
    if spot is None:
        weekly = rec.get("weekly") or []
        closes = [c for c in (_num(b.get("close")) for b in weekly if isinstance(b, dict)) if c is not None]
        if closes:
            spot = closes[-1]
    return spot


def _usd_per_unit(code: str) -> float | None:
    """USD value of one unit of ``code`` (USD itself = 1.0). Used for cross synthesis."""
    code = code.upper()
    if code == "USD":
        return 1.0
    usd_pair = _USD_PAIR_FOR_CCY.get(code)
    if not usd_pair:
        return None
    spot = _spot_from_record(_record_for_pair(usd_pair))
    if spot is None or spot == 0:
        return None
    # "XXX/USD" => USD per XXX is the spot; "USD/XXX" => USD per XXX is 1/spot.
    return spot if usd_pair.startswith(f"{code}/") else 1.0 / spot


def _spot_and_percentile(pair_id: str) -> tuple[float | None, float | None]:
    """Latest spot + 52-week price percentile for a canonical pair id.

    Resolution order: direct instrument -> COT-major instrument -> synthesize a
    cross from the two USD legs (so e.g. EUR/JPY = EUR/USD * USD/JPY).
    """
    rec = _record_for_pair(pair_id)
    spot = _spot_from_record(rec)

    if spot is None and "/" in pair_id:
        base, quote = pair_id.split("/", 1)
        base_usd, quote_usd = _usd_per_unit(base), _usd_per_unit(quote)
        if base_usd and quote_usd:
            spot = round(base_usd / quote_usd, 6)

    weekly = rec.get("weekly") or []
    closes = [c for c in (_num(b.get("close")) for b in weekly if isinstance(b, dict)) if c is not None]
    pctl = None
    if len(closes) >= 12:
        window = closes[-52:] if len(closes) >= 52 else closes
        current = window[-1]
        pctl = round(sum(1 for c in window if c <= current) / len(window) * 100.0, 1)
    return spot, pctl


def fx_valuation_fields_for_market(market: str, *, config_path: str | None = None) -> dict[str, Any]:
    """Return ``fx_*`` scalars + ``fx_valuation`` block, or ``{}`` if not an FX pair."""
    resolved = resolve_pair_currencies(market)
    if not resolved:
        return {}
    base, quote, pair_id = resolved
    spot, _pctl = _spot_and_percentile(pair_id)
    val = value_fx_pair_institutional(
        base,
        quote,
        spot=spot,
        config_path=config_path,
    )
    fields = dict(val.engine_fields())
    fields["fx_valuation"] = val.as_block()
    return fields


def clear_cache() -> None:
    _price_instruments.cache_clear()
