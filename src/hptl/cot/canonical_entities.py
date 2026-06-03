"""Canonical COT entities — ONE clean entity per real CFTC futures market.

COT is futures-leg data, not pair-level data. Every registry instrument resolves to a
``cot_status`` describing HOW (if at all) it relates to a canonical COT entity:

    direct_cot       — the instrument IS a canonical CFTC futures market with valid rows
    leg_derived_cot  — FX pair; positioning comes from its two currency-leg entities
    proxy_cot        — references one canonical entity as an approximate proxy (e.g. WTI→CL)
    macro_only       — no COT; macro transmission only
    no_cot_available — no COT and no usable proxy/leg
    broken_mapping   — claims a direct mapping but no valid canonical rows exist
    invalid_data     — mapped rows exist but all fail integrity validation

This module does NOT duplicate COT tables across pairs. Pairs reference leg entities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from hptl.markets.instrument_registry import InstrumentSpec, get_instrument, load_registry

# Canonical CFTC futures entities actually present in the normalized COT master.
# id -> (display, asset_class). These are the ONLY markets that own a COT table.
CANONICAL_COT_ENTITIES: Final[dict[str, dict[str, str]]] = {
    "NASDAQ / NQ": {"display": "Nasdaq 100 (CFTC)", "asset_class": "indices"},
    "S&P 500 / ES": {"display": "S&P 500 (CFTC)", "asset_class": "indices"},
    "Dow / YM": {"display": "Dow (CFTC)", "asset_class": "indices"},
    "Euro FX / 6E": {"display": "Euro FX leg (CFTC)", "asset_class": "fx"},
    "British Pound / 6B": {"display": "British Pound leg (CFTC)", "asset_class": "fx"},
    "Japanese Yen / 6J": {"display": "Japanese Yen leg (CFTC)", "asset_class": "fx"},
    "Swiss Franc / 6S": {"display": "Swiss Franc leg (CFTC)", "asset_class": "fx"},
    "Australian Dollar / 6A": {"display": "Australian Dollar leg (CFTC)", "asset_class": "fx"},
    "Canadian Dollar / 6C": {"display": "Canadian Dollar leg (CFTC)", "asset_class": "fx"},
    "NZ Dollar / 6N": {"display": "New Zealand Dollar leg (CFTC)", "asset_class": "fx"},
    "Gold": {"display": "Gold (CFTC)", "asset_class": "metals"},
    "Silver": {"display": "Silver (CFTC)", "asset_class": "metals"},
    "Copper / HG": {"display": "Copper (CFTC)", "asset_class": "metals"},
    "Crude Oil / CL": {"display": "WTI Crude (CFTC)", "asset_class": "commodities"},
    "Natural Gas / NG": {"display": "Natural Gas (CFTC)", "asset_class": "commodities"},
    "Coffee": {"display": "Coffee (CFTC)", "asset_class": "commodities"},
    "Cocoa": {"display": "Cocoa (CFTC)", "asset_class": "commodities"},
    "Corn": {"display": "Corn (CFTC)", "asset_class": "commodities"},
    "Wheat": {"display": "Wheat (CFTC)", "asset_class": "commodities"},
    "Soybeans": {"display": "Soybeans (CFTC)", "asset_class": "commodities"},
    # Mapped in registry/contracts but historically absent from the master CSV → broken.
    "Sugar": {"display": "Sugar (CFTC)", "asset_class": "commodities"},
    "Platinum": {"display": "Platinum (CFTC)", "asset_class": "metals"},
    "Palladium": {"display": "Palladium (CFTC)", "asset_class": "metals"},
}

# ISO currency code -> canonical leg COT entity (futures contract).
CURRENCY_LEG_ENTITY: Final[dict[str, str]] = {
    "EUR": "Euro FX / 6E",
    "GBP": "British Pound / 6B",
    "JPY": "Japanese Yen / 6J",
    "CHF": "Swiss Franc / 6S",
    "AUD": "Australian Dollar / 6A",
    "CAD": "Canadian Dollar / 6C",
    "NZD": "NZ Dollar / 6N",
    # USD has no single CFTC contract here; it is synthesized in the relative-strength layer.
}

COT_STATUS_DIRECT = "direct_cot"
COT_STATUS_LEG = "leg_derived_cot"
COT_STATUS_PROXY = "proxy_cot"
COT_STATUS_MACRO = "macro_only"
COT_STATUS_NONE = "no_cot_available"
COT_STATUS_BROKEN = "broken_mapping"
COT_STATUS_INVALID = "invalid_data"


@dataclass(frozen=True)
class CotResolution:
    instrument_id: str
    cot_status: str
    direct_cot_market: str | None
    leg_cot_markets: list[str]
    proxy_cot_markets: list[str]
    note: str


def is_canonical_entity(market_id: str) -> bool:
    return market_id in CANONICAL_COT_ENTITIES


def parse_currency_legs(instrument_id: str) -> tuple[str, str] | None:
    """Return (base, quote) ISO codes for a 3/3 FX pair id like 'GBP/NZD'."""
    if "/" not in instrument_id:
        return None
    base, _, quote = instrument_id.partition("/")
    base, quote = base.strip().upper(), quote.strip().upper()
    if len(base) == 3 and len(quote) == 3:
        return base, quote
    return None


def leg_entities_for_pair(instrument_id: str) -> list[str]:
    """Canonical leg COT entities backing an FX pair (only legs that have a CFTC contract)."""
    legs = parse_currency_legs(instrument_id)
    if not legs:
        return []
    base, quote = legs
    out: list[str] = []
    for code in (base, quote):
        ent = CURRENCY_LEG_ENTITY.get(code)
        if ent and ent not in out:
            out.append(ent)
    return out


def resolve_cot_status(
    spec: InstrumentSpec,
    *,
    valid_entities: set[str] | None = None,
) -> CotResolution:
    """Classify how an instrument relates to canonical COT entities.

    ``valid_entities`` is the set of canonical entities that currently have at least one
    integrity-valid COT row. When provided, direct mappings without valid rows are reported
    as ``broken_mapping`` (mapping exists, no rows) or ``invalid_data`` is decided by callers
    that also know row counts. Here we distinguish broken vs direct using valid_entities only.
    """
    iid = spec.id

    # 1) Direct canonical entity
    if iid in CANONICAL_COT_ENTITIES or spec.has_cot_mapping:
        canonical = iid if iid in CANONICAL_COT_ENTITIES else iid
        if valid_entities is not None and canonical not in valid_entities:
            return CotResolution(
                instrument_id=iid,
                cot_status=COT_STATUS_BROKEN,
                direct_cot_market=canonical,
                leg_cot_markets=[],
                proxy_cot_markets=[],
                note=f"Direct COT mapping declared but no valid rows for canonical entity '{canonical}'.",
            )
        return CotResolution(
            instrument_id=iid,
            cot_status=COT_STATUS_DIRECT,
            direct_cot_market=canonical,
            leg_cot_markets=[],
            proxy_cot_markets=[],
            note=f"Direct canonical CFTC entity: {canonical}.",
        )

    # 2) FX pair → leg-derived
    if spec.asset_class == "fx":
        legs = parse_currency_legs(iid)
        leg_entities = leg_entities_for_pair(iid)
        valid_legs = [l for l in leg_entities if (valid_entities is None or l in valid_entities)]
        has_usd_leg = bool(legs) and "USD" in legs
        if len(valid_legs) >= 2:
            return CotResolution(
                instrument_id=iid,
                cot_status=COT_STATUS_LEG,
                direct_cot_market=None,
                leg_cot_markets=valid_legs,
                proxy_cot_markets=[],
                note="Derived from currency leg COT (no direct pair COT exists).",
            )
        if len(valid_legs) == 1 and has_usd_leg:
            # USD has no single CFTC contract; it is synthesized in the relative-strength layer.
            # A USD pair is therefore legitimately driven by its one non-USD leg's COT.
            return CotResolution(
                instrument_id=iid,
                cot_status=COT_STATUS_LEG,
                direct_cot_market=None,
                leg_cot_markets=valid_legs,
                proxy_cot_markets=[],
                note="Derived from currency leg COT vs synthesized USD leg (no direct pair COT exists).",
            )
        if len(valid_legs) == 1:
            return CotResolution(
                instrument_id=iid,
                cot_status=COT_STATUS_MACRO,
                direct_cot_market=None,
                leg_cot_markets=valid_legs,
                proxy_cot_markets=[],
                note="Only one currency leg has CFTC COT (other leg has no contract); macro-only until both legs resolve.",
            )
        # No COT-backed legs (e.g. EM cross) → fall through to proxy/macro below.

    # 3) Proxy reference to a canonical entity
    if spec.cot_proxy_of:
        proxy = spec.cot_proxy_of
        if valid_entities is None or proxy in valid_entities:
            return CotResolution(
                instrument_id=iid,
                cot_status=COT_STATUS_PROXY,
                direct_cot_market=None,
                leg_cot_markets=[],
                proxy_cot_markets=[proxy],
                note=f"Proxy COT — references canonical entity '{proxy}' (approximate, not direct).",
            )
        return CotResolution(
            instrument_id=iid,
            cot_status=COT_STATUS_BROKEN,
            direct_cot_market=None,
            leg_cot_markets=[],
            proxy_cot_markets=[proxy],
            note=f"Proxy target '{proxy}' has no valid COT rows.",
        )

    # 4) No COT path at all
    return CotResolution(
        instrument_id=iid,
        cot_status=COT_STATUS_NONE,
        direct_cot_market=None,
        leg_cot_markets=[],
        proxy_cot_markets=[],
        note="No direct COT, no COT-backed legs, no proxy — macro/driver context only.",
    )


def canonical_entity_summary(valid_entities: set[str] | None = None) -> dict[str, int]:
    """Count instruments by cot_status across the whole registry."""
    reg = load_registry()
    tally: dict[str, int] = {}
    for iid in reg:
        res = resolve_cot_status(reg[iid], valid_entities=valid_entities)
        tally[res.cot_status] = tally.get(res.cot_status, 0) + 1
    return tally
