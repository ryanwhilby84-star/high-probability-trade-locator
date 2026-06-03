"""Instrument universe registry (OANDA expansion)."""

from hptl.markets.instrument_registry import (
    InstrumentSpec,
    TARGET_MARKETS,
    MARKET_ALIASES,
    all_instrument_ids,
    cot_mapped_ids,
    export_registry_json,
    get_instrument,
    load_registry,
)

__all__ = [
    "InstrumentSpec",
    "TARGET_MARKETS",
    "MARKET_ALIASES",
    "all_instrument_ids",
    "cot_mapped_ids",
    "export_registry_json",
    "get_instrument",
    "load_registry",
]
