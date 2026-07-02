"""Map CFTC financial futures labels to ISO currency legs and quote conventions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# Direct G10 COT markets in HPTL (financial futures cohort).
COT_CURRENCY_SOURCES: Final[dict[str, dict[str, object]]] = {
    "EUR": {"market": "Euro FX / 6E", "invert_cot": False, "quote": "EUR/USD"},
    "GBP": {"market": "British Pound / 6B", "invert_cot": False, "quote": "GBP/USD"},
    "JPY": {"market": "Japanese Yen / 6J", "invert_cot": True, "quote": "USD/JPY"},
    "CHF": {"market": "Swiss Franc / 6S", "invert_cot": True, "quote": "USD/CHF"},
    "AUD": {"market": "Australian Dollar / 6A", "invert_cot": False, "quote": "AUD/USD"},
    "CAD": {"market": "Canadian Dollar / 6C", "invert_cot": True, "quote": "USD/CAD"},
    "NZD": {"market": "NZ Dollar / 6N", "invert_cot": False, "quote": "NZD/USD"},
}

# ISO codes we can score on the main leaderboard (COT-backed + synthetic USD).
LEADERBOARD_CURRENCIES: Final[tuple[str, ...]] = (
    "CHF",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "CAD",
    "NZD",
    "USD",
)

DX_INSTRUMENT_ID: Final[str] = "US Dollar Index / DX"
USD_SYNTHETIC_LABEL: Final[str] = "Synthetic USD (G10 inverse basket)"
USD_DIRECT_LABEL: Final[str] = "Direct USD / DXY (ICE Dollar Index COT 098662)"


@dataclass(frozen=True)
class FxPairLegs:
    base: str
    quote: str
    instrument_id: str


def parse_fx_pair(instrument_id: str) -> FxPairLegs | None:
    if "/" not in instrument_id:
        return None
    base, quote = instrument_id.split("/", 1)
    base, quote = base.strip().upper(), quote.strip().upper()
    if len(base) != 3 or len(quote) != 3:
        return None
    return FxPairLegs(base=base, quote=quote, instrument_id=instrument_id)


def cot_market_for_currency(code: str) -> str | None:
    spec = COT_CURRENCY_SOURCES.get(code.upper())
    if not spec:
        return None
    return str(spec["market"])
