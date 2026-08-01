"""Canonical instrument identity for the 26 LEGACY_COT_MARKETS universe.

One row per instrument. Join key everywhere is ``instrument_id``.
Do not join on aliases, fuzzy display names, or polluted registry fields.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Final

from hptl.markets.instrument_registry import LEGACY_COT_MARKETS


@dataclass(frozen=True)
class CanonicalInstrument:
    instrument_id: str
    display_name: str
    exchange_symbol: str
    price_provider_symbol: str | None
    price_provider: str  # oanda | fred | none
    cftc_market_code: str
    cftc_market_name: str
    asset_class: str
    cot_report_type: str  # financial | disaggregated | legacy_futures_only | financial_futures_tff


# Authoritative identity table — single source for audits and registry wiring.
CANONICAL_INSTRUMENTS: Final[tuple[CanonicalInstrument, ...]] = (
    CanonicalInstrument(
        "NASDAQ / NQ",
        "NASDAQ / NQ",
        "NQ",
        "NAS100_USD",
        "oanda",
        "209742",
        "NASDAQ-100 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
        "indices",
        "financial",
    ),
    CanonicalInstrument(
        "S&P 500 / ES",
        "S&P 500 / ES",
        "ES",
        "SPX500_USD",
        "oanda",
        "13874A",
        "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
        "indices",
        "financial",
    ),
    CanonicalInstrument(
        "Dow / YM",
        "Dow / YM",
        "YM",
        "US30_USD",
        "oanda",
        "124603",
        "DJIA x $5 - CHICAGO BOARD OF TRADE",
        "indices",
        "financial",
    ),
    CanonicalInstrument(
        "Euro FX / 6E",
        "Euro FX / 6E",
        "6E",
        "EUR_USD",
        "oanda",
        "099741",
        "EURO FX - CHICAGO MERCANTILE EXCHANGE",
        "fx",
        "financial",
    ),
    CanonicalInstrument(
        "British Pound / 6B",
        "British Pound / 6B",
        "6B",
        "GBP_USD",
        "oanda",
        "096742",
        "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
        "fx",
        "financial",
    ),
    CanonicalInstrument(
        "Japanese Yen / 6J",
        "Japanese Yen / 6J",
        "6J",
        "6J=F",
        "yahoo",
        "097741",
        "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
        "fx",
        "financial",
    ),
    CanonicalInstrument(
        "Swiss Franc / 6S",
        "Swiss Franc / 6S",
        "6S",
        "USD_CHF",
        "oanda",
        "092741",
        "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
        "fx",
        "financial",
    ),
    CanonicalInstrument(
        "Australian Dollar / 6A",
        "Australian Dollar / 6A",
        "6A",
        "AUD_USD",
        "oanda",
        "232741",
        "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
        "fx",
        "financial",
    ),
    CanonicalInstrument(
        "Canadian Dollar / 6C",
        "Canadian Dollar / 6C",
        "6C",
        "USD_CAD",
        "oanda",
        "090741",
        "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
        "fx",
        "financial",
    ),
    CanonicalInstrument(
        "NZ Dollar / 6N",
        "NZ Dollar / 6N",
        "6N",
        "NZD_USD",
        "oanda",
        "112741",
        "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE",
        "fx",
        "financial",
    ),
    CanonicalInstrument(
        "Gold",
        "Gold",
        "GC",
        "XAU_USD",
        "oanda",
        "088691",
        "GOLD - COMMODITY EXCHANGE INC.",
        "metals",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Silver",
        "Silver",
        "SI",
        "XAG_USD",
        "oanda",
        "084691",
        "SILVER - COMMODITY EXCHANGE INC.",
        "metals",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Copper / HG",
        "Copper / HG",
        "HG",
        "XCU_USD",
        "oanda",
        "085692",
        "COPPER-GRADE #1 - COMMODITY EXCHANGE INC.",
        "metals",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Crude Oil / CL",
        "Crude Oil / CL",
        "CL",
        "WTICO_USD",
        "oanda",
        "067651",
        "CRUDE OIL, LIGHT SWEET-WTI - NEW YORK MERCANTILE EXCHANGE",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Natural Gas / NG",
        "Natural Gas / NG",
        "NG",
        "NATGAS_USD",
        "oanda",
        "023651",
        "NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Coffee",
        "Coffee",
        "KC",
        "KC=F",
        "yahoo",
        "083731",
        "COFFEE C - ICE FUTURES U.S.",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Cocoa",
        "Cocoa",
        "CC",
        "CC=F",
        "yahoo",
        "073732",
        "COCOA - ICE FUTURES U.S.",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Cotton",
        "Cotton",
        "CT",
        "CT=F",
        "yahoo",
        "033661",
        "COTTON NO. 2 - ICE FUTURES U.S.",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Corn",
        "Corn",
        "ZC",
        "ZC=F",
        "yahoo",
        "002602",
        "CORN - CHICAGO BOARD OF TRADE",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Wheat",
        "Wheat",
        "ZW",
        "WHEAT_USD",
        "oanda",
        "001602",
        "WHEAT - CHICAGO BOARD OF TRADE",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Soybeans",
        "Soybeans",
        "ZS",
        "SOYBN_USD",
        "oanda",
        "005602",
        "SOYBEANS - CHICAGO BOARD OF TRADE",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Sugar",
        "Sugar",
        "SB",
        "SUGAR_USD",
        "oanda",
        "080732",
        "SUGAR NO. 11 - ICE FUTURES U.S.",
        "commodities",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Platinum",
        "Platinum",
        "PL",
        "XPT_USD",
        "oanda",
        "076651",
        "PLATINUM - NEW YORK MERCANTILE EXCHANGE",
        "metals",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Palladium",
        "Palladium",
        "PA",
        "XPD_USD",
        "oanda",
        "075651",
        "PALLADIUM - NEW YORK MERCANTILE EXCHANGE",
        "metals",
        "disaggregated",
    ),
    CanonicalInstrument(
        "Bitcoin",
        "Bitcoin",
        "BTC",
        "BTC_USD",
        "oanda",
        "133741",
        "BITCOIN - CHICAGO MERCANTILE EXCHANGE",
        "crypto",
        "legacy_futures_only",
    ),
    CanonicalInstrument(
        "US Dollar Index / DX",
        "US Dollar Index / DX",
        "DX",
        None,
        "yahoo_futures",
        "098662",
        "USD INDEX - ICE FUTURES U.S.",
        "fx",
        "financial_futures_tff",
    ),
)

BY_ID: Final[dict[str, CanonicalInstrument]] = {c.instrument_id: c for c in CANONICAL_INSTRUMENTS}


def canonical_cftc_codes() -> dict[str, str]:
    return {c.instrument_id: c.cftc_market_code for c in CANONICAL_INSTRUMENTS}


def assert_universe_complete() -> None:
    ids = [c.instrument_id for c in CANONICAL_INSTRUMENTS]
    if len(ids) != len(set(ids)):
        raise AssertionError("Duplicate instrument_id in CANONICAL_INSTRUMENTS")
    missing = [m for m in LEGACY_COT_MARKETS if m not in BY_ID]
    extra = [m for m in ids if m not in LEGACY_COT_MARKETS]
    if missing or extra:
        raise AssertionError(f"Universe mismatch missing={missing} extra={extra}")


def identity_row(instrument_id: str) -> dict[str, Any]:
    c = BY_ID[instrument_id]
    return asdict(c)
