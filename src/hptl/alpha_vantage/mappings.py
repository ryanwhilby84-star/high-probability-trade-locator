"""HTPL instrument → Alpha Vantage function + symbol mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hptl.markets.instrument_registry import InstrumentSpec


@dataclass(frozen=True)
class AlphaVantageMapping:
    function: str
    symbol: str
    params: dict[str, str]
    category: str  # fx | commodity | index | crypto | rates | none


# Category probe endpoints (run once per audit).
CATEGORY_PROBES: list[tuple[str, str, dict[str, str]]] = [
    ("fx", "CURRENCY_EXCHANGE_RATE", {"from_currency": "EUR", "to_currency": "USD"}),
    ("commodity_wti", "WTI", {}),
    ("commodity_brent", "BRENT", {}),
    ("commodity_natgas", "NATURAL_GAS", {}),
    ("commodity_copper", "COPPER", {}),
    ("commodity_wheat", "WHEAT", {}),
    ("commodity_corn", "CORN", {}),
    ("commodity_all", "ALL_COMMODITIES", {}),
    ("commodity_sugar", "SUGAR", {}),
    ("commodity_coffee", "COFFEE", {}),
    ("commodity_metals", "GOLD_SILVER_SPOT", {}),
    ("index", "GLOBAL_QUOTE", {"symbol": "SPY"}),
    ("crypto", "DIGITAL_CURRENCY_DAILY", {"symbol": "BTC", "market": "USD"}),
    ("rates", "TREASURY_YIELD", {"interval": "daily", "maturity": "10year"}),
]

# HTPL instrument id → commodity function (dedicated AV commodity APIs).
_COMMODITY_FUNCTION: dict[str, str] = {
    "Crude Oil / CL": "WTI",
    "West Texas Oil": "WTI",
    "Brent Crude Oil": "BRENT",
    "Natural Gas / NG": "NATURAL_GAS",
    "Copper / HG": "COPPER",
    "Copper": "COPPER",
    "Wheat": "WHEAT",
    "Corn": "CORN",
    "Sugar": "SUGAR",
    "Coffee": "COFFEE",
}

# GLOBAL_QUOTE equity/index proxies (not futures boards).
_INDEX_SYMBOL: dict[str, str] = {
    "NASDAQ / NQ": "QQQ",
    "US Nas 100": "QQQ",
    "S&P 500 / ES": "SPY",
    "US SPX 500": "SPY",
    "Dow / YM": "DIA",
    "US Wall St 30": "DIA",
    "US Russ 2000": "IWM",
    "Germany 30": "EWG",
    "UK 100": "EWU",
    "Japan 225": "EWJ",
    "Australia 200": "EWA",
    "France 40": "EWQ",
    "Europe 50": "FEZ",
    "Hong Kong 33": "EWH",
    "China A50": "FXI",
    "India 50": "INDA",
    "Singapore 30": "EWS",
    "Netherlands 25": "EWN",
    "Taiwan Index": "EWT",
}

_CRYPTO_SYMBOL: dict[str, str] = {
    "Bitcoin": "BTC",
    "Bitcoin Cash": "BCH",
    "Ethereum/Ether": "ETH",
    "Litecoin": "LTC",
}

_RATES_FUNCTION: dict[str, tuple[str, dict[str, str]]] = {
    "US 2Y T-Note": ("TREASURY_YIELD", {"interval": "daily", "maturity": "2year"}),
    "US 5Y T-Note": ("TREASURY_YIELD", {"interval": "daily", "maturity": "5year"}),
    "US 10Y T-Note": ("TREASURY_YIELD", {"interval": "daily", "maturity": "10year"}),
    "US T-Bond": ("TREASURY_YIELD", {"interval": "daily", "maturity": "30year"}),
}


def _fx_pair_parts(spec: InstrumentSpec) -> tuple[str, str] | None:
    if spec.oanda_symbol and "_" in spec.oanda_symbol:
        a, b = spec.oanda_symbol.split("_", 1)
        if len(a) == 3 and len(b) == 3:
            return a, b
    if "/" in spec.id:
        a, b = spec.id.split("/", 1)
        return a.strip().upper(), b.strip().upper()
    # CME-style majors quoted vs USD
    legacy = {
        "Euro FX / 6E": ("EUR", "USD"),
        "British Pound / 6B": ("GBP", "USD"),
        "Japanese Yen / 6J": ("USD", "JPY"),
        "Swiss Franc / 6S": ("USD", "CHF"),
        "Australian Dollar / 6A": ("AUD", "USD"),
        "Canadian Dollar / 6C": ("USD", "CAD"),
        "NZ Dollar / 6N": ("NZD", "USD"),
    }
    return legacy.get(spec.id)


def resolve_alpha_mapping(spec: InstrumentSpec) -> AlphaVantageMapping | None:
    if spec.asset_class == "fx":
        parts = _fx_pair_parts(spec)
        if parts:
            fc, tc = parts
            return AlphaVantageMapping(
                function="CURRENCY_EXCHANGE_RATE",
                symbol=f"{fc}/{tc}",
                params={"from_currency": fc, "to_currency": tc},
                category="fx",
            )
        return None

    if spec.id in _COMMODITY_FUNCTION:
        fn = _COMMODITY_FUNCTION[spec.id]
        return AlphaVantageMapping(
            function=fn,
            symbol=fn,
            params={},
            category="commodity",
        )

    if spec.id in _INDEX_SYMBOL:
        sym = _INDEX_SYMBOL[spec.id]
        return AlphaVantageMapping(
            function="GLOBAL_QUOTE",
            symbol=sym,
            params={"symbol": sym},
            category="index",
        )

    if spec.id in _CRYPTO_SYMBOL:
        sym = _CRYPTO_SYMBOL[spec.id]
        return AlphaVantageMapping(
            function="DIGITAL_CURRENCY_DAILY",
            symbol=f"{sym}/USD",
            params={"symbol": sym, "market": "USD"},
            category="crypto",
        )

    if spec.id in _RATES_FUNCTION:
        fn, params = _RATES_FUNCTION[spec.id]
        return AlphaVantageMapping(
            function=fn,
            symbol=params.get("maturity", fn),
            params=dict(params),
            category="rates",
        )

    if spec.asset_class == "bonds" and spec.id == "Bund":
        return AlphaVantageMapping(
            function="GLOBAL_QUOTE",
            symbol="BUND",  # may fail — ETF proxy
            params={"symbol": "VGK"},
            category="rates",
        )
    if spec.id == "UK 10Y Gilt":
        return AlphaVantageMapping(
            function="GLOBAL_QUOTE",
            symbol="EWU",
            params={"symbol": "EWU"},
            category="rates",
        )

    if spec.asset_class == "metals" and spec.id.startswith("Gold/"):
        return AlphaVantageMapping(
            function="GOLD_SILVER_SPOT",
            symbol="XAU",
            params={},
            category="commodity",
        )
    if spec.asset_class == "metals" and spec.id.startswith("Silver/"):
        return AlphaVantageMapping(
            function="GOLD_SILVER_SPOT",
            symbol="XAG",
            params={},
            category="commodity",
        )
    if spec.id == "Gold/Silver":
        return AlphaVantageMapping(
            function="GOLD_SILVER_SPOT",
            symbol="XAU/XAG",
            params={},
            category="commodity",
        )

    return None


def mapping_to_evidence(
    mapping: AlphaVantageMapping,
    *,
    verified_functions: set[str],
    category_timestamps: dict[str, str],
    per_function_timestamps: dict[str, str],
) -> dict[str, Any]:
    fn = mapping.function
    ok = fn in verified_functions or mapping.category in category_timestamps
    ts = per_function_timestamps.get(fn) or category_timestamps.get(mapping.category, "")
    return {
        "source": "alpha_vantage",
        "symbol": mapping.symbol,
        "endpoint": f"https://www.alphavantage.co/query?function={fn}",
        "function": fn,
        "params": mapping.params,
        "category": mapping.category,
        "last_successful_response": ts if ok else None,
        "coverage_status": "supported" if ok else "unsupported",
    }
