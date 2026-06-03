"""Central OANDA / HPTL instrument registry — single source for universe expansion."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

REGISTRY_JSON_PATH = Path("data/config/instrument_registry.json")
EXPORT_JSON_PATH = Path("web-dashboard/public/data/instrument_registry.json")

# Legacy COT markets — IDs unchanged for backward compatibility.
LEGACY_COT_MARKETS: Final[tuple[str, ...]] = (
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
    "Euro FX / 6E",
    "British Pound / 6B",
    "Japanese Yen / 6J",
    "Swiss Franc / 6S",
    "Australian Dollar / 6A",
    "Canadian Dollar / 6C",
    "NZ Dollar / 6N",
    "Gold",
    "Silver",
    "Copper / HG",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Coffee",
    "Cocoa",
    "Corn",
    "Wheat",
    "Soybeans",
    "Sugar",
    "Platinum",
    "Palladium",
)

LEGACY_MARKET_ALIASES: Final[dict[str, list[str]]] = {
    "NASDAQ / NQ": ["NASDAQ 100 STOCK INDEX", "E-MINI NASDAQ 100", "NASDAQ MINI", "NASDAQ-100"],
    "S&P 500 / ES": ["S&P 500 STOCK INDEX", "E-MINI S&P 500", "S&P 500 CONSOLIDATED", "SP 500"],
    "Dow / YM": ["DOW JONES U.S. INDEX", "E-MINI DOW", "MINI DOW", "DJIA"],
    "Euro FX / 6E": ["EURO FX", "EURO CURRENCY", "EUR"],
    "British Pound / 6B": ["BRITISH POUND", "GBP", "STERLING"],
    "Japanese Yen / 6J": ["JAPANESE YEN", "JPY"],
    "Swiss Franc / 6S": ["SWISS FRANC", "CHF"],
    "Australian Dollar / 6A": ["AUSTRALIAN DOLLAR", "AUD"],
    "Canadian Dollar / 6C": ["CANADIAN DOLLAR", "CAD"],
    "NZ Dollar / 6N": ["NZ DOLLAR", "NEW ZEALAND DOLLAR", "NZD"],
    "Gold": ["GOLD -", "GOLD"],
    "Silver": ["SILVER -", "SILVER"],
    "Copper / HG": ["COPPER- #1", "COPPER-GRADE #1", "COPPER"],
    "Crude Oil / CL": ["CRUDE OIL, LIGHT SWEET-WTI", "WTI-PHYSICAL", "WTI FINANCIAL CRUDE OIL"],
    "Natural Gas / NG": [
        "NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE",
        "NAT GAS NYME",
        "HENRY HUB - NEW YORK MERCANTILE EXCHANGE",
        "NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
        "E-MINI NATURAL GAS",
    ],
    "Corn": ["CORN -"],
    "Soybeans": ["SOYBEANS -"],
    "Wheat": ["WHEAT -"],
    "Coffee": ["COFFEE C -", "COFFEE"],
    "Cocoa": ["COCOA -", "COCOA"],
    "Sugar": ["SUGAR NO. 11", "SUGAR NO 11", "SUGAR NO.11", "SUGAR -"],
    "Platinum": ["PLATINUM - NEW YORK MERCANTILE EXCHANGE", "PLATINUM -"],
    "Palladium": ["PALLADIUM - NEW YORK MERCANTILE EXCHANGE", "PALLADIUM -"],
}


@dataclass(frozen=True)
class InstrumentSpec:
    id: str
    display_name: str
    oanda_symbol: str | None
    asset_class: str
    subgroup: str
    tradeable: bool = True
    has_cot_mapping: bool = False
    cot_market_code: str | None = None
    cot_report_type: str | None = None
    macro_driver_profile: str = "generic"
    usd_sensitivity: float = 0.5
    rates_sensitivity: float = 0.5
    risk_sensitivity: float = 0.5
    china_sensitivity: float = 0.0
    commodity_linkage: float = 0.0
    safe_haven_score: float = 0.0
    risk_on_score: float = 0.5
    positioning_status: str = "no_direct_pair_cot"
    cot_proxy_of: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _oanda_sym(pair: str) -> str:
    return pair.replace("/", "_")


def _fx(
    pair: str,
    *,
    subgroup: str = "fx_cross",
    cot_proxy_of: str | None = None,
    usd: float = 0.5,
    commodity: float = 0.0,
    safe_haven: float = 0.0,
) -> InstrumentSpec:
    return InstrumentSpec(
        id=pair,
        display_name=pair,
        oanda_symbol=_oanda_sym(pair),
        asset_class="fx",
        subgroup=subgroup,
        has_cot_mapping=False,
        cot_proxy_of=cot_proxy_of,
        macro_driver_profile="fx",
        usd_sensitivity=usd,
        rates_sensitivity=0.6,
        risk_sensitivity=0.55,
        commodity_linkage=commodity,
        safe_haven_score=safe_haven,
        positioning_status="proxy_required" if cot_proxy_of else "no_direct_pair_cot",
    )


def _legacy_cot(
    market_id: str,
    *,
    asset_class: str,
    subgroup: str,
    macro_profile: str,
    cot_code: str | None = None,
    cot_type: str = "financial",
    **sens: float,
) -> InstrumentSpec:
    return InstrumentSpec(
        id=market_id,
        display_name=market_id,
        oanda_symbol=cot_code,
        asset_class=asset_class,
        subgroup=subgroup,
        has_cot_mapping=True,
        cot_market_code=cot_code or market_id,
        cot_report_type=cot_type,
        macro_driver_profile=macro_profile,
        positioning_status="cot_available",
        usd_sensitivity=sens.get("usd", 0.3),
        rates_sensitivity=sens.get("rates", 0.5),
        risk_sensitivity=sens.get("risk", 0.5),
        china_sensitivity=sens.get("china", 0.0),
        commodity_linkage=sens.get("commodity", 0.0),
        safe_haven_score=sens.get("safe_haven", 0.0),
        risk_on_score=sens.get("risk_on", 0.5),
    )


def _build_registry() -> dict[str, InstrumentSpec]:
    reg: dict[str, InstrumentSpec] = {}

    # --- Legacy COT (unchanged behaviour) ---
    legacy_specs = [
        _legacy_cot("NASDAQ / NQ", asset_class="indices", subgroup="us_index", macro_profile="equity", cot_code="NAS100USD", cot_type="financial", rates=0.85, risk=0.9, risk_on=0.85),
        _legacy_cot("S&P 500 / ES", asset_class="indices", subgroup="us_index", macro_profile="equity", cot_code="SPX500USD", rates=0.8, risk=0.88, risk_on=0.85),
        _legacy_cot("Dow / YM", asset_class="indices", subgroup="us_index", macro_profile="equity", cot_code="US30USD", rates=0.75, risk=0.85, risk_on=0.8),
        _legacy_cot("Euro FX / 6E", asset_class="fx", subgroup="fx_major", macro_profile="fx", cot_code="EUR_USD", cot_type="financial", usd=0.9, rates=0.7),
        _legacy_cot("British Pound / 6B", asset_class="fx", subgroup="fx_major", macro_profile="fx", cot_code="GBP_USD", usd=0.85, rates=0.65),
        _legacy_cot("Japanese Yen / 6J", asset_class="fx", subgroup="fx_major", macro_profile="fx", cot_code="USD_JPY", usd=0.9, rates=0.7, safe_haven=0.75),
        _legacy_cot("Swiss Franc / 6S", asset_class="fx", subgroup="fx_major", macro_profile="fx", cot_code="USD_CHF", usd=0.85, safe_haven=0.8),
        _legacy_cot("Australian Dollar / 6A", asset_class="fx", subgroup="fx_commodity", macro_profile="fx", cot_code="AUD_USD", usd=0.8, commodity=0.55, risk_on=0.7),
        _legacy_cot("Canadian Dollar / 6C", asset_class="fx", subgroup="fx_commodity", macro_profile="fx", cot_code="USD_CAD", usd=0.85, commodity=0.5),
        _legacy_cot("NZ Dollar / 6N", asset_class="fx", subgroup="fx_commodity", macro_profile="fx", cot_code="NZD_USD", usd=0.8, commodity=0.45, risk_on=0.65),
        _legacy_cot("Gold", asset_class="metals", subgroup="precious", macro_profile="gold", cot_type="disaggregated", usd=0.75, rates=0.9, safe_haven=0.85),
        _legacy_cot("Silver", asset_class="metals", subgroup="precious", macro_profile="silver", cot_type="disaggregated", usd=0.7, rates=0.85, commodity=0.4),
        _legacy_cot("Copper / HG", asset_class="metals", subgroup="industrial", macro_profile="copper", cot_type="disaggregated", usd=0.65, china=0.85, commodity=0.9),
        _legacy_cot("Crude Oil / CL", asset_class="commodities", subgroup="energy", macro_profile="oil", cot_type="disaggregated", usd=0.7, commodity=0.95),
        _legacy_cot("Natural Gas / NG", asset_class="commodities", subgroup="energy", macro_profile="natgas", cot_type="disaggregated", usd=0.5, commodity=0.9),
        _legacy_cot("Coffee", asset_class="commodities", subgroup="soft", macro_profile="soft", cot_type="disaggregated", usd=0.55, commodity=0.7),
        _legacy_cot("Cocoa", asset_class="commodities", subgroup="soft", macro_profile="soft", cot_type="disaggregated", usd=0.55, commodity=0.7),
        _legacy_cot("Corn", asset_class="commodities", subgroup="ag", macro_profile="ag", cot_type="disaggregated", usd=0.5, china=0.4, commodity=0.85),
        _legacy_cot("Wheat", asset_class="commodities", subgroup="ag", macro_profile="ag", cot_type="disaggregated", usd=0.5, china=0.35, commodity=0.85),
        _legacy_cot("Soybeans", asset_class="commodities", subgroup="ag", macro_profile="ag", cot_type="disaggregated", usd=0.5, china=0.7, commodity=0.85),
        _legacy_cot("Sugar", asset_class="commodities", subgroup="soft", macro_profile="soft", cot_type="disaggregated", usd=0.55, commodity=0.85),
        _legacy_cot("Platinum", asset_class="metals", subgroup="precious", macro_profile="gold", cot_type="disaggregated", usd=0.7, rates=0.85, commodity=0.6),
        _legacy_cot("Palladium", asset_class="metals", subgroup="precious", macro_profile="gold", cot_type="disaggregated", usd=0.65, rates=0.8, commodity=0.65),
    ]
    for spec in legacy_specs:
        reg[spec.id] = spec

    FX_CROSSES = [
        "AUD/CAD", "AUD/CHF", "AUD/HKD", "AUD/JPY", "AUD/NZD", "AUD/SGD", "AUD/USD",
        "CAD/HKD", "CAD/SGD",
        "CHF/HKD", "CHF/ZAR",
        "EUR/AUD", "EUR/CZK", "EUR/DKK", "EUR/HKD", "EUR/HUF", "EUR/NOK", "EUR/NZD", "EUR/SEK", "EUR/SGD", "EUR/TRY", "EUR/ZAR",
        "GBP/AUD", "GBP/HKD", "GBP/NZD", "GBP/PLN", "GBP/SGD", "GBP/ZAR",
        "HKD/JPY",
        "NZD/CAD", "NZD/CHF", "NZD/HKD", "NZD/JPY", "NZD/SGD", "NZD/USD",
        "SGD/CHF", "SGD/JPY",
        "TRY/JPY",
        "USD/CNH", "USD/CZK", "USD/DKK", "USD/HKD", "USD/HUF", "USD/INR", "USD/MXN", "USD/NOK", "USD/PLN", "USD/SAR", "USD/SEK", "USD/SGD", "USD/THB", "USD/TRY", "USD/ZAR",
        "ZAR/JPY",
    ]
    FX_EM = {"TRY", "ZAR", "INR", "MXN", "CNH", "HUF", "CZK", "PLN", "THB", "SAR"}
    FX_PROXY = {
        "AUD/USD": "Australian Dollar / 6A",
        "NZD/USD": "NZ Dollar / 6N",
        "EUR/USD": "Euro FX / 6E",
        "GBP/USD": "British Pound / 6B",
        "USD/JPY": "Japanese Yen / 6J",
        "USD/CAD": "Canadian Dollar / 6C",
        "USD/CHF": "Swiss Franc / 6S",
    }
    for pair in FX_CROSSES:
        em = any(c in pair for c in FX_EM)
        reg[pair] = _fx(
            pair,
            subgroup="fx_em" if em else "fx_cross",
            cot_proxy_of=FX_PROXY.get(pair),
            usd=0.85 if pair.startswith("USD/") or pair.endswith("/USD") else 0.55,
            commodity=0.35 if "AUD" in pair or "NZD" in pair or "CAD" in pair else 0.0,
            safe_haven=0.5 if "JPY" in pair or "CHF" in pair else 0.0,
        )

    # OANDA index display names (US names proxy legacy COT where applicable)
    INDICES = [
        ("Australia 200", "AU200AUD", "indices", "apac_index", "equity", None),
        ("China A50", "CN50USD", "indices", "apac_index", "equity", None),
        ("Europe 50", "EU50EUR", "indices", "eu_index", "equity", None),
        ("France 40", "FR40EUR", "indices", "eu_index", "equity", None),
        ("Germany 30", "DE30EUR", "indices", "eu_index", "equity", None),
        ("Hong Kong 33", "HK33HKD", "indices", "apac_index", "equity", None),
        ("India 50", "IN50USD", "indices", "apac_index", "equity", None),
        ("Japan 225", "JP225USD", "indices", "apac_index", "equity", None),
        ("Netherlands 25", "NL25EUR", "indices", "eu_index", "equity", None),
        ("Singapore 30", "SG30SGD", "indices", "apac_index", "equity", None),
        ("Taiwan Index", "TWIXUSD", "indices", "apac_index", "equity", None),
        ("UK 100", "UK100GBP", "indices", "eu_index", "equity", None),
        ("US Nas 100", "NAS100USD", "indices", "us_index", "equity", "NASDAQ / NQ"),
        ("US Russ 2000", "US2000USD", "indices", "us_index", "equity", None),  # no fut_fin code wired
        ("US SPX 500", "SPX500USD", "indices", "us_index", "equity", "S&P 500 / ES"),
        ("US Wall St 30", "US30USD", "indices", "us_index", "equity", "Dow / YM"),
    ]
    for name, sym, ac, sub, profile, proxy in INDICES:
        reg[name] = InstrumentSpec(
            id=name,
            display_name=name,
            oanda_symbol=sym,
            asset_class=ac,
            subgroup=sub,
            has_cot_mapping=False,
            cot_proxy_of=proxy,
            macro_driver_profile=profile,
            rates_sensitivity=0.75,
            risk_sensitivity=0.85,
            risk_on_score=0.8,
            positioning_status="proxy_required" if proxy else "no_direct_pair_cot",
        )

    METALS = [
        ("Copper", "XCUUSD", "metals", "industrial", "copper", "Copper / HG"),
        ("Gold/AUD", "XAU_AUD", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/CAD", "XAU_CAD", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/CHF", "XAU_CHF", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/EUR", "XAU_EUR", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/GBP", "XAU_GBP", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/HKD", "XAU_HKD", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/JPY", "XAU_JPY", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/NZD", "XAU_NZD", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/SGD", "XAU_SGD", "metals", "precious_cross", "gold", "Gold"),
        ("Gold/Silver", "XAU_XAG", "metals", "precious_cross", "gold", None),
        ("Silver/AUD", "XAG_AUD", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/CAD", "XAG_CAD", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/CHF", "XAG_CHF", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/EUR", "XAG_EUR", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/GBP", "XAG_GBP", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/HKD", "XAG_HKD", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/JPY", "XAG_JPY", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/NZD", "XAG_NZD", "metals", "precious_cross", "silver", "Silver"),
        ("Silver/SGD", "XAG_SGD", "metals", "precious_cross", "silver", "Silver"),
    ]
    for name, sym, ac, sub, profile, proxy in METALS:
        reg[name] = InstrumentSpec(
            id=name,
            display_name=name,
            oanda_symbol=sym,
            asset_class=ac,
            subgroup=sub,
            has_cot_mapping=False,
            cot_proxy_of=proxy,
            macro_driver_profile=profile,
            usd_sensitivity=0.65,
            rates_sensitivity=0.85 if profile in {"gold", "silver"} else 0.6,
            safe_haven_score=0.7 if profile == "gold" else 0.3,
            positioning_status="proxy_required" if proxy else "no_direct_pair_cot",
        )

    COMMODITIES = [
        ("Brent Crude Oil", "BCOUSD", "commodities", "energy", "oil", None),
        ("West Texas Oil", "WTICOUSD", "commodities", "energy", "oil", "Crude Oil / CL"),
    ]
    for name, sym, ac, sub, profile, proxy in COMMODITIES:
        reg[name] = InstrumentSpec(
            id=name,
            display_name=name,
            oanda_symbol=sym,
            asset_class=ac,
            subgroup=sub,
            has_cot_mapping=False,
            cot_proxy_of=proxy,
            macro_driver_profile=profile,
            usd_sensitivity=0.7,
            commodity_linkage=0.95,
            positioning_status="proxy_required" if proxy else "no_direct_pair_cot",
        )

    BONDS = [
        ("Bund", "DE10YBEUR", "bonds", "eu_rates", "bond"),
        ("UK 10Y Gilt", "UK10YBGBP", "bonds", "uk_rates", "bond"),
        ("US 2Y T-Note", "US2YUSD", "bonds", "us_rates", "bond"),
        ("US 5Y T-Note", "US5YUSD", "bonds", "us_rates", "bond"),
        ("US 10Y T-Note", "US10YUSD", "bonds", "us_rates", "bond"),
        ("US T-Bond", "USBONDUSD", "bonds", "us_rates", "bond"),
    ]
    for name, sym, ac, sub, profile in BONDS:
        reg[name] = InstrumentSpec(
            id=name,
            display_name=name,
            oanda_symbol=sym,
            asset_class=ac,
            subgroup=sub,
            macro_driver_profile=profile,
            rates_sensitivity=0.95,
            risk_sensitivity=0.4,
            positioning_status="no_direct_pair_cot",
        )

    CRYPTO = [
        ("Bitcoin", "BTCUSD", "crypto", "major", "crypto"),
        ("Bitcoin Cash", "BCHUSD", "crypto", "alt", "crypto"),
        ("Ethereum/Ether", "ETHUSD", "crypto", "major", "crypto"),
        ("Litecoin", "LTCUSD", "crypto", "alt", "crypto"),
    ]
    for name, sym, ac, sub, profile in CRYPTO:
        reg[name] = InstrumentSpec(
            id=name,
            display_name=name,
            oanda_symbol=sym,
            asset_class=ac,
            subgroup=sub,
            macro_driver_profile=profile,
            usd_sensitivity=0.8,
            rates_sensitivity=0.75,
            risk_sensitivity=0.9,
            risk_on_score=0.95,
            positioning_status="no_direct_pair_cot",
        )

    return reg


_REGISTRY: dict[str, InstrumentSpec] | None = None


def load_registry() -> dict[str, InstrumentSpec]:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _build_registry()
    return _REGISTRY


def get_instrument(market_id: str) -> InstrumentSpec | None:
    return load_registry().get(market_id)


def all_instrument_ids(*, tradeable_only: bool = True) -> list[str]:
    reg = load_registry()
    legacy = list(LEGACY_COT_MARKETS)
    rest = sorted(k for k in reg if k not in LEGACY_COT_MARKETS)
    ids = legacy + rest
    if tradeable_only:
        ids = [i for i in ids if reg[i].tradeable]
    return ids


def cot_mapped_ids() -> list[str]:
    """Markets with direct CFTC COT rows in the pipeline (legacy set only)."""
    return list(LEGACY_COT_MARKETS)


TARGET_MARKETS: list[str] = all_instrument_ids()
MARKET_ALIASES: dict[str, list[str]] = dict(LEGACY_MARKET_ALIASES)


def export_registry_json(path: Path | None = None) -> Path:
    """Write dashboard-facing registry JSON."""
    out = path or EXPORT_JSON_PATH
    reg = load_registry()
    payload = {
        "version": 1,
        "generated_from": "hptl.markets.instrument_registry",
        "markets": [reg[k].to_dict() for k in all_instrument_ids()],
        "legacy_cot_markets": list(LEGACY_COT_MARKETS),
        "total": len(reg),
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    REGISTRY_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


def canonical_priority_group(spec: InstrumentSpec | None, instrument_id: str = "") -> str:
    """Single board slot per economic exposure (e.g. Copper OANDA → Copper / HG)."""
    if spec is None:
        return instrument_id
    if spec.cot_proxy_of:
        return spec.cot_proxy_of
    if spec.has_cot_mapping:
        return spec.id
    return spec.id


def instrument_meta_for_record(market_id: str, record: dict[str, Any] | None = None) -> dict[str, Any]:
    spec = get_instrument(market_id)
    if not spec:
        return {"asset_class": "other", "subgroup": "unknown", "positioning_status": "unknown", "has_cot_mapping": False, "data_status": "no_data"}
    meta: dict[str, Any] = {
        "instrument_id": spec.id,
        "display_name": spec.display_name,
        "asset_class": spec.asset_class,
        "subgroup": spec.subgroup,
        "positioning_status": spec.positioning_status,
        "has_cot_mapping": spec.has_cot_mapping,
        "cot_proxy_of": spec.cot_proxy_of,
        "macro_driver_profile": spec.macro_driver_profile,
        "oanda_symbol": spec.oanda_symbol,
    }
    if record is not None:
        from hptl.markets.coverage_audit import data_status_for_record

        meta["data_status"] = data_status_for_record(record, spec)
    return meta
