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
    "Cotton",
    "Corn",
    "Wheat",
    "Soybeans",
    "Sugar",
    "Platinum",
    "Palladium",
    "Bitcoin",
    "US Dollar Index / DX",
)

# Macro Hub series promoted to first-class institutional markets (no COT except DX).
MACRO_INSTITUTIONAL_MARKETS: Final[tuple[str, ...]] = (
    "US Dollar Index / DX",
    "US 2-Year Treasury Yield",
    "US 10-Year Treasury Yield",
    "US 30-Year Treasury Yield",
    "2s10s Yield Curve",
    "10-Year Real Yield",
)

MACRO_RATE_MARKETS: Final[tuple[str, ...]] = tuple(
    m for m in MACRO_INSTITUTIONAL_MARKETS if m != "US Dollar Index / DX"
)

# CFTC Traders in Financial Futures — DXY + Treasury futures (leveraged-money cohort).
TFF_MACRO_MARKETS: Final[tuple[str, ...]] = (
    "US Dollar Index / DX",
    "US 2-Year T-Note / ZT",
    "US 5-Year T-Note / ZF",
    "US 10-Year T-Note / ZN",
    "Ultra 10-Year T-Note / TN",
    "US 30-Year T-Bond / ZB",
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
    "Cotton": ["COTTON NO. 2", "COTTON NO 2", "COTTON -", "COTTON"],
    "Sugar": ["SUGAR NO. 11", "SUGAR NO 11", "SUGAR NO.11", "SUGAR -"],
    "Platinum": ["PLATINUM - NEW YORK MERCANTILE EXCHANGE", "PLATINUM -"],
    "Palladium": ["PALLADIUM - NEW YORK MERCANTILE EXCHANGE", "PALLADIUM -"],
    "Bitcoin": ["BITCOIN - CHICAGO MERCANTILE EXCHANGE", "CME BITCOIN", "BITCOIN"],
    "US Dollar Index / DX": [
        "U.S. DOLLAR INDEX",
        "ICE U.S. DOLLAR INDEX",
        "US DOLLAR INDEX",
        "DOLLAR INDEX",
        "USD INDEX",
        "USD INDEX - ICE FUTURES U.S.",
    ],
    "US 2-Year T-Note / ZT": ["UST 2Y NOTE", "2-YEAR T-NOTE", "US 2Y T-NOTE"],
    "US 5-Year T-Note / ZF": ["UST 5Y NOTE", "5-YEAR T-NOTE", "US 5Y T-NOTE"],
    "US 10-Year T-Note / ZN": ["UST 10Y NOTE", "10-YEAR T-NOTE", "US 10Y T-NOTE"],
    "Ultra 10-Year T-Note / TN": ["ULTRA UST 10Y", "ULTRA 10-YEAR T-NOTE"],
    "US 30-Year T-Bond / ZB": ["UST BOND", "30-YEAR T-BOND", "US 30Y T-BOND"],
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
    is_macro_driver: bool = False
    is_fx_anchor: bool = False

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
    cftc_code: str,
    oanda_symbol: str | None = None,
    cot_type: str = "financial",
    **sens: float,
) -> InstrumentSpec:
    """Legacy COT market with separated CFTC code vs price-provider symbol."""
    return InstrumentSpec(
        id=market_id,
        display_name=market_id,
        oanda_symbol=oanda_symbol,
        asset_class=asset_class,
        subgroup=subgroup,
        has_cot_mapping=True,
        cot_market_code=cftc_code,
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

    # --- Legacy COT universe (CFTC code ≠ price symbol; see canonical_identity.py) ---
    legacy_specs = [
        _legacy_cot("NASDAQ / NQ", asset_class="indices", subgroup="us_index", macro_profile="equity", cftc_code="209742", oanda_symbol="NAS100_USD", cot_type="financial", rates=0.85, risk=0.9, risk_on=0.85),
        _legacy_cot("S&P 500 / ES", asset_class="indices", subgroup="us_index", macro_profile="equity", cftc_code="13874A", oanda_symbol="SPX500_USD", rates=0.8, risk=0.88, risk_on=0.85),
        _legacy_cot("Dow / YM", asset_class="indices", subgroup="us_index", macro_profile="equity", cftc_code="124603", oanda_symbol="US30_USD", rates=0.75, risk=0.85, risk_on=0.8),
        _legacy_cot("Euro FX / 6E", asset_class="fx", subgroup="fx_major", macro_profile="fx", cftc_code="099741", oanda_symbol="EUR_USD", cot_type="financial", usd=0.9, rates=0.7),
        _legacy_cot("British Pound / 6B", asset_class="fx", subgroup="fx_major", macro_profile="fx", cftc_code="096742", oanda_symbol="GBP_USD", usd=0.85, rates=0.65),
        # Price = CME Japanese Yen futures (Yahoo 6J=F): USD per JPY, rises when yen strengthens.
        # Do NOT use OANDA USD_JPY here — that quote is the inverse of yen value / 6J.
        _legacy_cot("Japanese Yen / 6J", asset_class="fx", subgroup="fx_major", macro_profile="fx", cftc_code="097741", oanda_symbol=None, usd=0.9, rates=0.7, safe_haven=0.75),
        _legacy_cot("Swiss Franc / 6S", asset_class="fx", subgroup="fx_major", macro_profile="fx", cftc_code="092741", oanda_symbol="USD_CHF", usd=0.85, safe_haven=0.8),
        _legacy_cot("Australian Dollar / 6A", asset_class="fx", subgroup="fx_commodity", macro_profile="fx", cftc_code="232741", oanda_symbol="AUD_USD", usd=0.8, commodity=0.55, risk_on=0.7),
        _legacy_cot("Canadian Dollar / 6C", asset_class="fx", subgroup="fx_commodity", macro_profile="fx", cftc_code="090741", oanda_symbol="USD_CAD", usd=0.85, commodity=0.5),
        _legacy_cot("NZ Dollar / 6N", asset_class="fx", subgroup="fx_commodity", macro_profile="fx", cftc_code="112741", oanda_symbol="NZD_USD", usd=0.8, commodity=0.45, risk_on=0.65),
        _legacy_cot("Gold", asset_class="metals", subgroup="precious", macro_profile="gold", cftc_code="088691", oanda_symbol="XAU_USD", cot_type="disaggregated", usd=0.75, rates=0.9, safe_haven=0.85),
        _legacy_cot("Silver", asset_class="metals", subgroup="precious", macro_profile="silver", cftc_code="084691", oanda_symbol="XAG_USD", cot_type="disaggregated", usd=0.7, rates=0.85, commodity=0.4),
        _legacy_cot("Copper / HG", asset_class="metals", subgroup="industrial", macro_profile="copper", cftc_code="085692", oanda_symbol="XCU_USD", cot_type="disaggregated", usd=0.65, china=0.85, commodity=0.9),
        _legacy_cot("Crude Oil / CL", asset_class="commodities", subgroup="energy", macro_profile="oil", cftc_code="067651", oanda_symbol="WTICO_USD", cot_type="disaggregated", usd=0.7, commodity=0.95),
        _legacy_cot("Natural Gas / NG", asset_class="commodities", subgroup="energy", macro_profile="natgas", cftc_code="023651", oanda_symbol="NATGAS_USD", cot_type="disaggregated", usd=0.5, commodity=0.9),
        _legacy_cot("Coffee", asset_class="commodities", subgroup="soft", macro_profile="soft", cftc_code="083731", oanda_symbol=None, cot_type="disaggregated", usd=0.55, commodity=0.7),
        _legacy_cot("Cocoa", asset_class="commodities", subgroup="soft", macro_profile="soft", cftc_code="073732", oanda_symbol=None, cot_type="disaggregated", usd=0.55, commodity=0.7),
        _legacy_cot("Cotton", asset_class="commodities", subgroup="soft", macro_profile="soft", cftc_code="033661", oanda_symbol=None, cot_type="disaggregated", usd=0.55, commodity=0.7),
        _legacy_cot("Corn", asset_class="commodities", subgroup="ag", macro_profile="ag", cftc_code="002602", oanda_symbol=None, cot_type="disaggregated", usd=0.5, china=0.4, commodity=0.85),
        _legacy_cot("Wheat", asset_class="commodities", subgroup="ag", macro_profile="ag", cftc_code="001602", oanda_symbol="WHEAT_USD", cot_type="disaggregated", usd=0.5, china=0.35, commodity=0.85),
        _legacy_cot("Soybeans", asset_class="commodities", subgroup="ag", macro_profile="ag", cftc_code="005602", oanda_symbol="SOYBN_USD", cot_type="disaggregated", usd=0.5, china=0.7, commodity=0.85),
        _legacy_cot("Sugar", asset_class="commodities", subgroup="soft", macro_profile="soft", cftc_code="080732", oanda_symbol="SUGAR_USD", cot_type="disaggregated", usd=0.55, commodity=0.85),
        _legacy_cot("Platinum", asset_class="metals", subgroup="precious", macro_profile="gold", cftc_code="076651", oanda_symbol="XPT_USD", cot_type="disaggregated", usd=0.7, rates=0.85, commodity=0.6),
        _legacy_cot("Palladium", asset_class="metals", subgroup="precious", macro_profile="gold", cftc_code="075651", oanda_symbol="XPD_USD", cot_type="disaggregated", usd=0.65, rates=0.8, commodity=0.65),
        InstrumentSpec(
            id="Bitcoin",
            display_name="Bitcoin",
            oanda_symbol="BTC_USD",
            asset_class="crypto",
            subgroup="major",
            has_cot_mapping=True,
            cot_market_code="133741",
            cot_report_type="legacy_futures_only",
            macro_driver_profile="crypto",
            positioning_status="cot_available",
            usd_sensitivity=0.8,
            rates_sensitivity=0.75,
            risk_sensitivity=0.9,
            risk_on_score=0.95,
        ),
        InstrumentSpec(
            id="US Dollar Index / DX",
            display_name="US Dollar Index / DX",
            oanda_symbol=None,
            asset_class="fx",
            subgroup="usd_index",
            has_cot_mapping=True,
            cot_market_code="098662",
            cot_report_type="financial_futures_tff",
            macro_driver_profile="usd_index",
            positioning_status="tff_and_legacy_cot",
            usd_sensitivity=1.0,
            rates_sensitivity=0.85,
            risk_sensitivity=0.7,
            safe_haven_score=0.6,
            is_macro_driver=True,
            is_fx_anchor=True,
        ),
        InstrumentSpec(
            id="US Dollar Index / DXY — ICE DX futures",
            display_name="US Dollar Index / DXY — ICE DX futures",
            oanda_symbol=None,
            asset_class="fx",
            subgroup="usd_index",
            has_cot_mapping=False,
            cot_market_code=None,
            cot_report_type=None,
            macro_driver_profile="usd_index",
            positioning_status="price_only_ice_dx",
            usd_sensitivity=1.0,
            rates_sensitivity=0.85,
            risk_sensitivity=0.7,
            safe_haven_score=0.6,
            is_macro_driver=False,
            is_fx_anchor=False,
        ),
        InstrumentSpec(
            id="Broad US Dollar Index — DTWEXBGS",
            display_name="Broad US Dollar Index — DTWEXBGS",
            oanda_symbol=None,
            asset_class="fx",
            subgroup="usd_index",
            has_cot_mapping=False,
            cot_market_code=None,
            cot_report_type=None,
            macro_driver_profile="usd_broad",
            positioning_status="fred_broad_only",
            usd_sensitivity=1.0,
            rates_sensitivity=0.85,
            risk_sensitivity=0.7,
            safe_haven_score=0.55,
            is_macro_driver=True,
            is_fx_anchor=False,
        ),
    ]
    for spec in legacy_specs:
        reg[spec.id] = spec

    macro_rate_specs = [
        ("US 2-Year Treasury Yield", "us_2y", 0.95),
        ("US 10-Year Treasury Yield", "us_10y", 1.0),
        ("US 30-Year Treasury Yield", "us_30y", 0.9),
        ("2s10s Yield Curve", "curve_2s10s", 0.85),
        ("10-Year Real Yield", "real_yield", 0.95),
    ]
    for mid, profile, rates_sens in macro_rate_specs:
        reg[mid] = InstrumentSpec(
            id=mid,
            display_name=mid,
            oanda_symbol=None,
            asset_class="macro",
            subgroup="us_rates",
            has_cot_mapping=False,
            macro_driver_profile=profile,
            positioning_status="macro_institutional",
            rates_sensitivity=rates_sens,
            risk_sensitivity=0.75,
            usd_sensitivity=0.7,
            is_macro_driver=True,
        )

    tff_treasury_specs = [
        ("US 2-Year T-Note / ZT", "042601", "us_2y_futures", 0.95),
        ("US 5-Year T-Note / ZF", "044601", "us_5y_futures", 0.95),
        ("US 10-Year T-Note / ZN", "043602", "us_10y_futures", 1.0),
        ("Ultra 10-Year T-Note / TN", "043607", "us_ultra_10y_futures", 0.9),
        ("US 30-Year T-Bond / ZB", "020601", "us_30y_futures", 0.9),
    ]
    for mid, code, profile, rates_sens in tff_treasury_specs:
        reg[mid] = InstrumentSpec(
            id=mid,
            display_name=mid,
            oanda_symbol=None,
            asset_class="macro",
            subgroup="us_treasury_futures",
            has_cot_mapping=True,
            cot_market_code=code,
            cot_report_type="financial_futures_tff",
            macro_driver_profile=profile,
            positioning_status="tff_positioning",
            rates_sensitivity=rates_sens,
            risk_sensitivity=0.8,
            usd_sensitivity=0.75,
            is_macro_driver=True,
        )

    FX_CROSSES = [
        "AUD/CAD", "AUD/CHF", "AUD/HKD", "AUD/JPY", "AUD/NZD", "AUD/SGD", "AUD/USD",
        "CAD/HKD", "CAD/SGD",
        "CHF/HKD", "CHF/ZAR",
        "EUR/AUD", "EUR/CZK", "EUR/DKK", "EUR/GBP", "EUR/HKD", "EUR/HUF", "EUR/NOK", "EUR/NZD", "EUR/SEK", "EUR/SGD", "EUR/TRY", "EUR/ZAR",
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
    macro_extra = [m for m in MACRO_RATE_MARKETS if m not in legacy]
    rest = sorted(
        k for k in reg if k not in LEGACY_COT_MARKETS and k not in MACRO_INSTITUTIONAL_MARKETS
    )
    ids = legacy + macro_extra + rest
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
        "macro_institutional_markets": list(MACRO_INSTITUTIONAL_MARKETS),
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
