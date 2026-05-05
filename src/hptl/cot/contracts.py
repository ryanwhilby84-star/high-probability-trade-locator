from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CotMarketMapping:
    dashboard_name: str
    cftc_market_name: str
    exchange: str
    code: str
    asset_class: str = ""


CME_FUTURES_ONLY_URL = "https://www.cftc.gov/dea/futures/deacmesf.htm"
LEGACY_FUTURES_ONLY_URL_TEMPLATE = "https://www.cftc.gov/files/dea/history/deacot{year}.zip"
FINANCIAL_FUTURES_ONLY_URL_TEMPLATE = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"

# Add future CFTC report-specific mappings here. Keep the original CFTC market
# name and code exact, because those are the safest identifiers when parsing
# CFTC text/HTML reports and historical compressed datasets.
CME_INDEX_CONTRACT_NAMES = {
    "E-MINI NASDAQ 100 - CHICAGO MERCANTILE EXCHANGE",
    "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",
    "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
}

CME_INDEX_NAME_TO_DASHBOARD = {
    "E-MINI NASDAQ 100 - CHICAGO MERCANTILE EXCHANGE": "NASDAQ",
    "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE": "NASDAQ",
    "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE": "S&P 500",
}

CME_INDEX_MAPPINGS: dict[str, CotMarketMapping] = {
    "209742": CotMarketMapping(
        dashboard_name="NASDAQ",
        cftc_market_name="E-MINI NASDAQ 100 - CHICAGO MERCANTILE EXCHANGE",
        exchange="CHICAGO MERCANTILE EXCHANGE",
        code="209742",
        asset_class="Equity Index",
    ),
    "13874A": CotMarketMapping(
        dashboard_name="S&P 500",
        cftc_market_name="E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
        exchange="CHICAGO MERCANTILE EXCHANGE",
        code="13874A",
        asset_class="Equity Index",
    ),
}

# The first/good workbook is intentionally limited to these dashboard markets.
# Keep this list as the source of truth so broad CFTC files cannot leak AECO,
# CAISO, carbon, electricity, ISO hubs, or random energy rows into the output.
GOOD_WORKBOOK_MARKET_ORDER = [
    "NASDAQ",
    "S&P 500",
    "GOLD",
    "SILVER",
    "COPPER",
    "CRUDE OIL",
    "NATURAL GAS",
    "COFFEE",
    "COCOA",
    "CORN",
    "WHEAT",
    "SOYBEANS",
]

GOOD_WORKBOOK_DISPLAY_NAMES = {
    "NASDAQ": "NASDAQ",
    "S&P 500": "S&P 500",
    "GOLD": "Gold",
    "SILVER": "Silver",
    "COPPER": "Copper",
    "CRUDE OIL": "Crude Oil",
    "NATURAL GAS": "Natural Gas",
    "COFFEE": "Coffee",
    "COCOA": "Cocoa",
    "CORN": "Corn",
    "WHEAT": "Wheat",
    "SOYBEANS": "Soybeans",
}

ALLOWED_GOOD_WORKBOOK_MARKETS = set(GOOD_WORKBOOK_MARKET_ORDER)

# Exact CFTC commodity contract names accepted from the existing commodity logic.
# These are deliberately narrow to preserve the original workbook filtering.
COMMODITY_NAME_TO_DASHBOARD = {
    "GOLD - COMMODITY EXCHANGE INC.": "GOLD",
    "SILVER - COMMODITY EXCHANGE INC.": "SILVER",
    "COPPER- #1 - COMMODITY EXCHANGE INC.": "COPPER",
    "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE": "CRUDE OIL",
    "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE": "CRUDE OIL",
    "NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE": "NATURAL GAS",
    "NATURAL GAS - NEW YORK MERCANTILE EXCHANGE": "NATURAL GAS",
    "COFFEE C - ICE FUTURES U.S.": "COFFEE",
    "COCOA - ICE FUTURES U.S.": "COCOA",
    "CORN - CHICAGO BOARD OF TRADE": "CORN",
    "WHEAT-SRW - CHICAGO BOARD OF TRADE": "WHEAT",
    "SOYBEANS - CHICAGO BOARD OF TRADE": "SOYBEANS",
}

# Short/partial market aliases after ``market_and_exchange_names`` has been split
# into market + exchange. These are still strict enough to avoid AECO/CAISO/
# carbon/electricity leakage because only the exact allowed contract labels are
# accepted.
COMMODITY_SHORT_NAME_TO_DASHBOARD = {
    "GOLD": "GOLD",
    "SILVER": "SILVER",
    "COPPER- #1": "COPPER",
    "COPPER": "COPPER",
    "WTI FINANCIAL CRUDE OIL": "CRUDE OIL",
    "CRUDE OIL, LIGHT SWEET": "CRUDE OIL",
    "CRUDE OIL": "CRUDE OIL",
    "NAT GAS NYME": "NATURAL GAS",
    "NATURAL GAS": "NATURAL GAS",
    "COFFEE C": "COFFEE",
    "COFFEE": "COFFEE",
    "COCOA": "COCOA",
    "CORN": "CORN",
    "WHEAT-SRW": "WHEAT",
    "WHEAT": "WHEAT",
    "SOYBEANS": "SOYBEANS",
}

# Optional code-level safety net for common Disaggregated Futures Only contracts.
# Names are still preferred, but these codes help if CFTC naming punctuation shifts.
COMMODITY_CODE_TO_DASHBOARD = {
    "088691": "GOLD",
    "084691": "SILVER",
    "085692": "COPPER",
    "067651": "CRUDE OIL",
    "023651": "NATURAL GAS",
    "083731": "COFFEE",
    "073732": "COCOA",
    "002602": "CORN",
    "001602": "WHEAT",
    "005602": "SOYBEANS",
}
