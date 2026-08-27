"""Macro Hub series registry and path constants."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from hptl.config import EXPORTS_DIR, PROCESSED_DIR, PROJECT_ROOT

SCHEMA_VERSION = 1

EXPORT_PATH = EXPORTS_DIR / "macro_hub_latest.json"
CANONICAL_PATH = PROCESSED_DIR / "macro_hub_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "macro_hub_latest.json"

# Rolling correlation windows (days) — data prep only; engine not built yet.
CORRELATION_WINDOWS_DAYS = (30, 90, 180)

# Preferred daily history windows (trading days).
HISTORY_WINDOWS_DAYS = {"1y": 252, "3y": 756, "5y": 1260}

# FRED observation starts for multi-year history pulls.
FRED_OBS_START_5Y = "2019-01-01"
FRED_OBS_START_3Y = "2022-01-01"

# Staleness thresholds (calendar days).
STALE_FRED_DAYS = 7
STALE_PRICE_DAYS = 5
STALE_COT_DAYS = 12


@dataclass(frozen=True)
class FredSeriesSpec:
    key: str
    series_id: str
    label: str
    obs_start: str = FRED_OBS_START_5Y


FRED_USD_DXY = FredSeriesSpec(
    key="dxy_broad",
    series_id="DTWEXBGS",
    label="Nominal Broad U.S. Dollar Index (FRED DTWEXBGS — DXY proxy)",
)

FRED_TREASURY_SERIES: tuple[FredSeriesSpec, ...] = (
    FredSeriesSpec("us_2y_yield", "DGS2", "US 2-Year Treasury Yield"),
    FredSeriesSpec("us_10y_yield", "DGS10", "US 10-Year Treasury Yield"),
    FredSeriesSpec("us_30y_yield", "DGS30", "US 30-Year Treasury Yield"),
    FredSeriesSpec("curve_2s10s", "T10Y2Y", "10Y–2Y Treasury spread (FRED T10Y2Y)"),
    FredSeriesSpec("real_yield_10y", "DFII10", "10-Year TIPS yield (FRED DFII10 — real yield proxy)"),
)

# CFTC Legacy Futures Only codes (not yet in legacy_cot_latest instrument map).
COT_CFTC_USD_INDEX = "098662"  # ICE U.S. Dollar Index
COT_CFTC_BITCOIN = "133741"  # CME Bitcoin


@dataclass(frozen=True)
class CrossAssetSpec:
    key: str
    instrument_id: str
    label: str


CROSS_ASSETS: tuple[CrossAssetSpec, ...] = (
    CrossAssetSpec("gold", "Gold", "Gold"),
    CrossAssetSpec("silver", "Silver", "Silver"),
    CrossAssetSpec("copper", "Copper / HG", "Copper / HG"),
    CrossAssetSpec("nasdaq", "NASDAQ / NQ", "NASDAQ / NQ"),
    CrossAssetSpec("sp500", "S&P 500 / ES", "S&P 500 / ES"),
    CrossAssetSpec("crude_oil", "Crude Oil / CL", "Crude Oil / CL"),
)

BITCOIN_INSTRUMENT_ID = "Bitcoin"

COT_3Y_PATHS: tuple[Path, ...] = (
    PROCESSED_DIR / "cot_3y_series_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json",
)

RATES_CLEAN_PATH = PROCESSED_DIR / "macro" / "rates_clean.csv"
