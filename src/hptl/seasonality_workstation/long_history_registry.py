"""Long-history acquisition registry for Institutional Edge seasonality research.

This registry separates three concepts that must not be silently conflated:
1. actual futures history,
2. spot/index proxy history,
3. back-tested/synthetic history.

The 48-year target is a research target, not permission to invent pre-inception data.
"""

from __future__ import annotations

TARGET_YEARS = 48
TARGET_START_YEAR = 1978  # practical target for a 2026 48-year study

# strategy:
#   futures_actual     - seek actual exchange futures history first
#   spot_bridge        - older spot/FX series may extend a shorter futures history
#   index_bridge       - cash index may extend a shorter equity-index futures history
#   inception_limited  - instrument genuinely cannot supply 48 actual years; flag explicitly
#
# NOTE: provider candidates are acquisition leads, not claims that data is free/licensed/available.
LONG_HISTORY_PLAN = {
    "NASDAQ / NQ": {"strategy": "index_bridge", "actual_start": 1985, "candidates": ["Nasdaq NDX", "FRED NASDAQ100", "CME/DataMine NQ"]},
    "S&P 500 / ES": {"strategy": "index_bridge", "candidates": ["S&P 500 cash index", "CME/DataMine equity futures"]},
    "Dow / YM": {"strategy": "index_bridge", "candidates": ["DJIA cash index", "CBOT/CME YM"]},
    "Euro FX / 6E": {"strategy": "spot_bridge", "actual_start": 1999, "candidates": ["legacy European FX proxy", "EURUSD spot", "CME/DataMine 6E"]},
    "British Pound / 6B": {"strategy": "futures_actual", "actual_start": 1972, "candidates": ["CME/DataMine 6B", "GBPUSD spot"]},
    "Japanese Yen / 6J": {"strategy": "futures_actual", "actual_start": 1972, "candidates": ["CME/DataMine 6J", "JPY spot"]},
    "Swiss Franc / 6S": {"strategy": "futures_actual", "actual_start": 1972, "candidates": ["CME/DataMine 6S", "CHF spot"]},
    "Australian Dollar / 6A": {"strategy": "spot_bridge", "actual_start": 1987, "candidates": ["AUDUSD spot", "CME/DataMine 6A"]},
    "Canadian Dollar / 6C": {"strategy": "futures_actual", "actual_start": 1972, "candidates": ["CME/DataMine 6C", "CAD spot"]},
    "NZ Dollar / 6N": {"strategy": "spot_bridge", "actual_start": 1997, "candidates": ["NZDUSD spot", "CME/DataMine 6N"]},
    "Gold": {"strategy": "futures_actual", "candidates": ["COMEX historical settlements", "CME/DataMine"]},
    "Silver": {"strategy": "futures_actual", "candidates": ["COMEX historical settlements", "CME/DataMine"]},
    "Copper / HG": {"strategy": "futures_actual", "candidates": ["COMEX historical settlements", "CME/DataMine"]},
    "Crude Oil / CL": {"strategy": "inception_limited", "actual_start": 1983, "candidates": ["NYMEX historical settlements", "CME/DataMine"]},
    "Natural Gas / NG": {"strategy": "inception_limited", "actual_start": 1990, "candidates": ["NYMEX historical settlements", "CME/DataMine"]},
    "Corn": {"strategy": "futures_actual", "candidates": ["CBOT historical settlements", "CME/DataMine"]},
    "Wheat": {"strategy": "futures_actual", "candidates": ["CBOT historical settlements", "CME/DataMine"]},
    "Soybeans": {"strategy": "futures_actual", "candidates": ["CBOT historical settlements", "CME/DataMine"]},
    "Coffee": {"strategy": "futures_actual", "candidates": ["ICE/legacy exchange history", "licensed historical vendor"]},
    "Cocoa": {"strategy": "futures_actual", "candidates": ["ICE/legacy exchange history", "licensed historical vendor"]},
    "Sugar": {"strategy": "futures_actual", "candidates": ["ICE/legacy exchange history", "licensed historical vendor"]},
    "Platinum": {"strategy": "futures_actual", "candidates": ["NYMEX/legacy exchange history", "licensed historical vendor"]},
    "Palladium": {"strategy": "futures_actual", "candidates": ["NYMEX/legacy exchange history", "licensed historical vendor"]},
}


def get_long_history_plan(market: str) -> dict[str, object]:
    return dict(LONG_HISTORY_PLAN.get(market, {"strategy": "unclassified", "candidates": []}))
