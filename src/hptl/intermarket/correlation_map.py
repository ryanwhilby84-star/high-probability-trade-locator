"""Static intermarket driver maps (V1 — heuristic only, not statistical correlation).

Each entry is (driver_key, relationship, short_label_for_UI).
driver_key:
  - cot:<TARGET_MARKETS name>  — another futures market in the HPTL cohort
  - macro:yields_1w           — DGS2/10/30 1w cohort direction (rates row)
  - macro:risk_signal         — resolved macro_signal string (risk_on / risk_off / …)
  - proxy:dxy                 — no COT series here; evaluated as unknown unless extended
  - proxy:real_yields         — proxied by yields_1w same as nominal 1w move in V1
  - proxy:vix                 — no series in pipeline
  - proxy:geopolitical        — narrative placeholder (unknown in V1)
"""
from __future__ import annotations

from typing import Final, Literal

Relationship = Literal[
    "inverse",
    "positive",
    "positive_when_yields_falling",
    "inverse_when_yields_rising",
    "inflation_positive",
    "mixed_growth",
    "positive_demand",
    "energy_complex_positive",
]

# (driver_key, relationship, label)
IntermarketRow = tuple[str, Relationship, str]

INTERMARKET_DRIVERS: Final[dict[str, tuple[IntermarketRow, ...]]] = {
    "Gold": (
        ("proxy:dxy", "inverse", "USD (DXY — not in cohort; monitor separately)"),
        ("macro:yields_1w", "inverse", "Treasury yields (1w cohort)"),
        ("proxy:real_yields", "inverse", "Real yield proxy (rates 1w)"),
        ("cot:Silver", "positive", "Silver (precious metals cohort)"),
        ("cot:Copper / HG", "mixed_growth", "Copper (growth / reflexivity)"),
        ("cot:Crude Oil / CL", "inflation_positive", "Crude (inflation impulse)"),
        ("proxy:geopolitical", "positive", "Geopolitical premium (narrative — not scored in V1)"),
    ),
    "Silver": (
        ("proxy:dxy", "inverse", "USD"),
        ("macro:yields_1w", "inverse", "Yields"),
        ("cot:Gold", "positive", "Gold"),
        ("cot:Copper / HG", "mixed_growth", "Copper"),
        ("cot:Crude Oil / CL", "inflation_positive", "Crude"),
    ),
    "NASDAQ / NQ": (
        ("macro:dgs2_1w", "inverse", "2Y yields (1w change)"),
        ("macro:yields_1w", "inverse", "10Y/30Y cohort (1w change)"),
        ("proxy:dxy", "inverse", "USD impulse (monitor DXY; not in cohort)"),
        ("cot:S&P 500 / ES", "positive", "S&P 500"),
        ("cot:Dow / YM", "positive", "Dow"),
        ("proxy:vix", "inverse", "VIX (not in dataset)"),
        ("cot:Crude Oil / CL", "mixed_growth", "Crude / reflation–policy balance"),
    ),
    "S&P 500 / ES": (
        ("macro:yields_1w", "inverse", "Yields"),
        ("cot:NASDAQ / NQ", "positive", "NASDAQ"),
        ("cot:Dow / YM", "positive", "Dow"),
        ("proxy:dxy", "inverse", "USD"),
        ("cot:Crude Oil / CL", "mixed_growth", "Crude"),
    ),
    "Dow / YM": (
        ("macro:yields_1w", "inverse", "Yields"),
        ("cot:S&P 500 / ES", "positive", "S&P 500"),
        ("cot:NASDAQ / NQ", "positive", "NASDAQ"),
        ("cot:Copper / HG", "positive_demand", "Copper demand"),
    ),
    "Copper / HG": (
        ("cot:Crude Oil / CL", "energy_complex_positive", "Crude / energy"),
        ("cot:Silver", "mixed_growth", "Silver"),
        ("proxy:dxy", "inverse", "USD"),
        ("cot:S&P 500 / ES", "positive_demand", "Equities demand"),
        ("macro:risk_signal", "positive", "Risk appetite (macro label)"),
    ),
    "Crude Oil / CL": (
        ("proxy:dxy", "inverse", "USD"),
        ("cot:Natural Gas / NG", "energy_complex_positive", "Natural gas"),
        ("cot:S&P 500 / ES", "positive_demand", "Equities / demand"),
        ("macro:yields_1w", "mixed_growth", "Yields as growth read"),
        ("proxy:geopolitical", "positive", "Geopolitical supply stories"),
    ),
    "Natural Gas / NG": (
        ("cot:Crude Oil / CL", "energy_complex_positive", "Crude"),
        ("proxy:dxy", "inverse", "USD"),
        ("cot:S&P 500 / ES", "positive_demand", "Demand proxy"),
    ),
    "Coffee": (
        ("proxy:dxy", "inverse", "USD / export FX"),
        ("cot:Crude Oil / CL", "inflation_positive", "Energy / freight"),
    ),
    "Cocoa": (
        ("proxy:dxy", "inverse", "USD / origin FX"),
        ("cot:Coffee", "positive", "Softs complex"),
    ),
    "Corn": (
        ("proxy:dxy", "inverse", "USD"),
        ("cot:Wheat", "positive", "Wheat"),
        ("cot:Soybeans", "positive", "Soybeans"),
        ("cot:Crude Oil / CL", "inflation_positive", "Energy"),
    ),
    "Wheat": (
        ("proxy:dxy", "inverse", "USD"),
        ("cot:Corn", "positive", "Corn"),
        ("cot:Soybeans", "positive", "Soybeans"),
    ),
    "Soybeans": (
        ("proxy:dxy", "inverse", "USD"),
        ("cot:Corn", "positive", "Corn"),
        ("cot:Wheat", "positive", "Wheat"),
    ),
}
