"""Per-market sensitivity profiles for contextual macro / news interpretation.

These are discretionary context labels — not signals, forecasts, or trade rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class AssetSensitivityProfile:
    market: str
    sensitivities: tuple[str, ...]
    stress_note: str
    news_lens: tuple[str, ...]


# Keys must match ``TARGET_MARKETS`` in ``build_decision_table``.
MARKET_SENSITIVITY: Final[dict[str, AssetSensitivityProfile]] = {
    "NASDAQ / NQ": AssetSensitivityProfile(
        market="NASDAQ / NQ",
        sensitivities=(
            "Treasury yields and real-rate repricing",
            "Liquidity and financial conditions",
            "Fed expectations and policy communication",
            "Broad risk appetite and growth narratives",
        ),
        stress_note=(
            "Rising real yields and broad tightening in financial conditions "
            "historically weigh on duration-heavy growth equities; easing yields and resilient liquidity tend to relieve that pressure."
        ),
        news_lens=("FOMC", "Fed speeches", "NFP/CPI shocks", "Big-tech regulation", "Curve steepening vs flattening"),
    ),
    "S&P 500 / ES": AssetSensitivityProfile(
        market="S&P 500 / ES",
        sensitivities=(
            "Broad risk appetite",
            "Earnings expectations and revisions",
            "Liquidity / financial conditions",
            "Fed expectations",
            "Global growth outlook",
        ),
        stress_note=(
            "The broad index aggregates many drivers; macro shocks often transmit through yields, USD, "
            "and revisions to earnings visibility rather than a single-factor story."
        ),
        news_lens=("Earnings season", "Guidance cuts", "Credit stress", "Geopolitical risk-off", "Soft/hard landing debate"),
    ),
    "Dow / YM": AssetSensitivityProfile(
        market="Dow / YM",
        sensitivities=(
            "Cyclical value / industrial tilt within large caps",
            "Risk appetite and broad equity flows",
            "Fed expectations and rates",
            "USD and global liquidity",
            "Growth and capex narratives",
        ),
        stress_note=(
            "Compared with pure growth indices, Dow-weighted exposures can tilt toward cyclicals; "
            "macro rotation (value vs growth) can move this complex differently than NQ alone."
        ),
        news_lens=("Industrial surveys", "Credit conditions", "Rate cuts/hikes narrative", "Dollar repricing"),
    ),
    "Gold": AssetSensitivityProfile(
        market="Gold",
        sensitivities=(
            "Real yields (opportunity cost of holding gold)",
            "USD strength",
            "Inflation and inflation-expectations shocks",
            "Geopolitical and tail-risk impulses",
            "Safe-haven and reserve-flow narratives",
        ),
        stress_note=(
            "Higher real yields and a broadly strong USD historically compress gold’s attractiveness on a macro axis; "
            "geopolitical stress and nominal uncertainty can temporarily dominate that channel."
        ),
        news_lens=("Real-rate repricing", "DXY", "CPI surprises", "Wars/conflicts", "Central-bank purchases"),
    ),
    "Silver": AssetSensitivityProfile(
        market="Silver",
        sensitivities=(
            "USD strength",
            "Real yields",
            "Inflation expectations",
            "Industrial demand and PM complex flows",
            "Commodity sentiment and ETF flows",
            "Broad risk appetite (precious + cyclical beta)",
        ),
        stress_note=(
            "Silver often blends precious-metal macro (real yields, USD) with industrial beta; "
            "conflicting signals between ‘safe-haven’ and ‘cyclical’ narratives can widen volatility without a clean single driver."
        ),
        news_lens=("Solar/industrial demand", "Copper lead/lag", "Inflation prints", "USD", "Rates volatility"),
    ),
    "Copper / HG": AssetSensitivityProfile(
        market="Copper / HG",
        sensitivities=(
            "China growth and credit impulse",
            "Global industrial production and capex",
            "USD (commodity invoicing channel)",
            "Broad risk appetite",
            "Mine supply disruption and concentrate markets",
        ),
        stress_note=(
            "Copper is often read as a China/global growth thermometer; USD strength and industrial slowdown narratives "
            "frequently align as headwinds, while supply shocks can temporarily decouple price from demand."
        ),
        news_lens=("China PMIs", "Property/credit news", "Mine strikes", "Smelter bottlenecks", "Inventory draws"),
    ),
    "Crude Oil / CL": AssetSensitivityProfile(
        market="Crude Oil / CL",
        sensitivities=(
            "Supply shocks and OPEC+ decisions",
            "Geopolitical risk (straits, sanctions, war premium)",
            "Global demand and freight/activity proxies",
            "USD (invoicing)",
            "Inflation pass-through narratives",
        ),
        stress_note=(
            "Crude can gap on supply headlines even when broad ‘risk’ is quiet; demand stories often dominate slower trends."
        ),
        news_lens=("OPEC+/IEA", "SPR releases", "Sanctions", "Refinery outages", "Macro demand surprises"),
    ),
    "Natural Gas / NG": AssetSensitivityProfile(
        market="Natural Gas / NG",
        sensitivities=(
            "Weather (heating/cooling degree days)",
            "Storage vs seasonal norms",
            "Production and infrastructure constraints",
            "Seasonal demand",
            "LNG/export and global gas arbitrage",
        ),
        stress_note=(
            "Henry Hub is highly weather- and storage-driven in the short run; global LNG headlines can matter when export capacity binds."
        ),
        news_lens=("Weather forecasts", "EIA storage", "Freeport/LNG headlines", "Production freeze-offs", "Hurricane risk"),
    ),
    "Wheat": AssetSensitivityProfile(
        market="Wheat",
        sensitivities=(
            "Weather and harvest conditions (key producing regions)",
            "Export restrictions and trade policy",
            "War / geopolitical disruption of Black Sea flows",
            "USDA/WASDE and global balance sheets",
            "USD (export competitiveness)",
            "Global demand and substitution",
        ),
        stress_note=(
            "Wheat often reprices quickly on export corridor risk and weather; balance-sheet surprises can shift the entire forward curve narrative."
        ),
        news_lens=("Black Sea headlines", "USDA", "Drought/freeze", "Export bans", "Currency moves in exporters"),
    ),
    "Corn": AssetSensitivityProfile(
        market="Corn",
        sensitivities=(
            "US and South American weather",
            "Ethanol and energy linkage",
            "Livestock feed demand",
            "USDA balance sheets",
            "Export competition and USD",
            "Energy and freight as secondary channels",
        ),
        stress_note=(
            "Corn balances energy-ethanol links with weather-driven supply; South American crop progress often sets the marginal narrative."
        ),
        news_lens=("USDA", "Brazil/Argentina weather", "Ethanol policy", "China import pace", "Freight"),
    ),
    "Soybeans": AssetSensitivityProfile(
        market="Soybeans",
        sensitivities=(
            "China import demand and trade flow",
            "South American production",
            "US planting/progress and yield risk",
            "Crush margins and meal/oil spreads",
            "USD and export competitiveness",
        ),
        stress_note=(
            "Soy complex is trade-flow sensitive; China headlines and South American production surprises frequently dominate a single season."
        ),
        news_lens=("China purchases", "Crush data", "Argentina drought", "US progress reports", "Trade policy"),
    ),
    "Coffee": AssetSensitivityProfile(
        market="Coffee",
        sensitivities=(
            "Weather in Brazil/Vietnam (frost, drought, rainfall)",
            "Harvest size and quality",
            "Stock-to-use and deficit narratives",
            "Export logistics and bottlenecks",
            "Producer currency (BRL, VND) vs USD",
            "Demand trends and substitution",
        ),
        stress_note=(
            "Coffee can gap on freeze/drought risk in major origins; currency moves in Brazil often matter for producer selling pressure."
        ),
        news_lens=("CONAB/IBGE", "Vietnam flows", "ICE stocks", "Logistics strikes", "BRL"),
    ),
    "Cocoa": AssetSensitivityProfile(
        market="Cocoa",
        sensitivities=(
            "West Africa weather and crop health",
            "Harvest and grind demand",
            "Supply deficits and disease risk",
            "Export logistics and port constraints",
            "Currency effects in origin countries",
            "Consumer demand and substitution",
        ),
        stress_note=(
            "Cocoa supply is geographically concentrated; disease, weather, and logistics can create sharp narrative shifts."
        ),
        news_lens=("Ghana/Ivory Coast crop tours", "Harmattan/drought", "Grind data", "Port delays", "FX in origins"),
    ),
}


def get_profile(market: str) -> AssetSensitivityProfile | None:
    return MARKET_SENSITIVITY.get(market)
