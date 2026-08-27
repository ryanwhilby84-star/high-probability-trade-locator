"""USD index instrument identity — never silently substitute series.

Three distinct instruments:

1. ``US Dollar Index / DX`` — ICE DX futures COT/TFF market (098662).
   Price must be ICE DX futures, not FRED broad USD.

2. ``US Dollar Index / DXY — ICE DX futures`` — explicit ICE DX futures
   price identity for seasonality / chart work (same futures family as DX).

3. ``Broad US Dollar Index — DTWEXBGS`` — FRED Nominal Broad USD index.
   Macro / valuation broad-dollar series only.
"""

from __future__ import annotations

from typing import Final

# COT / TFF / legacy scanner id (keep stable)
DX_COT_ID: Final[str] = "US Dollar Index / DX"

# Explicit ICE futures price instrument for seasonality / retail DXY charts
ICE_DXY_ID: Final[str] = "US Dollar Index / DXY — ICE DX futures"

# FRED broad dollar — formerly mis-bound to DX
BROAD_USD_ID: Final[str] = "Broad US Dollar Index — DTWEXBGS"

FRED_BROAD_SERIES: Final[str] = "DTWEXBGS"
ICE_DX_YAHOO_SYMBOL: Final[str] = "DX-Y.NYB"

# Instruments that share the ICE DX futures price series (never FRED)
ICE_DX_PRICE_IDS: Final[tuple[str, ...]] = (DX_COT_ID, ICE_DXY_ID)


def is_ice_dx_price_id(instrument_id: str) -> bool:
    return instrument_id in ICE_DX_PRICE_IDS


def is_broad_usd_id(instrument_id: str) -> bool:
    return instrument_id == BROAD_USD_ID


def seasonality_preferred_id(instrument_id: str) -> str:
    """Map legacy DX COT id to explicit ICE DXY id for seasonality curves."""
    if instrument_id == DX_COT_ID:
        return ICE_DXY_ID
    return instrument_id
