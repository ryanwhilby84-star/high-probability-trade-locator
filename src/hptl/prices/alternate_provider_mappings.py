"""Canonical non-OANDA mappings for the Current Price Service.

OANDA remains the preferred live provider whenever an instrument is actually
available on the configured account.  These mappings cover instruments that are
not available there, so discovery never leaves known markets silently unmapped.

Yahoo symbols are used only for current quotes.  FRED series are used for macro
rates/curves where a market quote symbol is not appropriate.
"""

from __future__ import annotations

from typing import Any

PROVIDER_YAHOO = "yahoo"
PROVIDER_FRED = "fred"

# internal instrument id -> provider metadata
ALTERNATE_PROVIDER_MAPPINGS: dict[str, dict[str, Any]] = {
    # Agricultural / soft commodities
    "Coffee": {"provider": PROVIDER_YAHOO, "provider_symbol": "KC=F", "currency": "USD", "price_precision": 2},
    "Cocoa": {"provider": PROVIDER_YAHOO, "provider_symbol": "CC=F", "currency": "USD", "price_precision": 0},
    "Cotton": {"provider": PROVIDER_YAHOO, "provider_symbol": "CT=F", "currency": "USD", "price_precision": 2},
    "Corn": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZC=F", "currency": "USD", "price_precision": 2},
    "Wheat": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZW=F", "currency": "USD", "price_precision": 2},
    # Metals
    "Platinum": {"provider": PROVIDER_YAHOO, "provider_symbol": "PL=F", "currency": "USD", "price_precision": 1},
    "Palladium": {"provider": PROVIDER_YAHOO, "provider_symbol": "PA=F", "currency": "USD", "price_precision": 2},
    # Crypto
    "Bitcoin": {"provider": PROVIDER_YAHOO, "provider_symbol": "BTC-USD", "currency": "USD", "price_precision": 2},
    "Bitcoin Cash": {"provider": PROVIDER_YAHOO, "provider_symbol": "BCH-USD", "currency": "USD", "price_precision": 2},
    "Ethereum/Ether": {"provider": PROVIDER_YAHOO, "provider_symbol": "ETH-USD", "currency": "USD", "price_precision": 2},
    "Litecoin": {"provider": PROVIDER_YAHOO, "provider_symbol": "LTC-USD", "currency": "USD", "price_precision": 2},
    # Equity indices
    "India 50": {"provider": PROVIDER_YAHOO, "provider_symbol": "^NSEI", "currency": "INR", "price_precision": 2},
    "Taiwan Index": {"provider": PROVIDER_YAHOO, "provider_symbol": "^TWII", "currency": "TWD", "price_precision": 2},
    # FX crosses unavailable on the configured OANDA account
    "USD/INR": {"provider": PROVIDER_YAHOO, "provider_symbol": "INR=X", "currency": "INR", "price_precision": 4},
    "USD/SAR": {"provider": PROVIDER_YAHOO, "provider_symbol": "SAR=X", "currency": "SAR", "price_precision": 4},
    # Treasury futures. Duplicate UI labels intentionally resolve to the same
    # canonical contract family so aliases cannot drift apart.
    "US 10-Year T-Note / ZN": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZN=F", "currency": "USD", "price_precision": 3},
    "US 10Y T-Note": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZN=F", "currency": "USD", "price_precision": 3},
    "US 2-Year T-Note / ZT": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZT=F", "currency": "USD", "price_precision": 3},
    "US 2Y T-Note": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZT=F", "currency": "USD", "price_precision": 3},
    "US 5-Year T-Note / ZF": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZF=F", "currency": "USD", "price_precision": 3},
    "US 5Y T-Note": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZF=F", "currency": "USD", "price_precision": 3},
    "US 30-Year T-Bond / ZB": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZB=F", "currency": "USD", "price_precision": 3},
    "US T-Bond": {"provider": PROVIDER_YAHOO, "provider_symbol": "ZB=F", "currency": "USD", "price_precision": 3},
    "Ultra 10-Year T-Note / TN": {"provider": PROVIDER_YAHOO, "provider_symbol": "TN=F", "currency": "USD", "price_precision": 3},
    # Treasury yields / curve are macro series, not futures prices.
    "US 2-Year Treasury Yield": {"provider": PROVIDER_FRED, "provider_symbol": "DGS2", "currency": "PCT", "price_precision": 3},
    "US 10-Year Treasury Yield": {"provider": PROVIDER_FRED, "provider_symbol": "DGS10", "currency": "PCT", "price_precision": 3},
    "US 30-Year Treasury Yield": {"provider": PROVIDER_FRED, "provider_symbol": "DGS30", "currency": "PCT", "price_precision": 3},
    "2s10s Yield Curve": {"provider": PROVIDER_FRED, "provider_symbol": "T10Y2Y", "currency": "PCT", "price_precision": 3},
    "10-Year Real Yield": {"provider": PROVIDER_FRED, "provider_symbol": "DFII10", "currency": "PCT", "price_precision": 3},
}


def alternate_mapping_for(instrument_id: str) -> dict[str, Any] | None:
    row = ALTERNATE_PROVIDER_MAPPINGS.get(instrument_id)
    return dict(row) if row else None
