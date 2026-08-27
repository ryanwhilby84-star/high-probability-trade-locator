"""FX pair → currency-leg decomposition (Phase 4).

Rules
-----
BASE/QUOTE LONG  →  +BASE, −QUOTE
BASE/QUOTE SHORT →  −BASE, +QUOTE

Non-FX instruments are not decomposed.
"""

from __future__ import annotations

from typing import Any

from hptl.fx.currency_map import parse_fx_pair
from hptl.trade_basket.models import DIRECTION_SIGN


CURRENCY_LABELS: dict[str, str] = {
    "AUD": "Australian-dollar",
    "NZD": "New-Zealand-dollar",
    "CHF": "Swiss-franc",
    "GBP": "British-pound",
    "EUR": "Euro",
    "JPY": "Japanese-yen",
    "CAD": "Canadian-dollar",
    "USD": "US-dollar",
    "SGD": "Singapore-dollar",
    "HKD": "Hong-Kong-dollar",
    "NOK": "Norwegian-krone",
    "SEK": "Swedish-krona",
    "DKK": "Danish-krone",
    "MXN": "Mexican-peso",
    "TRY": "Turkish-lira",
    "ZAR": "South-African-rand",
    "CNH": "Chinese-yuan",
    "INR": "Indian-rupee",
    "PLN": "Polish-zloty",
    "HUF": "Hungarian-forint",
    "CZK": "Czech-koruna",
    "THB": "Thai-baht",
    "SAR": "Saudi-riyal",
}


def currency_label(code: str) -> str:
    c = str(code or "").strip().upper()
    return CURRENCY_LABELS.get(c, c)


def is_fx_pair_id(instrument_id: str) -> bool:
    return parse_fx_pair(str(instrument_id or "").strip()) is not None


def decompose_fx_pair(
    instrument_id: str,
    direction: str,
) -> list[dict[str, Any]] | None:
    """Return signed currency legs for an FX pair trade, or None if not FX.

    Each leg: ``{"currency": "AUD", "sign": +1|-1}``.
    Signs are relative to the trade direction (LONG/SHORT).
    """
    legs = parse_fx_pair(str(instrument_id or "").strip())
    if legs is None:
        return None
    d = str(direction or "").strip().upper()
    if d not in DIRECTION_SIGN:
        return None
    trade_sign = DIRECTION_SIGN[d]
    # LONG BASE/QUOTE → +BASE −QUOTE; SHORT flips both.
    return [
        {"currency": legs.base, "sign": +1 * trade_sign},
        {"currency": legs.quote, "sign": -1 * trade_sign},
    ]


def trade_display_label(instrument_id: str, direction: str) -> str:
    return f"{instrument_id} {str(direction).strip().upper()}"
