"""FX pair overlay — DXY + Treasury TFF positioning vs institutional macro valuation."""

from __future__ import annotations

from typing import Any

from hptl.fx.fx_valuation import BIAS_BEARISH, BIAS_BULLISH, BIAS_NEUTRAL
from hptl.macro.dollar_positioning import (
    CROWDED_LONG_DOLLAR,
    CROWDED_SHORT_DOLLAR,
    DOLLAR_STRENGTHENING,
    DOLLAR_WEAKENING,
    STRONG_DOLLAR,
    WEAK_DOLLAR,
    DollarPositioningScore,
    score_dollar_positioning,
)
from hptl.macro.treasury_positioning import (
    BEARISH_BONDS,
    BULLISH_BONDS,
    TreasuryPositioningScore,
    score_treasury_positioning,
)

# Max adjustment to institutional pair score differential (−100…+100 scale).
_MAX_ADJ = 12.0

_SAFE_HAVEN = frozenset({"JPY", "CHF"})
_RISK_ON = frozenset({"AUD", "NZD", "CAD"})


def _dxy_pair_delta(base: str, quote: str, dxy: DollarPositioningScore) -> float:
    """Signed adjustment: + favors base vs quote."""
    if not dxy.available:
        return 0.0
    delta = 0.0
    primary = dxy.primary_label
    labels = set(dxy.score_labels)

    bullish_usd = labels & {STRONG_DOLLAR, DOLLAR_STRENGTHENING, CROWDED_LONG_DOLLAR}
    bearish_usd = labels & {WEAK_DOLLAR, DOLLAR_WEAKENING, CROWDED_SHORT_DOLLAR}

    if quote == "USD" and bullish_usd:
        delta -= 6.0
    elif quote == "USD" and bearish_usd:
        delta += 6.0
    elif base == "USD" and bullish_usd:
        delta += 6.0
    elif base == "USD" and bearish_usd:
        delta -= 6.0

    if primary in (CROWDED_LONG_DOLLAR, CROWDED_SHORT_DOLLAR):
        delta *= 0.5  # crowded = fade impulse
    return delta


def _treasury_pair_delta(base: str, quote: str, treasury: TreasuryPositioningScore) -> float:
    if not treasury.available:
        return 0.0
    delta = 0.0
    risk_off = treasury.bond_bias == BULLISH_BONDS
    risk_on = treasury.bond_bias == BEARISH_BONDS

    for ccy in (base, quote):
        sign = 1.0 if ccy == base else -1.0
        if ccy in _SAFE_HAVEN:
            if risk_off:
                delta += 4.0 * sign
            elif risk_on:
                delta -= 3.0 * sign
        if ccy in _RISK_ON:
            if risk_on:
                delta += 3.0 * sign
            elif risk_off:
                delta -= 3.0 * sign
    return delta


def _bias_from_delta(delta: float) -> str:
    if delta >= 4.0:
        return BIAS_BULLISH
    if delta <= -4.0:
        return BIAS_BEARISH
    return BIAS_NEUTRAL


def apply_macro_positioning_to_pair(
    base: str,
    quote: str,
    *,
    institutional_score_diff: float,
    tff_snapshot: dict[str, Any] | None = None,
    dollar_score: DollarPositioningScore | None = None,
    treasury_score: TreasuryPositioningScore | None = None,
) -> dict[str, Any]:
    """Return positioning overlay for a FX pair."""
    base = base.upper()
    quote = quote.upper()
    dxy = dollar_score or score_dollar_positioning(tff_snapshot)
    treas = treasury_score or score_treasury_positioning(tff_snapshot)

    dxy_delta = _dxy_pair_delta(base, quote, dxy)
    treas_delta = _treasury_pair_delta(base, quote, treas)
    total_delta = round(max(-_MAX_ADJ, min(_MAX_ADJ, dxy_delta + treas_delta)), 1)
    adjusted_diff = round(institutional_score_diff + total_delta, 1)
    positioning_bias = _bias_from_delta(total_delta)

    notes: list[str] = []
    if dxy_delta:
        notes.append(f"DXY overlay {dxy_delta:+.1f}")
    if treas_delta:
        notes.append(f"Treasury overlay {treas_delta:+.1f}")

    return {
        "pair": f"{base}/{quote}",
        "positioning_bias": positioning_bias,
        "positioning_score_adjustment": total_delta,
        "adjusted_pair_score_differential": adjusted_diff,
        "dxy_positioning": dxy.as_dict(),
        "treasury_positioning": treas.as_dict(),
        "notes": notes,
    }


def build_macro_positioning_document(tff_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    """Top-level macro positioning block for exports."""
    dxy = score_dollar_positioning(tff_snapshot)
    treas = score_treasury_positioning(tff_snapshot)
    return {
        "dollar_positioning": dxy.as_dict(),
        "treasury_positioning": treas.as_dict(),
        "rates_yield_sentiment": {
            "label": treas.yield_bias,
            "bond_bias": treas.bond_bias,
            "available": treas.available,
            "report_date": treas.report_date,
        },
    }
