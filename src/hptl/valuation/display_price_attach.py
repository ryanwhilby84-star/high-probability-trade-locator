"""Attach display-only live vs weekly price context to valuation export blocks.

Does not change fair-value model inputs (spot_price / deviation_pct remain weekly-model values).
"""

from __future__ import annotations

from typing import Any


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def attach_display_price_context(
    block: dict[str, Any],
    quote_ctx: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge live quote + historical OHLC metadata onto a valuation block."""
    out = dict(block)
    if not quote_ctx:
        return out

    model_spot = _num(out.get("spot_price"))
    fair = _num(out.get("fair_value"))
    live = _num(quote_ctx.get("live_price"))

    valuation_price = live if live is not None else model_spot
    valuation_source = "live/latest" if live is not None else "weekly_close_fallback"

    out["model_spot_price"] = model_spot
    out["historical_ohlc_source"] = quote_ctx.get("historical_ohlc_source")
    out["latest_completed_ohlc_date"] = quote_ctx.get("latest_completed_ohlc_date")
    out["latest_completed_ohlc_close"] = _num(quote_ctx.get("latest_completed_ohlc_close"))
    out["latest_cot_week"] = quote_ctx.get("latest_cot_week")
    out["live_price"] = live
    out["live_price_source"] = quote_ctx.get("live_price_source")
    out["live_price_as_of"] = quote_ctx.get("live_price_as_of")
    out["valuation_price_used"] = valuation_price
    out["valuation_price_source"] = valuation_source

    if fair is not None and valuation_price is not None and fair > 0:
        out["display_valuation_pct"] = round(100.0 * (valuation_price - fair) / fair, 2)

    ive = out.get("ive")
    if isinstance(ive, dict):
        ive_out = dict(ive)
        ive_out["model_spot_price"] = model_spot
        ive_out["live_price"] = live
        ive_out["live_price_source"] = quote_ctx.get("live_price_source")
        ive_out["live_price_as_of"] = quote_ctx.get("live_price_as_of")
        ive_out["display_current_price"] = valuation_price
        ive_out["display_valuation_pct"] = out.get("display_valuation_pct")
        ive_out["valuation_price_source"] = valuation_source
        ive_out["historical_ohlc_source"] = quote_ctx.get("historical_ohlc_source")
        ive_out["latest_completed_ohlc_close"] = out.get("latest_completed_ohlc_close")
        ive_out["latest_completed_ohlc_date"] = out.get("latest_completed_ohlc_date")
        out["ive"] = ive_out

    return out


def attach_display_prices_to_valuation(
    valuation_doc: dict[str, Any],
    live_quotes_doc: dict[str, Any],
) -> dict[str, Any]:
    """Apply display price context to all valuation instrument blocks."""
    out = dict(valuation_doc)
    quotes = live_quotes_doc.get("instruments") or {}
    instruments = dict(out.get("instruments") or {})
    for market, block in instruments.items():
        if not isinstance(block, dict):
            continue
        instruments[market] = attach_display_price_context(block, quotes.get(market))
    out["instruments"] = instruments
    out["live_quotes_generated_at"] = live_quotes_doc.get("generated_at")
    return out
