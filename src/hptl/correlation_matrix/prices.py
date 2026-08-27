"""Load canonical closing prices for correlation — no duplicate market maps."""

from __future__ import annotations

from typing import Any


def load_closes_for_correlation(
    instrument_id: str,
) -> tuple[list[tuple[str, float]], dict[str, Any]]:
    """Load daily closes via canonical timeline; DX → ICE DX futures."""
    from hptl.markets.usd_index_identity import ICE_DXY_ID, seasonality_preferred_id
    from hptl.seasonality_workstation.returns import load_daily_closes

    preferred = seasonality_preferred_id(instrument_id)
    closes, source, err = load_daily_closes(preferred)
    meta: dict[str, Any] = {
        "requested_instrument_id": instrument_id,
        "price_instrument_id": preferred,
        "source": source,
        "error": err,
        "ice_dxy_id": ICE_DXY_ID,
    }
    if (not closes or err) and preferred != instrument_id:
        closes2, source2, err2 = load_daily_closes(instrument_id)
        if closes2 and not err2:
            from hptl.prices.price_store import load_instrument_record_internal

            rec = load_instrument_record_internal(instrument_id) or {}
            scale = rec.get("price_scale") or {}
            if scale.get("series_id") == "DTWEXBGS" or scale.get("is_proxy"):
                meta["error"] = "refused_fred_broad_proxy_for_dxy_correlation"
                return [], meta
            return closes2, {
                **meta,
                "price_instrument_id": instrument_id,
                "source": source2,
                "error": None,
            }
    return closes, meta
