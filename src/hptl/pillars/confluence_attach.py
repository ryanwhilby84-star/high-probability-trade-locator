"""Attach valuation + seasonality fields to confluence rows."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from hptl.prices.data_integrity import _instrument_price_row
from hptl.prices.price_store import load_price_store

MACRO_MAPS_PATH = Path("web-dashboard/public/data/macro_relationship_maps_latest.json")


@lru_cache(maxsize=1)
def _price_instruments() -> dict[str, Any]:
    return (load_price_store().get("instruments") or {})


@lru_cache(maxsize=1)
def _macro_maps() -> dict[str, Any]:
    if not MACRO_MAPS_PATH.exists():
        return {}
    doc = json.loads(MACRO_MAPS_PATH.read_text(encoding="utf-8"))
    return doc.get("macro_relationship_maps") or {}


def _weekly_through(weekly: list[dict[str, Any]], week: str) -> list[dict[str, Any]]:
    w = str(week)[:10]
    return [b for b in weekly if str(b.get("date") or "")[:10] <= w]


def pillar_fields_for_market_week(market: str, week: str) -> dict[str, Any]:
    """Compute valuation + seasonality for one market-week."""
    from hptl.prices.data_integrity import integrity_status_for, unavailable_pillar_fields

    integrity = integrity_status_for(market)
    if integrity.status == "FAIL":
        reason = "; ".join(integrity.reasons[:3]) or "Price data integrity check failed."
        fields = unavailable_pillar_fields(reason=reason)
        fields["data_integrity_reasons"] = integrity.reasons
        return fields

    px = _instrument_price_row(market, _price_instruments())
    weekly_all = px.get("weekly") or []
    weekly = _weekly_through(weekly_all, week)
    macro = _macro_maps().get(market)

    from hptl.seasonality.engine import compute_seasonality
    from hptl.valuation.engine import compute_valuation

    val = compute_valuation(
        market=market,
        weekly_bars=weekly,
        range_52w=px.get("range_52w"),
        macro_map=macro,
        as_of_week=week,
    )
    sea = compute_seasonality(market=market, weekly_bars=weekly, as_of_week=week)

    return {
        "valuation_bias": val.get("valuation_bias"),
        "valuation_score": val.get("valuation_score"),
        "valuation_reason": val.get("valuation_reason"),
        "valuation_wired": val.get("wired"),
        "valuation_price_percentile_52w": val.get("price_percentile_52w"),
        "seasonality_bias": sea.get("seasonality_bias"),
        "seasonality_score": sea.get("seasonality_score"),
        "seasonality_reason": sea.get("seasonality_reason"),
        "seasonality_wired": sea.get("wired"),
        "seasonality_calendar_month": sea.get("calendar_month"),
        "data_integrity": "PASS",
    }
