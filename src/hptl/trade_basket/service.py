"""Phase 2A trade-basket service — API / CLI payload."""

from __future__ import annotations

import json
from typing import Any

from hptl.trade_basket.engine import ENGINE_VERSION, analyse_trade_basket


def build_trade_basket_payload(
    *,
    trades: list[Any] | None = None,
    frequency: str = "daily",
    lookback: int = 60,
) -> dict[str, Any]:
    """Service entrypoint for Phase 2A basket mathematics."""
    result = analyse_trade_basket(
        trades=list(trades or []),
        frequency=frequency,
        lookback=lookback,
    )
    payload = result.to_dict()
    payload["engine"] = ENGINE_VERSION
    payload["phase"] = "2A"
    # Explicit: risk stored on trades but unused in Phase 2A maths
    payload["risk_percent_affects_calculations"] = False
    return payload


def build_trade_basket_payload_from_json(raw: str | bytes | dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw, dict):
        body = raw
    else:
        body = json.loads(raw)
    return build_trade_basket_payload(
        trades=body.get("trades") or [],
        frequency=body.get("frequency") or "daily",
        lookback=body.get("lookback") or 60,
    )
