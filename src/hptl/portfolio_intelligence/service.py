"""Phase 3 service — enrich Phase 2A basket payload with portfolio intelligence."""

from __future__ import annotations

from typing import Any

from hptl.portfolio_intelligence.config import (
    ENGINE_VERSION,
    EXPOSURE_CLUSTER_ABS_THRESHOLD,
)
from hptl.portfolio_intelligence.explanations import build_explanations
from hptl.portfolio_intelligence.metrics import compute_portfolio_intelligence
from hptl.trade_basket.service import build_trade_basket_payload


def enrich_basket_with_portfolio_intelligence(
    basket_payload: dict[str, Any],
    *,
    exposure_cluster_threshold: float = EXPOSURE_CLUSTER_ABS_THRESHOLD,
) -> dict[str, Any]:
    """Attach portfolio_intelligence without altering Phase 2A fields."""
    out = dict(basket_payload)
    if out.get("status") != "ok":
        out["portfolio_intelligence"] = {
            "status": "skipped",
            "reason": "basket_not_ok",
            "engine": ENGINE_VERSION,
        }
        return out

    intel = compute_portfolio_intelligence(
        trades=list(out.get("trades") or []),
        pairs=list(out.get("pairs") or []),
        exposure_cluster_threshold=exposure_cluster_threshold,
    )
    intel["explanations"] = build_explanations(intel)
    intel["engine"] = ENGINE_VERSION
    intel["phase"] = "3"
    out["portfolio_intelligence"] = intel
    # Phase 3 marker; Phase 4 currency enrichment may overwrite to "4".
    out["workstation_phase"] = "3"
    return out


def build_portfolio_intelligence_payload(
    *,
    trades: list[Any] | None = None,
    frequency: str = "daily",
    lookback: int = 60,
    exposure_cluster_threshold: float = EXPOSURE_CLUSTER_ABS_THRESHOLD,
) -> dict[str, Any]:
    """Phase 2A basket + Phase 3 portfolio intelligence."""
    basket = build_trade_basket_payload(
        trades=trades,
        frequency=frequency,
        lookback=lookback,
    )
    return enrich_basket_with_portfolio_intelligence(
        basket,
        exposure_cluster_threshold=exposure_cluster_threshold,
    )
