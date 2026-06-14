"""Fundamental valuation pillar — fair value from approved asset-class models only.

When no approved model exists, returns UNAVAILABLE. Never substitutes price
percentile, rankings, or location reads for valuation.

FX architecture (Phase 2):
  - **Pillar / dashboard column:** ``fx_carry_real_yield_v3`` only (via ``export.py``).
  - **Legacy (confluence attach):** ``hptl.fx.fx_valuation`` V1 yield-differential.
  - **Secondary (setup ranking / panels):** ``hptl.fx.fx_institutional_valuation`` V2.

Do not route new FX dashboard valuation work through V1 or V2.
"""
from __future__ import annotations

from typing import Any

from hptl.markets.instrument_registry import get_instrument

BIAS_UNAVAILABLE = "UNAVAILABLE"

# Planned Valuation V3 models (not wired until phase gate passes).
ASSET_CLASS_ROADMAP: dict[str, dict[str, str]] = {
    "fx": {
        "phase": "V3.0",
        "model_id": "fx_carry_real_yield_v3",
        "drivers": "policy rates, 2Y yields, real yields, inflation, DXY, Treasury regime",
    },
    "metals": {
        "phase": "V3.1",
        "model_id": "metals_real_yield_dxy_v3",
        "drivers": "real yields, DXY, inflation expectations, positioning overlay",
    },
    "indices": {
        "phase": "V3.2",
        "model_id": "indices_erp_cape_v3",
        "drivers": "CAPE, earnings yield, dividend yield, 10Y yield, ERP (S&P 500 only)",
    },
    "energy": {
        "phase": "V3.3",
        "model_id": "energy_inventory_dxy_v3",
        "drivers": "EIA inventories, DXY, seasonality context",
    },
    "grains": {
        "phase": "V3.4",
        "model_id": "grains_stocks_to_use_v3",
        "drivers": "USDA/WASDE stocks-to-use, DXY, seasonality context",
    },
    "crypto": {
        "phase": "V3.5",
        "model_id": "crypto_liquidity_risk_v3",
        "drivers": "liquidity, DXY, real yields, risk appetite",
    },
    "softs": {
        "phase": "V3.6",
        "model_id": "softs_balance_sheet_v3",
        "drivers": "daily ICE price, origin balance sheets, DXY, weather (when available)",
    },
}


def _valuation_asset_class(market: str) -> str:
    spec = get_instrument(market)
    if spec is None:
        return "other"
    if spec.asset_class == "commodities":
        subgroup = spec.subgroup or ""
        if subgroup == "energy":
            return "energy"
        if subgroup == "ag":
            return "grains"
        if subgroup == "soft":
            return "softs"
    if spec.asset_class == "macro" and spec.subgroup == "usd_index":
        return "fx"
    return spec.asset_class or "other"


def compute_valuation(
    *,
    market: str,
    as_of_week: str | None = None,
    **_kwargs: Any,
) -> dict[str, Any]:
    """Return fundamental valuation state from approved V3 models only."""
    asset_class = _valuation_asset_class(market)
    if asset_class == "fx":
        from hptl.valuation.fx_carry_real_yield_v3 import compute_fx_market_v3

        return compute_fx_market_v3(market, as_of_week=as_of_week)

    roadmap = ASSET_CLASS_ROADMAP.get(
        asset_class,
        {"phase": "—", "model_id": "—", "drivers": "asset-class model not defined"},
    )
    phase = roadmap["phase"]
    model_id = roadmap["model_id"]
    drivers = roadmap["drivers"]
    reason = (
        f"No approved valuation model exists for this asset class ({asset_class}). "
        f"Planned: {phase} {model_id}. "
        f"Do not substitute location or price percentile for valuation."
    )
    return {
        "market": market,
        "as_of_week": as_of_week,
        "asset_class": asset_class,
        "wired": False,
        "valuation_state": BIAS_UNAVAILABLE,
        "valuation_bias": BIAS_UNAVAILABLE,
        "valuation_score": None,
        "fair_value": None,
        "deviation_pct": None,
        "confidence": "none",
        "model_id": model_id if model_id != "—" else None,
        "valuation_phase": phase,
        "driver_summary": drivers,
        "valuation_reason": reason,
        "pass": False,
    }


def valuation_pass(bias: str, direction: str) -> bool:
    if bias == BIAS_UNAVAILABLE:
        return False
    d = direction.lower()
    if d == "long":
        return bias == "Undervalued"
    if d == "short":
        return bias == "Overvalued"
    return bias in {"Fair Value", "Neutral"}
