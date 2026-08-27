"""FX Valuation V3 overrides for thesis scoring snaps."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.thesis_tracker.fx_valuation_snap import (
    V2_MODEL,
    apply_fx_valuation_to_snap,
    has_fx_valuation,
    is_fx_market,
)

V3_MODEL = "fx_carry_real_yield_v3"
_V3_CACHE: dict[str, Any] | None = None
_FOUNDATION_CACHE: dict[str, Any] | None = None

COT_MARKET_TO_PAIR = {
    "Euro FX / 6E": "EUR/USD",
    "British Pound / 6B": "GBP/USD",
    "Australian Dollar / 6A": "AUD/USD",
    "NZ Dollar / 6N": "NZD/USD",
    "Japanese Yen / 6J": "USD/JPY",
    "Canadian Dollar / 6C": "USD/CAD",
    "Swiss Franc / 6S": "USD/CHF",
}

FX_V3_LIVE_PAIRS = frozenset({"EUR/USD", "AUD/USD", "USD/CAD", "EUR/GBP", "EUR/AUD"})


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _v3_latest() -> dict[str, Any]:
    global _V3_CACHE
    if _V3_CACHE is None:
        _V3_CACHE = _load_json(PROJECT_ROOT / "web-dashboard/public/data/fx_valuation_v3_latest.json")
    return _V3_CACHE


def _foundation_latest() -> dict[str, Any]:
    global _FOUNDATION_CACHE
    if _FOUNDATION_CACHE is None:
        _FOUNDATION_CACHE = _load_json(
            PROJECT_ROOT / "web-dashboard/public/data/fx_valuation_data_foundation_audit.json"
        )
    return _FOUNDATION_CACHE


def _pair_for_row(row: dict[str, Any]) -> str | None:
    fx = row.get("fx_valuation") if isinstance(row.get("fx_valuation"), dict) else {}
    if fx.get("pair"):
        return str(fx["pair"]).upper()
    market = str(row.get("market") or "").strip()
    if "/" in market and len(market) == 7:
        return market.upper()
    return COT_MARKET_TO_PAIR.get(market)


def _foundation_pass(pair_id: str) -> bool:
    pairs = (_foundation_latest().get("pairs") or {})
    return (pairs.get(pair_id) or {}).get("overall_status") == "PASS"


def _v3_block_for_pair(pair_id: str) -> dict[str, Any] | None:
    pairs = (_v3_latest().get("pairs") or {})
    return pairs.get(pair_id)


def _valuation_score_from_deviation(dev: float | None) -> float | None:
    if dev is None:
        return None
    return round(min(10.0, max(0.0, abs(float(dev)) / 2.0)), 1)


def apply_v3_valuation_to_snap(row: dict[str, Any] | None, snap: dict[str, Any]) -> dict[str, Any]:
    """Wire thesis valuation from V3 export only when live-scope + foundation + audit pass."""
    if not is_fx_market(row):
        return snap
    pair_id = _pair_for_row(row or {})
    if not pair_id:
        return apply_fx_valuation_to_snap(row, snap)

    in_scope = pair_id in FX_V3_LIVE_PAIRS or (pair_id == "USD/CHF" and _foundation_pass(pair_id))
    block = _v3_block_for_pair(pair_id)
    if not in_scope or not block or block.get("wired") is not True:
        return {
            **snap,
            "valuation_bias": "UNAVAILABLE",
            "valuation_score": None,
            "valuation_reason": block.get("explanation") if block else "FX Valuation V3 not wired for this pair.",
            "valuation_wired": False,
            "valuation_source": V3_MODEL,
            "valuation_condition": "Unavailable",
            "valuation_confidence": None,
            "valuation_is_fx": True,
        }

    state = str(block.get("valuation_state") or "Unavailable")
    bias = state if state != "Unavailable" else "UNAVAILABLE"
    return {
        **snap,
        "valuation_bias": bias,
        "valuation_score": _valuation_score_from_deviation(block.get("deviation_pct")),
        "valuation_reason": block.get("explanation") or block.get("driver_summary"),
        "valuation_wired": True,
        "valuation_source": V3_MODEL,
        "valuation_condition": state,
        "valuation_confidence": block.get("confidence"),
        "valuation_model_type": V3_MODEL,
        "valuation_gap_pct": block.get("deviation_pct"),
        "valuation_fair_value": block.get("fair_value"),
        "valuation_spot_price": block.get("spot_price"),
        "valuation_is_fx": True,
    }


def apply_fx_valuation_to_snap_v3_first(row: dict[str, Any] | None, snap: dict[str, Any]) -> dict[str, Any]:
    """Prefer V3 when wired; never fall back to V2 percentile-style valuation."""
    if not is_fx_market(row):
        return snap
    pair_id = _pair_for_row(row or {})
    block = _v3_block_for_pair(pair_id) if pair_id else None
    if block and block.get("wired") is True:
        return apply_v3_valuation_to_snap(row, snap)
    return apply_v3_valuation_to_snap(row, snap)
