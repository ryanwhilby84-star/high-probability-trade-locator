"""Attach Macro Hub institutional reads to confluence rows for first-class macro assets."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from hptl.config import EXPORTS_DIR, PROCESSED_DIR, PROJECT_ROOT
from hptl.context.attention_engine import PRIORITY_LABELS
from hptl.macro_hub.macro_institutional import (
    MACRO_TRANSMISSION_TARGETS,
    build_macro_driver_transmission,
    build_macro_institutional_attention,
    build_macro_institutional_read,
    build_scanner_macro_drivers,
)
from hptl.markets.instrument_registry import MACRO_INSTITUTIONAL_MARKETS, MACRO_RATE_MARKETS

_MACRO_HUB_PATHS = (
    EXPORTS_DIR / "macro_hub_latest.json",
    PROCESSED_DIR / "macro_hub_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "macro_hub_latest.json",
)


@lru_cache(maxsize=1)
def _load_macro_hub_doc() -> dict[str, Any] | None:
    for path in _MACRO_HUB_PATHS:
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
    return None


def _institutional_context_for_macro(market_id: str, doc: dict[str, Any]) -> dict[str, Any]:
    read = build_macro_institutional_read(market_id, doc)
    tx = build_macro_driver_transmission(market_id, doc)
    attention = build_macro_institutional_attention(market_id, doc)
    tier = attention.get("priority_tier") or "watchlist"
    return {
        "data_mode": "macro_institutional",
        "positioning_status": "macro_institutional",
        "cot_available": market_id == "US Dollar Index / DX",
        "has_cot_mapping": market_id == "US Dollar Index / DX",
        "structural_regime": read.get("direction") or "flat",
        "structural_regime_label": read.get("current_stance"),
        "flow_momentum": read.get("weekly_change"),
        "flow_momentum_label": read.get("four_week_change"),
        "macro_alignment": tx.get("asset_alignment") or "mixed",
        "macro_alignment_label": read.get("current_stance"),
        "macro_signal": read.get("current_stance"),
        "positioning_extreme": "none",
        "tactical_posture": "watch",
        "tactical_posture_label": read.get("action_bias", "")[:80],
        "zone_focus": "Macro / Drivers",
        "setup_type": read.get("trader_read"),
        "confidence_label": "Medium",
        "scanner_display": {
            "structural": read.get("current_stance"),
            "flow": read.get("weekly_change"),
            "macro": read.get("macro_interpretation"),
            "exhaustion": read.get("four_week_change"),
            "tactical": read.get("action_bias"),
            "lines": [
                {"layer": "STANCE", "value": read.get("current_stance"), "detail": None},
                {"layer": "WEEKLY", "value": read.get("weekly_change"), "detail": None},
                {"layer": "4-WEEK", "value": read.get("four_week_change"), "detail": None},
                {"layer": "TRADER READ", "value": read.get("trader_read"), "detail": None},
                {"layer": "ACTION BIAS", "value": read.get("action_bias"), "detail": None},
            ],
        },
        "macro_transmission": tx,
        "macro_institutional_read": read,
        "attention": {
            **attention,
            "priority_label": PRIORITY_LABELS.get(tier, tier),
            "tactical_readable": read.get("action_bias"),
        },
        "internal_scores": {"macro_institutional": True, "confidence": 0.65},
    }


def macro_hub_institutional_fields_for_market(market_id: str) -> dict[str, Any]:
    doc = _load_macro_hub_doc()
    if not doc or market_id not in MACRO_INSTITUTIONAL_MARKETS:
        return {}
    read = build_macro_institutional_read(market_id, doc)
    if read.get("level") is None and market_id not in {"US Dollar Index / DX"}:
        # Allow DX through if COT exists even without DXY price
        if market_id == "US Dollar Index / DX":
            cot = (doc.get("usd") or {}).get("cot") or {}
            if cot.get("net") is None:
                return {}
        else:
            return {}

    inst = _institutional_context_for_macro(market_id, doc)
    tx = inst.get("macro_transmission") or {}
    fields: dict[str, Any] = {
        "macro_hub_institutional_attached": True,
        "data_mode": "macro_institutional",
        "data_status": "macro_institutional",
        "positioning_status": "macro_institutional" if market_id in MACRO_RATE_MARKETS else "cot_available",
        "macro_relationship_map": {"drives": list(MACRO_TRANSMISSION_TARGETS.get(market_id, []))},
        "macro_institutional_read": read,
        "institutional_context": inst,
        "macro_transmission": tx,
        "four_week_positioning_story": read.get("four_week_change"),
        "positioning_interpretation": read.get("trader_read"),
        "trader_action_note": read.get("action_bias"),
        "final_context": read.get("current_stance"),
        "final_context_reason": read.get("trader_read"),
        "technical_action_note": read.get("action_bias"),
        "macro_regime": read.get("current_stance"),
        "zone_focus": "Macro / Drivers",
        "setup_type": read.get("trader_read"),
        "confidence_label": "Medium",
        "missing_reason": None,
        "institutional_flow_summary": read.get("trader_read"),
        "cot_status": "macro_institutional" if market_id in MACRO_RATE_MARKETS else "cot_available",
        "cot_status_label": (
            "Macro institutional (Macro Hub series)"
            if market_id in MACRO_RATE_MARKETS
            else "COT + DXY (Macro Hub)"
        ),
    }
    if market_id in MACRO_RATE_MARKETS:
        fields["cot_bias"] = read.get("current_stance")
        fields["cot_reason"] = read.get("trader_read")
    return fields


def apply_macro_hub_institutional_fallback(records: list[dict[str, Any]]) -> int:
    """Patch latest-week macro institutional rows and attach scanner macro drivers."""
    if not records:
        return 0
    doc = _load_macro_hub_doc()
    if not doc:
        return 0

    latest_week = max(str(r.get("date") or "") for r in records)
    if not latest_week:
        return 0

    patched = 0
    for rec in records:
        if str(rec.get("date") or "") != latest_week:
            continue
        market = str(rec.get("market") or "")
        if market in MACRO_INSTITUTIONAL_MARKETS:
            fields = macro_hub_institutional_fields_for_market(market)
            if fields:
                rec.update(fields)
                meta = dict(rec.get("instrument_meta") or {})
                meta["positioning_status"] = fields.get("positioning_status")
                meta["data_status"] = "macro_institutional"
                if market == "US Dollar Index / DX":
                    meta["has_cot_mapping"] = True
                rec["instrument_meta"] = meta
                patched += 1

    # Scanner macro-driver panel for tradable assets on latest week
    for rec in records:
        if str(rec.get("date") or "") != latest_week:
            continue
        market = str(rec.get("market") or "")
        if market in MACRO_INSTITUTIONAL_MARKETS:
            continue
        drivers = build_scanner_macro_drivers(market, doc)
        if drivers.get("drivers"):
            rec["scanner_macro_drivers"] = drivers

    return patched


def clear_macro_hub_institutional_cache() -> None:
    _load_macro_hub_doc.cache_clear()
