"""Institutional-style context for instruments without direct COT (macro-only mode)."""

from __future__ import annotations

from typing import Any

from hptl.context.attention_engine import (
    PRIORITY_DEVELOPING,
    PRIORITY_HIGH,
    PRIORITY_LABELS,
    PRIORITY_LOW,
    PRIORITY_WATCHLIST,
)
from hptl.markets.instrument_registry import InstrumentSpec


def _alignment_from_transmission(macro_transmission: dict[str, Any]) -> tuple[str, str]:
    align = str(macro_transmission.get("asset_alignment") or "mixed")
    label = str(macro_transmission.get("asset_alignment_label") or "Macro mixed")
    mapping = {
        "supportive": ("supportive", "Macro Supportive"),
        "headwind": ("headwind", "Macro Headwind"),
        "conflicting": ("strong_contradiction", "Macro vs Macro-Only View"),
        "mixed": ("neutral", "Macro Neutral"),
    }
    return mapping.get(align, ("neutral", label))


def build_macro_only_attention(
    *,
    market: str,
    spec: InstrumentSpec,
    macro_transmission: dict[str, Any],
    macro_signal: str,
) -> dict[str, Any]:
    """Attention tier from macro transmission only (lower conviction)."""
    alerts: list[dict[str, str]] = []
    mvp = macro_transmission.get("macro_vs_price") or {}
    headline_tx = str(macro_transmission.get("headline") or "")

    if macro_transmission.get("asset_alignment") == "supportive":
        alerts.append({"icon": "🌐", "text": f"Macro supportive for {market} (no COT)", "kind": "macro"})
    elif macro_transmission.get("asset_alignment") == "headwind":
        alerts.append({"icon": "⚠️", "text": f"Macro headwind for {market} (no COT)", "kind": "macro"})
    if mvp.get("state", "").startswith("ignoring"):
        alerts.append(
            {
                "icon": "⚡",
                "text": "Macro vs price tension — positioning data unavailable",
                "kind": "macro",
            }
        )
    if spec.cot_proxy_of:
        alerts.append(
            {
                "icon": "🔗",
                "text": f"Related COT: {spec.cot_proxy_of} (proxy not auto-applied)",
                "kind": "proxy",
            }
        )
    if spec.positioning_status == "no_direct_pair_cot":
        alerts.append(
            {
                "icon": "ℹ️",
                "text": "No direct COT mapping — macro-only scanner mode",
                "kind": "data",
            }
        )

    score = 0.0
    if macro_transmission.get("asset_alignment") == "supportive":
        score += 22
    elif macro_transmission.get("asset_alignment") == "headwind":
        score += 20
    if mvp.get("state", "").startswith("ignoring"):
        score += 14
    if macro_signal == "risk_off":
        score += 8
    if spec.asset_class == "crypto" and macro_signal == "risk_on":
        score += 12

    tier = PRIORITY_LOW
    if macro_transmission.get("generic_rates_only"):
        if score >= 16:
            tier = PRIORITY_WATCHLIST
    elif score >= 40:
        tier = PRIORITY_DEVELOPING  # macro-only never raw HIGH; board may cap further
    elif score >= 28:
        tier = PRIORITY_DEVELOPING
    elif score >= 16:
        tier = PRIORITY_WATCHLIST

    if macro_transmission.get("generic_rates_only"):
        headline_tx = (
            macro_transmission.get("headline")
            or "Macro transmission incomplete — generic rates backdrop only."
        )
    dominant = headline_tx[:140] if headline_tx else f"{market} — macro-only context"
    return {
        "priority_tier": tier,
        "priority_label": PRIORITY_LABELS[tier],
        "priority_score": round(score, 1),
        "dominant_narrative": dominant,
        "priority_headline": (alerts[0]["text"] if alerts else dominant)[:72],
        "tactical_readable": "Macro context only — no COT positioning",
        "alerts": alerts[:5],
        "confidence": "low",
    }


def build_macro_only_institutional_context(
    *,
    market: str,
    spec: InstrumentSpec,
    macro_transmission: dict[str, Any],
    macro_signal: str | None,
    macro_score: float | None,
) -> dict[str, Any]:
    """Lightweight institutional_context bundle when COT is unavailable."""
    alignment, alignment_label = _alignment_from_transmission(macro_transmission)
    headline = str(macro_transmission.get("headline") or "Macro-only instrument")
    attention = build_macro_only_attention(
        market=market,
        spec=spec,
        macro_transmission=macro_transmission,
        macro_signal=str(macro_signal or "neutral"),
    )

    return {
        "data_mode": "macro_only",
        "positioning_status": spec.positioning_status,
        "cot_available": False,
        "has_cot_mapping": spec.has_cot_mapping,
        "cot_proxy_of": spec.cot_proxy_of,
        "structural_regime": "macro_context_only",
        "structural_regime_label": "Macro Context Only",
        "flow_momentum": "unavailable",
        "flow_momentum_label": "COT Unavailable",
        "macro_alignment": alignment,
        "macro_alignment_label": alignment_label,
        "macro_signal": macro_signal or "N/A",
        "positioning_extreme": "none",
        "tactical_posture": "watch",
        "tactical_posture_label": "Watch (macro only)",
        "zone_focus": "Macro / Drivers",
        "setup_type": "No direct COT — macro transmission only",
        "confidence_label": "Low",
        "scanner_display": {
            "structural": "No COT",
            "flow": "—",
            "macro": headline[:120],
            "exhaustion": "—",
            "tactical": attention["tactical_readable"],
            "lines": [
                {"layer": "STRUCTURAL", "value": "No direct COT", "detail": spec.positioning_status},
                {"layer": "FLOW", "value": "—", "detail": "Positioning requires COT or future proxy"},
                {"layer": "MACRO", "value": alignment_label, "detail": headline[:200]},
                {"layer": "EXHAUSTION", "value": "—", "detail": None},
                {"layer": "TACTICAL", "value": "Macro watch", "detail": attention["tactical_readable"]},
            ],
        },
        "macro_transmission": macro_transmission,
        "attention": attention,
        "internal_scores": {"macro_only": True, "confidence": 0.35},
    }
