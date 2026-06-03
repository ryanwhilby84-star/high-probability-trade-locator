"""User-facing scanner copy — narrative layers only (scores stay internal)."""

from __future__ import annotations

from typing import Any


def _structural_short(label: str, regime: str) -> str:
    lab = (label or "").strip()
    if lab.upper().startswith("STRUCTURAL "):
        return lab.replace("Structural ", "", 1).strip()
    mapping = {
        "structural_bullish": "Bullish",
        "structural_bearish": "Bearish",
        "accumulation": "Accumulation",
        "distribution": "Distribution",
        "neutral_rotation": "Neutral Rotation",
        "transitional": "Transitional",
    }
    return mapping.get(regime, lab or "—")


def _macro_narrative(alignment: str, macro_signal: str, structural_regime: str) -> str:
    sig = (macro_signal or "").lower().replace("-", "_")
    if alignment == "strong_tailwind":
        return "Strong macro tailwind"
    if alignment == "supportive":
        if structural_regime in {"structural_bullish", "accumulation"}:
            return "Still supportive"
        return "Macro supportive"
    if alignment == "strong_contradiction":
        return "Macro contradiction"
    if alignment == "headwind":
        if "risk_off" in sig:
            return "Risk-off pressure"
        if structural_regime in {"structural_bearish", "distribution"}:
            return "Still restrictive"
        return "Macro headwind"
    if alignment == "neutral" or not alignment:
        return "Macro neutral"
    if "risk_on" in sig:
        return "Liquidity supportive"
    if "risk_off" in sig:
        return "Risk-off pressure"
    return "Macro neutral"


def _exhaustion_narrative(extreme: str, label: str, net_pct: float | None) -> str:
    if not extreme or extreme == "none":
        return "Balanced"
    if extreme == "crowded_longs":
        if net_pct is not None and net_pct >= 80:
            return "Crowded longs developing"
        return "Crowded longs"
    if extreme == "euphoric_longs":
        return "Euphoric"
    if extreme == "crowded_shorts":
        if net_pct is not None and net_pct <= 20:
            return "Crowded shorts developing"
        return "Crowded shorts"
    if extreme == "capitulation_shorts":
        return "Capitulation"
    if extreme == "positioning_reset":
        return "Positioning reset"
    return label or extreme.replace("_", " ").title()


def _tactical_narrative(
    posture: str,
    posture_label: str,
    *,
    conflict: bool,
    conflict_narr: str | None,
    structural_regime: str,
    extreme: str,
) -> str:
    if conflict_narr:
        cn = conflict_narr.lower()
        if "pullback" in cn or "profit-taking" in cn:
            return "Wait for pullback into demand"
        if "covering" in cn:
            return "Rally likely first — avoid chasing breakdown"
    if posture == "stalk_long_pullback":
        return "Pullback within uptrend — stalk longs on pullback"
    if posture == "stalk_short_rally":
        return "Covering rally — tactical fade at supply"
    if posture == "avoid_chase":
        if extreme in {"crowded_longs", "euphoric_longs"}:
            return "Avoid chasing — crowded longs"
        if extreme in {"crowded_shorts", "capitulation_shorts"}:
            return "Short squeeze risk — avoid chasing downside"
        return "Avoid chasing"
    if posture == "stalk_long_continuation":
        return "Stalk longs on continuation"
    if posture == "stalk_short_continuation":
        return "Tactical short opportunity"
    if posture == "wait_confirmation":
        return "Wait for confirmation"
    if posture == "stand_aside":
        return "Stand aside — no clean edge"
    if posture == "watch":
        if structural_regime in {"structural_bullish", "accumulation"}:
            return "Watch — align flow before stalking longs"
        if structural_regime in {"structural_bearish", "distribution"}:
            return "Watch — align flow before stalking shorts"
        return "Watch"
    return posture_label or "—"


def build_scanner_display(
    *,
    structural_regime: str,
    structural_regime_label: str,
    flow_momentum_label: str,
    flow_l1_l2_conflict: bool,
    flow_conflict_narrative: str | None,
    macro_alignment: str,
    macro_signal: str,
    positioning_extreme: str,
    positioning_extreme_label: str,
    net_percentile: float | None,
    tactical_posture: str,
    tactical_posture_label: str,
) -> dict[str, Any]:
    """Five-line institutional scanner model (no blended CONF)."""
    structural = _structural_short(structural_regime_label, structural_regime)
    flow = flow_momentum_label or "Mixed"
    macro = _macro_narrative(macro_alignment, macro_signal, structural_regime)
    exhaustion = _exhaustion_narrative(positioning_extreme, positioning_extreme_label, net_percentile)
    tactical = _tactical_narrative(
        tactical_posture,
        tactical_posture_label,
        conflict=flow_l1_l2_conflict,
        conflict_narr=flow_conflict_narrative,
        structural_regime=structural_regime,
        extreme=positioning_extreme,
    )

    lines = [
        {"layer": "STRUCTURAL", "value": structural},
        {"layer": "FLOW", "value": flow},
        {"layer": "MACRO", "value": macro},
        {"layer": "EXHAUSTION", "value": exhaustion},
        {"layer": "TACTICAL", "value": tactical},
    ]
    if flow_l1_l2_conflict and flow_conflict_narrative:
        lines[1]["detail"] = flow_conflict_narrative

    return {
        "structural": structural,
        "flow": flow,
        "macro": macro,
        "exhaustion": exhaustion,
        "tactical": tactical,
        "lines": lines,
    }
