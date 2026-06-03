"""L5 — Tactical posture and action (sole source of setup_type / scanner action)."""

from __future__ import annotations

from typing import Any

POSTURE_LABELS = {
    "stalk_long_pullback": "Stalk Long on Pullback",
    "stalk_short_rally": "Stalk Short on Rally",
    "wait_confirmation": "Wait for Confirmation",
    "avoid_chase": "Avoid Chasing",
    "stand_aside": "Stand Aside",
    "stalk_long_continuation": "Stalk Long Continuation",
    "stalk_short_continuation": "Stalk Short Continuation",
    "watch": "Watch",
}


def _tactical_confidence(
    structural: dict[str, Any],
    flow: dict[str, Any],
    macro: dict[str, Any],
    exhaust: dict[str, Any],
    *,
    weeks_in_regime: int,
) -> float:
    base = abs(structural.get("structural_score_ema", 0)) * 0.45
    base += flow.get("flow_intensity", 0) * 0.15
    base += macro.get("macro_alignment_score", 50) * 0.25
    base -= exhaust.get("exhaustion_risk_score", 0) * 0.2
    if weeks_in_regime < 2:
        base *= 0.75
    if flow.get("flow_l1_l2_conflict"):
        base *= 0.9
    chase = exhaust.get("chase_downgrade", 0)
    base *= 1.0 - chase
    return max(5.0, min(95.0, base))


def compute_tactical_layer(
    *,
    structural_regime: str,
    structural_label: str,
    weeks_in_regime: int,
    flow: dict[str, Any],
    macro: dict[str, Any],
    exhaust: dict[str, Any],
    structural: dict[str, Any],
) -> dict[str, Any]:
    conflict = flow.get("flow_l1_l2_conflict")
    conflict_narr = flow.get("flow_conflict_narrative")
    extreme = exhaust.get("positioning_extreme", "none")
    chase_penalty = exhaust.get("chase_downgrade", 0)
    macro_align = macro.get("macro_alignment", "neutral")
    flow_m = flow.get("flow_momentum", "mixed")

    posture = "watch"
    setup_type = "No clean institutional edge"
    confidence_label = "Low"
    zone_focus = "Wait"

    if weeks_in_regime < 2 and structural_regime == "transitional":
        posture = "wait_confirmation"
        setup_type = "Regime transition — wait for confirmation"
        confidence_label = "Low"
    elif conflict and structural_regime in {"structural_bullish", "accumulation"}:
        posture = "stalk_long_pullback" if chase_penalty < 0.25 else "avoid_chase"
        setup_type = (
            "Pullback / profit-taking within structural uptrend"
            if posture == "stalk_long_pullback"
            else "Bullish but overextended — avoid chasing"
        )
        confidence_label = "Medium" if chase_penalty < 0.25 else "Low"
        zone_focus = "Demand on pullback"
    elif conflict and structural_regime in {"structural_bearish", "distribution"}:
        posture = "stalk_short_rally" if chase_penalty < 0.25 else "avoid_chase"
        setup_type = (
            "Covering rally within structural downtrend"
            if posture == "stalk_short_rally"
            else "Crowded shorts — squeeze risk, avoid chasing breakdown"
        )
        confidence_label = "Medium" if chase_penalty < 0.25 else "Low"
        zone_focus = "Supply on rally"
    elif structural_regime == "structural_bullish" and flow_m in {"accelerating", "improving"}:
        if extreme in {"crowded_longs", "euphoric_longs"} or chase_penalty >= 0.3:
            posture = "avoid_chase"
            setup_type = "Structural bullish but crowded — wait for pullback"
        elif macro_align in {"headwind", "strong_contradiction"}:
            posture = "watch"
            setup_type = "Structural bullish with macro headwind"
        else:
            posture = "stalk_long_continuation"
            setup_type = "Long continuation / demand reaction"
        confidence_label = "Medium to High" if posture == "stalk_long_continuation" else "Medium"
        zone_focus = "Demand"
    elif structural_regime == "structural_bearish" and flow_m in {"accelerating", "weakening"}:
        if extreme in {"crowded_shorts", "capitulation_shorts"}:
            posture = "avoid_chase"
            setup_type = "Structural bearish but crowded shorts — squeeze risk"
        elif macro_align in {"supportive", "strong_tailwind"}:
            posture = "watch"
            setup_type = "Structural bearish with macro tailwind — fade carefully"
        else:
            posture = "stalk_short_continuation"
            setup_type = "Short continuation / supply reaction"
        confidence_label = "Medium to High" if posture == "stalk_short_continuation" else "Medium"
        zone_focus = "Supply"
    elif structural_regime == "neutral_rotation":
        posture = "stand_aside"
        setup_type = "Neutral rotation — no clean edge"
        confidence_label = "Low"
    else:
        posture = "wait_confirmation"
        setup_type = "Transitional — wait for clearer multi-week structure"
        confidence_label = "Low"

    conf = _tactical_confidence(structural, flow, macro, exhaust, weeks_in_regime=weeks_in_regime)

    summary_lines = [
        structural_label,
        flow.get("flow_momentum_label", "Mixed flow"),
    ]
    if conflict_narr:
        summary_lines.append(conflict_narr)
    else:
        summary_lines.append(macro.get("macro_alignment_label", "Macro neutral"))
    if extreme != "none":
        summary_lines.append(exhaust.get("positioning_extreme_label", extreme))
    summary_lines.append(POSTURE_LABELS.get(posture, posture))

    return {
        "tactical_posture": posture,
        "tactical_posture_label": POSTURE_LABELS.get(posture, posture),
        "tactical_confidence": round(conf, 1),
        "setup_type": setup_type,
        "confidence_label": confidence_label,
        "zone_focus": zone_focus,
        "summary_lines": summary_lines[:5],
    }
