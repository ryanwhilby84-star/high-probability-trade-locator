"""One-sentence dominant market narrative (presentation layer over L1–L5)."""

from __future__ import annotations

from typing import Any


def build_dominant_narrative(
    *,
    structural_regime: str,
    structural_short: str,
    flow_momentum: str,
    flow_label: str,
    macro_alignment: str,
    macro_short: str,
    positioning_extreme: str,
    exhaustion_short: str,
    flow_conflict: bool,
    weeks_in_regime: int,
    tactical_readable: str,
    pending_flip: str | None,
) -> str:
    """Single headline sentence traders read first."""
    struct = structural_short or "Unclear structure"
    flow = flow_label or "Mixed flow"

    # Regime in flux
    if pending_flip:
        return f"Structure may be shifting toward {pending_flip.replace('_', ' ')} — wait for confirmation before leaning in."

    if weeks_in_regime <= 1 and structural_regime == "transitional":
        return "Regime in transition — no dominant edge until structure clarifies over 2–3 weeks."

    # Crowding dominates
    if positioning_extreme in {"euphoric_longs", "crowded_longs"}:
        if flow_momentum in {"weakening", "profit_taking", "long_liquidation"}:
            return "Bull trend cooling at crowded levels — avoid momentum chasing; pullbacks only."
        return "Crowded long positioning — extension risk elevated; fade chasing breakouts."

    if positioning_extreme in {"capitulation_shorts", "crowded_shorts"}:
        if flow_momentum in {"improving", "short_covering"}:
            return "Short squeeze risk building — avoid chasing breakdowns; rallies may run first."
        return "Crowded shorts — squeeze risk if flow keeps improving against structure."

    # L1/L2 tension (not reversal)
    if flow_conflict:
        if structural_regime in {"structural_bullish", "accumulation"}:
            return "Bull trend cooling — profit-taking within larger uptrend; buy pullbacks, not breakouts."
        if structural_regime in {"structural_bearish", "distribution"}:
            return "Bear trend with covering rally underway — fade strength carefully, not blind shorts."
        return f"{struct} with conflicting weekly flow — treat as two-way tape until flow realigns."

    # Macro tension
    if macro_alignment in {"strong_contradiction", "headwind", "risk_off_pressure"}:
        if structural_regime in {"structural_bullish", "accumulation"}:
            return f"{struct} tape but macro headwind — size down longs until backdrop improves."
        if structural_regime in {"structural_bearish", "distribution"}:
            return f"{struct} with macro still restrictive — rallies may be tactical, not regime change."

    if macro_alignment in {"strong_tailwind", "liquidity_supportive", "supportive"}:
        if structural_regime in {"structural_bullish", "accumulation"} and flow_momentum in {
            "accelerating",
            "improving",
        }:
            return "Macro improving while positioning builds — potential expansion phase if flow holds."
        if structural_regime in {"structural_bearish", "distribution"}:
            return "Macro supportive but structure still bearish — don't confuse backdrop with reversal."

    # Clean directional flow
    if structural_regime == "structural_bullish":
        if flow_momentum in {"accelerating", "improving"}:
            return f"Bull momentum intact — {tactical_readable.lower()}."
        if flow_momentum in {"weakening", "profit_taking"}:
            return "Bull trend softening — prefer pullbacks over chasing strength."
        return f"{struct} regime ({weeks_in_regime}w) — {flow} this week; {tactical_readable.lower()}."

    if structural_regime == "structural_bearish":
        if flow_momentum in {"accelerating", "weakening"}:
            return f"Bear pressure building — {tactical_readable.lower()}."
        if flow_momentum in {"improving", "short_covering"}:
            return "Bear structure with short covering — rally risk before trend resumes."
        return f"{struct} regime ({weeks_in_regime}w) — {flow} this week; {tactical_readable.lower()}."

    if structural_regime == "accumulation":
        return "Early accumulation behaviour — shorts covering while still net short; watch for structure upgrade."

    if structural_regime == "distribution":
        return "Distribution at highs — longs exiting into strength; breakout quality weakening."

    if structural_regime == "neutral_rotation":
        return "Neutral rotation — low urgency; capital better deployed elsewhere this week."

    return f"{struct} — {flow}, {macro_short}; {tactical_readable.lower()}."
