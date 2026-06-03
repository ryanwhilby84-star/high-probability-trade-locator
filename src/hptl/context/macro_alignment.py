"""L3 — Macro alignment vs structural regime (permission layer)."""

from __future__ import annotations

from typing import Any

MACRO_LABELS = {
    "strong_tailwind": "Strong Macro Tailwind",
    "supportive": "Macro Supportive",
    "neutral": "Macro Neutral",
    "headwind": "Macro Headwind",
    "strong_contradiction": "Macro Contradiction",
    "liquidity_supportive": "Liquidity Supportive",
    "risk_off_pressure": "Risk-Off Pressure",
}


def _structural_side(regime: str) -> str:
    if regime in {"structural_bullish", "accumulation"}:
        return "long"
    if regime in {"structural_bearish", "distribution"}:
        return "short"
    return "neutral"


def _macro_side(macro_signal: str) -> str:
    s = (macro_signal or "").lower().replace("-", "_")
    if "risk_on" in s or s == "riskon":
        return "long"
    if "risk_off" in s or s == "riskoff":
        return "short"
    return "neutral"


def compute_macro_layer(
    *,
    structural_regime: str,
    macro_signal: str | None,
    macro_score: float | None,
) -> dict[str, Any]:
    side = _structural_side(structural_regime)
    m_side = _macro_side(macro_signal or "")
    ms = float(macro_score) if macro_score is not None else 0.0

    sig = (macro_signal or "").lower().replace("-", "_")

    if side == "neutral" or m_side == "neutral" or not macro_signal:
        alignment = "neutral"
        score = 50.0
    elif side == m_side:
        alignment = "strong_tailwind" if ms >= 7 else "supportive"
        score = min(100.0, 55.0 + ms * 4.5)
        if "risk_on" in sig and alignment == "supportive":
            alignment = "liquidity_supportive"
    else:
        alignment = "strong_contradiction" if ms >= 6 else "headwind"
        score = max(0.0, 45.0 - ms * 4.0)
        if "risk_off" in sig and alignment == "headwind":
            alignment = "risk_off_pressure"

    return {
        "macro_alignment": alignment,
        "macro_alignment_label": MACRO_LABELS.get(alignment, alignment.replace("_", " ").title()),
        "macro_alignment_score": round(score, 1),
        "macro_signal": macro_signal or "N/A",
    }
