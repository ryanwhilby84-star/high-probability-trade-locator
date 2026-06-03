"""Sentiment *interference* (Layer 5): emotional crowding / distortion context.

Explicitly not directional sentiment for entries. Outputs describe whether
psychology may be distorting price discovery, to support human discretion.
"""
from __future__ import annotations

from hptl.news.contracts import NarrativeSnapshot, SentimentInterferenceLevel, SentimentInterferenceReport


def compute_sentiment_interference(
    narratives: list[NarrativeSnapshot],
    *,
    macro_regime_label: str = "",
) -> SentimentInterferenceReport:
    """Combine narrative intensity with simple heuristics for interference level."""
    if not narratives:
        return SentimentInterferenceReport(
            sentiment_interference=SentimentInterferenceLevel.LOW,
            emotional_flow_score=0.0,
            crowding_risk="low",
            narrative_dominance=0.0,
            sentiment_vs_macro_conflict="none",
            notes="No narrative headlines processed; interference treated as low.",
        )

    top = narratives[0]
    dominance = min(1.0, top.narrative_intensity / 8.0)
    flow = min(10.0, top.narrative_intensity)

    level = SentimentInterferenceLevel.LOW
    if top.narrative_intensity >= 6:
        level = SentimentInterferenceLevel.EXTREME
    elif top.narrative_intensity >= 4:
        level = SentimentInterferenceLevel.HIGH
    elif top.narrative_intensity >= 2:
        level = SentimentInterferenceLevel.MODERATE

    crowding = "low"
    if level in {SentimentInterferenceLevel.HIGH, SentimentInterferenceLevel.EXTREME}:
        crowding = "elevated_single_theme_attention"
    elif level == SentimentInterferenceLevel.MODERATE:
        crowding = "watch_theme_crowding"

    conflict = "none"
    ml = (macro_regime_label or "").lower()
    if ml and "risk_on" in ml and top.narrative_direction in {"pressure", "escalation", "financial_stress"}:
        conflict = "headline_stress_vs_risk_on_macro_label"
    if ml and "risk" in ml and "off" in ml and top.narrative_direction == "neutral":
        conflict = "macro_cautious_vs_quiet_headlines"

    return SentimentInterferenceReport(
        sentiment_interference=level,
        emotional_flow_score=round(flow, 2),
        crowding_risk=crowding,
        narrative_dominance=round(dominance, 3),
        sentiment_vs_macro_conflict=conflict,
        notes=f"Dominant theme: {top.narrative_theme}; intensity={top.narrative_intensity}.",
    )
