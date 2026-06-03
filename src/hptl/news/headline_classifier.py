"""Rule-based headline tagging for explainable narrative themes.

Not a price predictor: labels describe dominant *topics* in text.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class HeadlineClassification:
    narrative_theme: str
    narrative_direction: str
    theme_score: float


_LEXICON: list[tuple[str, str, str, float]] = [
    (r"inflation|cpi|pce|prices?\s+pressures?", "inflation_fears", "pressure", 1.0),
    (r"recession|slowdown|contraction|hard\s+landing", "recession_fears", "pressure", 1.0),
    (r"sanction|embargo|export\s+ban|blacklist", "sanctions", "escalation", 1.0),
    (r"war|missile|strike|military|invasion|conflict", "geopolitical_escalation", "escalation", 1.2),
    (r"drought|flood|hurricane|crop\s+failure|weather", "weather_supply", "supply", 0.9),
    (r"opec|oil\s+supply|pipeline|energy\s+crisis|power\s+outage", "energy_shock", "supply", 1.1),
    (r"liquidity|repo|funding\s+stress|credit\s+crunch", "liquidity_concerns", "financial_stress", 1.0),
    (r"safe\s+haven|flight\s+to\s+quality|gold\s+rush", "safe_haven_flows", "risk_off_tone", 0.8),
]


def classify_headline(text: str) -> HeadlineClassification:
    t = text.lower()
    best = HeadlineClassification("general_macro", "neutral", 0.2)
    for pattern, theme, direction, weight in _LEXICON:
        if re.search(pattern, t, re.I):
            return HeadlineClassification(theme, direction, weight)
    return best
