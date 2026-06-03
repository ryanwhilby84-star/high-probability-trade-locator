"""Aggregate classified headlines into narrative snapshots (Layer 4)."""
from __future__ import annotations

from collections import Counter, defaultdict

from hptl.news.contracts import NarrativeSnapshot
from hptl.news.headline_classifier import classify_headline


def build_narrative_snapshots(headlines: list[str]) -> list[NarrativeSnapshot]:
    """Group headlines by theme; intensity scales with count and classifier weight."""
    buckets: dict[str, list[HeadlineClassification]] = defaultdict(list)
    evidence: dict[str, list[str]] = defaultdict(list)
    for h in headlines:
        c = classify_headline(h)
        buckets[c.narrative_theme].append(c)
        evidence[c.narrative_theme].append(h[:200])
    snaps: list[NarrativeSnapshot] = []
    for theme, clist in buckets.items():
        intensity = sum(x.theme_score for x in clist)
        directions = Counter(x.narrative_direction for x in clist)
        direction = directions.most_common(1)[0][0]
        vol = "elevated" if intensity >= 3 else "moderate" if intensity >= 1.5 else "contained"
        macro_align = "mixed"
        if theme in {"inflation_fears", "recession_fears", "liquidity_concerns"}:
            macro_align = "headline_stress_vs_soft_macro_possible"
        snaps.append(
            NarrativeSnapshot(
                narrative_theme=theme,
                narrative_direction=direction,
                narrative_intensity=round(float(intensity), 3),
                affected_assets=_assets_for_theme(theme),
                volatility_risk=vol,
                macro_alignment=macro_align,
                evidence_headlines=tuple(evidence[theme][:5]),
            )
        )
    snaps.sort(key=lambda s: s.narrative_intensity, reverse=True)
    return snaps


def _assets_for_theme(theme: str) -> tuple[str, ...]:
    mapping = {
        "inflation_fears": ("rates", "gold", "real_assets"),
        "recession_fears": ("equities", "credit", "cyclicals"),
        "geopolitical_escalation": ("energy", "defense", "safe_havens"),
        "sanctions": ("commodities", "fx_em", "energy"),
        "weather_supply": ("agriculture", "food"),
        "energy_shock": ("energy", "transport", "inflation_linkages"),
        "liquidity_concerns": ("credit", "equities", "fx_funding"),
        "safe_haven_flows": ("gold", "usd", "bonds_long_duration"),
        "general_macro": ("cross_asset"),
    }
    return mapping.get(theme, ("cross_asset",))
