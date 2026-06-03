"""Filter economic calendar rows to high-signal macro prints (no invented labels)."""
from __future__ import annotations

import re

# CPI, FOMC, NFP, PMI, GDP, rates, employment, major central banks
_MACRO_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bcpi\b",
        r"\bconsumer price\b",
        r"\bpce\b",
        r"\bfomc\b",
        r"\bfed\b.*\b(rate|minutes|decision|chair)",
        r"\bnfp\b",
        r"\bnon[- ]?farm\b",
        r"\bpayroll\b",
        r"\bunemployment rate\b",
        r"\bjobless\b",
        r"\bpmi\b",
        r"\bism\b",
        r"\bgdp\b",
        r"\bgross domestic\b",
        r"\brate decision\b",
        r"\binterest rate\b",
        r"\bpolicy rate\b",
        r"\bcentral bank\b",
        r"\becb\b",
        r"\bboe\b",
        r"\bboj\b",
        r"\brba\b",
        r"\bsnb\b",
        r"\bretail sales\b",
        r"\btrade balance\b",
        r"\binitial claims\b",
        r"\bmanufacturing production\b",
        r"\bindustrial production\b",
    )
)


def is_macro_calendar_event(event_name: str) -> bool:
    text = (event_name or "").strip()
    if not text:
        return False
    return any(p.search(text) for p in _MACRO_PATTERNS)
