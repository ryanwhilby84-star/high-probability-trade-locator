"""Explicit-source intelligence pipeline (news, calendar, impulse) — no COT mutations.

Adapters return normalized records or explicit ``not available — source not configured``.
"""

from hptl.intelligence.catalyst_loader import default_catalyst_config_path, load_catalyst_config
from hptl.intelligence.event_adapter import fetch_normalized_events
from hptl.intelligence.impulse_adapter import compute_simple_impulse
from hptl.intelligence.intelligence_engine import build_intelligence_bundle
from hptl.intelligence.news_adapter import fetch_normalized_headlines

__all__ = [
    "build_intelligence_bundle",
    "compute_simple_impulse",
    "default_catalyst_config_path",
    "fetch_normalized_events",
    "fetch_normalized_headlines",
    "load_catalyst_config",
]
