"""Shared record shapes for calendar, narrative, and sentiment-interference layers.

All types describe *market environment and context* for discretionary use only.
They must not be interpreted as trade signals, entries, or predictions.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SentimentInterferenceLevel(str, Enum):
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    EXTREME = "EXTREME"


@dataclass(frozen=True)
class CalendarEventRecord:
    """Normalized economic calendar row (Layer 3)."""

    event_name: str
    country: str
    importance: str
    forecast: float | None
    actual: float | None
    previous: float | None
    surprise: float | None
    risk_bias: str
    affected_markets: tuple[str, ...]
    event_timestamp: datetime
    source: str
    macro_tags: tuple[str, ...] = field(default_factory=tuple)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NarrativeSnapshot:
    """Aggregated narrative view (Layer 4)."""

    narrative_theme: str
    narrative_direction: str
    narrative_intensity: float
    affected_assets: tuple[str, ...]
    volatility_risk: str
    macro_alignment: str
    evidence_headlines: tuple[str, ...] = field(default_factory=tuple)
    source_window: str = ""


@dataclass(frozen=True)
class SentimentInterferenceReport:
    """Emotional-distortion context (Layer 5), not directional sentiment."""

    sentiment_interference: SentimentInterferenceLevel
    emotional_flow_score: float
    crowding_risk: str
    narrative_dominance: float
    sentiment_vs_macro_conflict: str
    notes: str = ""
