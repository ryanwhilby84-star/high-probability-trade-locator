"""Macro Intelligence models — Phase 5 architecture.

Bias labels and contributor statuses are closed enums so aggregation and UI
remain deterministic. No probabilities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ENGINE_VERSION = "macro_intelligence_v5"
PHASE = "5"

MacroBias = Literal[
    "Strongly Bullish",
    "Moderately Bullish",
    "Neutral",
    "Moderately Bearish",
    "Strongly Bearish",
]

ContributorStatus = Literal[
    "Strongly Bullish",
    "Moderately Bullish",
    "Bullish",
    "Neutral",
    "Bearish",
    "Moderately Bearish",
    "Strongly Bearish",
    "Unavailable",
]

BIAS_LABELS: tuple[str, ...] = (
    "Strongly Bullish",
    "Moderately Bullish",
    "Neutral",
    "Moderately Bearish",
    "Strongly Bearish",
)


@dataclass(frozen=True)
class MacroContributorResult:
    """One independent macro contributor output.

    Fields mirror the Phase 5 interface. ``weight`` is reserved for future
    aggregation and must not affect Phase 5 overall bias.
    """

    name: str
    status: ContributorStatus
    strength: float | None
    summary: str
    last_updated: str | None
    weight: float = 0.0
    contributor_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if not d.get("contributor_id"):
            d["contributor_id"] = self.name.lower().replace(" ", "_")
        return d


@dataclass
class MacroIntelligenceResult:
    instrument_id: str
    overall_macro_bias: MacroBias
    contributors: list[MacroContributorResult] = field(default_factory=list)
    status: str = "ok"
    engine: str = ENGINE_VERSION
    phase: str = PHASE
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "engine": self.engine,
            "phase": self.phase,
            "instrument_id": self.instrument_id,
            "overall_macro_bias": self.overall_macro_bias,
            "contributors": [c.to_dict() for c in self.contributors],
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "notes": list(self.notes),
            "no_trade_signals": True,
            "architecture_only": True,
        }
