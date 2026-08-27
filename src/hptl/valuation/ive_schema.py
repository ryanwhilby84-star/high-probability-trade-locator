"""Institutional Valuation Engine (IVE) — universal output contract (Phase 0).

No confidence scores. No trust metrics. Fair value + auditable lineage only.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ModelStatus = Literal["VALIDATED", "MODEL_INCOMPLETE", "DATA_STALE", "DATA_MISSING"]
ValuationGrade = Literal["FAIR", "MILD", "SIGNIFICANT", "EXTREME"]
ValuationLabel = Literal["Undervalued", "Fair Value", "Overvalued", "—"]

# Legacy / internal keys stripped from public valuation export.
CONFIDENCE_EXPORT_KEYS: tuple[str, ...] = (
    "confidence",
    "confidence_v1",
    "confidence_v2_score",
    "confidence_subscores",
    "confidence_subscore_bands",
    "confidence_explanation",
    "trust_grade",
    "valuation_confidence",
    "valuation_trust_grade",
)


@dataclass
class SourceLineage:
    """Provenance for a single normalized input."""

    source_name: str
    source_id: str
    source_date: str
    last_refresh: str
    field: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "source_name": self.source_name,
            "source_id": self.source_id,
            "source_date": self.source_date,
            "last_refresh": self.last_refresh,
            "field": self.field,
        }


@dataclass
class CalculationStep:
    step: int | str
    description: str
    value: str | float | int | None

    def to_dict(self) -> dict[str, Any]:
        return {"step": self.step, "description": self.description, "value": self.value}


@dataclass
class IVEOutput:
    """Permanent valuation contract — every model must satisfy this shape."""

    instrument: str
    current_price: float | None
    fair_value: float | None
    valuation_pct: float | None
    valuation_label: str
    valuation_grade: str
    model_name: str
    source_names: list[str]
    source_dates: list[str]
    inputs: dict[str, Any]
    calculation_breakdown: list[dict[str, Any]]
    last_updated: str
    model_status: str
    source_lineage: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


def valuation_grade_from_pct(valuation_pct: float | None) -> ValuationGrade:
    """Magnitude band only — not confidence or probability."""
    if valuation_pct is None:
        return "FAIR"
    mag = abs(float(valuation_pct))
    if mag <= 5.0:
        return "FAIR"
    if mag <= 15.0:
        return "MILD"
    if mag <= 30.0:
        return "SIGNIFICANT"
    return "EXTREME"


def valuation_label_from_block(block: dict[str, Any]) -> str:
    raw = (
        block.get("valuation_state")
        or block.get("valuation_bias")
        or block.get("valuation_label")
        or "—"
    )
    s = str(raw).strip()
    if s.upper() in {"UNAVAILABLE", "UNAVAILABLE", "NONE", ""}:
        return "—"
    if "under" in s.lower():
        return "Undervalued"
    if "over" in s.lower():
        return "Overvalued"
    if "fair" in s.lower():
        return "Fair Value"
    return s


def model_status_from_block(block: dict[str, Any]) -> ModelStatus:
    """Surface data/model gaps — no silent fallbacks."""
    missing = list(block.get("missing_inputs") or [])
    stale = list(block.get("stale_inputs") or [])
    wired = block.get("wired") is True
    fair = block.get("fair_value")
    spot = block.get("spot_price") or block.get("current_price")

    if missing:
        return "DATA_MISSING"
    if not wired or fair is None or spot is None:
        return "MODEL_INCOMPLETE"
    if stale:
        return "DATA_STALE"
    state = str(block.get("valuation_state") or "").lower()
    if state == "unavailable":
        return "MODEL_INCOMPLETE"
    return "VALIDATED"


def strip_confidence_fields(block: dict[str, Any]) -> dict[str, Any]:
    """Remove deprecated confidence/trust keys from export payload."""
    out = dict(block)
    for key in CONFIDENCE_EXPORT_KEYS:
        out.pop(key, None)
    return out
