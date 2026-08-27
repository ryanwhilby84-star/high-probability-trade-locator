"""Reusable setup-ranking grades and pillar models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

ENGINE_VERSION = "FX Setup Ranking Engine V2 (Three-Layer)"

# Pillar weights (sum = 1.0). Location highest per spec.
PILLAR_WEIGHTS: dict[str, float] = {
    "relative_strength": 0.175,
    "valuation": 0.175,
    "positioning": 0.175,
    "seasonality": 0.175,
    "location": 0.30,
}

LOCATION_APLUS_MIN = 8.0
APLUS_MIN_SCORE = 90.0
A_MIN_SCORE = 80.0
BPLUS_MIN_SCORE = 70.0
B_MIN_SCORE = 60.0


@dataclass(frozen=True)
class PillarScore:
    key: str
    label: str
    score: float  # 0-10
    bias: str | None = None
    summary: str = ""
    detail: str = ""
    aligned: bool = False
    missing: bool = False
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "score": self.score,
            "score_display": f"{round(self.score)}/10",
            "bias": self.bias,
            "summary": self.summary,
            "detail": self.detail,
            "aligned": self.aligned,
            "missing": self.missing,
            "meta": dict(self.meta),
        }


def clamp_score(v: float, lo: float = 0.0, hi: float = 10.0) -> float:
    return round(max(lo, min(hi, float(v))), 1)


def setup_quality_score(pillars: dict[str, PillarScore]) -> float:
    total = 0.0
    for key, weight in PILLAR_WEIGHTS.items():
        p = pillars.get(key)
        if p is None or p.missing:
            continue
        total += weight * p.score * 10.0  # each pillar 0-10 → weighted 0-100
    return round(min(100.0, total), 1)


def grade_from_score_10(
    score_10: float,
    *,
    readiness_grade: str | None = None,
    for_aplus: bool = False,
) -> str:
    """Letter grade from 0-10 layer score."""
    s = float(score_10)
    if s >= 9.0:
        g = "A+"
    elif s >= 8.0:
        g = "A"
    elif s >= 7.0:
        g = "B+"
    elif s >= 6.0:
        g = "B"
    else:
        g = "C"
    if g == "A+" and for_aplus and readiness_grade:
        if grade_rank(readiness_grade) < 4:
            return "A"
    return g


def grade_from_score(setup_score: float, *, location_score: float | None) -> str:
    loc = location_score if location_score is not None else 0.0
    if setup_score >= APLUS_MIN_SCORE and loc >= LOCATION_APLUS_MIN:
        return "A+"
    if setup_score >= APLUS_MIN_SCORE:
        return "A"  # capped — location blocks A+
    if setup_score >= A_MIN_SCORE:
        return "A"
    if setup_score >= BPLUS_MIN_SCORE:
        return "B+"
    if setup_score >= B_MIN_SCORE:
        return "B"
    return "C"


def grade_rank(grade: str) -> int:
    return {"A+": 5, "A": 4, "B+": 3, "B": 2, "C": 1}.get(grade, 0)
