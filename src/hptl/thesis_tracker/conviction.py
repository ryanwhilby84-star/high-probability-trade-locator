"""Composite conviction, trend, and age — pure functions.

The HPTL pipeline has no single ``overall_conviction`` field, so the tracker
derives its own 0-100 composite from the components that exist today
(COT, macro, structural). Components that are not yet wired (valuation,
seasonality, retail positioning) are excluded entirely and the weights are
renormalized over the present components — nothing is fabricated.
"""

from __future__ import annotations

from typing import Any

# Relative weights for currently-available components. Renormalized over whichever
# components are present in a given snapshot. Future components can be appended
# here once their engines exist (valuation / seasonality / retail).
COMPONENT_WEIGHTS: dict[str, float] = {
    "cot": 0.45,
    "macro": 0.25,
    "structural": 0.30,
}

TREND_IMPROVING = "improving"
TREND_STABLE = "stable"
TREND_DETERIORATING = "deteriorating"
TREND_THRESHOLD = 3.0  # points of net change over the window


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _component_values(snapshot: dict[str, Any]) -> dict[str, float]:
    """Normalize the available raw fields to a 0-100 scale."""
    out: dict[str, float] = {}
    cot = snapshot.get("cot_score")
    if isinstance(cot, (int, float)):
        out["cot"] = _clamp(float(cot) * 10.0)  # engine 0-10 -> 0-100
    macro = snapshot.get("macro_score")
    if isinstance(macro, (int, float)):
        out["macro"] = _clamp(float(macro) * 10.0)  # engine 0-10 -> 0-100
    structural = snapshot.get("structural_score")
    if isinstance(structural, (int, float)):
        out["structural"] = _clamp(float(structural))  # already ~0-100
    return out


def compute_conviction(snapshot: dict[str, Any]) -> tuple[int | None, list[str]]:
    """Return (conviction 0-100, present component keys) for one snapshot."""
    comps = _component_values(snapshot)
    if not comps:
        return None, []
    total_w = sum(COMPONENT_WEIGHTS.get(k, 0.0) for k in comps)
    if total_w <= 0:
        return None, sorted(comps)
    score = sum(comps[k] * COMPONENT_WEIGHTS.get(k, 0.0) for k in comps) / total_w
    return int(round(_clamp(score))), sorted(comps)


def annotate_conviction(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill conviction_score + conviction_components_present on each snapshot."""
    for snap in snapshots:
        score, present = compute_conviction(snap)
        snap["conviction_score"] = score
        snap["conviction_components_present"] = present
    return snapshots


def conviction_series(snapshots: list[dict[str, Any]]) -> list[int]:
    """Ordered list of non-null conviction scores across weeks."""
    out: list[int] = []
    for snap in sorted(snapshots, key=lambda s: str(s.get("week") or "")):
        c = snap.get("conviction_score")
        if isinstance(c, (int, float)):
            out.append(int(round(c)))
    return out


def compute_trend(snapshots: list[dict[str, Any]], *, window: int = 4) -> str:
    """Trend direction over the last ``window`` weeks of conviction."""
    series = conviction_series(snapshots)
    if len(series) < 2:
        return TREND_STABLE
    tail = series[-window:]
    net = tail[-1] - tail[0]
    if net >= TREND_THRESHOLD:
        return TREND_IMPROVING
    if net <= -TREND_THRESHOLD:
        return TREND_DETERIORATING
    return TREND_STABLE


def compute_age_weeks(snapshots: list[dict[str, Any]]) -> int:
    """Thesis age = number of distinct weeks captured (>=1 once tracking starts)."""
    weeks = {str(s.get("week") or "").strip() for s in snapshots if s.get("week")}
    weeks.discard("")
    return len(weeks)


def current_conviction(snapshots: list[dict[str, Any]]) -> int | None:
    series = conviction_series(snapshots)
    return series[-1] if series else None
