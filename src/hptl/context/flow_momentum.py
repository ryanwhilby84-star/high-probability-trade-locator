"""L2 — Flow momentum (short-term only; never overrides L1 structural regime)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from hptl.cot.scoring_engine import _persistence_features, _tanh_norm
from hptl.validation import safe_float, safe_gte, safe_is_negative, safe_is_positive, safe_lt, safe_lte

_FLOW_SCALE = 12_000.0

FLOW_LABELS = {
    "improving": "Improving",
    "weakening": "Weakening",
    "accelerating": "Accelerating",
    "decelerating": "Decelerating",
    "profit_taking": "Profit Taking",
    "short_covering": "Short Covering",
    "long_liquidation": "Long Liquidation",
    "mixed": "Mixed Flow",
}

CONFLICT_NARRATIVES = {
    ("structural_bullish", "weakening"): "Pullback / profit-taking within structural uptrend",
    ("structural_bullish", "profit_taking"): "Pullback / profit-taking within structural uptrend",
    ("structural_bullish", "long_liquidation"): "Pullback / profit-taking within structural uptrend",
    ("structural_bullish", "decelerating"): "Pullback / profit-taking within structural uptrend",
    ("structural_bearish", "improving"): "Covering rally within structural downtrend",
    ("structural_bearish", "short_covering"): "Covering rally within structural downtrend",
    ("structural_bearish", "accelerating"): "Covering rally within structural downtrend",
    ("accumulation", "weakening"): "Two-way flow while still net short — accumulation phase",
    ("distribution", "improving"): "Two-way flow while still net long — distribution phase",
}


def _flow_from_deltas(
    net: float | None,
    w1: float | None,
    w4: float | None,
    long_w1: float | None,
    short_w1: float | None,
    persist: dict[str, float],
) -> tuple[str, float]:
    net = safe_float(net)
    w1 = safe_float(w1)
    w4 = safe_float(w4)
    long_w1 = safe_float(long_w1)
    short_w1 = safe_float(short_w1)

    if net is None or w1 is None:
        return "mixed", 0.0

    accel = persist.get("accel_ratio", 0.0)
    intensity = min(100.0, abs(_tanh_norm(w1, _FLOW_SCALE)) * 100.0)

    if safe_is_positive(net) and long_w1 is not None and short_w1 is not None and safe_is_negative(long_w1) and safe_is_positive(short_w1):
        return "profit_taking", intensity
    if safe_is_negative(net) and long_w1 is not None and short_w1 is not None and safe_is_positive(long_w1) and safe_is_negative(short_w1):
        return "short_covering", intensity
    if safe_is_positive(net) and safe_is_negative(w1):
        if long_w1 is not None and safe_lt(long_w1, -1000) and (short_w1 is None or safe_gte(short_w1, 0)):
            return "long_liquidation", intensity
        if w4 is None or safe_lte(w4, 0):
            return "profit_taking", intensity
    if safe_is_negative(net) and safe_is_positive(w1) and (w4 is None or safe_gte(w4, 0)):
        return "short_covering", intensity
    if safe_is_positive(net) and safe_is_negative(w1):
        return "weakening", intensity
    if safe_is_negative(net) and safe_is_positive(w1):
        return "improving", intensity
    if safe_gte(abs(accel), 1.2):
        if (safe_is_positive(net) and safe_is_positive(w1)) or (safe_is_negative(net) and safe_is_negative(w1)):
            return "accelerating", intensity
        return "decelerating", intensity
    if safe_is_positive(net) and safe_is_positive(w1):
        return "accelerating", intensity
    if safe_is_negative(net) and safe_is_negative(w1):
        return "accelerating", intensity
    return "mixed", intensity * 0.5


def l1_l2_conflict(structural_regime: str, flow_momentum: str) -> bool:
    bull = structural_regime in {"structural_bullish", "accumulation"}
    bear = structural_regime in {"structural_bearish", "distribution"}
    if bull and flow_momentum in {"weakening", "profit_taking", "long_liquidation", "decelerating", "mixed"}:
        if flow_momentum == "mixed":
            return False
        return True
    if bear and flow_momentum in {"improving", "short_covering", "accelerating"}:
        return True
    return False


def conflict_narrative(structural_regime: str, flow_momentum: str) -> str | None:
    return CONFLICT_NARRATIVES.get((structural_regime, flow_momentum))


def compute_flow_layer(
    *,
    net: float | None,
    w1: float | None,
    w4: float | None,
    long_w1: float | None,
    short_w1: float | None,
    hist: pd.DataFrame,
    structural_regime: str,
) -> dict[str, Any]:
    persist = _persistence_features(hist)
    flow, intensity = _flow_from_deltas(net, w1, w4, long_w1, short_w1, persist)
    conflict = l1_l2_conflict(structural_regime, flow)
    narrative = conflict_narrative(structural_regime, flow) if conflict else None

    return {
        "flow_momentum": flow,
        "flow_momentum_label": FLOW_LABELS.get(flow, flow.replace("_", " ").title()),
        "flow_intensity": round(intensity, 1),
        "flow_l1_l2_conflict": conflict,
        "flow_conflict_narrative": narrative,
    }
