"""L1 — Structural regime (slow, persistent). Anchor layer; not overridden by flow."""

from __future__ import annotations

from typing import Any

import pandas as pd

from hptl.cot.scoring_engine import _tanh_norm
from hptl.validation import (
    coerce_series_numeric,
    safe_float,
    safe_gt,
    safe_gte,
    safe_is_negative,
    safe_is_positive,
    safe_lt,
    safe_lte,
)

_NET_SCALE = 45_000.0
_FLOW_SCALE = 12_000.0

STRUCTURAL_REGIMES = frozenset(
    {
        "structural_bullish",
        "structural_bearish",
        "accumulation",
        "distribution",
        "neutral_rotation",
        "transitional",
    }
)

REGIME_LABELS = {
    "structural_bullish": "Structural Bullish",
    "structural_bearish": "Structural Bearish",
    "accumulation": "Accumulation",
    "distribution": "Distribution",
    "neutral_rotation": "Neutral Rotation",
    "transitional": "Transitional",
}

EMA_ALPHA = 0.35
BULL_THRESHOLD = 22.0
BEAR_THRESHOLD = -22.0
TRANSITION_BAND = 12.0


def _hist_features(hist: pd.DataFrame) -> dict[str, float]:
    out = {
        "aligned_ratio": 0.0,
        "leg_accum_weeks": 0.0,
        "leg_dist_weeks": 0.0,
        "w12_net": 0.0,
        "w4_net": 0.0,
    }
    if hist.empty or len(hist) < 2:
        return out

    h = hist.copy()
    if "cot_report_date" in h.columns:
        h["cot_report_date"] = pd.to_datetime(h["cot_report_date"], errors="coerce")
    h = h.sort_values("cot_report_date").tail(12)
    nets = coerce_series_numeric(h.get("net_value"))
    w1s = coerce_series_numeric(h.get("weekly_change"))
    long_d = coerce_series_numeric(h.get("long_weekly_change"))
    short_d = coerce_series_numeric(h.get("short_weekly_change"))

    aligned = 0
    total = 0
    accum = 0
    dist = 0
    for i in range(1, len(h)):
        n = safe_float(nets.iloc[i - 1]) if i - 1 < len(nets) else None
        w = safe_float(w1s.iloc[i]) if i < len(w1s) else None
        dl = safe_float(long_d.iloc[i]) if i < len(long_d) else None
        ds = safe_float(short_d.iloc[i]) if i < len(short_d) else None
        if n is None or n == 0:
            continue
        total += 1
        if w is not None:
            if (safe_is_positive(n) and safe_is_positive(w)) or (safe_is_negative(n) and safe_is_negative(w)):
                aligned += 1
        if safe_is_negative(n) and dl is not None and ds is not None and safe_gt(dl, 500) and safe_lt(ds, -500):
            accum += 1
        if safe_is_positive(n) and dl is not None and ds is not None and safe_lt(dl, -500) and safe_gt(ds, 500):
            dist += 1

    if total:
        out["aligned_ratio"] = aligned / total
    out["leg_accum_weeks"] = accum
    out["leg_dist_weeks"] = dist

    if len(nets) >= 5 and nets.notna().sum() >= 2:
        out["w4_net"] = float(nets.iloc[-1] - nets.iloc[-5]) if len(nets) >= 5 else 0.0
    if len(nets) >= 9 and nets.notna().sum() >= 2:
        out["w12_net"] = float(nets.iloc[-1] - nets.iloc[-9])
    elif len(nets) >= 2:
        out["w12_net"] = float(nets.iloc[-1] - nets.iloc[0])

    return out


def _raw_structural_impulse(
    net: float | None,
    w1: float | None,
    w4: float | None,
    hist_feats: dict[str, float],
) -> float:
    net = safe_float(net)
    w1 = safe_float(w1)
    w4 = safe_float(w4)
    if net is None:
        return 0.0
    sign = 1.0 if safe_is_positive(net) else -1.0 if safe_is_negative(net) else 0.0
    if sign == 0:
        return 0.0

    level = sign * _tanh_norm(net, _NET_SCALE) * 55.0
    drift4 = sign * _tanh_norm(w4, _FLOW_SCALE * 1.6) * 20.0 if w4 is not None else 0.0
    drift12 = sign * _tanh_norm(hist_feats.get("w12_net"), _FLOW_SCALE * 3.0) * 15.0
    align_bonus = (hist_feats.get("aligned_ratio", 0) - 0.5) * sign * 20.0

    return max(-100.0, min(100.0, level + drift4 + drift12 + align_bonus))


def _propose_regime(
    score_ema: float,
    net: float | None,
    hist_feats: dict[str, float],
) -> str:
    net = safe_float(net)
    if net is not None and safe_is_negative(net) and safe_gte(hist_feats.get("leg_accum_weeks", 0), 3):
        return "accumulation"
    if net is not None and safe_is_positive(net) and safe_gte(hist_feats.get("leg_dist_weeks", 0), 3):
        return "distribution"

    if safe_gte(score_ema, BULL_THRESHOLD):
        return "structural_bullish"
    if safe_lte(score_ema, BEAR_THRESHOLD):
        return "structural_bearish"
    if abs(score_ema) <= TRANSITION_BAND:
        return "neutral_rotation"
    return "transitional"


def _block_one_week_flip(
    prev_regime: str,
    proposed: str,
    net: float | None,
    w1: float | None,
) -> str:
    net = safe_float(net)
    w1 = safe_float(w1)
    bullish = {"structural_bullish", "accumulation"}
    bearish = {"structural_bearish", "distribution"}
    if prev_regime in bullish and proposed in bearish:
        return "transitional"
    if prev_regime in bearish and proposed in bullish:
        return "transitional"
    if net is not None and w1 is not None:
        if safe_is_positive(net) and safe_is_negative(w1) and proposed == "structural_bearish":
            return "distribution" if prev_regime in bullish else "transitional"
        if safe_is_negative(net) and safe_is_positive(w1) and proposed == "structural_bullish":
            return "accumulation" if prev_regime in bearish else "transitional"
    return proposed


def compute_structural_layer(
    *,
    net: float | None,
    w1: float | None,
    w4: float | None,
    hist: pd.DataFrame,
    prev_score_ema: float,
    prev_regime: str,
) -> dict[str, Any]:
    hist_feats = _hist_features(hist)
    impulse = _raw_structural_impulse(net, w1, w4, hist_feats)
    score_ema = (1.0 - EMA_ALPHA) * prev_score_ema + EMA_ALPHA * impulse
    proposed = _propose_regime(score_ema, net, hist_feats)
    proposed = _block_one_week_flip(prev_regime, proposed, net, w1)

    conviction = "low"
    if abs(score_ema) >= 45:
        conviction = "high"
    elif abs(score_ema) >= 28:
        conviction = "medium"

    label_regime = proposed
    label = REGIME_LABELS.get(label_regime, label_regime.replace("_", " ").title())
    return {
        "structural_regime": proposed,
        "structural_regime_label": label,
        "structural_score": round(score_ema, 1),
        "structural_score_ema": score_ema,
        "structural_conviction": conviction,
        "structural_aligned_ratio": round(hist_feats.get("aligned_ratio", 0), 2),
    }
