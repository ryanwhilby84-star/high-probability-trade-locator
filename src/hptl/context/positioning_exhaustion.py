"""L4 — Crowding / exhaustion from historical percentiles."""

from __future__ import annotations

from typing import Any

EXTREME_LABELS = {
    "none": "Balanced",
    "crowded_longs": "Crowded Longs",
    "crowded_shorts": "Crowded Shorts",
    "euphoric_longs": "Euphoric",
    "capitulation_shorts": "Capitulation",
    "positioning_reset": "Positioning Reset",
}


def _pct_from_ctx(ctx: dict[str, Any] | None) -> float | None:
    if not ctx or not isinstance(ctx, dict):
        return None
    v = ctx.get("current_net_percentile")
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def compute_exhaustion_layer(
    *,
    structural_regime: str,
    full_loaded_ctx: dict[str, Any] | None,
    expanding_ctx: dict[str, Any] | None,
    w1: float | None,
) -> dict[str, Any]:
    pct = _pct_from_ctx(full_loaded_ctx)
    if pct is None:
        pct = _pct_from_ctx(expanding_ctx)

    extreme = "none"
    exhaustion_score = 0.0

    if pct is not None:
        if pct >= 93:
            extreme = "euphoric_longs" if structural_regime in {"structural_bullish", "distribution"} else "crowded_longs"
            exhaustion_score = min(100.0, pct)
        elif pct >= 82:
            extreme = "crowded_longs"
            exhaustion_score = pct * 0.85
        elif pct >= 75:
            extreme = "crowded_longs"
            exhaustion_score = pct * 0.6
        elif pct <= 7:
            extreme = "capitulation_shorts" if structural_regime in {"structural_bearish", "accumulation"} else "crowded_shorts"
            exhaustion_score = min(100.0, 100.0 - pct)
        elif pct <= 18:
            extreme = "crowded_shorts"
            exhaustion_score = (100.0 - pct) * 0.85
        elif pct <= 25:
            extreme = "crowded_shorts"
            exhaustion_score = (100.0 - pct) * 0.55
        elif w1 is not None and pct is not None:
            if 40 <= pct <= 60 and abs(w1) > 8000:
                extreme = "positioning_reset"
                exhaustion_score = 35.0

    chase_penalty = 0.0
    if extreme in {"crowded_longs", "euphoric_longs"}:
        chase_penalty = 0.35
    elif extreme in {"crowded_shorts", "capitulation_shorts"}:
        chase_penalty = 0.30

    return {
        "positioning_extreme": extreme,
        "positioning_extreme_label": EXTREME_LABELS.get(extreme, extreme),
        "net_percentile": round(pct, 1) if pct is not None else None,
        "exhaustion_risk_score": round(exhaustion_score, 1),
        "chase_downgrade": chase_penalty,
    }
