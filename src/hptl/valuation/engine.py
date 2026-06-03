"""Valuation pillar: Bullish / Neutral / Bearish from price percentile + macro map."""
from __future__ import annotations

from typing import Any

BIAS_BULLISH = "Bullish"
BIAS_NEUTRAL = "Neutral"
BIAS_BEARISH = "Bearish"


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _weekly_closes(weekly: list[dict[str, Any]]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for b in weekly or []:
        if not isinstance(b, dict):
            continue
        c = _num(b.get("close"))
        d = str(b.get("date") or "")[:10]
        if c is not None and d:
            out.append((d, c))
    out.sort(key=lambda x: x[0])
    return out


def price_percentile(closes: list[float], *, window: int = 52) -> float | None:
    if len(closes) < 12:
        return None
    window_closes = closes[-window:] if len(closes) >= window else closes
    current = window_closes[-1]
    if not window_closes:
        return None
    rank = sum(1 for c in window_closes if c <= current) / len(window_closes)
    return rank * 100.0


def _bias_from_percentile(pct: float) -> str:
    if pct <= 33.0:
        return BIAS_BULLISH
    if pct >= 67.0:
        return BIAS_BEARISH
    return BIAS_NEUTRAL


def _score_from_percentile(pct: float) -> float:
    """0–10 conviction from distance from fair-value middle (50th pct)."""
    return round(min(10.0, max(0.0, abs(pct - 50.0) / 5.0)), 1)


def _macro_adjustment(macro_map: dict[str, Any] | None) -> tuple[str | None, float]:
    """Optional nudge from macro relationship map correlation regime."""
    if not macro_map or macro_map.get("available") is not True:
        return None, 0.0
    corr = _num(macro_map.get("latest_rolling_corr_20"))
    if corr is None:
        return None, 0.0
    regime = str(macro_map.get("correlation_regime") or "").lower()
    if abs(corr) < 0.2:
        return "weak_macro_link", -0.5
    if "positive" in regime or corr > 0.35:
        return "macro_aligned", 0.5
    if "negative" in regime or corr < -0.35:
        return "macro_inverse", 0.0
    return None, 0.0


def compute_valuation(
    *,
    market: str,
    weekly_bars: list[dict[str, Any]] | None = None,
    range_52w: dict[str, Any] | None = None,
    macro_map: dict[str, Any] | None = None,
    as_of_week: str | None = None,
) -> dict[str, Any]:
    """
    Returns valuation_bias, valuation_score (0–10), valuation_reason, pass (None until aligned).
    """
    closes_series = _weekly_closes(weekly_bars or [])
    closes = [c for _, c in closes_series]

    if len(closes) < 12:
        r52 = range_52w or {}
        hi, lo = _num(r52.get("high")), _num(r52.get("low"))
        if hi is not None and lo is not None and hi > lo:
            last = closes[-1] if closes else None
            if last is None and weekly_bars:
                last = _num(weekly_bars[-1].get("close"))
            pct = (last - lo) / (hi - lo) * 100.0 if last is not None else None
        else:
            pct = None
    else:
        pct = price_percentile(closes)

    if pct is None:
        return {
            "market": market,
            "as_of_week": as_of_week,
            "wired": False,
            "valuation_bias": "UNAVAILABLE",
            "valuation_score": None,
            "valuation_reason": "Insufficient weekly price history for valuation percentile.",
            "price_percentile_52w": None,
            "pass": False,
        }

    bias = _bias_from_percentile(pct)
    score = _score_from_percentile(pct)
    macro_note, score_adj = _macro_adjustment(macro_map)
    if score_adj:
        score = round(min(10.0, max(0.0, score + score_adj)), 1)

    reason = (
        f"52-week price percentile {pct:.0f}% "
        f"({'lower third' if bias == BIAS_BULLISH else 'upper third' if bias == BIAS_BEARISH else 'middle range'})."
    )
    if macro_note == "weak_macro_link":
        reason += " Macro relationship link is weak — score capped."
    elif macro_note == "macro_aligned":
        reason += " Macro driver correlation supports the read."

    return {
        "market": market,
        "as_of_week": as_of_week,
        "wired": True,
        "valuation_bias": bias,
        "valuation_score": score,
        "valuation_reason": reason,
        "price_percentile_52w": round(pct, 1),
        "pass": False,
    }


def valuation_pass(bias: str, direction: str) -> bool:
    if bias == "UNAVAILABLE":
        return False
    d = direction.lower()
    if d == "long":
        return bias == BIAS_BULLISH
    if d == "short":
        return bias == BIAS_BEARISH
    return bias == BIAS_NEUTRAL
