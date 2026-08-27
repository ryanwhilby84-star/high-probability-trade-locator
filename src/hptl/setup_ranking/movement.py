"""Movement Potential Engine — separate from setup quality (ATR / range / expansion)."""

from __future__ import annotations

from typing import Any

from hptl.setup_ranking.grades import clamp_score


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _atr(bars: list[dict[str, Any]], period: int) -> float | None:
    if len(bars) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(bars)):
        h = _num(bars[i].get("high"))
        l = _num(bars[i].get("low"))
        pc = _num(bars[i - 1].get("close"))
        if h is None or l is None or pc is None:
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    window = trs[-period:]
    return sum(window) / len(window)


def _weekly_range_pct(weekly: list[dict[str, Any]], n: int = 4) -> float | None:
    if not weekly:
        return None
    sub = weekly[-n:]
    spot = _num(sub[-1].get("close"))
    if spot is None or spot == 0:
        return None
    highs = [_num(b.get("high")) for b in sub]
    lows = [_num(b.get("low")) for b in sub]
    highs = [x for x in highs if x is not None]
    lows = [x for x in lows if x is not None]
    if not highs or not lows:
        return None
    return (max(highs) - min(lows)) / abs(spot) * 100.0


def _trend_expansion(daily: list[dict[str, Any]], lookback: int = 20) -> float | None:
    if len(daily) < lookback:
        return None
    closes = [_num(b.get("close")) for b in daily[-lookback:]]
    closes = [c for c in closes if c is not None]
    if len(closes) < 4:
        return None
    early = sum(closes[: len(closes) // 2]) / (len(closes) // 2)
    late = sum(closes[len(closes) // 2 :]) / (len(closes) - len(closes) // 2)
    if early == 0:
        return None
    return abs(late - early) / abs(early) * 100.0


def compute_movement_metrics(
    daily: list[dict[str, Any]] | None,
    weekly: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    daily = daily or []
    weekly = weekly or []
    atr30 = _atr(daily, 30)
    atr90 = _atr(daily, 90) if len(daily) >= 91 else _atr(daily, min(30, max(1, len(daily) - 1)))
    wr = _weekly_range_pct(weekly, 4)
    exp = _trend_expansion(daily, 20)
    spot = _num(daily[-1].get("close")) if daily else None
    atr30_pct = (atr30 / spot * 100.0) if atr30 and spot else None
    atr90_pct = (atr90 / spot * 100.0) if atr90 and spot else None
    return {
        "atr_30d": atr30,
        "atr_90d": atr90,
        "atr_30d_pct": round(atr30_pct, 4) if atr30_pct is not None else None,
        "atr_90d_pct": round(atr90_pct, 4) if atr90_pct is not None else None,
        "weekly_range_pct": round(wr, 4) if wr is not None else None,
        "trend_expansion_pct": round(exp, 4) if exp is not None else None,
    }


def movement_score_from_metrics(metrics: dict[str, Any], *, percentile: float | None) -> float:
    """Map raw metrics + cross-sectional percentile to 0-100."""
    if percentile is not None:
        return round(max(0.0, min(100.0, percentile)), 1)
    # Fallback when no cross-section: blend normalized components
    parts: list[float] = []
    for key, scale in (("atr_30d_pct", 0.8), ("atr_90d_pct", 0.6), ("weekly_range_pct", 1.2), ("trend_expansion_pct", 1.0)):
        v = _num(metrics.get(key))
        if v is not None:
            parts.append(min(100.0, v * scale * 10.0))
    if not parts:
        return 0.0
    return round(sum(parts) / len(parts), 1)


def percentile_rank(values: list[float], value: float) -> float:
    if not values:
        return 50.0
    below = sum(1 for v in values if v < value)
    equal = sum(1 for v in values if v == value)
    return round((below + 0.5 * equal) / len(values) * 100.0, 1)
