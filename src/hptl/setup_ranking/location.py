"""Location pillar — weekly/daily supply-demand zones and volume nodes from price store."""

from __future__ import annotations

from typing import Any

from hptl.setup_ranking.grades import PillarScore, clamp_score


def _bars_slice(bars: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    if not bars:
        return []
    return bars[-n:] if len(bars) >= n else list(bars)


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _swing_zones(bars: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    if not bars:
        return None, None
    highs = [_num(b.get("high")) for b in bars]
    lows = [_num(b.get("low")) for b in bars]
    highs = [x for x in highs if x is not None]
    lows = [x for x in lows if x is not None]
    if not highs or not lows:
        return None, None
    return max(highs), min(lows)


def _volume_nodes(daily: list[dict[str, Any]], *, bins: int = 24) -> tuple[float | None, float | None]:
    """HVN / LVN proxy from volume-weighted close histogram."""
    if len(daily) < 5:
        return None, None
    lo = min(_num(b.get("low")) or 1e18 for b in daily)
    hi = max(_num(b.get("high")) or -1e18 for b in daily)
    if hi <= lo:
        return None, None
    hist = [0.0] * bins
    for b in daily:
        c = _num(b.get("close"))
        v = _num(b.get("volume")) or 1.0
        if c is None:
            continue
        idx = min(bins - 1, max(0, int((c - lo) / (hi - lo) * bins)))
        hist[idx] += max(v, 0.0)
    if not any(hist):
        return None, None
    hvn_idx = max(range(bins), key=lambda i: hist[i])
    lvn_idx = min(range(bins), key=lambda i: hist[i] if hist[i] > 0 else 1e18)
    step = (hi - lo) / bins
    hvn = lo + (hvn_idx + 0.5) * step
    lvn = lo + (lvn_idx + 0.5) * step
    return hvn, lvn


def _range_position(spot: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5
    return max(0.0, min(1.0, (spot - lo) / (hi - lo)))


def score_location_pillar(
    *,
    pair: str,
    direction: str,
    daily: list[dict[str, Any]] | None,
    weekly: list[dict[str, Any]] | None,
    zone_focus: str | None = None,
) -> PillarScore:
    """Score 0-10 location alignment for a FX pair trade direction."""
    daily = daily or []
    weekly = weekly or []
    spot = _num(daily[-1].get("close")) if daily else None

    if spot is None and not weekly:
        zf = str(zone_focus or "").strip()
        if zf and zf.lower() not in {"wait", "n/a"}:
            aligned = ("demand" in zf.lower() and direction == "long") or (
                "supply" in zf.lower() and direction == "short"
            )
            return PillarScore(
                key="location",
                label="Location",
                score=clamp_score(6.0 if aligned else 3.0),
                bias=zf,
                summary=zf,
                detail="Price zones unavailable — using institutional zone_focus heuristic only.",
                aligned=aligned,
                missing=False,
                meta={"source": "zone_focus_fallback"},
            )
        return PillarScore(
            key="location",
            label="Location",
            score=0.0,
            bias=None,
            summary="Location data unavailable.",
            detail="No price bars for zone analysis.",
            aligned=False,
            missing=True,
        )

    w_supply, w_demand = _swing_zones(_bars_slice(weekly, 13))
    d_supply, d_demand = _swing_zones(_bars_slice(daily, 20))
    hvn, lvn = _volume_nodes(_bars_slice(daily, 30))

    pos_w = _range_position(spot, w_demand or spot, w_supply or spot) if spot is not None else 0.5
    pos_d = _range_position(spot, d_demand or spot, d_supply or spot) if spot is not None else 0.5

    # Combined location read
    if direction == "long":
        at_zone = pos_w <= 0.35 or pos_d <= 0.30
        approaching = pos_w <= 0.50 or pos_d <= 0.45
        wrong_zone = pos_w >= 0.75 or pos_d >= 0.70
        if at_zone:
            score, state = 10.0, "Inside weekly/daily demand"
        elif approaching:
            score, state = 7.5, "Approaching demand zone"
        elif wrong_zone:
            score, state = 2.5, "Extended — near supply (poor long location)"
        else:
            score, state = 5.0, "Mid-range — await demand"
        aligned = at_zone or approaching
    elif direction == "short":
        at_zone = pos_w >= 0.65 or pos_d >= 0.70
        approaching = pos_w >= 0.50 or pos_d >= 0.55
        wrong_zone = pos_w <= 0.25 or pos_d <= 0.30
        if at_zone:
            score, state = 10.0, "Inside weekly/daily supply"
        elif approaching:
            score, state = 7.5, "Approaching supply zone"
        elif wrong_zone:
            score, state = 2.5, "Extended — near demand (poor short location)"
        else:
            score, state = 5.0, "Mid-range — await supply"
        aligned = at_zone or approaching
    else:
        score, state, aligned = 5.0, "Neutral — no directional location edge", False

    parts = [state]
    if w_demand is not None and w_supply is not None:
        parts.append(f"Weekly range {w_demand:.5g}–{w_supply:.5g}")
    if hvn is not None:
        parts.append(f"HVN ≈ {hvn:.5g}")
    if lvn is not None:
        parts.append(f"LVN ≈ {lvn:.5g}")
    if zone_focus:
        parts.append(f"Context: {zone_focus}")

    return PillarScore(
        key="location",
        label="Location",
        score=clamp_score(score),
        bias=state,
        summary=state,
        detail=" · ".join(parts),
        aligned=aligned,
        missing=False,
        meta={
            "weekly_supply": w_supply,
            "weekly_demand": w_demand,
            "daily_supply": d_supply,
            "daily_demand": d_demand,
            "hvn": hvn,
            "lvn": lvn,
            "weekly_range_position": round(pos_w, 3),
            "daily_range_position": round(pos_d, 3),
            "spot": spot,
        },
    )
