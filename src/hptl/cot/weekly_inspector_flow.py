"""Weekly Inspector flow layer — direction, temperature, cross-group spreads.

Built on top of positioning_research_engine group-state series (expanding
net-positioning percentiles, no look-ahead). Pure packaging for the UI —
does not invent new COT values or event detection.
"""

from __future__ import annotations

import math
from typing import Any

from hptl.cot.positioning_research_engine import (
    GROUP_COMMERCIAL,
    GROUP_NONCOMMERCIAL,
    GROUP_NONREPORTABLE,
    build_group_state_series,
    build_spread_series,
)
from hptl.cot.positioning_percentiles import empirical_percentile_rank

# ---------------------------------------------------------------------------
# Direction thresholds (percentile-point movement). Easy to tune.
# ---------------------------------------------------------------------------
PCT_CHG_STRONG = 7.0
PCT_CHG_MILD = 2.0

# Temperature bands on expanding net-positioning percentile.
EXTREME_HIGH = 90.0
HIGH = 70.0
LOW = 30.0
EXTREME_LOW = 10.0

MEASURE = "net_positioning_expanding_percentile"
MEASURE_LABEL = "Net positioning percentile (expanding, point-in-time)"


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def classify_direction(pct_change_1w: float | None) -> str:
    """Map 1W percentile-point change → direction token."""
    d = _finite(pct_change_1w)
    if d is None:
        return "unknown"
    if d >= PCT_CHG_STRONG:
        return "strongly_increasing"
    if d >= PCT_CHG_MILD:
        return "increasing"
    if d > -PCT_CHG_MILD:
        return "stable"
    if d > -PCT_CHG_STRONG:
        return "decreasing"
    return "strongly_decreasing"


def direction_arrow(direction: str) -> str:
    return {
        "strongly_increasing": "▲▲",
        "increasing": "▲",
        "stable": "→",
        "decreasing": "▼",
        "strongly_decreasing": "▼▼",
        "unknown": "·",
    }.get(direction, "·")


def classify_temperature(
    percentile: float | None,
    pct_change_1w: float | None,
    pct_change_4w: float | None = None,
) -> tuple[str, str]:
    """Return (temperature_token, state_label) from level + recent flow."""
    p = _finite(percentile)
    d1 = _finite(pct_change_1w)
    d4 = _finite(pct_change_4w)
    if p is None:
        return "unknown", "Unavailable"

    rising = d1 is not None and d1 >= PCT_CHG_MILD
    falling = d1 is not None and d1 <= -PCT_CHG_MILD
    strong_up = d1 is not None and d1 >= PCT_CHG_STRONG
    strong_down = d1 is not None and d1 <= -PCT_CHG_STRONG
    # Prefer 4W when 1W is flat but multi-week trend is clear.
    if d1 is not None and abs(d1) < PCT_CHG_MILD and d4 is not None:
        if d4 >= PCT_CHG_STRONG:
            rising, strong_up = True, True
        elif d4 >= PCT_CHG_MILD:
            rising = True
        elif d4 <= -PCT_CHG_STRONG:
            falling, strong_down = True, True
        elif d4 <= -PCT_CHG_MILD:
            falling = True

    if p >= EXTREME_HIGH or p >= HIGH:
        if strong_up:
            return "heating_rapidly", "Deeper into extreme"
        if rising:
            return "heating", "Deeper into extreme"
        if strong_down:
            return "cooling_from_extreme", "Cooling from extreme"
        if falling:
            return "cooling_from_extreme", "Cooling from extreme"
        return "elevated_stable", "Elevated / stable"

    if p <= EXTREME_LOW or p <= LOW:
        if strong_down:
            return "deepening_extreme", "Deeper into low extreme"
        if falling:
            return "deepening_extreme", "Deeper into low extreme"
        if strong_up:
            return "recovering_strong", "Strong rotation away from extreme"
        if rising:
            return "recovering", "Moving out of extreme"
        return "depressed_stable", "Depressed / stable"

    # Mid-range
    if strong_up or (d4 is not None and d4 >= PCT_CHG_STRONG):
        return "building", "Rotation strengthening"
    if rising:
        return "building", "Rotation strengthening"
    if strong_down or (d4 is not None and d4 <= -PCT_CHG_STRONG):
        return "weakening", "Rotation weakening"
    if falling:
        return "weakening", "Rotation weakening"
    return "neutral", "Neutral"


def _obs_count_through(nets: list[float | None], idx: int) -> int:
    return sum(1 for v in nets[: idx + 1] if v is not None)


def pack_group_week(state: dict[str, Any], nets: list[float | None]) -> dict[str, Any]:
    """Pack one participant week into the inspector schema."""
    idx = int(state.get("index") or 0)
    vel = state.get("velocity") or {}
    v1 = vel.get("1w") or {}
    v4 = vel.get("4w") or {}
    v12 = vel.get("12w") or {}
    pct = _finite((state.get("percentiles") or {}).get("long_history"))
    pct_1w = _finite(v1.get("percentile_change"))
    pct_4w = _finite(v4.get("percentile_change"))
    pct_12w = _finite(v12.get("percentile_change"))
    direction = classify_direction(pct_1w)
    temperature, state_label = classify_temperature(pct, pct_1w, pct_4w)
    is_extreme = pct is not None and (pct >= EXTREME_HIGH or pct <= EXTREME_LOW)

    return {
        "net": _finite(state.get("net")),
        "weekly_change": _finite(v1.get("net_change")),
        "four_week_change": _finite(v4.get("net_change")),
        "twelve_week_change": _finite(v12.get("net_change")),
        "percentile": pct,
        "percentile_change_1w": pct_1w,
        "percentile_change_4w": pct_4w,
        "percentile_change_12w": pct_12w,
        "percentile_observation_count": _obs_count_through(nets, idx),
        "direction": direction,
        "direction_arrow": direction_arrow(direction),
        "temperature": temperature,
        "state_label": state_label,
        "is_extreme": is_extreme,
        "measure": MEASURE,
    }


def _expanding_spread_percentiles(
    spreads: list[float | None],
) -> list[float | None]:
    out: list[float | None] = []
    hist: list[float] = []
    for v in spreads:
        if v is not None:
            hist.append(float(v))
            p = empirical_percentile_rank(hist, v)
            out.append(None if not math.isfinite(p) else round(float(p), 2))
        else:
            out.append(None)
    return out


def classify_relationship(c_pct: float | None, nc_pct: float | None) -> str:
    """Aligned / opposed / mixed based on percentile levels vs 50."""
    c = _finite(c_pct)
    nc = _finite(nc_pct)
    if c is None or nc is None:
        return "unavailable"
    c_side = 1 if c >= 55 else (-1 if c <= 45 else 0)
    nc_side = 1 if nc >= 55 else (-1 if nc <= 45 else 0)
    if c_side == 0 or nc_side == 0:
        return "mixed"
    if c_side == nc_side:
        return "aligned"
    spread = abs(c - nc)
    if spread >= 60:
        return "strong_opposition"
    return "opposed"


def classify_spread_flow(change_1w: float | None, change_4w: float | None) -> str:
    d1 = _finite(change_1w)
    d4 = _finite(change_4w)
    # For Comm−NC percentile spread, rising magnitude of |spread| when opposed
    # is handled at a higher level; here we report raw spread change direction.
    if d1 is None and d4 is None:
        return "unavailable"
    primary = d1 if d1 is not None else d4
    assert primary is not None
    if primary >= PCT_CHG_STRONG:
        return "opposition_widening_rapidly" if primary > 0 else "opposition_narrowing_rapidly"
    # Positive Comm−NC spread change = commercials rising vs NC in percentile space
    if abs(primary) < PCT_CHG_MILD:
        return "stable"
    if primary >= PCT_CHG_MILD:
        return "spread_widening"
    return "spread_narrowing"


def pack_cross_week(
    c: dict[str, Any],
    nc: dict[str, Any],
    nr: dict[str, Any],
    comm_nc_spread: float | None,
    comm_nc_spread_pct: float | None,
    comm_nc_chg_1w: float | None,
    comm_nc_chg_4w: float | None,
    comm_nr_spread: float | None,
    comm_nr_spread_pct: float | None,
) -> dict[str, Any]:
    c_pct = c.get("percentile")
    nc_pct = nc.get("percentile")
    nr_pct = nr.get("percentile")
    relationship = classify_relationship(c_pct, nc_pct)
    flow = classify_spread_flow(comm_nc_chg_1w, comm_nc_chg_4w)

    # Refine flow wording when relationship is opposed and |spread| grows.
    if relationship in ("opposed", "strong_opposition") and _finite(comm_nc_chg_1w) is not None:
        # Spread here is C_pct - NC_pct; opposition widens when |spread| increases.
        # Approximate: if abs(spread) large and change moves further from 0.
        s = _finite(comm_nc_spread)
        d = _finite(comm_nc_chg_1w)
        if s is not None and d is not None:
            widening = (s >= 0 and d > 0) or (s < 0 and d < 0)
            if abs(d) >= PCT_CHG_STRONG:
                flow = "opposition_widening_rapidly" if widening else "opposition_narrowing_rapidly"
            elif abs(d) >= PCT_CHG_MILD:
                flow = "opposition_widening" if widening else "opposition_narrowing"

    return {
        "commercial_percentile": c_pct,
        "noncommercial_percentile": nc_pct,
        "nonreportable_percentile": nr_pct,
        "comm_nc_spread": comm_nc_spread,
        "comm_nc_spread_percentile": comm_nc_spread_pct,
        "comm_nc_spread_change_1w": comm_nc_chg_1w,
        "comm_nc_spread_change_4w": comm_nc_chg_4w,
        "comm_nr_spread": comm_nr_spread,
        "comm_nr_spread_percentile": comm_nr_spread_pct,
        "relationship": relationship,
        "flow": flow,
        "measure": MEASURE,
    }


def participant_summary_line(group_label: str, packed: dict[str, Any]) -> str:
    """Deterministic one-line interpretation for a participant group."""
    pct = _finite(packed.get("percentile"))
    d4 = _finite(packed.get("percentile_change_4w"))
    d1 = _finite(packed.get("percentile_change_1w"))
    label = packed.get("state_label") or "Neutral"
    if pct is None:
        return f"{group_label} net positioning is unavailable for this week."

    move = d4 if d4 is not None else d1
    if move is None:
        return (
            f"{group_label} positioning is at the {pct:.0f}th net percentile. "
            f"State: {label}."
        )
    verb = "risen" if move > 0 else ("fallen" if move < 0 else "been unchanged")
    horizon = "four weeks" if d4 is not None else "one week"
    pts = abs(move)
    return (
        f"{group_label} positioning is at the {pct:.0f}th net percentile and has {verb} "
        f"{pts:.0f} percentile points over {horizon}. {label}."
    )


def _nets(series: list[dict[str, Any]], key: str) -> list[float | None]:
    return [_finite(r.get(key)) for r in series]


def build_weekly_inspector_series(
    series: list[dict[str, Any]],
    *,
    commercial_states: list[dict[str, Any]] | None = None,
    noncommercial_states: list[dict[str, Any]] | None = None,
    nonreportable_states: list[dict[str, Any]] | None = None,
    spreads_nr: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build compact weekly inspector payload for one market's cot3y series."""
    if not series:
        return {
            "available": False,
            "measure": MEASURE,
            "measure_label": MEASURE_LABEL,
            "weeks": [],
        }

    commercial = commercial_states or build_group_state_series(series, GROUP_COMMERCIAL)
    noncommercial = noncommercial_states or build_group_state_series(
        series, GROUP_NONCOMMERCIAL
    )
    nonreportable = nonreportable_states or build_group_state_series(
        series, GROUP_NONREPORTABLE
    )
    if spreads_nr is None:
        spreads_nr = build_spread_series(commercial, nonreportable)

    from hptl.cot.positioning_research_engine import GROUP_NET_KEY

    c_nets = _nets(series, GROUP_NET_KEY[GROUP_COMMERCIAL])
    nc_nets = _nets(series, GROUP_NET_KEY[GROUP_NONCOMMERCIAL])
    nr_nets = _nets(series, GROUP_NET_KEY[GROUP_NONREPORTABLE])

    # Comm − NC in percentile space + expanding percentile of that spread.
    raw_comm_nc: list[float | None] = []
    for c, nc in zip(commercial, noncommercial):
        cp = _finite((c.get("percentiles") or {}).get("long_history"))
        ncp = _finite((nc.get("percentiles") or {}).get("long_history"))
        if cp is None or ncp is None:
            raw_comm_nc.append(None)
        else:
            raw_comm_nc.append(round(cp - ncp, 2))
    comm_nc_pcts = _expanding_spread_percentiles(raw_comm_nc)

    weeks: list[dict[str, Any]] = []
    for i in range(len(series)):
        c_pack = pack_group_week(commercial[i], c_nets)
        nc_pack = pack_group_week(noncommercial[i], nc_nets)
        nr_pack = pack_group_week(nonreportable[i], nr_nets)

        cn_spread = raw_comm_nc[i]
        cn_spread_pct = comm_nc_pcts[i]
        cn_1w = None
        cn_4w = None
        if i >= 1 and cn_spread is not None and raw_comm_nc[i - 1] is not None:
            cn_1w = round(cn_spread - raw_comm_nc[i - 1], 2)
        if i >= 4 and cn_spread is not None and raw_comm_nc[i - 4] is not None:
            cn_4w = round(cn_spread - raw_comm_nc[i - 4], 2)

        nr_row = spreads_nr[i]
        cross = pack_cross_week(
            c_pack,
            nc_pack,
            nr_pack,
            cn_spread,
            cn_spread_pct,
            cn_1w,
            cn_4w,
            _finite(nr_row.get("spread")),
            _finite(nr_row.get("spread_percentile")),
        )

        weeks.append(
            {
                "date": commercial[i]["date"],
                "commercial": c_pack,
                "noncommercial": nc_pack,
                "nonreportable": nr_pack,
                "cross": cross,
                "summaries": {
                    "commercial": participant_summary_line("Commercial", c_pack),
                    "noncommercial": participant_summary_line("Non-Commercial", nc_pack),
                    "nonreportable": participant_summary_line("Non-Reportable", nr_pack),
                },
            }
        )

    return {
        "available": True,
        "measure": MEASURE,
        "measure_label": MEASURE_LABEL,
        "direction_thresholds": {
            "strong_percentile_points": PCT_CHG_STRONG,
            "mild_percentile_points": PCT_CHG_MILD,
        },
        "weeks": weeks,
        "week_count": len(weeks),
    }


def attach_weekly_inspector_to_market(
    market_block: dict[str, Any],
    series: list[dict[str, Any]],
) -> dict[str, Any]:
    """Attach weekly_inspector series onto an existing research market block."""
    out = dict(market_block)
    out["weekly_inspector"] = build_weekly_inspector_series(series)
    return out
