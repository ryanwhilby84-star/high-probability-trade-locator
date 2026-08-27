"""COT Positioning State / Divergence / Historical Price-Response Engine.

Instrument-agnostic research engine (not an execution signal). For every
supported COT series market and every week:

* per-group normalized positioning state (expanding + rolling percentiles)
* journey / velocity / persistence
* Commercial ↔ Non-Reportable normalized spread
* configuration events (absolute, local, rotation, divergence)
* independent historical analogues + forward price returns

No look-ahead in historical qualification. Threshold bands are predeclared
and audited — not cherry-picked to flattering returns.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Sequence

from hptl.cot.positioning_percentiles import empirical_percentile_rank

MIN_HISTORY = 52
ROLLING_WINDOWS = {
    "1y": 52,
    "2y": 104,
    "3y": 156,
    "5y": 260,
}
JOURNEY_LAGS = (1, 4, 12, 26, 52)
VELOCITY_LAGS = (1, 4, 12)
FORWARD_HORIZONS = (1, 4, 8, 12, 26)

# Predeclared bands — all reported in audit; primary markers use 90/10.
SPREAD_BANDS = (
    {"name": "80_20", "high": 80.0, "low": 20.0},
    {"name": "85_15", "high": 85.0, "low": 15.0},
    {"name": "90_10", "high": 90.0, "low": 10.0},
    {"name": "95_5", "high": 95.0, "low": 5.0},
)
PRIMARY_BAND = "90_10"

ABSOLUTE_HIGH = 90.0
ABSOLUTE_LOW = 10.0
LOCAL_HIGH = 90.0
LOCAL_LOW = 10.0
ROTATION_PCT_MOVE_26W = 40.0
RAPID_PCT_MOVE_4W = 25.0
PERSISTENCE_WEEKS = 8
PERSISTENCE_MIN_PCT_MOVE = 15.0

EVENT_COOLDOWN_WEEKS = 8
ANALOGUE_COOLDOWN_WEEKS = 12
ANALOGUE_PCT_TOLERANCE = 15.0
ANALOGUE_SPREAD_TOLERANCE = 12.0

GROUP_COMMERCIAL = "commercial"
GROUP_NONCOMMERCIAL = "noncommercial"
GROUP_NONREPORTABLE = "nonreportable"

GROUP_NET_KEY = {
    GROUP_COMMERCIAL: "commercial_net",
    GROUP_NONCOMMERCIAL: "institutional_net",
    GROUP_NONREPORTABLE: "retail_net",
}

GROUP_LABEL = {
    GROUP_COMMERCIAL: "Commercials",
    GROUP_NONCOMMERCIAL: "Non-Commercials",
    GROUP_NONREPORTABLE: "Non-Reportables",
}

# Legacy probe id only — never used as the default export universe.
DEFAULT_VALIDATION_MARKET = "Gold"


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _pct(window: Sequence[float], value: float | None) -> float | None:
    # n>=1: first observation ranks at 50 via empirical_percentile_rank; never
    # withhold a percentile when the underlying net value exists.
    if value is None or len(window) < 1:
        return None
    p = empirical_percentile_rank(window, value)
    return None if not math.isfinite(p) else round(float(p), 2)


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    return float(statistics.median(vals))


def sample_quality(n: int) -> str:
    if n < 5:
        return "INSUFFICIENT SAMPLE"
    if n < 8:
        return "LOW CONFIDENCE"
    if n < 15:
        return "MODERATE SAMPLE"
    return "STRONGER SAMPLE"


def directional_tendency(summary: dict[str, Any] | None) -> str:
    if not summary or not summary.get("n"):
        return "INSUFFICIENT EVIDENCE"
    n = int(summary["n"])
    if n < 5:
        return "INSUFFICIENT EVIDENCE"
    higher = int(summary.get("higher_count") or 0)
    lower = int(summary.get("lower_count") or 0)
    share = higher / n
    if share >= 0.65:
        return "BULLISH ASYMMETRY"
    if share <= 0.35:
        return "BEARISH ASYMMETRY"
    return "MIXED"


def _series_nets(series: list[dict[str, Any]], key: str) -> list[float | None]:
    return [_finite(r.get(key)) for r in series]


def _expanding_percentiles(nets: list[float | None]) -> list[float | None]:
    """Point-in-time expanding percentile of nets (no future weeks).

    Each week t is ranked only against observations from the start through t.
    """
    out: list[float | None] = []
    hist: list[float] = []
    for v in nets:
        if v is not None:
            hist.append(v)
        out.append(_pct(hist, v) if v is not None else None)
    return out


def _rolling_percentiles(nets: list[float | None], window: int) -> list[float | None]:
    out: list[float | None] = []
    for i, v in enumerate(nets):
        if v is None:
            out.append(None)
            continue
        start = max(0, i - window + 1)
        hist = [x for x in nets[start : i + 1] if x is not None]
        out.append(_pct(hist, v) if len(hist) >= 2 else None)
    return out


def _lag_value(xs: list[float | None], idx: int, lag: int) -> float | None:
    if idx < lag:
        return None
    return xs[idx - lag]


def _change(xs: list[float | None], idx: int, lag: int) -> float | None:
    a = xs[idx]
    b = _lag_value(xs, idx, lag)
    if a is None or b is None:
        return None
    return a - b


def _persistence_weeks(nets: list[float | None], idx: int) -> dict[str, Any]:
    """Count consecutive same-sign 1W net changes ending at idx."""
    if idx < 1 or nets[idx] is None or nets[idx - 1] is None:
        return {"weeks": 0, "direction": "flat", "net_move": None, "pct_move": None}
    direction = 0
    weeks = 0
    j = idx
    while j >= 1 and nets[j] is not None and nets[j - 1] is not None:
        d = nets[j] - nets[j - 1]
        if d == 0:
            break
        sign = 1 if d > 0 else -1
        if direction == 0:
            direction = sign
        elif sign != direction:
            break
        weeks += 1
        j -= 1
    net_move = None
    if weeks > 0 and nets[idx] is not None and nets[idx - weeks] is not None:
        net_move = nets[idx] - nets[idx - weeks]
    return {
        "weeks": weeks,
        "direction": "accumulation" if direction > 0 else ("distribution" if direction < 0 else "flat"),
        "net_move": None if net_move is None else round(net_move, 2),
    }


def build_group_state_series(
    series: list[dict[str, Any]],
    group: str,
) -> list[dict[str, Any]]:
    key = GROUP_NET_KEY[group]
    nets = _series_nets(series, key)
    long_pct = _expanding_percentiles(nets)
    rolling = {name: _rolling_percentiles(nets, w) for name, w in ROLLING_WINDOWS.items()}

    states: list[dict[str, Any]] = []
    for i, row in enumerate(series):
        persistence = _persistence_weeks(nets, i)
        journey = {}
        for lag in JOURNEY_LAGS:
            journey[f"{lag}w"] = {
                "net": _lag_value(nets, i, lag),
                "long_history_percentile": _lag_value(long_pct, i, lag),
            }
        velocity = {}
        for lag in VELOCITY_LAGS:
            velocity[f"{lag}w"] = {
                "net_change": _change(nets, i, lag),
                "percentile_change": _change(long_pct, i, lag),
            }
        states.append(
            {
                "date": str(row.get("date") or "")[:10],
                "index": i,
                "group": group,
                "net": nets[i],
                "percentiles": {
                    "long_history": long_pct[i],
                    **{name: rolling[name][i] for name in ROLLING_WINDOWS},
                },
                "journey": journey,
                "velocity": velocity,
                "persistence": persistence,
            }
        )
    return states


def build_spread_series(
    commercial_states: list[dict[str, Any]],
    nonreportable_states: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Normalized Comm↔NR spread in percentile space + expanding spread percentile.

    Formula (primary):
      spread = commercial.long_history_percentile - nonreportable.long_history_percentile
      spread_percentile = expanding percentile rank of ``spread`` (no look-ahead)
    """
    raw_spreads: list[float | None] = []
    for c, nr in zip(commercial_states, nonreportable_states):
        cp = (c.get("percentiles") or {}).get("long_history")
        np_ = (nr.get("percentiles") or {}).get("long_history")
        if cp is None or np_ is None:
            raw_spreads.append(None)
        else:
            raw_spreads.append(round(float(cp) - float(np_), 2))

    spread_pcts = _expanding_percentiles(raw_spreads)
    out: list[dict[str, Any]] = []
    for i, (c, nr) in enumerate(zip(commercial_states, nonreportable_states)):
        out.append(
            {
                "date": c["date"],
                "index": i,
                "spread": raw_spreads[i],
                "spread_percentile": spread_pcts[i],
                "commercial_percentile": (c.get("percentiles") or {}).get("long_history"),
                "nonreportable_percentile": (nr.get("percentiles") or {}).get("long_history"),
                "formula": "commercial_long_history_pct - nonreportable_long_history_pct",
            }
        )
    return out


def _forward_path_stats(prices: list[float | None], idx: int, horizon: int) -> dict[str, Any] | None:
    p0 = prices[idx]
    if p0 is None or p0 == 0 or idx + horizon >= len(prices):
        return None
    p1 = prices[idx + horizon]
    if p1 is None:
        return None
    end_ret = (p1 - p0) / p0 * 100.0
    window = prices[idx : idx + horizon + 1]
    fav = adv = None
    if all(p is not None for p in window):
        path = [(p - p0) / p0 * 100.0 for p in window[1:]]  # type: ignore[operator]
        fav = max(path) if path else None
        adv = min(path) if path else None
    return {
        "return_pct": round(end_ret, 4),
        "higher": end_ret > 0,
        "favourable_excursion_pct": None if fav is None else round(fav, 4),
        "adverse_excursion_pct": None if adv is None else round(adv, 4),
    }


def summarize_outcomes(cases: list[dict[str, Any]], horizon: int) -> dict[str, Any]:
    vals = []
    for c in cases:
        o = (c.get("outcomes") or {}).get(str(horizon))
        if o and o.get("return_pct") is not None:
            vals.append(float(o["return_pct"]))
    n = len(vals)
    if n == 0:
        return {
            "horizon_weeks": horizon,
            "n": 0,
            "sample_quality": sample_quality(0),
            "higher_count": 0,
            "lower_count": 0,
            "pct_higher": None,
            "pct_lower": None,
            "median_return_pct": None,
            "avg_return_pct": None,
            "best_return_pct": None,
            "worst_return_pct": None,
            "dispersion_stdev_pct": None,
            "headline_allowed": False,
        }
    higher = sum(1 for v in vals if v > 0)
    lower = sum(1 for v in vals if v < 0)
    med = _median(vals)
    avg = sum(vals) / n
    stdev = statistics.pstdev(vals) if n >= 2 else 0.0
    return {
        "horizon_weeks": horizon,
        "n": n,
        "sample_quality": sample_quality(n),
        "higher_count": higher,
        "lower_count": lower,
        "pct_higher": round(100.0 * higher / n, 1),
        "pct_lower": round(100.0 * lower / n, 1),
        "median_return_pct": None if med is None else round(med, 3),
        "avg_return_pct": round(avg, 3),
        "best_return_pct": round(max(vals), 3),
        "worst_return_pct": round(min(vals), 3),
        "dispersion_stdev_pct": round(stdev, 3),
        "headline_allowed": n >= 8,
    }


def _cluster_independent(indices: list[int], cooldown: int) -> list[int]:
    """Keep earliest index in each cooldown cluster."""
    indices = sorted(indices)
    kept: list[int] = []
    last = -10_000
    for i in indices:
        if i - last < cooldown:
            continue
        kept.append(i)
        last = i
    return kept


def detect_configuration_events(
    commercial: list[dict[str, Any]],
    noncommercial: list[dict[str, Any]],
    nonreportable: list[dict[str, Any]],
    spreads: list[dict[str, Any]],
    *,
    primary_band: str = PRIMARY_BAND,
) -> list[dict[str, Any]]:
    """Detect research configurations with episode cooldown (no look-ahead)."""
    band = next(b for b in SPREAD_BANDS if b["name"] == primary_band)
    high, low = band["high"], band["low"]
    events: list[dict[str, Any]] = []
    last_by_kind: dict[str, int] = {}

    def can_emit(kind: str, idx: int) -> bool:
        prev = last_by_kind.get(kind, -10_000)
        return idx - prev >= EVENT_COOLDOWN_WEEKS

    def emit(kind: str, idx: int, payload: dict[str, Any]) -> None:
        if not can_emit(kind, idx):
            return
        last_by_kind[kind] = idx
        events.append(payload)

    n = len(commercial)
    for i in range(MIN_HISTORY - 1, n):
        c, nc, nr, sp = commercial[i], noncommercial[i], nonreportable[i], spreads[i]
        c_long = (c.get("percentiles") or {}).get("long_history")
        nc_long = (nc.get("percentiles") or {}).get("long_history")
        nr_long = (nr.get("percentiles") or {}).get("long_history")
        c_3y = (c.get("percentiles") or {}).get("3y")
        nc_3y = (nc.get("percentiles") or {}).get("3y")
        nr_3y = (nr.get("percentiles") or {}).get("3y")
        c_26 = ((c.get("journey") or {}).get("26w") or {}).get("long_history_percentile")
        nc_26 = ((nc.get("journey") or {}).get("26w") or {}).get("long_history_percentile")
        nr_26 = ((nr.get("journey") or {}).get("26w") or {}).get("long_history_percentile")
        c_4w_pct = ((c.get("velocity") or {}).get("4w") or {}).get("percentile_change")
        spread_pct = sp.get("spread_percentile")
        spread = sp.get("spread")

        base = {
            "date": c["date"],
            "index": i,
            "commercial": {
                "net": c.get("net"),
                "long_history_percentile": c_long,
                "percentile_3y": c_3y,
                "velocity": c.get("velocity"),
                "persistence": c.get("persistence"),
                "journey_26w_percentile": c_26,
            },
            "noncommercial": {
                "net": nc.get("net"),
                "long_history_percentile": nc_long,
                "percentile_3y": nc_3y,
                "velocity": nc.get("velocity"),
                "persistence": nc.get("persistence"),
                "journey_26w_percentile": nc_26,
            },
            "nonreportable": {
                "net": nr.get("net"),
                "long_history_percentile": nr_long,
                "percentile_3y": nr_3y,
                "velocity": nr.get("velocity"),
                "persistence": nr.get("persistence"),
                "journey_26w_percentile": nr_26,
            },
            "spread": {
                "value": spread,
                "percentile": spread_pct,
                "formula": sp.get("formula"),
            },
            "thresholds": {
                "primary_band": primary_band,
                "spread_high": high,
                "spread_low": low,
                "absolute_high": ABSOLUTE_HIGH,
                "absolute_low": ABSOLUTE_LOW,
                "rotation_26w": ROTATION_PCT_MOVE_26W,
                "event_cooldown_weeks": EVENT_COOLDOWN_WEEKS,
            },
        }

        # Absolute extremes (commercial)
        if c_long is not None and c_long >= ABSOLUTE_HIGH:
            emit(
                "absolute_extreme_commercial_bull",
                i,
                {
                    **base,
                    "event_type": "absolute_extreme",
                    "layer": "absolute_extreme",
                    "side": "commercial_bullish",
                    "label": "ABSOLUTE EXTREME · COMMERCIAL BULLISH",
                    "group": GROUP_COMMERCIAL,
                },
            )
        if c_long is not None and c_long <= ABSOLUTE_LOW:
            emit(
                "absolute_extreme_commercial_bear",
                i,
                {
                    **base,
                    "event_type": "absolute_extreme",
                    "layer": "absolute_extreme",
                    "side": "commercial_bearish",
                    "label": "ABSOLUTE EXTREME · COMMERCIAL BEARISH",
                    "group": GROUP_COMMERCIAL,
                },
            )

        # Absolute extremes (non-commercial) — same thresholds, NC series only
        if nc_long is not None and nc_long >= ABSOLUTE_HIGH:
            emit(
                "absolute_extreme_noncommercial_bull",
                i,
                {
                    **base,
                    "event_type": "absolute_extreme",
                    "layer": "absolute_extreme",
                    "side": "noncommercial_bullish",
                    "label": "ABSOLUTE EXTREME · NON-COMMERCIAL BULLISH",
                    "group": GROUP_NONCOMMERCIAL,
                },
            )
        if nc_long is not None and nc_long <= ABSOLUTE_LOW:
            emit(
                "absolute_extreme_noncommercial_bear",
                i,
                {
                    **base,
                    "event_type": "absolute_extreme",
                    "layer": "absolute_extreme",
                    "side": "noncommercial_bearish",
                    "label": "ABSOLUTE EXTREME · NON-COMMERCIAL BEARISH",
                    "group": GROUP_NONCOMMERCIAL,
                },
            )

        # Local / relative extremes (3Y) when not absolute
        if (
            c_3y is not None
            and c_3y >= LOCAL_HIGH
            and (c_long is None or c_long < ABSOLUTE_HIGH)
        ):
            emit(
                "local_extreme_commercial_bull",
                i,
                {
                    **base,
                    "event_type": "local_extreme",
                    "layer": "local_extreme",
                    "side": "commercial_bullish_local",
                    "label": "LOCAL EXTREME · COMMERCIAL BULLISH (3Y)",
                    "group": GROUP_COMMERCIAL,
                },
            )
        if (
            c_3y is not None
            and c_3y <= LOCAL_LOW
            and (c_long is None or c_long > ABSOLUTE_LOW)
        ):
            emit(
                "local_extreme_commercial_bear",
                i,
                {
                    **base,
                    "event_type": "local_extreme",
                    "layer": "local_extreme",
                    "side": "commercial_bearish_local",
                    "label": "LOCAL EXTREME · COMMERCIAL BEARISH (3Y)",
                    "group": GROUP_COMMERCIAL,
                },
            )

        # Local / relative extremes (3Y) — non-commercial
        if (
            nc_3y is not None
            and nc_3y >= LOCAL_HIGH
            and (nc_long is None or nc_long < ABSOLUTE_HIGH)
        ):
            emit(
                "local_extreme_noncommercial_bull",
                i,
                {
                    **base,
                    "event_type": "local_extreme",
                    "layer": "local_extreme",
                    "side": "noncommercial_bullish_local",
                    "label": "LOCAL EXTREME · NON-COMMERCIAL BULLISH (3Y)",
                    "group": GROUP_NONCOMMERCIAL,
                },
            )
        if (
            nc_3y is not None
            and nc_3y <= LOCAL_LOW
            and (nc_long is None or nc_long > ABSOLUTE_LOW)
        ):
            emit(
                "local_extreme_noncommercial_bear",
                i,
                {
                    **base,
                    "event_type": "local_extreme",
                    "layer": "local_extreme",
                    "side": "noncommercial_bearish_local",
                    "label": "LOCAL EXTREME · NON-COMMERCIAL BEARISH (3Y)",
                    "group": GROUP_NONCOMMERCIAL,
                },
            )

        # Major rotation (26W percentile migration)
        if c_long is not None and c_26 is not None and abs(c_long - c_26) >= ROTATION_PCT_MOVE_26W:
            emit(
                "major_rotation_commercial",
                i,
                {
                    **base,
                    "event_type": "major_rotation",
                    "layer": "major_rotation",
                    "side": "commercial_rotation",
                    "label": (
                        f"MAJOR ROTATION · COMMERCIAL "
                        f"{c_26:.0f}th → {c_long:.0f}th (26W)"
                    ),
                    "group": GROUP_COMMERCIAL,
                    "rotation_delta_26w": round(c_long - c_26, 2),
                },
            )
        if (
            nc_long is not None
            and nc_26 is not None
            and abs(nc_long - nc_26) >= ROTATION_PCT_MOVE_26W
        ):
            emit(
                "major_rotation_noncommercial",
                i,
                {
                    **base,
                    "event_type": "major_rotation",
                    "layer": "major_rotation",
                    "side": "noncommercial_rotation",
                    "label": (
                        f"MAJOR ROTATION · NON-COMMERCIAL "
                        f"{nc_26:.0f}th → {nc_long:.0f}th (26W)"
                    ),
                    "group": GROUP_NONCOMMERCIAL,
                    "rotation_delta_26w": round(nc_long - nc_26, 2),
                },
            )
        if nr_long is not None and nr_26 is not None and abs(nr_long - nr_26) >= ROTATION_PCT_MOVE_26W:
            emit(
                "major_rotation_nonreportable",
                i,
                {
                    **base,
                    "event_type": "major_rotation",
                    "layer": "major_rotation",
                    "side": "nonreportable_rotation",
                    "label": (
                        f"MAJOR ROTATION · NON-REPORTABLE "
                        f"{nr_26:.0f}th → {nr_long:.0f}th (26W)"
                    ),
                    "group": GROUP_NONREPORTABLE,
                    "rotation_delta_26w": round(nr_long - nr_26, 2),
                },
            )

        # Rapid velocity
        if c_4w_pct is not None and abs(c_4w_pct) >= RAPID_PCT_MOVE_4W:
            emit(
                "rapid_commercial",
                i,
                {
                    **base,
                    "event_type": "rapid_velocity",
                    "layer": "major_rotation",
                    "side": "commercial_rapid",
                    "label": f"RAPID VELOCITY · COMMERCIAL {c_4w_pct:+.0f} pctile / 4W",
                    "group": GROUP_COMMERCIAL,
                },
            )

        # Sustained persistence
        pers = c.get("persistence") or {}
        if (
            int(pers.get("weeks") or 0) >= PERSISTENCE_WEEKS
            and c_long is not None
            and c_26 is not None
            and abs(c_long - c_26) >= PERSISTENCE_MIN_PCT_MOVE
        ):
            emit(
                "persistence_commercial",
                i,
                {
                    **base,
                    "event_type": "sustained_persistence",
                    "layer": "major_rotation",
                    "side": f"commercial_{pers.get('direction')}",
                    "label": (
                        f"SUSTAINED {str(pers.get('direction') or '').upper()} · "
                        f"COMMERCIAL {pers.get('weeks')}W"
                    ),
                    "group": GROUP_COMMERCIAL,
                },
            )

        # Comm ↔ NR divergence (primary research marker)
        if spread_pct is not None and spread_pct >= high:
            emit(
                "comm_nr_divergence_high",
                i,
                {
                    **base,
                    "event_type": "comm_nr_divergence",
                    "layer": "comm_nr_divergence",
                    "side": "commercial_vs_nr_high_spread",
                    "label": "COMM ↔ NR DIVERGENCE · HIGH SPREAD",
                    "group": "multi",
                    "explanation": (
                        "Commercials and Non-Reportables occupy unusually opposed "
                        "normalized positioning states (high commercial−NR percentile spread)."
                    ),
                },
            )
        if spread_pct is not None and spread_pct <= low:
            emit(
                "comm_nr_divergence_low",
                i,
                {
                    **base,
                    "event_type": "comm_nr_divergence",
                    "layer": "comm_nr_divergence",
                    "side": "commercial_vs_nr_low_spread",
                    "label": "COMM ↔ NR DIVERGENCE · LOW SPREAD",
                    "group": "multi",
                    "explanation": (
                        "Commercials and Non-Reportables occupy unusually opposed "
                        "normalized positioning states (low commercial−NR percentile spread)."
                    ),
                },
            )

    return events


def find_configuration_analogues(
    spreads: list[dict[str, Any]],
    commercial: list[dict[str, Any]],
    nonreportable: list[dict[str, Any]],
    prices: list[float | None],
    target_idx: int,
    *,
    side: str | None = None,
) -> dict[str, Any]:
    """Match independent historical weeks with comparable Comm/NR/spread state."""
    if target_idx < MIN_HISTORY:
        return {
            "independent_case_count": 0,
            "cases": [],
            "outcomes_by_horizon": {},
            "sample_quality": sample_quality(0),
            "matching_method": "insufficient_history",
        }

    t_sp = spreads[target_idx]
    t_c = commercial[target_idx]
    t_nr = nonreportable[target_idx]
    t_spread_pct = t_sp.get("spread_percentile")
    t_c_pct = (t_c.get("percentiles") or {}).get("long_history")
    t_nr_pct = (t_nr.get("percentiles") or {}).get("long_history")
    t_c_26 = ((t_c.get("journey") or {}).get("26w") or {}).get("long_history_percentile")
    t_side = side
    if t_side is None and t_spread_pct is not None:
        if t_spread_pct >= 80:
            t_side = "high"
        elif t_spread_pct <= 20:
            t_side = "low"

    raw: list[dict[str, Any]] = []
    # Leave room for longest forward horizon on historical cases
    max_i = target_idx - 1
    for i in range(MIN_HISTORY - 1, max_i + 1):
        if i + min(FORWARD_HORIZONS) >= len(prices):
            continue
        sp = spreads[i]
        c = commercial[i]
        nr = nonreportable[i]
        sp_pct = sp.get("spread_percentile")
        c_pct = (c.get("percentiles") or {}).get("long_history")
        nr_pct = (nr.get("percentiles") or {}).get("long_history")
        if sp_pct is None or t_spread_pct is None or c_pct is None or t_c_pct is None:
            continue
        if nr_pct is None or t_nr_pct is None:
            continue

        rules: list[str] = []
        if abs(sp_pct - t_spread_pct) <= ANALOGUE_SPREAD_TOLERANCE:
            rules.append(
                f"spread_pct≈ ({sp_pct:.1f} vs {t_spread_pct:.1f})"
            )
        else:
            continue

        if t_side == "high" and sp_pct < 70:
            continue
        if t_side == "low" and sp_pct > 30:
            continue
        if t_side:
            rules.append(f"spread_side={t_side}")

        if abs(c_pct - t_c_pct) <= ANALOGUE_PCT_TOLERANCE:
            rules.append(f"commercial_pct≈ ({c_pct:.1f} vs {t_c_pct:.1f})")
        else:
            continue

        if abs(nr_pct - t_nr_pct) <= ANALOGUE_PCT_TOLERANCE:
            rules.append(f"nr_pct≈ ({nr_pct:.1f} vs {t_nr_pct:.1f})")
        else:
            continue

        # Journey: same sign of 26W commercial percentile migration when available
        c_26 = ((c.get("journey") or {}).get("26w") or {}).get("long_history_percentile")
        if t_c_26 is not None and c_26 is not None and t_c_pct is not None:
            t_delta = t_c_pct - t_c_26
            h_delta = c_pct - c_26
            if abs(t_delta) >= 10 and abs(h_delta) >= 10:
                if (t_delta > 0) == (h_delta > 0):
                    rules.append("commercial_26w_journey_same_sign")
                else:
                    continue

        if len(rules) < 3:
            continue

        outcomes = {}
        for h in FORWARD_HORIZONS:
            fo = _forward_path_stats(prices, i, h)
            if fo:
                outcomes[str(h)] = fo
        if not outcomes:
            continue

        raw.append(
            {
                "date": c["date"],
                "index": i,
                "matched_rules": rules,
                "commercial_percentile": c_pct,
                "nonreportable_percentile": nr_pct,
                "spread": sp.get("spread"),
                "spread_percentile": sp_pct,
                "outcomes": outcomes,
            }
        )

    raw.sort(key=lambda m: m["index"])
    independent: list[dict[str, Any]] = []
    last_idx = -10_000
    for m in raw:
        if m["index"] - last_idx < ANALOGUE_COOLDOWN_WEEKS:
            continue
        independent.append(m)
        last_idx = m["index"]

    by_h = {str(h): summarize_outcomes(independent, h) for h in FORWARD_HORIZONS}
    best = None
    for h in (12, 8, 4, 26, 1):
        s = by_h[str(h)]
        if s["n"] <= 0:
            continue
        if best is None or (s["headline_allowed"] and not best["headline_allowed"]) or (
            s["headline_allowed"] == best["headline_allowed"] and s["n"] > best["n"]
        ):
            best = s

    return {
        "matching_method": (
            f"Comparable Comm/NR long-history percentiles (±{ANALOGUE_PCT_TOLERANCE}), "
            f"spread percentile (±{ANALOGUE_SPREAD_TOLERANCE}), same spread side, "
            f"same-sign 26W commercial journey when material. "
            f"Independent cases ≥{ANALOGUE_COOLDOWN_WEEKS}w apart."
        ),
        "target": {
            "date": commercial[target_idx]["date"],
            "index": target_idx,
            "commercial_percentile": t_c_pct,
            "nonreportable_percentile": t_nr_pct,
            "spread": t_sp.get("spread"),
            "spread_percentile": t_spread_pct,
            "side": t_side,
        },
        "independent_case_count": len(independent),
        "raw_match_count_before_dedup": len(raw),
        "cases": independent,
        "outcomes_by_horizon": by_h,
        "best_supported_horizon": best,
        "sample_quality": sample_quality(len(independent)),
        "directional_tendency": directional_tendency(
            by_h.get("12") if (by_h.get("12") or {}).get("n", 0) >= 5 else by_h.get("4")
        ),
        "analogue_cooldown_weeks": ANALOGUE_COOLDOWN_WEEKS,
    }


def audit_spread_bands(
    spreads: list[dict[str, Any]],
    prices: list[float | None],
) -> dict[str, Any]:
    """Evaluate ALL predeclared bands — do not cherry-pick the flattering one."""
    audit: dict[str, Any] = {}
    for band in SPREAD_BANDS:
        high_idxs = [
            s["index"]
            for s in spreads
            if s.get("spread_percentile") is not None
            and s["spread_percentile"] >= band["high"]
            and s["index"] >= MIN_HISTORY - 1
            and s["index"] + 12 < len(prices)
        ]
        low_idxs = [
            s["index"]
            for s in spreads
            if s.get("spread_percentile") is not None
            and s["spread_percentile"] <= band["low"]
            and s["index"] >= MIN_HISTORY - 1
            and s["index"] + 12 < len(prices)
        ]
        high_ind = _cluster_independent(high_idxs, ANALOGUE_COOLDOWN_WEEKS)
        low_ind = _cluster_independent(low_idxs, ANALOGUE_COOLDOWN_WEEKS)

        def _band_outcomes(idxs: list[int]) -> dict[str, Any]:
            cases = []
            for i in idxs:
                outcomes = {}
                for h in FORWARD_HORIZONS:
                    fo = _forward_path_stats(prices, i, h)
                    if fo:
                        outcomes[str(h)] = fo
                if outcomes:
                    cases.append({"index": i, "outcomes": outcomes})
            return {str(h): summarize_outcomes(cases, h) for h in FORWARD_HORIZONS}

        audit[band["name"]] = {
            "high_threshold": band["high"],
            "low_threshold": band["low"],
            "high_spread": {
                "independent_cases": len(high_ind),
                "outcomes_by_horizon": _band_outcomes(high_ind),
                "directional_tendency_12w": directional_tendency(
                    _band_outcomes(high_ind).get("12")
                ),
            },
            "low_spread": {
                "independent_cases": len(low_ind),
                "outcomes_by_horizon": _band_outcomes(low_ind),
                "directional_tendency_12w": directional_tendency(
                    _band_outcomes(low_ind).get("12")
                ),
            },
        }
    return audit


def _compact_current_interpretation(
    commercial: dict[str, Any],
    nonreportable: dict[str, Any],
    noncommercial: dict[str, Any],
    spread: dict[str, Any],
    analogues: dict[str, Any],
) -> dict[str, Any]:
    c_pct = (commercial.get("percentiles") or {}).get("long_history")
    c_3y = (commercial.get("percentiles") or {}).get("3y")
    c_26 = ((commercial.get("journey") or {}).get("26w") or {}).get("long_history_percentile")
    nr_pct = (nonreportable.get("percentiles") or {}).get("long_history")
    o12 = (analogues.get("outcomes_by_horizon") or {}).get("12") or {}
    tendency = analogues.get("directional_tendency") or "INSUFFICIENT EVIDENCE"

    journey_txt = "n/a"
    if c_pct is not None and c_26 is not None:
        journey_txt = f"{c_26:.0f}th → {c_pct:.0f}th over 26 weeks"

    research_action = "No actionable asymmetry from this sample alone."
    if tendency == "BULLISH ASYMMETRY" and analogues.get("independent_case_count", 0) >= 8:
        research_action = (
            "Investigate long-side setup context; check remaining alignment factors."
        )
    elif tendency == "BEARISH ASYMMETRY" and analogues.get("independent_case_count", 0) >= 8:
        research_action = (
            "Investigate short-side setup context; check remaining alignment factors."
        )
    elif tendency == "MIXED":
        research_action = "Historical response mixed — do not lean on positioning alone."

    return {
        "commercial": {
            "net": commercial.get("net"),
            "long_history_percentile": c_pct,
            "percentile_3y": c_3y,
            "journey_26w": journey_txt,
            "velocity_4w_percentile": ((commercial.get("velocity") or {}).get("4w") or {}).get(
                "percentile_change"
            ),
            "persistence": commercial.get("persistence"),
        },
        "noncommercial": {
            "net": noncommercial.get("net"),
            "long_history_percentile": (noncommercial.get("percentiles") or {}).get(
                "long_history"
            ),
        },
        "nonreportable": {
            "net": nonreportable.get("net"),
            "long_history_percentile": nr_pct,
            "percentile_3y": (nonreportable.get("percentiles") or {}).get("3y"),
        },
        "spread": {
            "value": spread.get("spread"),
            "percentile": spread.get("spread_percentile"),
            "formula": spread.get("formula"),
        },
        "analogues": {
            "independent_cases": analogues.get("independent_case_count"),
            "sample_quality": analogues.get("sample_quality"),
            "directional_tendency": tendency,
            "horizon_12w": {
                "higher_count": o12.get("higher_count"),
                "n": o12.get("n"),
                "median_return_pct": o12.get("median_return_pct"),
                "avg_return_pct": o12.get("avg_return_pct"),
            },
        },
        "interpretation": tendency,
        "research_action": research_action,
        "disclaimer": (
            "Historical tendency from comparable positioning configurations only — "
            "not a buy/sell recommendation."
        ),
    }


def _compact_ui_marker(e: dict[str, Any]) -> dict[str, Any]:
    """Serialize an already-detected event for workstation chart markers."""
    return {
        "date": e["date"],
        "event_type": e["event_type"],
        "layer": e.get("layer") or e.get("event_type"),
        "side": e.get("side"),
        "label": e.get("label"),
        "group": e.get("group"),
        "explanation": e.get("explanation"),
        "commercial": e.get("commercial"),
        "noncommercial": e.get("noncommercial"),
        "nonreportable": e.get("nonreportable"),
        "spread": e.get("spread"),
        "thresholds": e.get("thresholds"),
        "analogues": {
            "independent_case_count": (e.get("analogues") or {}).get("independent_case_count"),
            "sample_quality": (e.get("analogues") or {}).get("sample_quality"),
            "directional_tendency": (e.get("analogues") or {}).get("directional_tendency"),
            "outcomes_by_horizon": {
                k: {
                    "n": v.get("n"),
                    "higher_count": v.get("higher_count"),
                    "lower_count": v.get("lower_count"),
                    "median_return_pct": v.get("median_return_pct"),
                    "avg_return_pct": v.get("avg_return_pct"),
                    "sample_quality": v.get("sample_quality"),
                }
                for k, v in ((e.get("analogues") or {}).get("outcomes_by_horizon") or {}).items()
            },
            "cases": [
                {
                    "date": c["date"],
                    "spread_percentile": c.get("spread_percentile"),
                    "outcomes": {
                        k: {"return_pct": (v or {}).get("return_pct")}
                        for k, v in (c.get("outcomes") or {}).items()
                    },
                }
                for c in ((e.get("analogues") or {}).get("cases") or [])[:20]
            ],
        },
    }


def build_market_positioning_research(
    market: str,
    block: dict[str, Any],
    *,
    attach_analogues_to_events: bool = True,
    max_event_analogues: int = 80,
) -> dict[str, Any]:
    series = list(block.get("series") or [])
    if len(series) < MIN_HISTORY:
        return {
            "market": market,
            "available": False,
            "reason": f"insufficient_history<{MIN_HISTORY}",
            "weeks": len(series),
        }

    commercial = build_group_state_series(series, GROUP_COMMERCIAL)
    noncommercial = build_group_state_series(series, GROUP_NONCOMMERCIAL)
    nonreportable = build_group_state_series(series, GROUP_NONREPORTABLE)
    spreads = build_spread_series(commercial, nonreportable)
    prices = [_finite(r.get("price")) for r in series]

    events = detect_configuration_events(
        commercial, noncommercial, nonreportable, spreads
    )

    # Attach analogues to UI-facing events (reuse existing matcher — no new logic).
    ui_analogue_types = {"comm_nr_divergence", "absolute_extreme", "local_extreme"}
    analogue_targets = [e for e in events if e.get("event_type") in ui_analogue_types]
    if attach_analogues_to_events:
        for e in analogue_targets[:max_event_analogues]:
            side = None
            if e.get("event_type") == "comm_nr_divergence":
                side = "high" if "high" in str(e.get("side")) else "low"
            e["analogues"] = find_configuration_analogues(
                spreads,
                commercial,
                nonreportable,
                prices,
                e["index"],
                side=side,
            )
    divergence_events = [e for e in events if e.get("event_type") == "comm_nr_divergence"]

    idx = len(series) - 1
    current_analogues = find_configuration_analogues(
        spreads, commercial, nonreportable, prices, idx
    )
    band_audit = audit_spread_bands(spreads, prices)
    primary = band_audit.get(PRIMARY_BAND) or {}

    # Compact weekly series for optional spread chart + inspector join keys.
    # Full weekly percentile/flow series is exported separately
    # (cot_weekly_inspector_latest.json) to keep this research payload small.
    spread_series_ui = [
        {
            "date": s["date"],
            "spread": s["spread"],
            "spread_percentile": s["spread_percentile"],
            "commercial_percentile": s.get("commercial_percentile"),
            "nonreportable_percentile": s.get("nonreportable_percentile"),
        }
        for s in spreads
        if s.get("spread") is not None
    ]

    # Package already-detected events for on-chart UI (shapes/layers decided in frontend).
    ui_marker_types = {
        "absolute_extreme",
        "local_extreme",
        "major_rotation",
        "comm_nr_divergence",
    }
    markers = []
    for e in events:
        if e.get("event_type") not in ui_marker_types:
            continue
        markers.append(_compact_ui_marker(e))

    interpretation = _compact_current_interpretation(
        commercial[idx],
        nonreportable[idx],
        noncommercial[idx],
        spreads[idx],
        current_analogues,
    )

    return {
        "market": market,
        "available": True,
        "source_week": commercial[idx]["date"],
        "weeks": len(series),
        "engine": "positioning_research_v1",
        "normalization": {
            "long_history_percentile": (
                "Expanding empirical percentile rank of group net positioning "
                "using only history through each week (no look-ahead)."
            ),
            "rolling_percentiles": ROLLING_WINDOWS,
            "spread_formula": (
                "commercial_long_history_percentile - nonreportable_long_history_percentile; "
                "spread_percentile = expanding percentile of that spread series."
            ),
            "primary_marker_band": PRIMARY_BAND,
            "all_bands_audited": [b["name"] for b in SPREAD_BANDS],
            "event_cooldown_weeks": EVENT_COOLDOWN_WEEKS,
            "analogue_cooldown_weeks": ANALOGUE_COOLDOWN_WEEKS,
            "forward_horizons_weeks": list(FORWARD_HORIZONS),
        },
        "current_state": {
            "commercial": commercial[idx],
            "noncommercial": noncommercial[idx],
            "nonreportable": nonreportable[idx],
            "spread": spreads[idx],
        },
        "current_interpretation": interpretation,
        "current_analogues": {
            "independent_case_count": current_analogues.get("independent_case_count"),
            "raw_match_count_before_dedup": current_analogues.get(
                "raw_match_count_before_dedup"
            ),
            "sample_quality": current_analogues.get("sample_quality"),
            "directional_tendency": current_analogues.get("directional_tendency"),
            "matching_method": current_analogues.get("matching_method"),
            "outcomes_by_horizon": current_analogues.get("outcomes_by_horizon"),
            "cases": [
                {
                    "date": c["date"],
                    "spread_percentile": c.get("spread_percentile"),
                    "commercial_percentile": c.get("commercial_percentile"),
                    "nonreportable_percentile": c.get("nonreportable_percentile"),
                    "matched_rules": c.get("matched_rules"),
                    "outcomes": c.get("outcomes"),
                }
                for c in (current_analogues.get("cases") or [])
            ],
        },
        "configuration_events": {
            "total": len(events),
            "by_type": _count_by(events, "event_type"),
            "divergence_count": len(divergence_events),
            "events": events,
        },
        "markers": markers,
        "spread_series": spread_series_ui,
        "band_audit": band_audit,
        "primary_band_summary": {
            "band": PRIMARY_BAND,
            "high_spread_independent_cases": (primary.get("high_spread") or {}).get(
                "independent_cases"
            ),
            "low_spread_independent_cases": (primary.get("low_spread") or {}).get(
                "independent_cases"
            ),
            "high_spread_12w": ((primary.get("high_spread") or {}).get("outcomes_by_horizon") or {}).get(
                "12"
            ),
            "low_spread_12w": ((primary.get("low_spread") or {}).get("outcomes_by_horizon") or {}).get(
                "12"
            ),
            "high_tendency_12w": (primary.get("high_spread") or {}).get(
                "directional_tendency_12w"
            ),
            "low_tendency_12w": (primary.get("low_spread") or {}).get(
                "directional_tendency_12w"
            ),
        },
        "disclaimer": (
            "Investigation / evidence only. Not buy/sell advice. "
            "All threshold bands are audited; primary markers use predeclared 90/10."
        ),
    }


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for it in items:
        k = str(it.get(key) or "unknown")
        out[k] = out.get(k, 0) + 1
    return out


def build_positioning_research_doc(
    cot3y_doc: dict[str, Any],
    *,
    markets: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build research for the full COT series universe unless ``markets`` is set.

    Default scope is every key in ``cot3y_doc['markets']`` — instrument-agnostic.
    Passing ``markets`` only narrows for tests/debug; production exports omit it.
    """
    all_markets = cot3y_doc.get("markets") or {}
    if markets is None:
        selected = sorted(str(k) for k in all_markets.keys())
    else:
        selected = list(markets)
    out: dict[str, Any] = {}
    for mid in selected:
        block = all_markets.get(mid)
        resolved = mid
        if not block:
            # fuzzy contains (debug CLI ids only)
            for k, v in all_markets.items():
                if mid.lower() in str(k).lower():
                    block = v
                    resolved = str(k)
                    break
        if not block:
            out[mid] = {"market": mid, "available": False, "reason": "market_not_found"}
            continue
        out[resolved] = build_market_positioning_research(resolved, block)

    available = sum(1 for m in out.values() if m.get("available"))
    unavailable = {
        mid: m.get("reason")
        for mid, m in out.items()
        if not m.get("available")
    }
    return {
        "version": "cot_positioning_research_v1",
        "engine": "positioning_research_engine",
        "scope": "full_cot3y_universe",
        "validation_scope": selected,
        "generated_note": (
            "Universe-wide Comm↔NR spread + journey-aware analogues for every "
            "supported COT series market with sufficient history. "
            "Not an execution signal."
        ),
        "markets": out,
        "summary": {
            "markets_in_source": len(all_markets),
            "markets_requested": len(selected),
            "markets_available": available,
            "markets_unavailable": len(unavailable),
            "unavailable_reasons": unavailable,
        },
    }
