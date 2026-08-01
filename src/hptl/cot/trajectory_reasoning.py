"""Trajectory-aware positioning / price reasoning for Weekly Analysis.

Methodology-aligned workflow:

NORMAL → OPPOSITION_BUILDING → OPPOSITION_MATURE → ROTATION_WATCH
→ EARLY_ROTATION → CONFIRMED_ROTATION → POST_ROTATION

When PROSE_ENABLED is True, also renders Weekly Analysis UI sections from
trajectory classifications (never from analyst_intelligence templates).
"""

from __future__ import annotations

import math
from typing import Any

PROSE_ENABLED = True
VERSION = "cot_weekly_analysis_trajectory_v1"
ENGINE = "trajectory_reasoning"
DISCLAIMER = (
    "Weekly Analysis organises trajectory-aware COT classifications into an evidence "
    "narrative. It is not a forecast, trade signal, or substitute for your own judgement."
)

BULL_EXTREME = 90.0
BEAR_EXTREME = 10.0
NEAR_HIGH_PCT = 0.15
NEAR_LOW_PCT = 0.15
MAX_CYCLE_WEEKS = 52
CYCLE_BULL_FLOOR = 75.0  # local max must reach this to start a bullish cycle
CYCLE_BEAR_CEILING = 25.0

PARTICIPANT_CLASSES = (
    "DEEPENING_BULLISH",
    "FIRST_ROTATION_ATTEMPT",
    "EARLY_ROTATION_WATCH",
    "EXITING_BULLISH_EXTREME",
    "ROTATING_BEARISH",
    "DEEPENING_BEARISH",
    "EXITING_BEARISH_EXTREME",
    "ROTATING_BULLISH",
    "STABLE",
    "ERRATIC",
)

WORKFLOW_STAGES = (
    "NORMAL",
    "OPPOSITION_BUILDING",
    "OPPOSITION_MATURE",
    "ROTATION_WATCH",
    "EARLY_ROTATION",
    "CONFIRMED_ROTATION",
    "POST_ROTATION",
)

PRICE_CLASSES = (
    "TRENDING_UP",
    "TRENDING_DOWN",
    "STALLING_HIGH",
    "STALLING_LOW",
    "REVERSING_DOWN",
    "REVERSING_UP",
    "RANGE_BOUND",
    "STRUCTURE_BREAK_DOWN",
    "STRUCTURE_BREAK_UP",
)

PP_CLASSES = (
    "POSITIONING_LEADS_PRICE",
    "PRICE_CONFIRMING_ROTATION",
    "PRICE_LAGGING_ROTATION",
    "PRICE_OPPOSING_ROTATION",
    "PRICE_STALLED_AT_EXTREME",
    "NO_CLEAR_RELATIONSHIP",
)

CROSS_CLASSES = (
    "COMMERCIAL_LED_ROTATION",
    "NON_COMMERCIAL_LED_ROTATION",
    "COORDINATED_ROTATION",
    "OPPOSITION_BUILDING",
    "OPPOSITION_MATURE",
    "OPPOSITION_UNWINDING",
    "MIXED_PARTICIPATION",
    "DEVELOPING_ROTATION",
)

STORIES = (
    "POSITIONING_EXTREME_BUILDING",
    "MATURE_OPPOSITION_ROTATION_WATCH",
    "POSITIONING_ROTATING_AHEAD_OF_PRICE",
    "PRICE_CONFIRMING_POSITIONING_ROTATION",
    "PRICE_RESISTING_POSITIONING_ROTATION",
    "COMMERCIAL_LED_TRANSITION",
    "NON_COMMERCIAL_LED_TRANSITION",
    "COORDINATED_UNWIND",
    "EARLY_ROTATION_DEVELOPING",
    "MIXED_NO_CLEAR_EDGE",
)

# Rotation Factor bands mapped to workflow language (not auto-confirmed).
ROTATION_BANDS = (
    (0, 19, "NO_ROTATION"),
    (20, 39, "ROTATION_WATCH"),
    (40, 59, "DEVELOPING_ROTATION"),
    (60, 79, "EARLY_ROTATION"),
    (80, 100, "CONFIRMED_ROTATION"),
)


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _slice_date(v: Any) -> str:
    return str(v or "")[:10]


def _sign(v: float | None) -> int:
    if v is None or v == 0:
        return 0
    return 1 if v > 0 else -1


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _pct_at(weeks: list[dict[str, Any]], group: str, idx: int) -> float | None:
    if idx < 0 or idx >= len(weeks):
        return None
    return _finite((weeks[idx].get(group) or {}).get("percentile"))


def _net_at(weeks: list[dict[str, Any]], group: str, idx: int) -> float | None:
    if idx < 0 or idx >= len(weeks):
        return None
    return _finite((weeks[idx].get(group) or {}).get("net"))


def _lookback_idx(i: int, weeks_back: int) -> int:
    return i - weeks_back


def consecutive_direction_weeks(weeks: list[dict[str, Any]], group: str, i: int) -> tuple[int, int]:
    if i <= 0:
        return 0, 0
    cur = _pct_at(weeks, group, i)
    prev = _pct_at(weeks, group, i - 1)
    if cur is None or prev is None:
        return 0, 0
    s = _sign(cur - prev)
    if s == 0:
        return 0, 0
    n = 1
    j = i - 1
    while j > 0:
        a = _pct_at(weeks, group, j)
        b = _pct_at(weeks, group, j - 1)
        if a is None or b is None:
            break
        if _sign(a - b) != s:
            break
        n += 1
        j -= 1
    return n, s


def _local_extrema(weeks: list[dict[str, Any]], group: str, start: int, end: int) -> tuple[list[int], list[int]]:
    highs: list[int] = []
    lows: list[int] = []
    for j in range(max(start + 1, 1), end):
        a = _pct_at(weeks, group, j - 1)
        b = _pct_at(weeks, group, j)
        c = _pct_at(weeks, group, j + 1) if j + 1 <= end else None
        if a is None or b is None:
            continue
        if c is None:
            # endpoint candidate vs prior only
            if b >= a + 1:
                highs.append(j)
            if b <= a - 1:
                lows.append(j)
            continue
        if b >= a and b >= c and b >= CYCLE_BEAR_CEILING:
            highs.append(j)
        if b <= a and b <= c and b <= CYCLE_BULL_FLOOR:
            lows.append(j)
    return highs, lows


def identify_positioning_cycle(
    weeks: list[dict[str, Any]], group: str, i: int
) -> dict[str, Any]:
    """Anchor trajectory to the start of the *current* positioning cycle.

    Bullish-cycle peak: most recent local max >= CYCLE_BULL_FLOOR within MAX_CYCLE_WEEKS
    when the path is declining / high.
    Bearish-cycle trough: most recent local min <= CYCLE_BEAR_CEILING when rising / low.
    """
    pct = _pct_at(weeks, group, i)
    streak, sign = consecutive_direction_weeks(weeks, group, i)
    start = max(0, i - MAX_CYCLE_WEEKS)
    highs, lows = _local_extrema(weeks, group, start, i)

    bull_anchor = None
    bear_anchor = None

    # Prefer cycle peak when declining or still elevated.
    if pct is not None and (sign <= 0 or pct >= 60):
        for j in reversed(highs):
            p = _pct_at(weeks, group, j)
            if p is not None and p >= CYCLE_BULL_FLOOR:
                bull_anchor = {
                    "date": _slice_date(weeks[j].get("date")),
                    "index": j,
                    "percentile": p,
                    "net": _net_at(weeks, group, j),
                    "kind": "cycle_bull_peak",
                }
                break
        # If currently at extreme high with no prior local max, use most recent max pct week.
        if bull_anchor is None and pct >= BULL_EXTREME:
            best_j, best_p = i, pct
            for j in range(i, start - 1, -1):
                p = _pct_at(weeks, group, j)
                if p is not None and p >= best_p:
                    best_p, best_j = p, j
            bull_anchor = {
                "date": _slice_date(weeks[best_j].get("date")),
                "index": best_j,
                "percentile": best_p,
                "net": _net_at(weeks, group, best_j),
                "kind": "cycle_bull_peak",
            }

    if pct is not None and (sign >= 0 or pct <= 40):
        for j in reversed(lows):
            p = _pct_at(weeks, group, j)
            if p is not None and p <= CYCLE_BEAR_CEILING:
                bear_anchor = {
                    "date": _slice_date(weeks[j].get("date")),
                    "index": j,
                    "percentile": p,
                    "net": _net_at(weeks, group, j),
                    "kind": "cycle_bear_trough",
                }
                break
        if bear_anchor is None and pct <= BEAR_EXTREME:
            best_j, best_p = i, pct
            for j in range(i, start - 1, -1):
                p = _pct_at(weeks, group, j)
                if p is not None and p <= best_p:
                    best_p, best_j = p, j
            bear_anchor = {
                "date": _slice_date(weeks[best_j].get("date")),
                "index": best_j,
                "percentile": best_p,
                "net": _net_at(weeks, group, best_j),
                "kind": "cycle_bear_trough",
            }

    # Supersede older cycle once a later opposite extreme is printed.
    # A bull peak before a later bear trough is no longer the *current* cycle.
    if bull_anchor and bear_anchor:
        if bear_anchor["index"] > bull_anchor["index"]:
            bull_anchor = None
        elif bull_anchor["index"] > bear_anchor["index"]:
            bear_anchor = None

    # Bounce-and-roll: after a trough, look for a post-trough local high as the
    # cycle peak for a subsequent decline (must still clear CYCLE_BULL_FLOOR).
    if bear_anchor is not None and sign <= 0 and bull_anchor is None:
        for j in reversed(highs):
            if j <= bear_anchor["index"]:
                break
            p = _pct_at(weeks, group, j)
            if p is not None and p >= CYCLE_BULL_FLOOR:
                bull_anchor = {
                    "date": _slice_date(weeks[j].get("date")),
                    "index": j,
                    "percentile": p,
                    "net": _net_at(weeks, group, j),
                    "kind": "cycle_bull_peak",
                }
                break

    if bull_anchor is not None and sign >= 0 and bear_anchor is None:
        for j in reversed(lows):
            if j <= bull_anchor["index"]:
                break
            p = _pct_at(weeks, group, j)
            if p is not None and p <= CYCLE_BEAR_CEILING:
                bear_anchor = {
                    "date": _slice_date(weeks[j].get("date")),
                    "index": j,
                    "percentile": p,
                    "net": _net_at(weeks, group, j),
                    "kind": "cycle_bear_trough",
                }
                break

    # Active cycle = anchor matching current direction; else nearer remaining.
    active = None
    if sign < 0 and bull_anchor is not None:
        active = bull_anchor
    elif sign > 0 and bear_anchor is not None:
        active = bear_anchor
    elif bull_anchor and bear_anchor:
        active = bull_anchor if bull_anchor["index"] >= bear_anchor["index"] else bear_anchor
    else:
        active = bull_anchor or bear_anchor

    return {
        "bull_cycle_peak": bull_anchor,
        "bear_cycle_trough": bear_anchor,
        "active_cycle_anchor": active,
        "cycle_length_weeks": (i - active["index"]) if active else None,
        "direction_sign": sign,
        "streak": streak,
    }


def build_participant_trajectory(
    weeks: list[dict[str, Any]], group: str, i: int
) -> dict[str, Any]:
    rules: list[str] = []
    pct = _pct_at(weeks, group, i)
    net = _net_at(weeks, group, i)
    lags = {f"percentile_{w}w_ago": _pct_at(weeks, group, _lookback_idx(i, w)) for w in (1, 2, 4, 8, 12)}
    streak, sign = consecutive_direction_weeks(weeks, group, i)
    v1 = None if lags["percentile_1w_ago"] is None or pct is None else pct - lags["percentile_1w_ago"]
    v_prev = None
    p1 = lags["percentile_1w_ago"]
    p2 = lags["percentile_2w_ago"]
    if p1 is not None and p2 is not None:
        v_prev = p1 - p2
    accel = None if v1 is None or v_prev is None else v1 - v_prev

    cycle = identify_positioning_cycle(weeks, group, i)
    bull = cycle.get("bull_cycle_peak")
    bear = cycle.get("bear_cycle_trough")
    active = cycle.get("active_cycle_anchor")

    weeks_since_bull = (i - bull["index"]) if bull else None
    weeks_since_bear = (i - bear["index"]) if bear else None
    dist_bull_pct = (pct - bull["percentile"]) if bull and pct is not None else None
    dist_bear_pct = (pct - bear["percentile"]) if bear and pct is not None else None
    dist_bull_net = (
        (net - bull["net"]) if bull and net is not None and bull.get("net") is not None else None
    )
    dist_bear_net = (
        (net - bear["net"]) if bear and net is not None and bear.get("net") is not None else None
    )

    flips = 0
    for k in range(max(1, i - 3), i + 1):
        a = _pct_at(weeks, group, k)
        b = _pct_at(weeks, group, k - 1)
        if a is None or b is None or k < 2:
            continue
        if abs(a - b) >= 3:
            c = _pct_at(weeks, group, k - 2)
            if c is not None and _sign(a - b) != 0 and _sign(b - c) != 0 and _sign(a - b) != _sign(b - c):
                flips += 1

    if pct is None:
        rules.append("class:STABLE (missing percentile)")
        classification = "STABLE"
    else:
        # Pass distances via temporary attributes on cycle peaks for _phase_from_path
        # Rebuild classifier inputs cleanly:
        classification = _classify_participant(
            pct=pct,
            streak=streak,
            sign=sign,
            dist_bull=dist_bull_pct,
            dist_bear=dist_bear_pct,
            weeks_since_bull=weeks_since_bull,
            weeks_since_bear=weeks_since_bear,
            velocity=v1,
            prev_velocity=v_prev,
            rules=rules,
        )
        if (
            flips >= 2
            and streak <= 1
            and classification in ("STABLE", "DEEPENING_BULLISH", "DEEPENING_BEARISH")
            and abs(dist_bull_pct or 0) < 15
            and abs(dist_bear_pct or 0) < 15
        ):
            rules.append("class:ERRATIC")
            classification = "ERRATIC"

    phase_map = {
        "DEEPENING_BULLISH": "deepening",
        "DEEPENING_BEARISH": "deepening",
        "FIRST_ROTATION_ATTEMPT": "first_attempt",
        "EARLY_ROTATION_WATCH": "early_watch",
        "EXITING_BULLISH_EXTREME": "exiting",
        "EXITING_BEARISH_EXTREME": "exiting",
        "ROTATING_BEARISH": "rotating",
        "ROTATING_BULLISH": "rotating",
        "STABLE": "stabilising",
        "ERRATIC": "erratic",
    }

    return {
        "group": group,
        "date": _slice_date(weeks[i].get("date")),
        "net": net,
        "percentile": pct,
        **lags,
        "cycle": {
            "active_anchor": active,
            "bull_cycle_peak": bull,
            "bear_cycle_trough": bear,
            "cycle_length_weeks": cycle.get("cycle_length_weeks"),
        },
        "latest_bullish_extreme": bull,  # cycle peak (not multi-year absolute)
        "latest_bearish_extreme": bear,  # cycle trough
        "weeks_since_bullish_extreme": weeks_since_bull,
        "weeks_since_bearish_extreme": weeks_since_bear,
        "percentile_distance_from_bullish_extreme": dist_bull_pct,
        "percentile_distance_from_bearish_extreme": dist_bear_pct,
        "net_distance_from_bullish_extreme": dist_bull_net,
        "net_distance_from_bearish_extreme": dist_bear_net,
        "consecutive_weeks_current_direction": streak,
        "direction_sign": sign,
        "velocity_1w_percentile_pts": v1,
        "previous_velocity_1w_percentile_pts": v_prev,
        "acceleration": accel,
        "phase": phase_map.get(classification, "stabilising"),
        "classification": classification,
        "rules_fired": rules,
    }


def _classify_participant(
    *,
    pct: float,
    streak: int,
    sign: int,
    dist_bull: float | None,
    dist_bear: float | None,
    weeks_since_bull: int | None,
    weeks_since_bear: int | None,
    velocity: float | None,
    prev_velocity: float | None,
    rules: list[str],
) -> str:
    accel = None if velocity is None or prev_velocity is None else velocity - prev_velocity
    at_bull = pct >= BULL_EXTREME
    at_bear = pct <= BEAR_EXTREME
    # Cycle must be current (≤ MAX_CYCLE_WEEKS already enforced by finder)
    bull_ok = weeks_since_bull is not None and weeks_since_bull <= MAX_CYCLE_WEEKS
    bear_ok = weeks_since_bear is not None and weeks_since_bear <= MAX_CYCLE_WEEKS
    material_bull = bull_ok and dist_bull is not None and dist_bull <= -12
    material_bear = bear_ok and dist_bear is not None and dist_bear >= 12

    if at_bull and sign < 0 and streak == 1:
        rules.append("class:FIRST_ROTATION_ATTEMPT (1W turn at bull extreme)")
        return "FIRST_ROTATION_ATTEMPT"
    if at_bear and sign > 0 and streak == 1:
        rules.append("class:FIRST_ROTATION_ATTEMPT (1W turn at bear extreme)")
        return "FIRST_ROTATION_ATTEMPT"

    if sign < 0 and streak == 2 and (at_bull or (bull_ok and dist_bull is not None and dist_bull <= -4)):
        rules.append("class:EARLY_ROTATION_WATCH (2W)")
        return "EARLY_ROTATION_WATCH"
    if sign > 0 and streak == 2 and (at_bear or (bear_ok and dist_bear is not None and dist_bear >= 4)):
        rules.append("class:EARLY_ROTATION_WATCH (2W)")
        return "EARLY_ROTATION_WATCH"

    # True ROTATING needs travel from a *current* elevated/depressed cycle anchor
    # (not a long-stale mid-cycle print). Anchor percentile must have been extreme-ish.
    bull_was_elevated = bull_ok and dist_bull is not None and (
        (weeks_since_bull is not None and weeks_since_bull <= 26)
        or abs(dist_bull) >= 25
    )
    bear_was_depressed = bear_ok and dist_bear is not None and (
        (weeks_since_bear is not None and weeks_since_bear <= 26)
        or abs(dist_bear) >= 25
    )
    rotating_bearish = (
        bull_ok
        and bull_was_elevated
        and dist_bull is not None
        and dist_bull <= -20
        and pct < BULL_EXTREME - 10
        and streak >= 3
        and sign <= 0
    )
    rotating_bullish = (
        bear_ok
        and bear_was_depressed
        and dist_bear is not None
        and dist_bear >= 20
        and pct > BEAR_EXTREME + 10
        and streak >= 3
        and sign >= 0
    )
    exiting_bull = sign <= 0 and bull_ok and (
        (streak >= 3 and dist_bull is not None and dist_bull <= -8)
        or (streak >= 2 and material_bull)
    )
    exiting_bear = sign >= 0 and bear_ok and (
        (streak >= 3 and dist_bear is not None and dist_bear >= 8)
        or (streak >= 2 and material_bear)
    )

    if rotating_bearish:
        rules.append("class:ROTATING_BEARISH")
        return "ROTATING_BEARISH"
    if rotating_bullish:
        rules.append("class:ROTATING_BULLISH")
        return "ROTATING_BULLISH"
    if exiting_bull:
        rules.append("class:EXITING_BULLISH_EXTREME")
        return "EXITING_BULLISH_EXTREME"
    if exiting_bear:
        rules.append("class:EXITING_BEARISH_EXTREME")
        return "EXITING_BEARISH_EXTREME"

    if at_bull and sign >= 0:
        rules.append("class:DEEPENING_BULLISH")
        return "DEEPENING_BULLISH"
    if at_bear and sign <= 0:
        rules.append("class:DEEPENING_BEARISH")
        return "DEEPENING_BEARISH"
    if at_bull:
        rules.append("class:DEEPENING_BULLISH (at extreme)")
        return "DEEPENING_BULLISH"
    if at_bear:
        rules.append("class:DEEPENING_BEARISH (at extreme)")
        return "DEEPENING_BEARISH"

    if streak >= 2 and abs(velocity or 0) >= 1.5:
        rules.append("class:EARLY_ROTATION_WATCH (mid-range persistence)")
        return "EARLY_ROTATION_WATCH"
    if streak <= 1 and abs(velocity or 0) < 1.0:
        rules.append("class:STABLE")
        return "STABLE"
    if accel is not None and abs(velocity or 0) < 1.0 and abs(accel) < 0.5:
        rules.append("class:STABLE (low velocity)")
        return "STABLE"
    rules.append("class:STABLE (default)")
    return "STABLE"


def _align_price_closes(
    weekly_ohlc: list[dict[str, Any]], report_date: str
) -> list[tuple[str, float]]:
    rows: list[tuple[str, float]] = []
    for b in weekly_ohlc or []:
        d = _slice_date(b.get("date"))
        c = _finite(b.get("close"))
        if not d or c is None or d > report_date:
            continue
        rows.append((d, c))
    rows.sort(key=lambda x: x[0])
    return rows


def _swing_points(closes: list[float], *, left: int = 2, right: int = 2) -> tuple[list[int], list[int]]:
    highs: list[int] = []
    lows: list[int] = []
    n = len(closes)
    for i in range(left, n - right):
        window = closes[i - left : i + right + 1]
        if closes[i] >= max(window) and closes[i] == window[left]:
            highs.append(i)
        if closes[i] <= min(window) and closes[i] == window[left]:
            lows.append(i)
    return highs, lows


def build_price_trajectory(
    weekly_ohlc: list[dict[str, Any]], report_date: str
) -> dict[str, Any]:
    rules: list[str] = []
    series = _align_price_closes(weekly_ohlc, report_date)
    if len(series) < 3:
        return {
            "available": False,
            "report_date": report_date,
            "classification": "RANGE_BOUND",
            "rules_fired": ["price:insufficient_history"],
        }
    dates = [d for d, _ in series]
    closes = [c for _, c in series]
    i = len(closes) - 1
    px = closes[i]

    def ret(w: int) -> float | None:
        if i - w < 0 or closes[i - w] == 0:
            return None
        return (px / closes[i - w] - 1.0) * 100.0

    window = closes[max(0, i - 11) : i + 1]
    hi12 = max(window)
    lo12 = min(window)
    span = hi12 - lo12
    dist_high = ((hi12 - px) / span * 100.0) if span > 0 else 0.0
    dist_low = ((px - lo12) / span * 100.0) if span > 0 else 0.0

    swing_hi_idx, swing_lo_idx = _swing_points(closes)
    last_sh = swing_hi_idx[-1] if swing_hi_idx else None
    last_sl = swing_lo_idx[-1] if swing_lo_idx else None
    dist_swing_high = (
        ((closes[last_sh] - px) / closes[last_sh] * 100.0)
        if last_sh is not None and closes[last_sh]
        else None
    )
    dist_swing_low = (
        ((px - closes[last_sl]) / closes[last_sl] * 100.0)
        if last_sl is not None and closes[last_sl]
        else None
    )

    r1, r2, r4, r8, r12 = ret(1), ret(2), ret(4), ret(8), ret(12)
    momentum = r4 if r4 is not None else r1
    prior4 = closes[max(0, i - 4) : i]
    structure_break_up = bool(prior4) and px > max(prior4) and (r1 or 0) > 0
    structure_break_down = bool(prior4) and px < min(prior4) and (r1 or 0) < 0
    near_high = span > 0 and dist_high <= NEAR_HIGH_PCT * 100
    near_low = span > 0 and dist_low <= NEAR_LOW_PCT * 100

    classification = "RANGE_BOUND"
    if structure_break_down and (r4 or 0) < -1:
        classification = "STRUCTURE_BREAK_DOWN"
        rules.append("price:STRUCTURE_BREAK_DOWN")
    elif structure_break_up and (r4 or 0) > 1:
        classification = "STRUCTURE_BREAK_UP"
        rules.append("price:STRUCTURE_BREAK_UP")
    elif near_high and abs(r1 or 0) < 0.8 and (r4 or 0) > 0:
        classification = "STALLING_HIGH"
        rules.append("price:STALLING_HIGH")
    elif near_low and abs(r1 or 0) < 0.8 and (r4 or 0) < 0:
        classification = "STALLING_LOW"
        rules.append("price:STALLING_LOW")
    elif (r4 or 0) > 2 and (r1 or 0) < -1 and near_high:
        classification = "REVERSING_DOWN"
        rules.append("price:REVERSING_DOWN")
    elif (r4 or 0) < -2 and (r1 or 0) > 1 and near_low:
        classification = "REVERSING_UP"
        rules.append("price:REVERSING_UP")
    elif (r4 or 0) > 3 and (r8 or 0) > 3:
        classification = "TRENDING_UP"
        rules.append("price:TRENDING_UP")
    elif (r4 or 0) < -3 and (r8 or 0) < -3:
        classification = "TRENDING_DOWN"
        rules.append("price:TRENDING_DOWN")
    else:
        rules.append("price:RANGE_BOUND")

    if abs(r1 or 0) < 0.4 and abs(r4 or 0) < 1.5:
        state = "stalling"
    elif classification in ("REVERSING_DOWN", "REVERSING_UP", "STRUCTURE_BREAK_DOWN", "STRUCTURE_BREAK_UP"):
        state = "reversing"
    elif classification in ("TRENDING_UP", "TRENDING_DOWN"):
        state = "advancing"
    else:
        state = "advancing" if abs(r4 or 0) >= 1.5 else "stalling"

    return {
        "available": True,
        "report_date": report_date,
        "price_asof_date": dates[i],
        "close": px,
        "return_1w_pct": r1,
        "return_2w_pct": r2,
        "return_4w_pct": r4,
        "return_8w_pct": r8,
        "return_12w_pct": r12,
        "distance_from_12w_high_pct_of_range": dist_high,
        "distance_from_12w_low_pct_of_range": dist_low,
        "distance_from_swing_high_pct": dist_swing_high,
        "distance_from_swing_low_pct": dist_swing_low,
        "weekly_trend": classification,
        "weekly_momentum_4w_pct": momentum,
        "price_state": state,
        "structure_break_up": structure_break_up,
        "structure_break_down": structure_break_down,
        "near_12w_high": near_high,
        "near_12w_low": near_low,
        "classification": classification,
        "rules_fired": rules,
    }


def _is_rotation_class(cls: str | None) -> bool:
    return cls in (
        "FIRST_ROTATION_ATTEMPT",
        "EARLY_ROTATION_WATCH",
        "EXITING_BULLISH_EXTREME",
        "EXITING_BEARISH_EXTREME",
        "ROTATING_BEARISH",
        "ROTATING_BULLISH",
    )


def _is_true_exit_or_rotate(cls: str | None) -> bool:
    return cls in (
        "EXITING_BULLISH_EXTREME",
        "EXITING_BEARISH_EXTREME",
        "ROTATING_BEARISH",
        "ROTATING_BULLISH",
    )


def classify_positioning_price_relationship(
    commercial: dict[str, Any],
    noncommercial: dict[str, Any],
    price: dict[str, Any],
) -> dict[str, Any]:
    rules: list[str] = []
    c_cls = commercial.get("classification")
    p_cls = price.get("classification")
    c_streak = commercial.get("consecutive_weeks_current_direction") or 0
    rotating_bear = c_cls in (
        "EXITING_BULLISH_EXTREME",
        "ROTATING_BEARISH",
        "EARLY_ROTATION_WATCH",
        "FIRST_ROTATION_ATTEMPT",
    ) and (commercial.get("direction_sign") or 0) < 0
    rotating_bull = c_cls in (
        "EXITING_BEARISH_EXTREME",
        "ROTATING_BULLISH",
        "EARLY_ROTATION_WATCH",
        "FIRST_ROTATION_ATTEMPT",
    ) and (commercial.get("direction_sign") or 0) > 0

    price_stalled_high = p_cls in ("STALLING_HIGH",) or bool(price.get("near_12w_high"))
    price_stalled_low = p_cls in ("STALLING_LOW",) or bool(price.get("near_12w_low"))
    price_confirms_bear = p_cls in ("TRENDING_DOWN", "STRUCTURE_BREAK_DOWN", "REVERSING_DOWN")
    price_confirms_bull = p_cls in ("TRENDING_UP", "STRUCTURE_BREAK_UP", "REVERSING_UP")

    # Price confirmation of rotation requires positioning persistence (A5 / NG).
    if rotating_bear and price_confirms_bear and c_streak >= 2:
        rules.append("pp:PRICE_CONFIRMING_ROTATION")
        return {"classification": "PRICE_CONFIRMING_ROTATION", "rules_fired": rules}
    if rotating_bull and price_confirms_bull and c_streak >= 2:
        rules.append("pp:PRICE_CONFIRMING_ROTATION")
        return {"classification": "PRICE_CONFIRMING_ROTATION", "rules_fired": rules}

    if rotating_bear and (price_stalled_high or price_confirms_bull) and not price_confirms_bear:
        if c_streak >= 2:
            rules.append("pp:POSITIONING_LEADS_PRICE")
            return {"classification": "POSITIONING_LEADS_PRICE", "rules_fired": rules}
        rules.append("pp:PRICE_OPPOSING_ROTATION (early attempt)")
        return {"classification": "PRICE_OPPOSING_ROTATION", "rules_fired": rules}
    if rotating_bull and (price_stalled_low or price_confirms_bear) and not price_confirms_bull:
        if c_streak >= 2:
            rules.append("pp:POSITIONING_LEADS_PRICE")
            return {"classification": "POSITIONING_LEADS_PRICE", "rules_fired": rules}
        rules.append("pp:PRICE_OPPOSING_ROTATION (early attempt)")
        return {"classification": "PRICE_OPPOSING_ROTATION", "rules_fired": rules}

    if rotating_bear and price_confirms_bull:
        rules.append("pp:PRICE_OPPOSING_ROTATION")
        return {"classification": "PRICE_OPPOSING_ROTATION", "rules_fired": rules}
    if rotating_bull and price_confirms_bear:
        rules.append("pp:PRICE_OPPOSING_ROTATION")
        return {"classification": "PRICE_OPPOSING_ROTATION", "rules_fired": rules}

    if c_cls in ("DEEPENING_BULLISH", "DEEPENING_BEARISH") and (
        price_stalled_high or price_stalled_low or price_confirms_bull or price_confirms_bear
    ):
        rules.append("pp:PRICE_STALLED_AT_EXTREME / trend with extreme")
        return {"classification": "PRICE_STALLED_AT_EXTREME", "rules_fired": rules}

    rules.append("pp:NO_CLEAR_RELATIONSHIP")
    return {"classification": "NO_CLEAR_RELATIONSHIP", "rules_fired": rules}


def classify_cross_group(
    commercial: dict[str, Any],
    noncommercial: dict[str, Any],
    nonreportable: dict[str, Any],
    weeks: list[dict[str, Any]],
    i: int,
    *,
    price: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rules: list[str] = []
    c_cls = commercial["classification"]
    nc_cls = noncommercial["classification"]
    c_sign = commercial.get("direction_sign") or 0
    nc_sign = noncommercial.get("direction_sign") or 0
    c_streak = commercial.get("consecutive_weeks_current_direction") or 0
    nc_streak = noncommercial.get("consecutive_weeks_current_direction") or 0
    c_pct = _finite(commercial.get("percentile"))
    nc_pct = _finite(noncommercial.get("percentile"))

    oppose = c_sign != 0 and nc_sign != 0 and c_sign != nc_sign
    oppose_extremes = (
        c_pct is not None
        and nc_pct is not None
        and (
            (c_pct <= BEAR_EXTREME and nc_pct >= BULL_EXTREME)
            or (c_pct >= BULL_EXTREME and nc_pct <= BEAR_EXTREME)
        )
    )
    near_oppose_extremes = (
        c_pct is not None
        and nc_pct is not None
        and (
            (c_pct <= 15 and nc_pct >= 85)
            or (c_pct >= 85 and nc_pct <= 15)
        )
    )

    spreads = []
    for j in range(max(0, i - 4), i + 1):
        s = _finite((weeks[j].get("cross") or {}).get("comm_nc_spread"))
        if s is not None:
            spreads.append(s)
    spread_now = spreads[-1] if spreads else None
    spread_4w = spreads[0] if len(spreads) >= 2 else None
    spread_change = (spread_now - spread_4w) if spread_now is not None and spread_4w is not None else None
    widening = spread_change is not None and abs(spread_now or 0) > abs(spread_4w or 0) + 3
    narrowing = spread_change is not None and abs(spread_now or 0) + 3 < abs(spread_4w or 0)

    nr_sign = nonreportable.get("direction_sign") or 0
    nr_supports = nr_sign != 0 and c_sign != 0 and nr_sign == c_sign

    price_agrees = False
    if price:
        p_cls = price.get("classification")
        if c_sign < 0 and p_cls in ("TRENDING_DOWN", "STRUCTURE_BREAK_DOWN", "REVERSING_DOWN"):
            price_agrees = True
        if c_sign > 0 and p_cls in ("TRENDING_UP", "STRUCTURE_BREAK_UP", "REVERSING_UP"):
            price_agrees = True

    c_rotating = _is_rotation_class(c_cls)
    nc_responding = oppose and (
        _is_rotation_class(nc_cls) or nc_cls in ("DEEPENING_BULLISH", "DEEPENING_BEARISH") or nc_streak >= 2
    )
    multiweek = c_streak >= 2 and nc_streak >= 2

    # A5: Coordinated Rotation requires C rotation + NC response + multi-week
    # persistence + (preferably) initial price agreement. Otherwise Developing.
    if (
        _is_true_exit_or_rotate(c_cls)
        and nc_responding
        and multiweek
        and price_agrees
        and c_streak >= 3
    ):
        rules.append("cross:COORDINATED_ROTATION (strict)")
        cls = "COORDINATED_ROTATION"
    elif c_rotating and nc_responding and multiweek:
        rules.append("cross:DEVELOPING_ROTATION (needs price agreement)")
        cls = "DEVELOPING_ROTATION"
    elif c_rotating and nc_responding and not multiweek:
        rules.append("cross:DEVELOPING_ROTATION (persistence incomplete)")
        cls = "DEVELOPING_ROTATION"
    elif oppose_extremes:
        rules.append("cross:OPPOSITION_MATURE")
        cls = "OPPOSITION_MATURE"
    elif near_oppose_extremes and widening:
        rules.append("cross:OPPOSITION_BUILDING")
        cls = "OPPOSITION_BUILDING"
    elif oppose and widening:
        rules.append("cross:OPPOSITION_BUILDING")
        cls = "OPPOSITION_BUILDING"
    elif oppose and narrowing and multiweek:
        rules.append("cross:OPPOSITION_UNWINDING")
        cls = "OPPOSITION_UNWINDING"
    elif c_rotating and c_streak >= nc_streak + 2 and multiweek:
        rules.append("cross:COMMERCIAL_LED_ROTATION")
        cls = "COMMERCIAL_LED_ROTATION"
    elif c_rotating and nc_streak >= c_streak + 2 and multiweek:
        rules.append("cross:NON_COMMERCIAL_LED_ROTATION")
        cls = "NON_COMMERCIAL_LED_ROTATION"
    else:
        rules.append("cross:MIXED_PARTICIPATION")
        cls = "MIXED_PARTICIPATION"

    return {
        "classification": cls,
        "trajectories_oppose": oppose,
        "opposition_widening": widening,
        "opposition_narrowing": narrowing,
        "oppose_extremes": oppose_extremes,
        "near_oppose_extremes": near_oppose_extremes,
        "both_rotating": c_rotating and _is_rotation_class(nc_cls),
        "crowded": oppose_extremes or near_oppose_extremes,
        "non_reportable_supports": nr_supports,
        "price_agrees_with_commercial": price_agrees,
        "comm_nc_spread": spread_now,
        "comm_nc_spread_change_4w": spread_change,
        "leading_group": (
            "commercial"
            if cls == "COMMERCIAL_LED_ROTATION"
            else "noncommercial"
            if cls == "NON_COMMERCIAL_LED_ROTATION"
            else None
        ),
        "rules_fired": rules,
    }


def _score_commercial_transition(c: dict[str, Any]) -> tuple[float, str]:
    cls = c.get("classification")
    streak = c.get("consecutive_weeks_current_direction") or 0
    dist = c.get("percentile_distance_from_bullish_extreme")
    dist_b = c.get("percentile_distance_from_bearish_extreme")
    cycle_len = (c.get("cycle") or {}).get("cycle_length_weeks")

    if cls == "FIRST_ROTATION_ATTEMPT":
        return 4.0, "first_attempt_1w"
    if cls == "EARLY_ROTATION_WATCH":
        return _clamp(8 + min(4, streak), 0, 12), f"early_watch streak={streak}"
    if cls == "ROTATING_BEARISH":
        mag = abs(dist or 0)
        # Require current-cycle travel; old extremes already excluded by cycle finder
        score = 14 + min(8, mag / 6) + min(3, max(0, streak - 2))
        return _clamp(score, 0, 25), f"rotating_bearish mag={mag:.1f} cycle={cycle_len}"
    if cls == "EXITING_BULLISH_EXTREME":
        mag = abs(dist or 0)
        return _clamp(10 + min(8, mag / 4) + min(4, streak), 0, 20), f"exiting_bull mag={mag:.1f}"
    if cls == "ROTATING_BULLISH":
        mag = abs(dist_b or 0)
        score = 14 + min(8, mag / 6) + min(3, max(0, streak - 2))
        return _clamp(score, 0, 25), f"rotating_bullish mag={mag:.1f}"
    if cls == "EXITING_BEARISH_EXTREME":
        mag = abs(dist_b or 0)
        return _clamp(10 + min(8, mag / 4) + min(4, streak), 0, 20), f"exiting_bear mag={mag:.1f}"
    if cls in ("DEEPENING_BULLISH", "DEEPENING_BEARISH"):
        return 3.0, "deepening_extreme"
    return 0.0, "no_transition"


def _score_nc_response(c: dict[str, Any], nc: dict[str, Any]) -> tuple[float, str]:
    c_sign = c.get("direction_sign") or 0
    nc_sign = nc.get("direction_sign") or 0
    nc_streak = nc.get("consecutive_weeks_current_direction") or 0
    if c_sign == 0 or nc_sign == 0:
        return 0.0, "missing_direction"
    if c_sign == nc_sign:
        return 1.0, "nc_not_opposing"
    base = 6 + min(8, nc_streak * 2)
    if _is_rotation_class(nc.get("classification")) or nc.get("classification") in (
        "DEEPENING_BULLISH",
        "DEEPENING_BEARISH",
    ):
        base += 4
    return _clamp(base, 0, 20), f"nc_opposes streak={nc_streak}"


def _score_spread(cross: dict[str, Any]) -> tuple[float, str]:
    ch = _finite(cross.get("comm_nc_spread_change_4w"))
    if ch is None:
        return 0.0, "no_spread_change"
    mag = abs(ch)
    if cross.get("opposition_narrowing"):
        return _clamp(6 + min(6, mag / 4), 0, 15), f"narrowing d4={ch:.1f}"
    if cross.get("opposition_widening"):
        return _clamp(5 + min(5, mag / 5), 0, 12), f"widening d4={ch:.1f}"
    return _clamp(min(6, mag / 4), 0, 10), f"d4={ch:.1f}"


def _score_persistence(c: dict[str, Any], nc: dict[str, Any]) -> tuple[float, str]:
    cs = c.get("consecutive_weeks_current_direction") or 0
    ns = nc.get("consecutive_weeks_current_direction") or 0
    if cs < 2:
        return 0.0, "commercial_persistence<2"
    score = min(6, (cs - 1) * 2) + min(4, ns)
    return _clamp(score, 0, 10), f"c={cs} nc={ns}"


def _score_acceleration(c: dict[str, Any]) -> tuple[float, str]:
    acc = _finite(c.get("acceleration"))
    vel = _finite(c.get("velocity_1w_percentile_pts"))
    streak = c.get("consecutive_weeks_current_direction") or 0
    if streak < 2:
        return 0.0, "accel_ignored_until_2w"
    if acc is None or vel is None:
        return 0.0, "no_acceleration"
    if (vel < 0 and acc < 0) or (vel > 0 and acc > 0):
        return _clamp(3 + min(5, abs(acc) / 2), 0, 10), f"accel={acc:.2f}"
    return 2.0, f"decel={acc:.2f}"


def _score_price_relationship(pp: dict[str, Any], c: dict[str, Any]) -> tuple[float, str]:
    cls = pp.get("classification")
    streak = c.get("consecutive_weeks_current_direction") or 0
    if cls == "PRICE_CONFIRMING_ROTATION":
        return (12.0 if streak >= 2 else 4.0), cls
    if cls == "POSITIONING_LEADS_PRICE":
        return (9.0 if streak >= 2 else 3.0), cls
    if cls == "PRICE_LAGGING_ROTATION":
        return 6.0, cls
    if cls == "PRICE_STALLED_AT_EXTREME":
        return 5.0, cls
    if cls == "PRICE_OPPOSING_ROTATION":
        return 2.0, cls
    return 0.0, cls or "none"


def _score_nr_support(cross: dict[str, Any], nr: dict[str, Any]) -> tuple[float, str]:
    if cross.get("non_reportable_supports"):
        streak = nr.get("consecutive_weeks_current_direction") or 0
        return _clamp(2 + min(3, streak), 0, 5), "nr_supports"
    return 0.0, "nr_not_supportive"


def compute_rotation_factor(
    commercial: dict[str, Any],
    noncommercial: dict[str, Any],
    nonreportable: dict[str, Any],
    cross: dict[str, Any],
    pp: dict[str, Any],
) -> dict[str, Any]:
    c_score, c_why = _score_commercial_transition(commercial)
    nc_score, nc_why = _score_nc_response(commercial, noncommercial)
    sp_score, sp_why = _score_spread(cross)
    pe_score, pe_why = _score_persistence(commercial, noncommercial)
    ac_score, ac_why = _score_acceleration(commercial)
    pr_score, pr_why = _score_price_relationship(pp, commercial)
    nr_score, nr_why = _score_nr_support(cross, nonreportable)

    c_streak = commercial.get("consecutive_weeks_current_direction") or 0
    nc_streak = noncommercial.get("consecutive_weeks_current_direction") or 0
    positioning_core = c_score + nc_score + sp_score + pe_score + ac_score

    # Guards
    if c_streak < 2:
        c_score = min(c_score, 6)
        pe_score = 0
        ac_score = 0
        pr_score = min(pr_score, 4)
    if positioning_core < 25:
        pr_score = min(pr_score, 6)

    # Confirmed band requires true exit/rotate + multi-week + price agreement.
    total = c_score + nc_score + sp_score + pe_score + ac_score + pr_score + nr_score
    can_confirm = (
        _is_true_exit_or_rotate(commercial.get("classification"))
        and c_streak >= 3
        and nc_streak >= 2
        and bool(cross.get("price_agrees_with_commercial"))
    )
    if not can_confirm:
        total = min(total, 59.0)  # max DEVELOPING / EARLY edge before confirm

    # Mid-range / first-attempt watches stay in Watch–Developing, not Early/Confirmed.
    if commercial.get("classification") in ("FIRST_ROTATION_ATTEMPT", "EARLY_ROTATION_WATCH"):
        total = min(total, 49.0)

    total = _clamp(total, 0, 100)
    band = "NO_ROTATION"
    for lo, hi, name in ROTATION_BANDS:
        if lo <= total <= hi:
            band = name
            break

    return {
        "rotation_factor": round(total, 1),
        "classification": band,
        "components": {
            "commercial_transition": round(c_score, 1),
            "non_commercial_response": round(nc_score, 1),
            "spread_change": round(sp_score, 1),
            "persistence": round(pe_score, 1),
            "acceleration": round(ac_score, 1),
            "price_relationship": round(pr_score, 1),
            "non_reportable_support": round(nr_score, 1),
        },
        "component_notes": {
            "commercial_transition": c_why,
            "non_commercial_response": nc_why,
            "spread_change": sp_why,
            "persistence": pe_why,
            "acceleration": ac_why,
            "price_relationship": pr_why,
            "non_reportable_support": nr_why,
        },
        "max_components": {
            "commercial_transition": 25,
            "non_commercial_response": 20,
            "spread_change": 15,
            "persistence": 10,
            "acceleration": 10,
            "price_relationship": 15,
            "non_reportable_support": 5,
        },
        "guards": {
            "one_week_cap_applied": c_streak < 2,
            "price_alone_cap_applied": positioning_core < 25,
            "confirmed_requires_persistence": not can_confirm,
            "can_confirm": can_confirm,
        },
    }


def classify_workflow_stage(
    commercial: dict[str, Any],
    noncommercial: dict[str, Any],
    cross: dict[str, Any],
    pp: dict[str, Any],
    rotation: dict[str, Any],
) -> dict[str, Any]:
    """NORMAL → … → POST_ROTATION workflow stage."""
    rules: list[str] = []
    c_cls = commercial.get("classification")
    c_streak = commercial.get("consecutive_weeks_current_direction") or 0
    rf = rotation.get("rotation_factor") or 0
    cross_cls = cross.get("classification")

    c_pct = _finite(commercial.get("percentile"))
    mid_range = c_pct is not None and 25 < c_pct < 75
    structural = "NORMAL"
    stage = "NORMAL"

    if (
        rotation.get("guards", {}).get("can_confirm")
        and rf >= 80
        and _is_true_exit_or_rotate(c_cls)
    ):
        rules.append("workflow:CONFIRMED_ROTATION")
        stage = "CONFIRMED_ROTATION"
        structural = "CONFIRMED_ROTATION"
    elif mid_range and c_cls == "STABLE" and cross_cls == "OPPOSITION_UNWINDING":
        rules.append("workflow:POST_ROTATION")
        stage = "POST_ROTATION"
        structural = "POST_ROTATION"
    elif cross.get("oppose_extremes") and not _is_true_exit_or_rotate(c_cls):
        # Mature opposition; workflow focus = watch for first coordinated switch
        rules.append("workflow:ROTATION_WATCH (mature opposition)")
        stage = "ROTATION_WATCH"
        structural = "OPPOSITION_MATURE"
    elif c_cls in (
        "EXITING_BULLISH_EXTREME",
        "EXITING_BEARISH_EXTREME",
        "ROTATING_BEARISH",
        "ROTATING_BULLISH",
    ) and c_streak >= 3:
        rules.append("workflow:EARLY_ROTATION")
        stage = "EARLY_ROTATION"
        structural = "EARLY_ROTATION"
    elif c_cls in ("EXITING_BULLISH_EXTREME", "EXITING_BEARISH_EXTREME") and c_streak >= 2:
        rules.append("workflow:EARLY_ROTATION")
        stage = "EARLY_ROTATION"
        structural = "EARLY_ROTATION"
    elif c_cls in ("FIRST_ROTATION_ATTEMPT", "EARLY_ROTATION_WATCH") or cross_cls == "DEVELOPING_ROTATION":
        rules.append("workflow:ROTATION_WATCH")
        stage = "ROTATION_WATCH"
        structural = "DEVELOPING_ROTATION" if cross_cls == "DEVELOPING_ROTATION" else "ROTATION_WATCH"
    elif cross_cls == "OPPOSITION_MATURE":
        rules.append("workflow:OPPOSITION_MATURE")
        stage = "OPPOSITION_MATURE"
        structural = "OPPOSITION_MATURE"
    elif cross_cls == "OPPOSITION_BUILDING" or cross.get("near_oppose_extremes"):
        rules.append("workflow:OPPOSITION_BUILDING")
        stage = "OPPOSITION_BUILDING"
        structural = "OPPOSITION_BUILDING"
    else:
        rules.append("workflow:NORMAL")
        stage = "NORMAL"
        structural = "NORMAL"

    return {
        "workflow_stage": stage,
        "structural_state": structural,
        "rules_fired": rules,
        "sequence": list(WORKFLOW_STAGES),
    }


def select_dominant_story(
    commercial: dict[str, Any],
    noncommercial: dict[str, Any],
    cross: dict[str, Any],
    pp: dict[str, Any],
    rotation: dict[str, Any],
    workflow: dict[str, Any],
) -> dict[str, Any]:
    rules: list[str] = []
    c_cls = commercial["classification"]
    rf = rotation["rotation_factor"]
    pp_cls = pp["classification"]
    stage = workflow.get("workflow_stage")
    structural = workflow.get("structural_state")

    if structural == "OPPOSITION_MATURE" or (
        stage == "ROTATION_WATCH" and cross.get("oppose_extremes")
    ):
        rules.append("story:MATURE_OPPOSITION_ROTATION_WATCH")
        story = "MATURE_OPPOSITION_ROTATION_WATCH"
    elif stage == "CONFIRMED_ROTATION" and pp_cls == "PRICE_CONFIRMING_ROTATION":
        rules.append("story:PRICE_CONFIRMING_POSITIONING_ROTATION")
        story = "PRICE_CONFIRMING_POSITIONING_ROTATION"
    elif stage == "CONFIRMED_ROTATION":
        rules.append("story:COORDINATED_UNWIND")
        story = "COORDINATED_UNWIND"
    elif pp_cls == "PRICE_OPPOSING_ROTATION" and _is_rotation_class(c_cls):
        rules.append("story:PRICE_RESISTING_POSITIONING_ROTATION")
        story = "PRICE_RESISTING_POSITIONING_ROTATION"
    elif pp_cls == "POSITIONING_LEADS_PRICE" and stage in ("EARLY_ROTATION", "ROTATION_WATCH", "CONFIRMED_ROTATION"):
        rules.append("story:POSITIONING_ROTATING_AHEAD_OF_PRICE")
        story = "POSITIONING_ROTATING_AHEAD_OF_PRICE"
    elif stage in ("EARLY_ROTATION", "ROTATION_WATCH") and rf >= 20:
        rules.append("story:EARLY_ROTATION_DEVELOPING")
        story = "EARLY_ROTATION_DEVELOPING"
    elif c_cls in ("DEEPENING_BULLISH", "DEEPENING_BEARISH"):
        rules.append("story:POSITIONING_EXTREME_BUILDING")
        story = "POSITIONING_EXTREME_BUILDING"
    elif cross.get("classification") == "COMMERCIAL_LED_ROTATION":
        rules.append("story:COMMERCIAL_LED_TRANSITION")
        story = "COMMERCIAL_LED_TRANSITION"
    elif cross.get("classification") == "NON_COMMERCIAL_LED_ROTATION":
        rules.append("story:NON_COMMERCIAL_LED_TRANSITION")
        story = "NON_COMMERCIAL_LED_TRANSITION"
    else:
        rules.append("story:MIXED_NO_CLEAR_EDGE")
        story = "MIXED_NO_CLEAR_EDGE"

    phase = {
        "NORMAL": "none",
        "OPPOSITION_BUILDING": "building",
        "OPPOSITION_MATURE": "mature_opposition",
        "ROTATION_WATCH": "watch",
        "EARLY_ROTATION": "early",
        "CONFIRMED_ROTATION": "confirmed",
        "POST_ROTATION": "post",
    }.get(stage, "none")

    return {
        "dominant_story": story,
        "phase": phase,
        "workflow_stage": stage,
        "structural_state": structural,
        "rules_fired": rules,
    }


def build_confirmation_invalidation(
    commercial: dict[str, Any],
    noncommercial: dict[str, Any],
    price: dict[str, Any],
    pp: dict[str, Any],
    story: dict[str, Any],
    workflow: dict[str, Any],
) -> dict[str, Any]:
    c_cls = commercial["classification"]
    stage = workflow.get("workflow_stage")
    confirm: list[str] = []
    invalid: list[str] = []

    if stage in ("OPPOSITION_MATURE", "ROTATION_WATCH") and story.get("dominant_story") == "MATURE_OPPOSITION_ROTATION_WATCH":
        confirm.append("First 2-week Commercial turn away from the extreme")
        confirm.append("Non-commercials begin to unwind their opposing extreme")
        confirm.append("Weekly price stalls or loses structure against the crowded side")
        invalid.append("Commercials deepen further into the extreme")
        invalid.append("Non-commercials keep adding into their extreme with price follow-through")
    elif c_cls == "FIRST_ROTATION_ATTEMPT":
        confirm.append("Second consecutive week in the same exit direction")
        confirm.append("Opposing Non-commercial response begins")
        invalid.append("Next week snaps back and re-deepens the extreme")
    elif c_cls in ("EARLY_ROTATION_WATCH", "EXITING_BULLISH_EXTREME", "EXITING_BEARISH_EXTREME"):
        confirm.append("Third consecutive week of Commercial exit")
        confirm.append("Non-commercial opposing response persists")
        confirm.append("Initial weekly price agreement with the Commercial direction")
        invalid.append("Commercial exit fails and percentile re-enters the extreme")
    elif c_cls in ("ROTATING_BEARISH", "ROTATING_BULLISH"):
        confirm.append("Price confirms via weekly structure break")
        confirm.append("Non-commercial unwind continues")
        invalid.append("Commercials reverse back toward the cycle peak/trough")
    else:
        confirm.append("Multi-week Commercial directional move (≥2 weeks)")
        confirm.append("Clear opposing Non-commercial response")
        invalid.append("Positioning reverts to two-sided noise")

    return {
        "confirmation": confirm,
        "invalidation": invalid,
        "next_development": _next_development(commercial, price, pp, story, workflow),
    }


def _next_development(
    commercial: dict[str, Any],
    price: dict[str, Any],
    pp: dict[str, Any],
    story: dict[str, Any],
    workflow: dict[str, Any],
) -> str:
    stage = workflow.get("workflow_stage")
    if story.get("dominant_story") == "MATURE_OPPOSITION_ROTATION_WATCH":
        return (
            "Opposition is mature. The high-value next event is the first coordinated "
            "positioning switch — Commercials turning for 2+ weeks while Non-commercials "
            "begin to unwind — before price confirmation is required."
        )
    if stage == "ROTATION_WATCH":
        return (
            "This is a rotation watch. A second consecutive Commercial week in the exit "
            "direction would advance the setup into early rotation."
        )
    if stage == "EARLY_ROTATION":
        return (
            "Early rotation is developing. Persistence through a third week plus Non-commercial "
            "response would open a path to confirmation; price agreement would strengthen it."
        )
    if stage == "CONFIRMED_ROTATION":
        return (
            "Rotation is confirmed on positioning criteria. Continued price follow-through "
            "would mature the move; a Commercial snap-back would challenge it."
        )
    return (
        "Monitor whether opposition continues to build or the first multi-week Commercial "
        "exit begins a rotation watch."
    )


def build_market_trajectory_analysis(
    instrument_id: str,
    *,
    weeks: list[dict[str, Any]],
    weekly_ohlc: list[dict[str, Any]] | None,
    report_date: str | None = None,
) -> dict[str, Any]:
    if not weeks:
        return {
            "instrument_id": instrument_id,
            "available": False,
            "reason": "no_inspector_weeks",
        }

    if report_date:
        target = _slice_date(report_date)
        i = next((k for k, w in enumerate(weeks) if _slice_date(w.get("date")) == target), None)
        if i is None:
            i = next(
                (k for k in range(len(weeks) - 1, -1, -1) if _slice_date(weeks[k].get("date")) <= target),
                len(weeks) - 1,
            )
    else:
        i = len(weeks) - 1

    asof = _slice_date(weeks[i].get("date"))
    commercial = build_participant_trajectory(weeks, "commercial", i)
    noncommercial = build_participant_trajectory(weeks, "noncommercial", i)
    nonreportable = build_participant_trajectory(weeks, "nonreportable", i)
    price = build_price_trajectory(weekly_ohlc or [], asof)
    pp = classify_positioning_price_relationship(commercial, noncommercial, price)
    cross = classify_cross_group(
        commercial, noncommercial, nonreportable, weeks, i, price=price
    )
    rotation = compute_rotation_factor(commercial, noncommercial, nonreportable, cross, pp)
    workflow = classify_workflow_stage(commercial, noncommercial, cross, pp, rotation)
    story = select_dominant_story(commercial, noncommercial, cross, pp, rotation, workflow)
    ci = build_confirmation_invalidation(commercial, noncommercial, price, pp, story, workflow)

    rules: list[str] = []
    for block in (commercial, noncommercial, nonreportable, price, pp, cross, workflow, story):
        rules.extend(block.get("rules_fired") or [])

    analysis = {
        "instrument_id": instrument_id,
        "available": True,
        "report_date": asof,
        "prose_enabled": PROSE_ENABLED,
        "engine": ENGINE,
        "version": VERSION,
        "workflow": workflow,
        "participants": {
            "commercial": commercial,
            "non_commercial": noncommercial,
            "non_reportable": nonreportable,
        },
        "price_trajectory": price,
        "positioning_price_relationship": pp,
        "cross_group": cross,
        "rotation_factor": rotation,
        "dominant_story": story,
        "confirmation": ci["confirmation"],
        "invalidation": ci["invalidation"],
        "next_development": ci["next_development"],
        "rules_fired": rules,
    }
    if PROSE_ENABLED:
        analysis["prose"] = render_trajectory_prose(analysis)
    return analysis


def _humanize(code: str | None) -> str:
    if not code:
        return "Unavailable"
    return str(code).replace("_", " ").strip().title()


STORY_NARRATIVES = {
    "MATURE_OPPOSITION_ROTATION_WATCH": (
        "Commercials and Non-commercials sit at opposing extremes. Price is still "
        "travelling with the crowded side. The high-value event is the first coordinated "
        "positioning switch — not trend continuation."
    ),
    "POSITIONING_ROTATING_AHEAD_OF_PRICE": (
        "Positioning is rotating while price has not yet confirmed. This is a positioning-led "
        "development: persistence and opposing-group response matter more than a single print."
    ),
    "PRICE_CONFIRMING_POSITIONING_ROTATION": (
        "Weekly price is beginning to agree with the Commercial rotation. Confirmation still "
        "depends on multi-week positioning persistence, not price alone."
    ),
    "PRICE_RESISTING_POSITIONING_ROTATION": (
        "Commercials are attempting to rotate, but weekly price is still opposing that move. "
        "Treat this as early / developing until positioning persists."
    ),
    "POSITIONING_EXTREME_BUILDING": (
        "A participant group is still deepening into an extreme. Opposition may be building, "
        "but a rotation watch has not yet begun."
    ),
    "EARLY_ROTATION_DEVELOPING": (
        "An early rotation is developing. The setup remains a watch until multi-week "
        "Commercial exit, Non-commercial response, and preferably initial price agreement align."
    ),
    "COMMERCIAL_LED_TRANSITION": (
        "Commercials are leading the transition. Watch whether Non-commercials respond and "
        "whether the move persists beyond a single week."
    ),
    "NON_COMMERCIAL_LED_TRANSITION": (
        "Non-commercials are leading the transition. Commercial confirmation would raise "
        "conviction; without it this remains mixed."
    ),
    "COORDINATED_UNWIND": (
        "Positioning shows a coordinated unwind underway. Continue to test persistence and "
        "price follow-through against invalidation."
    ),
    "MIXED_NO_CLEAR_EDGE": (
        "Participant paths are mixed. No clear high-probability rotation edge is resolved "
        "from the current trajectory."
    ),
}


def _pct_phrase(pct: float | None) -> str:
    if pct is None:
        return "n/a"
    return f"{pct:.1f}th percentile"


def _group_trajectory_line(label: str, g: dict[str, Any]) -> str:
    cls = g.get("classification") or "STABLE"
    streak = g.get("consecutive_weeks_current_direction") or 0
    pct = _finite(g.get("percentile"))
    sign = g.get("direction_sign") or 0
    direction = "rising" if sign > 0 else "falling" if sign < 0 else "flat"
    streak_bit = f", {streak}W {direction}" if streak else ""
    return f"{label}: {_humanize(cls)} at {_pct_phrase(pct)}{streak_bit}."


def render_trajectory_prose(analysis: dict[str, Any]) -> dict[str, Any]:
    """Build Weekly Analysis section prose from trajectory classifications only."""
    story = analysis.get("dominant_story") or {}
    workflow = analysis.get("workflow") or {}
    parts = analysis.get("participants") or {}
    c = parts.get("commercial") or {}
    nc = parts.get("non_commercial") or {}
    nr = parts.get("non_reportable") or {}
    price = analysis.get("price_trajectory") or {}
    pp = analysis.get("positioning_price_relationship") or {}
    cross = analysis.get("cross_group") or {}
    rf = analysis.get("rotation_factor") or {}
    story_code = story.get("dominant_story") or "MIXED_NO_CLEAR_EDGE"
    stage = workflow.get("workflow_stage") or "NORMAL"
    structural = workflow.get("structural_state") or stage

    dominant_narrative = STORY_NARRATIVES.get(
        story_code,
        f"Dominant story: {_humanize(story_code)}.",
    )
    if analysis.get("next_development"):
        dominant_narrative = f"{dominant_narrative} {analysis['next_development']}"

    workflow_narrative = (
        f"Workflow state is {_humanize(stage)} "
        f"(structural: {_humanize(structural)}). "
        f"Sequence: {' → '.join(_humanize(s) for s in (workflow.get('sequence') or WORKFLOW_STAGES))}."
    )

    positioning_lines = [
        _group_trajectory_line("Commercials", c),
        _group_trajectory_line("Non-commercials", nc),
        _group_trajectory_line("Non-reportables", nr),
        f"Cross-group: {_humanize(cross.get('classification'))}"
        + (" (crowded opposition)" if cross.get("crowded") else "")
        + ".",
    ]

    price_cls = price.get("classification") or "RANGE_BOUND"
    pp_cls = pp.get("classification") or "NO_CLEAR_RELATIONSHIP"
    r1 = _finite(price.get("return_1w_pct"))
    r4 = _finite(price.get("return_4w_pct"))
    ret_bits = []
    if r1 is not None:
        ret_bits.append(f"1W {r1:+.1f}%")
    if r4 is not None:
        ret_bits.append(f"4W {r4:+.1f}%")
    price_narrative = (
        f"Price trajectory: {_humanize(price_cls)}"
        + (f" ({', '.join(ret_bits)})" if ret_bits else "")
        + f". Positioning–price relationship: {_humanize(pp_cls)}."
    )

    rf_val = rf.get("rotation_factor")
    rf_band = rf.get("classification") or "NO_ROTATION"
    comps = rf.get("components") or {}
    top_comps = sorted(
        ((k, v) for k, v in comps.items() if isinstance(v, (int, float))),
        key=lambda kv: abs(kv[1]),
        reverse=True,
    )[:3]
    comp_bit = (
        "; leading components: "
        + ", ".join(f"{_humanize(k)} {v:.0f}" for k, v in top_comps)
        if top_comps
        else ""
    )
    rotation_narrative = (
        f"Rotation Factor {rf_val if rf_val is not None else 'n/a'} "
        f"→ {_humanize(rf_band)}{comp_bit}."
    )
    if rf.get("guards", {}).get("one_week_cap_applied"):
        rotation_narrative += " One-week turns are capped below confirmation."
    if not rf.get("guards", {}).get("can_confirm", False):
        rotation_narrative += " Confirmed rotation is not yet unlocked."

    return {
        "dominant_story": {
            "code": story_code,
            "label": _humanize(story_code),
            "narrative": dominant_narrative,
        },
        "workflow_state": {
            "stage": stage,
            "structural_state": structural,
            "label": _humanize(stage),
            "sequence": list(workflow.get("sequence") or WORKFLOW_STAGES),
            "narrative": workflow_narrative,
        },
        "positioning_trajectory": {
            "lines": positioning_lines,
            "narrative": " ".join(positioning_lines),
            "commercial_class": c.get("classification"),
            "noncommercial_class": nc.get("classification"),
            "nonreportable_class": nr.get("classification"),
            "cross_class": cross.get("classification"),
        },
        "price_relationship": {
            "price_class": price_cls,
            "relationship_class": pp_cls,
            "narrative": price_narrative,
        },
        "rotation_factor": {
            "rotation_factor": rf_val,
            "band": rf_band,
            "label": _humanize(rf_band),
            "components": comps,
            "narrative": rotation_narrative,
            "guards": rf.get("guards") or {},
        },
        "confirmation": list(analysis.get("confirmation") or []),
        "invalidation": list(analysis.get("invalidation") or []),
        "next_development": analysis.get("next_development") or "",
    }


def _historical_context_from_research(research_block: dict[str, Any] | None) -> dict[str, Any]:
    """Prefer research analogues when present; otherwise trajectory placeholder."""
    research = research_block if research_block and research_block.get("available", True) else {}
    analogues = research.get("current_analogues") or {}
    if not analogues.get("independent_case_count"):
        interp = research.get("current_interpretation") or {}
        nested = interp.get("analogues") or {}
        if nested:
            analogues = {
                "independent_case_count": nested.get("independent_cases")
                or nested.get("independent_case_count")
                or 0,
                "sample_quality": nested.get("sample_quality"),
                "directional_tendency": nested.get("directional_tendency")
                or interp.get("interpretation"),
            }
    n = int(analogues.get("independent_case_count") or 0)
    if n > 0:
        tendency = analogues.get("directional_tendency") or "no clear directional tendency"
        quality = analogues.get("sample_quality") or "unrated"
        return {
            "summary": (
                f"Historical analogues ({n} independent cases, sample {quality}): {tendency}."
            ),
            "outcomes_note": (
                "Analogue outcomes are descriptive context only — they do not confirm the "
                "current trajectory stage."
            ),
            "independent_case_count": n,
            "sample_quality": quality,
            "source": "positioning_research",
        }
    return {
        "summary": (
            "Historical analogue pack is not available for this print. Trajectory "
            "classifications stand on current-cycle positioning and price relationship alone."
        ),
        "outcomes_note": "Placeholder — trajectory engine does not invent analogue history.",
        "independent_case_count": 0,
        "sample_quality": "unavailable",
        "source": "trajectory_placeholder",
    }


def to_weekly_analysis_ui_block(
    analysis: dict[str, Any],
    *,
    research_block: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Map trajectory analysis → Weekly Analysis panel payload (no legacy templates)."""
    if not analysis.get("available"):
        return {
            "instrument_id": analysis.get("instrument_id"),
            "available": False,
            "reason": analysis.get("reason") or "trajectory_unavailable",
            "title": "Weekly Analysis",
            "engine": ENGINE,
            "version": VERSION,
            "prose_enabled": PROSE_ENABLED,
            "disclaimer": DISCLAIMER,
        }

    prose = analysis.get("prose") or render_trajectory_prose(analysis)
    hist = _historical_context_from_research(research_block)
    summary = prose["dominant_story"]["narrative"]

    return {
        "instrument_id": analysis.get("instrument_id"),
        "available": True,
        "title": "Weekly Analysis",
        "engine": ENGINE,
        "version": VERSION,
        "prose_enabled": True,
        "source_week": analysis.get("report_date"),
        "disclaimer": DISCLAIMER,
        "summary": summary,
        "dominant_story": prose["dominant_story"],
        "workflow_state": prose["workflow_state"],
        "positioning_trajectory": prose["positioning_trajectory"],
        "price_relationship": prose["price_relationship"],
        "rotation_factor": {
            **(analysis.get("rotation_factor") or {}),
            **prose["rotation_factor"],
        },
        "confirmation": prose["confirmation"],
        "invalidation": prose["invalidation"],
        "next_development": prose["next_development"],
        "historical_context": hist,
        # Structured trajectory retained for inspection / future UI
        "trajectory": {
            "workflow": analysis.get("workflow"),
            "participants": analysis.get("participants"),
            "price_trajectory": analysis.get("price_trajectory"),
            "positioning_price_relationship": analysis.get("positioning_price_relationship"),
            "cross_group": analysis.get("cross_group"),
            "dominant_story": analysis.get("dominant_story"),
            "rules_fired": analysis.get("rules_fired"),
        },
        "sources": ["weekly_inspector", "workstation_ohlc", "positioning_research"],
    }


def build_trajectory_weekly_analysis(
    *,
    weekly_inspector: dict[str, Any],
    workstation_ohlc: dict[str, Any] | None = None,
    positioning_research: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build full Weekly Analysis document from the trajectory engine only."""
    from hptl.cot.weekly_inspector_export import expand_compact_market

    wi_markets = weekly_inspector.get("markets") or {}
    research_markets = (positioning_research or {}).get("markets") or {}
    ohlc_instruments = (workstation_ohlc or {}).get("instruments") or {}
    markets: dict[str, Any] = {}

    for mid, block in wi_markets.items():
        if not block or not block.get("available", True):
            markets[mid] = to_weekly_analysis_ui_block(
                {
                    "instrument_id": mid,
                    "available": False,
                    "reason": "weekly_inspector unavailable",
                }
            )
            continue
        expanded = expand_compact_market(block) if block.get("rows") else block
        weeks = expanded.get("weeks") or []
        bars = (ohlc_instruments.get(mid) or {}).get("weekly_ohlc") or []
        analysis = build_market_trajectory_analysis(
            mid,
            weeks=weeks,
            weekly_ohlc=bars,
        )
        markets[mid] = to_weekly_analysis_ui_block(
            analysis,
            research_block=research_markets.get(mid),
        )

    available = sum(1 for m in markets.values() if m.get("available"))
    return {
        "version": VERSION,
        "engine": ENGINE,
        "prose_enabled": PROSE_ENABLED,
        "title": "Weekly Analysis",
        "markets": markets,
        "summary": {
            "markets_total": len(markets),
            "markets_available": available,
        },
        "disclaimer": DISCLAIMER,
    }
