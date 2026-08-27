"""Recurring seasonal turning-window detection."""

from __future__ import annotations

from typing import Any

from hptl.seasonality_workstation.models import TURN_FOLLOW_WEEKS, TURN_HALF_WINDOW_WEEKS
from hptl.seasonality_workstation.stats import bucket_stats


def _week_return(week_stats: dict[int, dict[str, Any]], w: int) -> float:
    st = week_stats.get(w) or {}
    r = st.get("trimmed_mean")
    if r is None:
        r = st.get("median")
    if r is None:
        r = st.get("mean")
    return float(r or 0.0)


def detect_turning_windows(
    week_stats: dict[int, dict[str, Any]],
    rows: list[dict[str, Any]],
    years: list[int],
) -> list[dict[str, Any]]:
    """Find recurring seasonal turns from the cumulative seasonal path.

    A turn is a local extremum on the compounded trimmed-mean path, confirmed by
    multi-year forward-return consistency — not a single-day spike.
    """
    # Build cumulative path
    idx = 100.0
    path: dict[int, float] = {}
    for w in range(1, 53):
        idx *= 1.0 + _week_return(week_stats, w)
        path[w] = idx

    candidates: list[tuple[int, str]] = []
    for w in range(3, 51):
        left = path[w - 1]
        mid = path[w]
        right = path[w + 1]
        if mid >= left and mid >= right and mid > path.get(w - 2, mid) and mid > path.get(w + 2, mid):
            candidates.append((w, "BEARISH_TURN"))  # seasonal peak → expect weakness
        if mid <= left and mid <= right and mid < path.get(w - 2, mid) and mid < path.get(w + 2, mid):
            candidates.append((w, "BULLISH_TURN"))  # seasonal trough → expect strength

    # Per-year cumulative forward returns from each candidate week
    by_year: dict[int, dict[int, float]] = {}
    for r in rows:
        y = r["iso_year"]
        if y not in years or r.get("return") is None:
            continue
        by_year.setdefault(y, {})[int(r["iso_week"])] = float(r["return"])

    turns: list[dict[str, Any]] = []
    for week, kind in candidates:
        forwards: list[float] = []
        for y, wr in by_year.items():
            c = 1.0
            ok = True
            for w in range(week + 1, min(52, week + TURN_FOLLOW_WEEKS) + 1):
                if w not in wr:
                    ok = False
                    break
                c *= 1.0 + wr[w]
            if ok:
                forwards.append(c - 1.0)
        st = bucket_stats(forwards)
        if st["n"] < 5:
            continue
        mean = st["mean"] or 0.0
        # Direction should match turn kind
        if kind == "BULLISH_TURN" and mean <= 0:
            continue
        if kind == "BEARISH_TURN" and mean >= 0:
            continue
        hit = st["positive_frequency"] if kind == "BULLISH_TURN" else (
            1.0 - (st["positive_frequency"] or 0.0)
        )
        disp = st["dispersion"] or 0.0
        conf = "HIGH" if (st["n"] >= 12 and (hit or 0) >= 0.65 and disp < 0.08) else (
            "MEDIUM" if (st["n"] >= 8 and (hit or 0) >= 0.55) else "LOW"
        )
        start = max(1, week - TURN_HALF_WINDOW_WEEKS)
        end = min(52, week + TURN_HALF_WINDOW_WEEKS)
        turns.append(
            {
                "kind": kind,
                "center_week": week,
                "window": {
                    "start_week": start,
                    "end_week": end,
                    "label": f"ISO weeks {start}–{end}",
                },
                "historical_consistency": round(hit or 0.0, 3),
                "average_follow_return_pct": round(mean * 100, 2),
                "follow_horizon_weeks": TURN_FOLLOW_WEEKS,
                "hit_rate": round(hit or 0.0, 3),
                "confidence": conf,
                "dispersion": round(disp, 4),
                "sample_years": st["n"],
            }
        )

    # Prefer stronger consistency; keep top windows
    turns.sort(key=lambda t: (-(t["hit_rate"] or 0), -(t["sample_years"] or 0)))
    # De-duplicate overlapping windows of same kind
    kept: list[dict[str, Any]] = []
    for t in turns:
        overlap = False
        for k in kept:
            if t["kind"] != k["kind"]:
                continue
            if abs(t["center_week"] - k["center_week"]) <= TURN_HALF_WINDOW_WEEKS + 1:
                overlap = True
                break
        if not overlap:
            kept.append(t)
    return kept[:8]
