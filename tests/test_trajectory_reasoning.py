"""Trajectory reasoning — methodology-aligned classifications (no prose)."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.cot.trajectory_reasoning import build_market_trajectory_analysis


def _week(date_s: str, c_pct: float, nc_pct: float, c_net: float = 0.0, nc_net: float = 0.0):
    return {
        "date": date_s,
        "commercial": {"net": c_net, "percentile": c_pct},
        "noncommercial": {"net": nc_net, "percentile": nc_pct},
        "nonreportable": {"net": 0.0, "percentile": 50.0},
        "cross": {
            "comm_nc_spread": c_pct - nc_pct,
            "comm_nc_spread_change_4w": 0.0,
        },
    }


def test_one_week_turn_is_first_attempt_not_exiting():
    d0 = date(2026, 1, 6)
    c_path = [80, 85, 90, 92, 94, 95, 96, 97, 97, 97, 97, 97, 95]
    nc_path = [20, 15, 10, 8, 6, 5, 4, 3, 3, 3, 3, 3, 5]
    weeks = [
        _week((d0 + timedelta(weeks=i)).isoformat(), c, nc)
        for i, (c, nc) in enumerate(zip(c_path, nc_path))
    ]
    out = build_market_trajectory_analysis("OneWeek", weeks=weeks, weekly_ohlc=[])
    assert out["participants"]["commercial"]["classification"] == "FIRST_ROTATION_ATTEMPT"
    assert out["workflow"]["workflow_stage"] == "ROTATION_WATCH"
    assert out["rotation_factor"]["rotation_factor"] < 60


def test_example_a_multiweek_exit_positioning_leads():
    d0 = date(2026, 4, 7)
    c_path = [50, 60, 70, 80, 90, 97, 95, 90, 85, 80, 76, 74, 72]
    nc_path = [50, 45, 40, 35, 30, 25, 28, 32, 36, 40, 44, 46, 48]
    weeks = [
        _week((d0 + timedelta(weeks=i)).isoformat(), c, nc, 10000 - i * 200, -8000 + i * 150)
        for i, (c, nc) in enumerate(zip(c_path, nc_path))
    ]
    closes = [
        {"date": (d0 + timedelta(weeks=i)).isoformat(), "close": 100.0 + min(i, 8) * 0.5}
        for i in range(len(weeks))
    ]
    out = build_market_trajectory_analysis("Synthetic A", weeks=weeks, weekly_ohlc=closes)
    c = out["participants"]["commercial"]
    assert c["classification"] in (
        "EXITING_BULLISH_EXTREME",
        "ROTATING_BEARISH",
        "EARLY_ROTATION_WATCH",
    )
    assert out["rotation_factor"]["classification"] != "CONFIRMED_ROTATION" or out[
        "rotation_factor"
    ]["guards"]["can_confirm"]
    # Should not be capped as first-attempt
    assert c["classification"] != "FIRST_ROTATION_ATTEMPT"


def test_mature_opposition_is_rotation_watch_not_crowded_continuation():
    d0 = date(2026, 1, 6)
    # C deep short extreme, NC deep long extreme, stable-ish deepening
    weeks = []
    for i in range(13):
        c = max(2.0, 8.0 - i * 0.4)
        nc = min(98.0, 90.0 + i * 0.5)
        weeks.append(_week((d0 + timedelta(weeks=i)).isoformat(), c, nc, -50000, 60000))
    closes = [
        {"date": (d0 + timedelta(weeks=i)).isoformat(), "close": 100.0 + i * 1.2}
        for i in range(13)
    ]
    out = build_market_trajectory_analysis("CopperLike", weeks=weeks, weekly_ohlc=closes)
    assert out["workflow"]["structural_state"] == "OPPOSITION_MATURE"
    assert out["workflow"]["workflow_stage"] == "ROTATION_WATCH"
    assert out["dominant_story"]["dominant_story"] == "MATURE_OPPOSITION_ROTATION_WATCH"


def test_price_alone_cannot_confirm():
    d0 = date(2026, 1, 6)
    weeks = [_week((d0 + timedelta(weeks=i)).isoformat(), 92.0 - (0.5 if i == 12 else 0), 8.0) for i in range(13)]
    # last week tiny turn
    weeks[-1]["commercial"]["percentile"] = 91.0
    closes = [
        {"date": (d0 + timedelta(weeks=i)).isoformat(), "close": 50.0 - i * 1.5}
        for i in range(13)
    ]
    out = build_market_trajectory_analysis("NGLike", weeks=weeks, weekly_ohlc=closes)
    assert out["participants"]["commercial"]["classification"] == "FIRST_ROTATION_ATTEMPT"
    assert out["rotation_factor"]["classification"] in (
        "NO_ROTATION",
        "ROTATION_WATCH",
        "DEVELOPING_ROTATION",
    )
    assert out["rotation_factor"]["rotation_factor"] < 60


def test_stale_bull_peak_superseded_is_watch_not_coordinated():
    """Soybeans-like: old mild bull peak, later bear trough, mid-range roll → Watch/Developing."""
    d0 = date(2025, 8, 5)
    # ~50 weeks: mild bull peak early, deep trough mid, bounce then 3W decline mid-range
    c_path = []
    nc_path = []
    for i in range(51):
        if i == 0:
            c, nc = 77.0, 25.0
        elif i < 40:
            # grind down into bear trough
            t = i / 40.0
            c = 77.0 - t * 69.0  # → ~8
            nc = 25.0 + t * 65.0
        elif i < 48:
            # bounce
            t = (i - 40) / 8.0
            c = 8.0 + t * 30.0  # → ~38
            nc = 90.0 - t * 20.0
        else:
            # 3-week decline from bounce high
            t = i - 47
            c = 38.0 - t * 7.0  # 31, 24, 17
            nc = 70.0 + t * 3.0
        c_path.append(c)
        nc_path.append(nc)
    weeks = [
        _week((d0 + timedelta(weeks=i)).isoformat(), c, nc)
        for i, (c, nc) in enumerate(zip(c_path, nc_path))
    ]
    closes = [
        {"date": (d0 + timedelta(weeks=i)).isoformat(), "close": 100.0 + i * 0.3}
        for i in range(len(weeks))
    ]
    out = build_market_trajectory_analysis("SoyLike", weeks=weeks, weekly_ohlc=closes)
    c_cls = out["participants"]["commercial"]["classification"]
    assert c_cls not in ("ROTATING_BEARISH", "EXITING_BULLISH_EXTREME")
    assert out["cross_group"]["classification"] != "COORDINATED_ROTATION"
    assert out["rotation_factor"]["rotation_factor"] < 60
    assert out["rotation_factor"]["classification"] in (
        "NO_ROTATION",
        "ROTATION_WATCH",
        "DEVELOPING_ROTATION",
    )
    assert out["workflow"]["workflow_stage"] in ("ROTATION_WATCH", "OPPOSITION_BUILDING", "NORMAL")
