"""Phase 5A price-anchored discovery tests."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np

from hptl.cot.intelligence_phase5a.config import (
    COOLDOWN_BY_HORIZON,
    HORIZONS_WEEKS,
    RALLY_PERCENTILE,
    SELLOFF_PERCENTILE,
    frozen_definitions_payload,
)
from hptl.cot.intelligence_phase5a.features import build_market_panel, extract_case_features
from hptl.cot.intelligence_phase5a.moves import detect_market_moves


def _synthetic_series(n: int = 200):
    """Series with embedded large rallies / selloffs and drifting COT."""
    start = date(2018, 1, 2)
    rows = []
    price = 100.0
    c_net = 0.0
    nc_net = 0.0
    nr_net = 0.0
    for i in range(n):
        # inject a sharp rally around week 120 and selloff around 160
        shock = 0.0
        if 120 <= i < 128:
            shock = 0.04
        elif 160 <= i < 168:
            shock = -0.04
        else:
            shock = ((i % 11) - 5) * 0.002
        price *= 1.0 + shock
        # COT drifts ahead of moves
        if 108 <= i < 120:
            c_net -= 2000
            nc_net += 1500
        if 148 <= i < 160:
            c_net += 2000
            nc_net -= 1500
        c_net += ((i % 5) - 2) * 100
        nc_net += ((i % 7) - 3) * 80
        nr_net = -(c_net + nc_net) * 0.3
        rows.append(
            {
                "date": (start + timedelta(weeks=i)).isoformat(),
                "price": price,
                "commercial_net": c_net,
                "institutional_net": nc_net,
                "retail_net": nr_net,
            }
        )
    return rows


def test_frozen_definitions_explicit():
    cfg = frozen_definitions_payload()
    assert cfg["not_validated"] is True
    assert cfg["not_for_live_alerts"] is True
    assert set(cfg["price_move_detection"]["horizons_weeks"]) == set(HORIZONS_WEEKS)
    cool = cfg["price_move_detection"]["independence"]["cooldown_weeks_by_horizon"]
    assert cool[8] == COOLDOWN_BY_HORIZON[8]
    assert "silhouette" in cfg["clustering"]["k_selection"]


def test_market_specific_thresholds_and_independence():
    series = _synthetic_series()
    dates = [r["date"] for r in series]
    prices = [r["price"] for r in series]
    moves = detect_market_moves("Gold", "metals", dates, prices)
    assert moves, "expected some detected moves"
    indep = [m for m in moves if m["independent"]]
    assert indep
    # thresholds differ by horizon
    thr = {(m["horizon_weeks"], m["direction"]): m["price_threshold_pct"] for m in moves}
    assert len(thr) >= 2
    # independence cooldown respected
    for h in HORIZONS_WEEKS:
        for d in ("rally", "selloff"):
            idxs = sorted(
                m["onset_index"]
                for m in indep
                if m["horizon_weeks"] == h and m["direction"] == d
            )
            for a, b in zip(idxs, idxs[1:]):
                assert b - a >= COOLDOWN_BY_HORIZON[h]


def test_features_are_pit_safe_and_sequences_exist():
    series = _synthetic_series()
    panel = build_market_panel(series)
    move = {
        "market": "Gold",
        "asset_class": "metals",
        "onset_index": 120,
        "onset_date": series[120]["date"],
        "horizon_weeks": 8,
        "direction": "rally",
        "forward_return_pct": 10.0,
        "mfe_pct": 12.0,
        "mae_pct": -2.0,
        "independent": True,
    }
    feat, seq = extract_case_features(panel, move)
    assert feat["c_pct"] is not None or feat["c_net"] is not None
    assert "commercial_sequence" in seq
    assert "cross_sequence" in seq
    # no look-ahead keys
    assert "future" not in str(feat).lower()


def test_percentile_constants():
    assert RALLY_PERCENTILE == 90.0
    assert SELLOFF_PERCENTILE == 10.0
