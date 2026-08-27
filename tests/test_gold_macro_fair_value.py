"""Tests for Gold Macro Fair Value V2 (research only)."""

from __future__ import annotations

import math

from hptl.valuation.gold_focused_macro_valuation import CB_PUBLICATION_LAG_DAYS
from hptl.valuation.gold_macro_fair_value import (
    FEATURE_ORDER,
    MODEL_ID,
    PUBLISHED_GOLD_MODEL_ID,
    _dollar_contributions,
    _walk_forward_engine,
    render_markdown,
)
from hptl.valuation.gold_structural_valuation_research import _asof_with_lag
from hptl.valuation.metals_valuation_v1 import MODEL_ID as LIVE_ID


def test_production_safety_constants():
    assert PUBLISHED_GOLD_MODEL_ID == "metals_real_yield_v1"
    assert LIVE_ID == "metals_real_yield_v1"
    assert MODEL_ID == "gold_macro_fair_value_v2"
    assert FEATURE_ORDER == ["dxy", "real10y", "us2y", "us30y", "inflation", "cb_demand"]


def test_publication_lag_carry_forward():
    daily = {"2022-03-31": 100.0, "2022-06-30": 200.0}
    dates = ["2022-04-15", "2022-07-01", "2022-10-01"]
    lagged = _asof_with_lag(daily, dates, lag_days=CB_PUBLICATION_LAG_DAYS)
    assert lagged[0] is None
    assert lagged[-1] == 200.0


def test_contribution_reconcilies_to_fair_minus_base():
    names = list(FEATURE_ORDER)
    slopes = [-0.05, -0.04, -0.03, 0.01, 0.02, 0.03]
    feats = [1.0, 0.5, 0.2, -0.1, 0.8, 1.2]
    alpha = math.log(3000.0)
    out = _dollar_contributions(
        alpha=alpha, slopes=slopes, names=names, feats=feats, spot=3100.0
    )
    assert out["fair_value"] > 0
    net = sum(d["dollar_contribution"] for d in out["drivers"])
    assert abs(net - out["net_macro_effect_usd"]) < 0.15
    assert abs(out["fair_value"] - out["base_fair_value"] - out["net_macro_effect_usd"]) < 0.2


def test_expanding_estimation_is_causal_and_deterministic():
    n = 220
    dates = [f"2020-01-{(i % 28) + 1:02d}" for i in range(n)]
    # Fix dates properly
    from datetime import date, timedelta

    d0 = date(2018, 1, 5)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(n)]
    prices = [1200.0 + i for i in range(n)]
    y = [math.log(p) for p in prices]
    # Synthetic features: higher dxy lowers residual gold
    dxy = [((i % 40) - 20) / 10.0 for i in range(n)]
    real = [((i % 30) - 15) / 10.0 for i in range(n)]
    us2 = [((i % 25) - 12) / 10.0 for i in range(n)]
    us30 = [0.0 for _ in range(n)]
    infl = [((i % 20) - 5) / 10.0 for i in range(n)]
    cb = [max(0.0, (i % 15) / 10.0) for i in range(n)]
    cols = [dxy, real, us2, us30, infl, cb]
    eng1 = _walk_forward_engine(
        dates, prices, y, cols, list(FEATURE_ORDER), min_train=104, step=13
    )
    eng2 = _walk_forward_engine(
        dates, prices, y, cols, list(FEATURE_ORDER), min_train=104, step=13
    )
    assert eng1["history"]
    assert eng1["history"][-1]["fair_value"] == eng2["history"][-1]["fair_value"]
    # Train uses only past: first fair index >= min_train
    first_i = next(i for i, fl in enumerate(eng1["fair_logs"]) if fl is not None)
    assert first_i >= 104


def test_coefficient_stability_structure():
    n = 200
    from datetime import date, timedelta

    d0 = date(2019, 1, 4)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(n)]
    prices = [1500.0 + 0.5 * i for i in range(n)]
    y = [math.log(p) for p in prices]
    cols = [[0.01 * math.sin(i / 7.0 + k) for i in range(n)] for k in range(6)]
    eng = _walk_forward_engine(
        dates, prices, y, cols, list(FEATURE_ORDER), min_train=100, step=13
    )
    stab = eng["stability"]
    assert "dxy" in stab
    assert "expected_sign_share" in stab["dxy"]


def test_markdown_research_only():
    md = render_markdown(
        {
            "generated_at": "t",
            "model_id": MODEL_ID,
            "verdict": {"verdict": "USEFUL_BUT_RESEARCH", "narrative": "n"},
            "equation": "e",
            "panel": {"aligned_n": 1, "aligned_start": "a", "aligned_end": "b", "cb": {}},
            "tip": {
                "drivers_usd": {"DXY": -1.0},
                "net_macro_effect_usd": 1.0,
                "fair_value": 100.0,
                "market_price": 99.0,
                "premium_discount": "Discount",
                "deviation_pct": -1.0,
                "bucket": "near_fair_value",
                "coefficients": {},
            },
            "oos": {},
            "stability": {},
            "spread_13w": {},
            "spread_52w": {},
            "forward_returns": [],
            "charts": [],
            "runtime_sec": 0.1,
        }
    )
    assert "Research only" in md
    assert "Fair Value" in md
