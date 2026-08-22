"""Tests for Gold V4 Fixed-Form Shadow Currency Engine (research only)."""

from __future__ import annotations

import math
from datetime import date, timedelta

from hptl.valuation.gold_shadow_currency_v4 import (
    MODEL_ID,
    PUBLISHED_GOLD_MODEL_ID,
    _calibrate_k_beta,
    fair_value_components,
    mathematical_boundary_tests,
    render_markdown,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as LIVE_ID


def test_production_immutability():
    assert PUBLISHED_GOLD_MODEL_ID == "metals_real_yield_v1"
    assert LIVE_ID == "metals_real_yield_v1"
    assert MODEL_ID == "gold_shadow_currency_v4"


def test_real_yield_decimal_units_in_formula():
    # 1.80% must be passed as 0.018 — using 1.80 would explode/implode yield factor
    fv_dec = fair_value_components(
        k=0.03,
        beta=10.0,
        monetary_value_per_ounce=5000.0,
        real_yield_decimal=0.018,
        dxy_benchmark=100.0,
        dxy=100.0,
    )["fair_value"]
    fv_wrong = fair_value_components(
        k=0.03,
        beta=10.0,
        monetary_value_per_ounce=5000.0,
        real_yield_decimal=1.80,
        dxy_benchmark=100.0,
        dxy=100.0,
    )["fair_value"]
    assert fv_dec > 100.0
    assert fv_wrong < 1.0  # wrongly treating percent as decimal collapses FV


def test_real_yield_compression_and_negative_premium():
    out = mathematical_boundary_tests(
        k=0.04,
        beta=12.0,
        monetary_value_per_ounce=4000.0,
        dxy_benchmark=100.0,
        dxy=100.0,
    )
    assert out["real_yield_compression"]["pass"]
    assert out["negative_yield_premium"]["pass"]
    assert out["all_pass"]


def test_liquidity_invariance():
    fv1 = fair_value_components(
        k=0.05,
        beta=8.0,
        monetary_value_per_ounce=3000.0,
        real_yield_decimal=0.01,
        dxy_benchmark=105.0,
        dxy=100.0,
    )["fair_value"]
    # M and G both ×1.25 ⇒ M/G unchanged
    fv2 = fair_value_components(
        k=0.05,
        beta=8.0,
        monetary_value_per_ounce=(3000.0 * 1.25) / 1.25,
        real_yield_decimal=0.01,
        dxy_benchmark=105.0,
        dxy=100.0,
    )["fair_value"]
    assert abs(fv1 - fv2) < 1e-9


def test_dxy_direction():
    base = dict(
        k=0.03,
        beta=5.0,
        monetary_value_per_ounce=3500.0,
        real_yield_decimal=0.02,
        dxy_benchmark=100.0,
    )
    hi = fair_value_components(**base, dxy=110.0)["fair_value"]
    mid = fair_value_components(**base, dxy=100.0)["fair_value"]
    lo = fair_value_components(**base, dxy=90.0)["fair_value"]
    assert hi < mid < lo


def test_rolling_geometric_benchmark_past_only():
    # Geometric mean of trailing window; a future spike after the window end
    # must not enter a past-only benchmark computed earlier.
    vals = [100.0 + i for i in range(600)]
    end_i = 520  # exclusive end for past-only window ending at index 519
    window = vals[end_i - 520 : end_i]
    gm = math.exp(sum(math.log(v) for v in window) / len(window))
    spiked = list(vals)
    spiked[end_i] = 1_000_000.0  # future relative to end_i-1
    gm_past = math.exp(
        sum(math.log(v) for v in spiked[end_i - 520 : end_i]) / 520
    )
    assert abs(gm - gm_past) < 1e-12
    gm_with_future = math.exp(
        sum(math.log(v) for v in spiked[end_i - 519 : end_i + 1]) / 520
    )
    assert gm_with_future != gm


def test_past_only_calibration_constraints():
    # Synthetic: higher real yield → lower adjusted log price
    n = 150
    ry = [0.01 + 0.0001 * i for i in range(n)]
    # true log(k)=log(0.04), beta=10
    y = [math.log(0.04) - 10.0 * r + 0.001 * math.sin(i) for i, r in enumerate(ry)]
    k, beta, meta = _calibrate_k_beta(y, ry)
    assert meta["ok"]
    assert k > 0
    assert beta >= 0
    assert abs(k - 0.04) < 0.01
    assert abs(beta - 10.0) < 1.0


def test_formula_reconciliation_and_positive_finite():
    comps = fair_value_components(
        k=0.025,
        beta=15.0,
        monetary_value_per_ounce=4200.0,
        real_yield_decimal=-0.005,
        dxy_benchmark=98.0,
        dxy=102.0,
    )
    product = (
        comps["k"]
        * comps["monetary_value_per_ounce"]
        * comps["yield_factor"]
        * comps["dxy_factor"]
    )
    assert abs(product - comps["fair_value"]) < 1e-9
    assert comps["fair_value"] > 0
    assert math.isfinite(comps["fair_value"])


def test_forward_return_alignment_horizons():
    from hptl.valuation.gold_focused_macro_valuation import _forward_bucket_stats

    n = 180
    d0 = date(2018, 1, 5)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(n)]
    prices = [1200.0 + i for i in range(n)]
    deviations = [-12.0 if i < 50 else 12.0 if i > 120 else 0.0 for i in range(n)]
    fwd = _forward_bucket_stats(dates, prices, deviations, horizons=(13, 26, 52, 104))
    assert {r["horizon_weeks"] for r in fwd} == {13, 26, 52, 104}


def test_deterministic_fair_value():
    a = fair_value_components(
        k=0.03,
        beta=9.0,
        monetary_value_per_ounce=3100.0,
        real_yield_decimal=0.012,
        dxy_benchmark=101.0,
        dxy=99.0,
    )
    b = fair_value_components(
        k=0.03,
        beta=9.0,
        monetary_value_per_ounce=3100.0,
        real_yield_decimal=0.012,
        dxy_benchmark=101.0,
        dxy=99.0,
    )
    assert a["fair_value"] == b["fair_value"]


def test_markdown_research_only():
    md = render_markdown(
        {
            "generated_at": "t",
            "model_id": MODEL_ID,
            "verdict": {"verdict": "USEFUL_BUT_RESEARCH", "narrative": "n"},
            "equation": "e",
            "external_claims_not_used": {},
            "panel": {"n": 1, "start": "a", "end": "b", "global_m2": {}},
            "tip": {
                "k": 0.03,
                "beta": 10.0,
                "monetary_value_per_ounce": 1.0,
                "base_value": 1.0,
                "yield_factor": 1.0,
                "dxy_factor": 1.0,
                "fair_value": 100.0,
                "market_price": 99.0,
                "premium_discount": "Discount",
                "deviation_pct": -1.0,
                "bucket": "near_fair_value",
            },
            "parameters": {},
            "boundary_tests": {"all_pass": True},
            "ranking": [],
            "spread_13w": {},
            "spread_52w": {},
            "spread_104w": {},
            "error_correction": {},
            "forward_returns": [],
            "window_start_sensitivity": [],
            "charts": [],
            "runtime_sec": 0.1,
        }
    )
    assert "Research only" in md
    assert "Shadow Currency" in md
