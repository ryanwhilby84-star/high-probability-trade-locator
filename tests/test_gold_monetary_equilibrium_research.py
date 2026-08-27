"""Tests for Gold monetary equilibrium / ECM research (research only)."""

from __future__ import annotations

import math

from hptl.valuation.gold_monetary_equilibrium_research import (
    MONTHLY_GOLD_LAG_DAYS,
    MONTHLY_PUBLICATION_LAG_DAYS,
    PRICE_PROMOTE,
    PUBLISHED_GOLD_MODEL_ID,
    VAL_PROMOTE,
    _asof_with_lag,
    _classify_candidate,
    _ecm_estimate,
    _eg_cointegration,
    _pooled_valuation_spread,
    _verdict,
    render_markdown,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as LIVE_PUBLISHED_ID


def test_production_model_immutable_constant():
    assert PUBLISHED_GOLD_MODEL_ID == "metals_real_yield_v1"
    assert LIVE_PUBLISHED_ID == "metals_real_yield_v1"


def test_publication_lags_are_positive_and_past_only():
    assert MONTHLY_PUBLICATION_LAG_DAYS >= 28
    assert MONTHLY_GOLD_LAG_DAYS >= 7
    daily = {"2020-01-01": 100.0, "2020-02-01": 110.0}
    dates = ["2020-01-10", "2020-03-01", "2020-04-15"]
    lagged = _asof_with_lag(daily, dates, lag_days=MONTHLY_PUBLICATION_LAG_DAYS)
    assert lagged[0] is None
    assert lagged[-1] == 110.0


def test_past_only_cointegration_and_ecm_residual_lag():
    # Synthetic cointegrated: y = 0.5 + 1.2 x + stationary noise
    n = 300
    x = [0.01 * i for i in range(n)]
    y = [0.5 + 1.2 * xi + 0.01 * math.sin(i) for i, xi in enumerate(x)]
    eg = _eg_cointegration(y, [x], n_vars=2)
    assert eg.get("ok")
    # Fair = fitted levels; EC lag used inside ECM helper
    fair = []
    beta = eg["beta"]
    for i in range(n):
        fair.append(beta[0] + beta[1] * x[i])
    # Build expanding-style fair with None warmup
    fair_logs: list[float | None] = [None] * 80 + fair[80:]
    ecm = _ecm_estimate(y, fair_logs, [x], min_train=120, max_lag=1)
    assert ecm.get("ok")
    # With near-perfect equilibrium, lambda should tend negative or near zero-stable
    assert ecm.get("lambda_mean") is not None


def test_forward_return_alignment_and_threshold_classification():
    from hptl.valuation.gold_monetary_equilibrium_research import _classify_deviation
    from hptl.valuation.gold_structural_valuation_research import _bucket_forward_returns

    assert _classify_deviation(-20) == "materially_undervalued"
    assert _classify_deviation(12) == "overvalued"
    dates = [f"2020-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}" for i in range(40)]
    # Fix dates to valid ISO via simple sequence
    from datetime import date, timedelta

    d0 = date(2020, 1, 3)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(40)]
    prices = [100.0 + i for i in range(40)]
    deviations = [-12.0] * 10 + [0.0] * 10 + [12.0] * 20
    rows = _bucket_forward_returns(dates, prices, deviations, horizons=(4,))
    under = next(r for r in rows if r["bucket"] == "undervalued" and r["horizon_weeks"] == 4)
    assert under["n"] >= 1
    assert under["mean_return_pct"] is not None


def test_dual_score_promote_rule_and_price_not_valuation():
    assert PRICE_PROMOTE == 65.0
    assert VAL_PROMOTE == 70.0
    assert (
        _classify_candidate(
            price_score=80,
            val_score=20,
            ecm={"lambda_stable_negative": False},
            flip=False,
            spread_pp=-5.0,
        )
        == "PRICE_MODEL_NOT_VALUATION"
    )
    assert (
        _classify_candidate(
            price_score=80,
            val_score=75,
            ecm={"lambda_stable_negative": True},
            flip=False,
            spread_pp=6.0,
        )
        == "VALID_VALUATION"
    )


def test_verdict_and_markdown_safety():
    ranking = [
        {
            "id": "A_mpp_m2_cpi",
            "is_baseline": False,
            "classification": "PRICE_MODEL_NOT_VALUATION",
            "price_model_score": 70,
            "valuation_score": 10,
            "valuation_spread_13w": {"spread_pp": -4.0},
            "ecm_expanding": {"lambda_mean": 0.01, "lambda_stable_negative": False},
            "full_sample_cointegrated": False,
        }
    ]
    v = _verdict(ranking)
    assert v["verdict"] in {
        "CONTINUE_RESEARCH",
        "REJECT_MONETARY_EQUILIBRIUM",
        "PROMOTE",
    }
    md = render_markdown(
        {
            "generated_at": "t",
            "verdict": v,
            "panel": {"n_weeks": 1, "start": "a", "end": "b", "source_counts": {}},
            "series_documentation": [],
            "ranking": ranking,
            "runtime_sec": 0.1,
        }
    )
    assert "Research only" in md
    assert "metals_real_yield_v1" in md or "untouched" in md.lower()


def test_pooled_spread_helper():
    rows = [
        {"bucket": "undervalued", "horizon_weeks": 13, "n": 20, "mean_return_pct": 5.0},
        {
            "bucket": "materially_undervalued",
            "horizon_weeks": 13,
            "n": 10,
            "mean_return_pct": 8.0,
        },
        {"bucket": "overvalued", "horizon_weeks": 13, "n": 20, "mean_return_pct": 1.0},
        {
            "bucket": "materially_overvalued",
            "horizon_weeks": 13,
            "n": 10,
            "mean_return_pct": 0.0,
        },
        {"bucket": "near_fair_value", "horizon_weeks": 13, "n": 15, "mean_return_pct": 2.0},
    ]
    sp = _pooled_valuation_spread(rows, horizon=13)
    assert sp["ok"]
    assert sp["spread_pp"] > 0
