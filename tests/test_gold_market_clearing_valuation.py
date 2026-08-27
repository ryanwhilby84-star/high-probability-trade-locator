"""Tests for Gold V5 market-clearing valuation (research only)."""

from __future__ import annotations

import math
from datetime import date, timedelta

from hptl.valuation.gold_market_clearing_valuation import (
    DELTA_LOG_BOUND,
    GDT_PUB_LAG_DAYS,
    MODEL_ID,
    PUBLISHED_GOLD_MODEL_ID,
    _add_days,
    _fit_sector,
    _stage_specs,
    render_markdown,
    solve_market_clearing,
)
from hptl.valuation.gold_structural_valuation_research import _asof_with_lag
from hptl.valuation.metals_valuation_v1 import MODEL_ID as LIVE_ID


def test_production_immutability():
    assert PUBLISHED_GOLD_MODEL_ID == "metals_real_yield_v1"
    assert LIVE_ID == "metals_real_yield_v1"
    assert MODEL_ID == "gold_market_clearing_valuation_v5"


def test_publication_lag_handling():
    daily = {"2024-03-31": 100.0, "2024-06-30": 200.0}
    dates = ["2024-05-01", "2024-07-01", "2024-10-01"]
    lagged = _asof_with_lag(daily, dates, lag_days=GDT_PUB_LAG_DAYS)
    assert lagged[0] is None
    assert lagged[-1] == 200.0
    assert _add_days("2024-03-31", GDT_PUB_LAG_DAYS) > "2024-03-31"


def test_sector_sign_constraints():
    rows = []
    for i in range(12):
        rows.append(
            {
                "jewellery": 500 - 20 * math.log(2000 + 50 * i) + 100 * (0.02),
                "gdp_growth": 0.02,
                "log_gold": math.log(2000 + 50 * i),
            }
        )
    spec = {
        "id": "jewellery",
        "y": "jewellery",
        "features": ["gdp_growth", "log_gold"],
        "signs": {"gdp_growth": ">=0", "log_gold": "<=0"},
        "price_feature": "log_gold",
    }
    fit = _fit_sector(rows, spec)
    assert fit["ok"]
    assert fit["beta"]["log_gold"] <= 1e-9
    assert fit["beta"]["gdp_growth"] >= -1e-9


def test_demand_supply_aggregation_and_solver_reconciliation():
    d_specs = _stage_specs(1)["demand"]
    s_specs = _stage_specs(1)["supply"]
    # Synthetic fits with known elasticities
    d_fits = [
        {
            "id": "fabrication",
            "alpha": 800.0,
            "beta": {"gdp_growth": 100.0, "log_gold": -50.0},
            "price_elasticity": -50.0,
            "exogenous": False,
        },
        {
            "id": "investment",
            "alpha": 400.0,
            "beta": {"real_yield": -10.0, "dxy": -1.0, "vix": 2.0, "log_gold": -20.0},
            "price_elasticity": -20.0,
            "exogenous": False,
        },
        {"id": "cb", "alpha": 200.0, "beta": {}, "price_elasticity": 0.0, "exogenous": True},
    ]
    s_fits = [
        {
            "id": "mine",
            "alpha": 900.0,
            "beta": {"mine_lag": 0.0, "log_gold_lag": 0.0},
            "price_elasticity": 0.0,
            "exogenous": False,
        },
        {
            "id": "recycling",
            "alpha": 100.0,
            "beta": {"log_gold": 30.0, "dlog_gold": 5.0, "gdp_growth": -10.0},
            "price_elasticity": 30.0,
            "exogenous": False,
        },
    ]
    row = {
        "gold_price": math.exp(7.6),
        "log_gold": 7.6,
        "log_gold_lag": 7.5,
        "gdp_growth": 0.02,
        "real_yield": 1.5,
        "dxy": 100.0,
        "vix": 18.0,
        "dlog_gold": 0.01,
        "mine_lag": 900.0,
        "indpro_growth": 0.01,
    }
    sol = solve_market_clearing(
        demand_fits=d_fits,
        supply_fits=s_fits,
        demand_specs=d_specs,
        supply_specs=s_specs,
        row=row,
    )
    assert sol["net_elasticity"] > 0
    assert sol["identity_check"]
    # Rounded CSV fields: allow 0.01 oz tolerance
    assert abs(sol["fair_value"] - sol["gold_price"] * math.exp(sol["delta_log_price"])) < 0.01
    assert abs(
        sol["D0"]
        - (
            sol["demand_parts"]["fabrication"]
            + sol["demand_parts"]["investment"]
            + sol["demand_parts"]["cb"]
        )
    ) < 0.01


def test_net_elasticity_positivity_and_bounds():
    d_specs = _stage_specs(1)["demand"]
    s_specs = _stage_specs(1)["supply"]
    d_fits = [
        {"id": "fabrication", "alpha": 2000.0, "beta": {"log_gold": -5.0}, "price_elasticity": -5.0},
        {"id": "investment", "alpha": 500.0, "beta": {}, "price_elasticity": 0.0},
        {"id": "cb", "alpha": 200.0, "beta": {}, "price_elasticity": 0.0, "exogenous": True},
    ]
    s_fits = [
        {"id": "mine", "alpha": 500.0, "beta": {}, "price_elasticity": 0.0},
        {"id": "recycling", "alpha": 100.0, "beta": {"log_gold": 2.0}, "price_elasticity": 2.0},
    ]
    row = {
        "gold_price": 2000.0,
        "log_gold": math.log(2000.0),
        "log_gold_lag": math.log(1900.0),
        "gdp_growth": 0.0,
        "real_yield": 0.0,
        "dxy": 100.0,
        "vix": 20.0,
        "dlog_gold": 0.0,
        "mine_lag": 500.0,
        "indpro_growth": 0.0,
    }
    sol = solve_market_clearing(
        demand_fits=d_fits,
        supply_fits=s_fits,
        demand_specs=d_specs,
        supply_specs=s_specs,
        row=row,
    )
    assert sol["net_elasticity"] > 0
    assert abs(sol["delta_log_price"]) <= DELTA_LOG_BOUND + 1e-12
    if abs(sol["imbalance"] / sol["net_elasticity"]) > DELTA_LOG_BOUND:
        assert sol["bound_hit"] is True


def test_past_only_fitting_uses_train_prefix():
    rows = []
    for i in range(10):
        rows.append(
            {
                "recycling": 300 + 10 * i,
                "log_gold": 7.0 + 0.01 * i,
                "dlog_gold": 0.01,
                "gdp_growth": 0.02,
            }
        )
    fit1 = _fit_sector(rows[:6], {
        "id": "recycling",
        "y": "recycling",
        "features": ["log_gold", "dlog_gold", "gdp_growth"],
        "signs": {"log_gold": ">=0", "dlog_gold": ">=0", "gdp_growth": "<=0"},
        "price_feature": "log_gold",
    })
    fit2 = _fit_sector(rows[:6], {
        "id": "recycling",
        "y": "recycling",
        "features": ["log_gold", "dlog_gold", "gdp_growth"],
        "signs": {"log_gold": ">=0", "dlog_gold": ">=0", "gdp_growth": "<=0"},
        "price_feature": "log_gold",
    })
    assert fit1["alpha"] == fit2["alpha"]
    assert fit1["n"] == 6


def test_no_current_price_identity_leakage_flag_structure():
    from hptl.valuation.gold_market_clearing_valuation import _price_identity_leakage

    hist = [
        {"fair_value": 2000 + i, "gold_price": 2000 + i, "price_term_demand_share": 0.9}
        for i in range(8)
    ]
    leak = _price_identity_leakage(hist)
    assert leak["ok"]
    assert leak["identity_leakage"] is True


def test_forward_return_alignment():
    from hptl.valuation.gold_focused_macro_valuation import _forward_bucket_stats

    n = 160
    d0 = date(2019, 1, 4)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(n)]
    prices = [1500.0 + i for i in range(n)]
    deviations = [-12.0 if i < 40 else 12.0 if i > 100 else 0.0 for i in range(n)]
    fwd = _forward_bucket_stats(dates, prices, deviations, horizons=(13, 26, 52, 104))
    assert {r["horizon_weeks"] for r in fwd} == {13, 26, 52, 104}


def test_deterministic_solver():
    d_specs = _stage_specs(1)["demand"]
    s_specs = _stage_specs(1)["supply"]
    d_fits = [
        {"id": "fabrication", "alpha": 700.0, "beta": {"log_gold": -40.0}, "price_elasticity": -40.0},
        {"id": "investment", "alpha": 300.0, "beta": {"log_gold": -10.0}, "price_elasticity": -10.0},
        {"id": "cb", "alpha": 150.0, "beta": {}, "price_elasticity": 0.0, "exogenous": True},
    ]
    s_fits = [
        {"id": "mine", "alpha": 900.0, "beta": {}, "price_elasticity": 0.0},
        {"id": "recycling", "alpha": 200.0, "beta": {"log_gold": 25.0}, "price_elasticity": 25.0},
    ]
    row = {
        "gold_price": 2500.0,
        "log_gold": math.log(2500.0),
        "log_gold_lag": math.log(2400.0),
        "gdp_growth": 0.01,
        "real_yield": 1.0,
        "dxy": 103.0,
        "vix": 16.0,
        "dlog_gold": 0.02,
        "mine_lag": 900.0,
        "indpro_growth": 0.01,
    }
    a = solve_market_clearing(
        demand_fits=d_fits, supply_fits=s_fits, demand_specs=d_specs, supply_specs=s_specs, row=row
    )
    b = solve_market_clearing(
        demand_fits=d_fits, supply_fits=s_fits, demand_specs=d_specs, supply_specs=s_specs, row=row
    )
    assert a["fair_value"] == b["fair_value"]


def test_markdown_research_only():
    md = render_markdown(
        {
            "generated_at": "t",
            "model_id": MODEL_ID,
            "best_stage": 1,
            "verdict": {"verdict": "USEFUL_BUT_RESEARCH", "narrative": "n"},
            "equation": "e",
            "panel": {"n_quarters": 1, "start": "a", "end": "b", "note": "n", "gdt_counts": {}},
            "ranking": [],
            "tip": {
                "fair_value": 1.0,
                "market_price": 1.0,
                "premium_discount": "Fair",
                "deviation_pct": 0.0,
                "bucket": "near_fair_value",
            },
            "spread_13w": {},
            "spread_52w": {},
            "spread_104w": {},
            "error_correction": {},
            "price_identity_leakage": {},
            "forecast_rmse": {},
            "forward_returns": [],
            "charts": [],
            "runtime_sec": 0.1,
        }
    )
    assert "Research only" in md
    assert "Market-Clearing" in md
