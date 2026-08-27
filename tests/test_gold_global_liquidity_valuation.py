"""Tests for Gold Valuation V3 — Global Liquidity (research only)."""

from __future__ import annotations

import math
from datetime import date, timedelta

from hptl.valuation.gold_global_liquidity_valuation import (
    ANNUAL_SERIES_PUB_LAG_DAYS,
    BIS_PUB_LAG_DAYS,
    MODEL_ID,
    PUBLISHED_GOLD_MODEL_ID,
    _add_days,
    _convert_local_to_usd,
    _log_contributions,
    _sign_bounds,
    _walk_forward,
    render_markdown,
)
from hptl.valuation.gold_structural_valuation_research import _asof_with_lag
from hptl.valuation.metals_valuation_v1 import MODEL_ID as LIVE_ID


def test_production_immutability_constants():
    assert PUBLISHED_GOLD_MODEL_ID == "metals_real_yield_v1"
    assert LIVE_ID == "metals_real_yield_v1"
    assert MODEL_ID == "gold_global_liquidity_valuation_v3"
    assert BIS_PUB_LAG_DAYS["CN"] >= 30
    assert ANNUAL_SERIES_PUB_LAG_DAYS >= 90


def test_publication_lags_on_annual_and_monthly():
    daily = {"2020-12-31": 100.0, "2021-12-31": 200.0}
    dates = ["2021-03-01", "2021-05-15", "2022-05-01"]
    lagged = _asof_with_lag(daily, dates, lag_days=ANNUAL_SERIES_PUB_LAG_DAYS)
    assert lagged[0] is None
    assert lagged[-1] == 200.0


def test_contemporaneous_fx_conversion_past_only():
    local = {"2020-01-31": 100.0, "2020-02-29": 200.0}
    # EURUSD rises after Jan observation — Jan conversion must use Jan FX.
    fx = {"2020-01-15": 1.10, "2020-01-31": 1.10, "2020-02-15": 1.20, "2020-02-29": 1.20}
    out = _convert_local_to_usd(local, fx, fx_mode="eur_usd", pub_lag_days=14)
    jan_usable = _add_days("2020-01-31", 14)
    feb_usable = _add_days("2020-02-29", 14)
    assert abs(out[jan_usable] - 110.0) < 1e-9
    assert abs(out[feb_usable] - 240.0) < 1e-9


def test_local_per_usd_fx_mode():
    local = {"2020-06-30": 1_000_000.0}
    fx = {"2020-06-30": 100.0}  # JPY per USD
    out = _convert_local_to_usd(local, fx, fx_mode="local_per_usd", pub_lag_days=35)
    usable = _add_days("2020-06-30", 35)
    assert abs(out[usable] - 10_000.0) < 1e-6


def test_sign_constraints():
    lo, hi = _sign_bounds(["real10y", "log_liq_per_oz", "reserve_share", "dxy"])
    assert hi[0] == 0.0  # real
    assert lo[1] == 0.0  # liq
    assert lo[2] == 0.0  # reserve
    assert hi[3] == 0.0  # dxy


def test_contribution_reconciliation():
    names = ["real10y", "log_liq_per_oz", "reserve_share"]
    slopes = [-0.04, 0.8, 1.2]
    feats = [1.5, 2.0, 0.12]
    alpha = math.log(2500.0)
    out = _log_contributions(
        alpha=alpha, slopes=slopes, names=names, feats=feats, spot=2600.0
    )
    recon = out["alpha"] + sum(d["log_contribution"] for d in out["drivers"])
    assert abs(recon - out["log_fair"]) < 1e-6
    net = sum(d["dollar_contribution"] for d in out["drivers"])
    assert abs(net - out["net_macro_effect_usd"]) < 0.2
    assert abs(out["fair_value"] - out["base_fair_value"] - out["net_macro_effect_usd"]) < 0.25


def test_past_only_walk_forward_deterministic():
    n = 240
    d0 = date(2016, 1, 8)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(n)]
    prices = [1200.0 + 2.0 * i for i in range(n)]
    y = [math.log(p) for p in prices]
    real = [1.0 - 0.01 * (i % 40) for i in range(n)]
    liq = [3.0 + 0.002 * i for i in range(n)]
    eng1 = _walk_forward(
        dates, prices, y, [real, liq], ["real10y", "log_liq_per_oz"], min_train=104, step=13
    )
    eng2 = _walk_forward(
        dates, prices, y, [real, liq], ["real10y", "log_liq_per_oz"], min_train=104, step=13
    )
    assert eng1["history"]
    assert eng1["history"][-1]["fair_value"] == eng2["history"][-1]["fair_value"]
    first_i = next(i for i, fl in enumerate(eng1["fair_logs"]) if fl is not None)
    assert first_i >= 104


def test_forward_return_alignment_horizons():
    from hptl.valuation.gold_focused_macro_valuation import _forward_bucket_stats

    n = 160
    d0 = date(2018, 1, 5)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(n)]
    prices = [1000.0 + i for i in range(n)]
    # First third undervalued, last third overvalued
    deviations = []
    for i in range(n):
        if i < 40:
            deviations.append(-12.0)
        elif i > 100:
            deviations.append(12.0)
        else:
            deviations.append(0.0)
    fwd = _forward_bucket_stats(dates, prices, deviations, horizons=(13, 26, 52, 104))
    hs = {r["horizon_weeks"] for r in fwd}
    assert hs == {13, 26, 52, 104}
    under = [r for r in fwd if r["bucket"] == "undervalued" and r["horizon_weeks"] == 13]
    assert under and under[0]["n"] > 0


def test_above_ground_supply_alignment_and_lag():
    from hptl.valuation.gold_global_liquidity_valuation import _build_above_ground_annual

    annual = _build_above_ground_annual()
    assert annual
    # Usable dates are lagged into the following year (Apr 30)
    keys = sorted(annual)
    assert keys[0].endswith("-04-30")
    vals = [annual[k] for k in keys]
    assert all(vals[i] <= vals[i + 1] for i in range(len(vals) - 1))
    # Ounces, not tonnes
    assert vals[-1] > 1e9


def test_markdown_research_only():
    md = render_markdown(
        {
            "generated_at": "t",
            "model_id": MODEL_ID,
            "best_model_id": "A_structural_core",
            "verdict": {"verdict": "USEFUL_BUT_RESEARCH", "narrative": "n"},
            "equation": "e",
            "panel": {"n_core": 1, "core_start": "a", "core_end": "b"},
            "ranking": [],
            "tip": {
                "drivers_usd": {"Real Yield": -1.0},
                "net_contribution_usd": 1.0,
                "fair_value": 100.0,
                "market_price": 99.0,
                "premium_discount": "Discount",
                "deviation_pct": -1.0,
                "bucket": "near_fair_value",
                "intercept_alpha": 4.6,
                "coefficients": {},
            },
            "oos": {},
            "stability": {},
            "spread_13w": {},
            "spread_52w": {},
            "spread_104w": {},
            "error_correction": {},
            "time_trend_placebo": {},
            "era_coverage": [],
            "forward_returns": [],
            "charts": [],
            "runtime_sec": 0.1,
        }
    )
    assert "Research only" in md
    assert "Global Liquidity" in md
