"""Hard identities for Gold market-clearing tip + history."""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from hptl.valuation.gold_market_clearing_valuation import (
    DELTA_LOG_BOUND,
    build_quarterly_panel,
    solve_market_clearing,
    _classify_deviation,
    _stage_specs,
    _fit_sector,
    _enrich_row_features,
    MIN_TRAIN_Q,
)
from hptl.valuation.gold_market_clearing_export import (
    build_gold_valuation_document,
    _dev_pct,
)


def test_negative_real_yield_quarters_not_silently_dropped():
    panel = build_quarterly_panel()
    assert panel.get("ok")
    # Official GDT has 66 quarters; formerly 12 were dropped by require_positive real yield
    assert panel["meta"]["n_quarters"] >= 60
    assert panel["meta"].get("gdt_quarters_loaded") == 66


def test_solve_never_publishes_price_as_fv_when_invalid():
    panel = build_quarterly_panel()
    rows = list(panel["rows"])
    _enrich_row_features(rows)
    specs = _stage_specs(3)
    # Force invalid by using empty training with exogenous-only if needed — use last window
    t = len(rows) - 1
    train, test = rows[:t], rows[t]
    d_fits = [_fit_sector(train, sp) for sp in specs["demand"]]
    s_fits = [_fit_sector(train, sp) for sp in specs["supply"]]
    # Break net elas artificially
    for f in d_fits:
        f["price_elasticity"] = 50.0
    for f in s_fits:
        f["price_elasticity"] = 1.0
    sol = solve_market_clearing(
        demand_fits=d_fits,
        supply_fits=s_fits,
        demand_specs=specs["demand"],
        supply_specs=specs["supply"],
        row=test,
    )
    assert sol["solver_status"] == "SOLVER_INVALID"
    assert sol["fair_value"] is None
    assert sol["deviation_pct"] is None
    assert sol["gold_price"] is not None
    # Must not equal "fair value = price" fallback
    assert sol.get("displayed_fair_value") is None


def test_bound_hit_not_published_as_fair_value():
    specs = _stage_specs(3)
    # Minimal exogenous demand/supply with known imbalance and tiny net elas
    d_fits = [
        {"id": sp["id"], "exogenous": True, "alpha": 800.0, "beta": {}, "price_elasticity": -0.05}
        for sp in specs["demand"]
    ]
    s_fits = [
        {"id": sp["id"], "exogenous": True, "alpha": 100.0, "beta": {}, "price_elasticity": 0.06}
        for sp in specs["supply"]
    ]
    # net_elas = 0.06 - (-0.05*n_demand) … force explicitly
    for f in d_fits:
        f["price_elasticity"] = 0.0
    for f in s_fits:
        f["price_elasticity"] = 0.05
    # D0≈800*n_d, S0≈100*n_s → large positive imbalance / 0.05 >> 0.5
    row = {
        "gold_price": 2000.0,
        "log_gold": math.log(2000.0),
        "log_gold_lag": math.log(1900.0),
    }
    sol = solve_market_clearing(
        demand_fits=d_fits,
        supply_fits=s_fits,
        demand_specs=specs["demand"],
        supply_specs=specs["supply"],
        row=row,
    )
    assert sol["solver_status"] == "BOUND_HIT_INVALID"
    assert sol["fair_value"] is None
    assert sol["raw_delta_log_price"] is not None
    assert abs(sol["raw_delta_log_price"]) > DELTA_LOG_BOUND
    assert abs(sol["bounded_delta_log_price"]) == pytest.approx(DELTA_LOG_BOUND)


def test_ok_solve_identities_and_export_deviation():
    panel = build_quarterly_panel()
    rows = list(panel["rows"])
    _enrich_row_features(rows)
    assert len(rows) > MIN_TRAIN_Q + 2
    # Find an OK tip via walk of stage 3
    specs = _stage_specs(3)
    ok_sol = None
    for t in range(MIN_TRAIN_Q, len(rows)):
        train, test = rows[:t], rows[t]
        d_fits = [_fit_sector(train, sp) for sp in specs["demand"]]
        s_fits = [_fit_sector(train, sp) for sp in specs["supply"]]
        sol = solve_market_clearing(
            demand_fits=d_fits,
            supply_fits=s_fits,
            demand_specs=specs["demand"],
            supply_specs=specs["supply"],
            row=test,
        )
        if sol["solver_status"] == "OK" and sol["fair_value"] is not None:
            ok_sol = sol
            break
    if ok_sol is None:
        pytest.skip("No OK solve in current panel — environment data dependent")

    assert abs(ok_sol["imbalance"] - (ok_sol["D0"] - ok_sol["S0"])) < 1e-2
    assert ok_sol["net_elasticity"] > 0
    assert abs(
        ok_sol["raw_delta_log_price"] - ok_sol["imbalance"] / ok_sol["net_elasticity"]
    ) < 1e-5
    assert abs(
        ok_sol["fair_value"]
        - ok_sol["gold_price"] * math.exp(ok_sol["raw_delta_log_price"])
    ) < 0.05
    calc_dev = 100.0 * (ok_sol["gold_price"] - ok_sol["fair_value"]) / ok_sol["fair_value"]
    assert abs(calc_dev - ok_sol["deviation_pct"]) < 0.01
    assert ok_sol["bucket"] == _classify_deviation(ok_sol["deviation_pct"])


def test_live_spot_vs_fv_example_identity():
    # User example: spot 4045, FV 4562 → ~-11.3%
    dev = _dev_pct(4045.0, 4562.0)
    assert dev == pytest.approx(-11.3327, abs=0.01)
    assert _classify_deviation(dev) == "undervalued"


def test_dashboard_doc_does_not_zero_out_invalid_tip(monkeypatch, tmp_path):
    # Minimal synthetic invalid tip payload
    payload = {
        "ok": True,
        "generated_at": "2026-01-01T00:00:00+00:00",
        "model_id": "gold_market_clearing_valuation_v5",
        "equation": "test",
        "best_stage": 2,
        "panel": {"n_quarters": 66, "gdt_quarters_loaded": 66, "start": "2010-03-31", "end": "2026-06-30"},
        "missing_quarter_audit": [],
        "tip": {
            "date": "2026-06-30",
            "market_price": 4561.8,
            "fair_value": None,
            "deviation_pct": None,
            "net_imbalance_tonnes": -230.8,
            "raw_delta_log_price": None,
            "solver_status": "SOLVER_INVALID",
            "solve_ok": False,
            "total_demand": 994.0,
            "total_supply": 1225.0,
        },
        "_best_history": [],
    }
    doc = build_gold_valuation_document(payload, rerun=False)
    inst = doc["instrument"]
    assert inst["model_valid"] is False
    assert inst["fair_value"] is None
    assert inst["deviation_pct"] is None
    assert inst["valuation_bucket_label"] == "MODEL INVALID"
    assert inst["net_imbalance_tonnes"] == pytest.approx(-230.8)
