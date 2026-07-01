"""Tests for IVE Phase 0 contract."""
from __future__ import annotations

from hptl.valuation.ive_adapter import attach_ive_to_export_block, legacy_block_to_ive
from hptl.valuation.ive_schema import (
    model_status_from_block,
    strip_confidence_fields,
    valuation_grade_from_pct,
)


def test_valuation_grade_bands():
    assert valuation_grade_from_pct(3.0) == "FAIR"
    assert valuation_grade_from_pct(-10.0) == "MILD"
    assert valuation_grade_from_pct(27.0) == "SIGNIFICANT"
    assert valuation_grade_from_pct(-42.0) == "EXTREME"


def test_model_status_data_missing():
    block = {"wired": True, "fair_value": 1.0, "spot_price": 1.1, "missing_inputs": ["EUR.y2"]}
    assert model_status_from_block(block) == "DATA_MISSING"


def test_model_status_data_stale():
    block = {
        "wired": True,
        "fair_value": 1.0,
        "spot_price": 1.1,
        "missing_inputs": [],
        "stale_inputs": ["USD.cpi_yoy"],
    }
    assert model_status_from_block(block) == "DATA_STALE"


def test_model_status_validated():
    block = {"wired": True, "fair_value": 1.0, "spot_price": 1.1, "missing_inputs": [], "stale_inputs": []}
    assert model_status_from_block(block) == "VALIDATED"


def test_fx_ive_adapter_has_breakdown():
    block = {
        "wired": True,
        "model_id": "fx_carry_real_yield_v3",
        "spot_price": 1.38,
        "fair_value": 1.50,
        "deviation_pct": -8.0,
        "valuation_state": "Undervalued",
        "drivers": {
            "policy_rate_diff": 1.37,
            "yield_2y_diff": 1.37,
            "real_yield_diff": 1.17,
            "inflation_diff": 0.2,
        },
        "regression": {"r_squared": 0.48, "n": 2609},
        "input_freshness": {"quote_rates_as_of": "2026-06-05"},
        "missing_inputs": [],
        "stale_inputs": ["USD.cpi_yoy"],
    }
    ive = legacy_block_to_ive(block, "Canadian Dollar / 6C", generated_at="2026-06-19T00:00:00Z")
    assert ive.valuation_grade == "MILD"
    assert ive.model_status == "DATA_STALE"
    assert len(ive.calculation_breakdown) >= 4
    assert ive.model_name == "fx_carry_real_yield_v3"


def test_attach_strips_confidence():
    block = {
        "wired": True,
        "model_id": "metals_real_yield_v1",
        "spot_price": 2000.0,
        "fair_value": 1500.0,
        "deviation_pct": 33.0,
        "valuation_state": "Overvalued",
        "confidence": "high",
        "confidence_v2_score": 79.0,
        "trust_grade": "A",
        "drivers": {"real_yield_10y": 2.1, "dxy_broad": 120.0},
        "regression": {"r_squared": 0.33, "n": 389, "features": {"real_yield": 0.1, "log_dxy": -0.7}},
        "input_freshness": {"price_as_of": "2026-06-09", "inputs_fresh": True},
    }
    out = attach_ive_to_export_block(block, "Gold", generated_at="2026-06-19T00:00:00Z")
    assert "confidence" not in out
    assert "confidence_v2_score" not in out
    assert "trust_grade" not in out
    assert out["valuation_grade"] == "EXTREME"
    assert out["ive"]["instrument"] == "Gold"
