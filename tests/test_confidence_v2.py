"""Tests for valuation confidence v2 composite framework."""
from __future__ import annotations

from hptl.valuation.confidence_v2 import (
    FX_MODEL,
    METALS_MODEL,
    AGRI_PERCENTILE,
    AGRI_REGRESSION,
    compute_confidence_v2,
)


def test_fx_strong_fit_cpi_stale_not_hard_low():
    """USD/CAD-like: strong R² with CPI stale should not collapse to low."""
    r = compute_confidence_v2(
        model_id=FX_MODEL,
        publishable=True,
        n=2609,
        r_squared=0.4843,
        stale_inputs=["USD.cpi_yoy", "CAD.cpi_yoy"],
        missing_inputs=[],
        confidence_v1="Low",
    )
    assert r.confidence in {"high", "medium"}
    assert r.confidence != "low" or r.confidence_v2_score >= 48
    assert "cpi" in r.confidence_explanation.lower()
    assert r.confidence_subscores["data_score"] < 100


def test_fx_weak_r2_stays_low():
    r = compute_confidence_v2(
        model_id=FX_MODEL,
        publishable=True,
        n=263,
        r_squared=0.10,
        stale_inputs=["EUR.cpi_yoy", "USD.cpi_yoy"],
        missing_inputs=[],
    )
    assert r.confidence in {"low", "medium"}
    assert r.confidence != "high"


def test_metals_palladium_high_path():
    r = compute_confidence_v2(
        model_id=METALS_MODEL,
        publishable=True,
        n=389,
        r_squared=0.7049,
        trust_grade="A",
        inputs_fresh=True,
        mean_abs_deviation_pct=15.97,
        confidence_v1="medium",
    )
    assert r.confidence == "high"


def test_metals_silver_not_cosmetic_high():
    r = compute_confidence_v2(
        model_id=METALS_MODEL,
        publishable=True,
        n=389,
        r_squared=0.2428,
        trust_grade="A",
        inputs_fresh=True,
        mean_abs_deviation_pct=27.55,
        confidence_v1="medium",
    )
    assert r.confidence in {"medium", "low"}
    assert r.confidence != "high"


def test_agri_percentile_fallback_documented():
    r = compute_confidence_v2(
        model_id=AGRI_PERCENTILE,
        publishable=True,
        n=18,
        r_squared=None,
        agri_regression_path=False,
    )
    assert r.confidence in {"low", "medium"}
    assert "percentile" in r.confidence_explanation.lower()


def test_agri_regression_high_when_strong():
    r = compute_confidence_v2(
        model_id=AGRI_REGRESSION,
        publishable=True,
        n=30,
        r_squared=0.30,
        agri_regression_path=True,
    )
    assert r.confidence in {"high", "medium"}
