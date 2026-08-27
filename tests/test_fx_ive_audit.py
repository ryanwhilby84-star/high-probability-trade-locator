"""Smoke tests for FX IVE Phase 1A audit."""
from __future__ import annotations

from hptl.valuation.fx_ive_audit import FX_IVE_AUDIT_PAIRS, audit_fx_pair
from hptl.fx.fx_macro_history import currency_histories


def test_fx_ive_audit_pair_structure():
    histories = currency_histories()
    doc = audit_fx_pair("USD/JPY", histories)
    assert doc["model_information"]["pair"] == "USD/JPY"
    assert doc["regression"]["coefficients"]["real_yield_diff_fixed"] == 0.055
    assert doc["institutional_review"]["rating"] in {
        "PRODUCTION_READY",
        "NEEDS_IMPROVEMENT",
        "REBUILD_REQUIRED",
    }
    assert doc["driver_attribution"]["reconciliation_error"] == 0.0


def test_fx_ive_audit_scope():
    assert len(FX_IVE_AUDIT_PAIRS) == 7
    assert "EUR/USD" in FX_IVE_AUDIT_PAIRS
