"""Tests for metals valuation V1."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hptl.valuation.engine import compute_valuation
from hptl.valuation.metals_valuation_v1 import (
    METALS_MARKETS,
    MODEL_ID,
    _bias_from_deviation,
    build_all_metals_valuations,
    compute_metals_valuation,
    is_metals_valuation_market,
    run_backtest_diagnostics,
)
from hptl.valuation.metals_valuation_export import (
    merge_metals_into_valuation_latest,
    write_metals_valuation_exports,
)


def test_metals_market_registry():
    assert is_metals_valuation_market("Gold")
    assert is_metals_valuation_market("Copper / HG")
    assert not is_metals_valuation_market("Corn")
    assert len(METALS_MARKETS) == 5


def test_bias_thresholds():
    assert _bias_from_deviation(-6.0) == "Undervalued"
    assert _bias_from_deviation(6.0) == "Overvalued"
    assert _bias_from_deviation(1.0) == "Fair Value"


def test_compute_metals_shape():
    val = compute_metals_valuation(market="Gold")
    assert val["market"] == "Gold"
    assert val["model_id"] == MODEL_ID
    assert val["valuation_pillar"] == "metals_real_yield"
    assert "valuation_reason" in val
    if val.get("wired"):
        assert val["fair_value"] is not None
        assert val["deviation_pct"] is not None
        assert val["valuation_bias"] in {"Undervalued", "Fair Value", "Overvalued"}
        assert val.get("trust_grade") in {"A", "B", "C"}
        assert val.get("regression", {}).get("r_squared") is not None


def test_copper_china_pmi_placeholder():
    val = compute_metals_valuation(market="Copper / HG")
    pmi = val.get("china_pmi") or {}
    assert pmi.get("placeholder") is True
    assert pmi.get("wired") is False


def test_engine_routes_metals():
    val = compute_valuation(market="Silver")
    assert val["asset_class"] == "metals"
    assert val["model_id"] == MODEL_ID


def test_build_all_metals():
    doc = build_all_metals_valuations()
    assert doc["engine"] == MODEL_ID
    assert doc["summary"]["total_instruments"] == 5
    assert len(doc["instruments"]) == 5


def test_backtest_diagnostics_shape():
    bt = run_backtest_diagnostics()
    assert bt["model_id"] == MODEL_ID
    assert "Gold" in bt["markets"]


def test_merge_metals_into_valuation_latest():
    base = {"instruments": {"Gold": {"wired": True, "deviation_pct": 1.0}}, "summary": {}}
    out = merge_metals_into_valuation_latest(base)
    assert out["instruments"]["Gold"]["valuation_pillar"] == "metals_real_yield"
    assert out["metals_pillar_engine"] == MODEL_ID


def test_write_exports(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "hptl.valuation.metals_valuation_export.METALS_OUT",
        tmp_path / "metals_valuation_latest.json",
    )
    monkeypatch.setattr(
        "hptl.valuation.metals_valuation_export.PUBLIC_METALS_OUT",
        tmp_path / "public" / "metals_valuation_latest.json",
    )
    monkeypatch.setattr(
        "hptl.valuation.metals_valuation_export.DESIGN_MD",
        tmp_path / "design.md",
    )
    monkeypatch.setattr(
        "hptl.valuation.metals_valuation_export.BACKTEST_JSON",
        tmp_path / "backtest.json",
    )
    monkeypatch.setattr(
        "hptl.valuation.metals_valuation_export.AUDIT_MD",
        tmp_path / "audit.md",
    )
    paths = write_metals_valuation_exports()
    assert paths["metals_valuation"].exists()
    payload = json.loads(paths["metals_valuation"].read_text(encoding="utf-8"))
    assert payload["summary"]["total_instruments"] == 5


@pytest.mark.parametrize("market", list(METALS_MARKETS))
def test_all_metals_compute_or_fail_gracefully(market):
    val = compute_metals_valuation(market=market)
    assert val["market"] == market
    if not val.get("wired"):
        assert val["valuation_bias"] == "UNAVAILABLE"
        assert val.get("unavailable_reason") or val.get("valuation_reason")
