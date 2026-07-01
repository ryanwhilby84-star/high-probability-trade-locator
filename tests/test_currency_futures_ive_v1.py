"""Tests for currency futures IVE v1 (Phase 1C — futures-native calculation)."""
from __future__ import annotations

import json

from hptl.valuation.currency_futures_ive_v1 import (
    BANNED_PAIR_SYMBOLS,
    DEPENDENT_SERIES,
    FUTURES_REGISTRY,
    build_currency_futures_ive_export,
    compute_futures_instrument,
    is_currency_futures_instrument,
    valuation_label_from_pct,
    write_currency_futures_ive_export,
)
from hptl.valuation.ive_schema import CONFIDENCE_EXPORT_KEYS


def test_valuation_pct_formula():
    current = 1.10
    fair = 1.00
    pct = round((current - fair) / fair * 100.0, 2)
    assert pct == 10.0
    assert valuation_label_from_pct(pct) == "Overvalued"
    assert valuation_label_from_pct(-10.0) == "Undervalued"


def test_all_target_symbols_present():
    doc = build_currency_futures_ive_export()
    for sym in ("DX", "6E", "6B", "6A", "6C", "6J", "6S", "6N"):
        assert sym in doc["by_symbol"]
        assert sym in FUTURES_REGISTRY


def test_no_pair_symbols_exported():
    doc = build_currency_futures_ive_export()
    for banned in BANNED_PAIR_SYMBOLS:
        assert banned not in doc.get("by_symbol", {})
        assert banned not in doc.get("instruments", {})
    for block in doc["instruments"].values():
        assert block.get("pair_derived") is False
        assert block.get("legacy_pair_model_used") is False


def test_no_confidence_fields():
    doc = build_currency_futures_ive_export()
    for block in doc["instruments"].values():
        for key in CONFIDENCE_EXPORT_KEYS:
            assert key not in block


def test_no_legacy_fx_v3_or_fixed_betas():
    doc = build_currency_futures_ive_export()
    for block in doc["instruments"].values():
        assert block.get("legacy_fx_v3_used") is False
        assert block.get("legacy_pair_model_used") is False
        reg = (block.get("inputs") or {}).get("regression") or {}
        assert reg.get("legacy_fixed_betas_used") is False
        assert reg.get("legacy_fx_v3_used") is False
        assert reg.get("legacy_pair_model_used") is False
        assert reg.get("dependent_series") == DEPENDENT_SERIES
        assert block.get("dependent_series") == DEPENDENT_SERIES
        assert block.get("model_family")


def test_6s_dedicated_safe_haven_model():
    doc = build_currency_futures_ive_export()
    block = doc["by_symbol"]["6S"]
    assert block["model_name"] == "chf_futures_safe_haven_v1"
    assert block["model_family"] == "futures_ols_log_safe_haven"
    assert block.get("legacy_fx_v3_used") is False
    assert block.get("usdchf_pair_model_used") is False
    assert block["model_name"] != "fx_carry_real_yield_v3"
    features = (block.get("inputs") or {}).get("regression", {}).get("features") or []
    assert "broad_usd_index" in features


def test_stale_chf_blocks_validated():
    block = compute_futures_instrument("6S")
    stale = block.get("inputs", {}).get("_stale_inputs") or []
    if any("CHF.y2" in s or "CHF.y10" in s for s in stale):
        assert block["model_status"] in ("DATA_STALE", "DATA_MISSING")
        assert block.get("fair_value") is None or block["model_status"] != "VALIDATED"


def test_futures_instrument_ids():
    assert is_currency_futures_instrument("Euro FX / 6E")
    assert is_currency_futures_instrument("Swiss Franc / 6S")
    assert not is_currency_futures_instrument("EUR/USD")


def test_per_instrument_model_names():
    doc = build_currency_futures_ive_export()
    expected = {
        "DX": "dx_futures_broad_macro_v1",
        "6E": "eur_futures_macro_v1",
        "6B": "gbp_futures_macro_v1",
        "6A": "aud_futures_macro_v1",
        "6C": "cad_futures_macro_v1",
        "6J": "jpy_futures_macro_v1",
        "6S": "chf_futures_safe_haven_v1",
        "6N": "nzd_futures_macro_v1",
    }
    for sym, model in expected.items():
        assert doc["by_symbol"][sym]["model_name"] == model


def test_export_writes_file(tmp_path, monkeypatch):
    from hptl.valuation import currency_futures_ive_v1 as mod

    out = tmp_path / "currency_futures_ive_latest.json"
    monkeypatch.setattr(mod, "PUBLIC_JSON", out)
    monkeypatch.setattr(mod, "DATA_JSON", tmp_path / "data.json")
    paths = write_currency_futures_ive_export()
    assert paths["public_json"].exists()
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert len(doc["instruments"]) == 8
    assert doc["valuation_phase"].startswith("1C")
