"""Tests for agriculture fundamental valuation engine."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hptl.valuation.agri_fundamental_valuation import (
    AGRI_VALUATION_MARKETS,
    BALANCE_SHEET_DIR,
    MODEL_ID_PERCENTILE,
    MODEL_ID_REGRESSION,
    PRIORITY_MARKETS,
    build_all_agri_valuations,
    build_data_inventory,
    compute_agri_valuation,
    discover_instrument_data,
    is_agri_valuation_market,
)
from hptl.valuation.agri_valuation_export import (
    merge_agri_into_valuation_latest,
    render_agri_audit_md,
    render_data_inventory_md,
    write_agri_valuation_exports,
)


def test_agri_market_registry():
    assert is_agri_valuation_market("Soybeans")
    assert is_agri_valuation_market("Corn")
    assert not is_agri_valuation_market("Gold")
    assert len(PRIORITY_MARKETS) == 5
    assert len(AGRI_VALUATION_MARKETS) == 7


def test_discover_instrument_data_priority_markets():
    for market in PRIORITY_MARKETS:
        inv = discover_instrument_data(market)
        assert inv["market"] == market
        assert inv["recommended_model_type"] in {
            "stu_price_regression",
            "stu_percentile_fair_value",
            "blocked_no_balance_sheet",
            "blocked_no_data",
        }
        if not inv["balance_sheet_on_disk"]:
            assert "USDA WASDE/PSD" in (inv["data_missing"][0] if inv["data_missing"] else "")


def test_compute_unavailable_without_balance_sheet():
    val = compute_agri_valuation(market="Soybeans")
    assert val["wired"] is False
    assert val["fair_value"] is None
    assert val["deviation_pct"] is None
    assert "USDA WASDE/PSD" in (val.get("unavailable_reason") or "")
    assert val.get("valuation_pillar") is None or val.get("valuation_pillar") != "fx"


def test_build_all_agri_exports_shape(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "hptl.valuation.agri_valuation_export.AGRI_OUT",
        tmp_path / "agri_valuation_latest.json",
    )
    monkeypatch.setattr(
        "hptl.valuation.agri_valuation_export.PUBLIC_AGRI_OUT",
        tmp_path / "public" / "agri_valuation_latest.json",
    )
    monkeypatch.setattr(
        "hptl.valuation.agri_valuation_export.INVENTORY_MD",
        tmp_path / "agri_valuation_data_inventory.md",
    )
    monkeypatch.setattr(
        "hptl.valuation.agri_valuation_export.AUDIT_MD",
        tmp_path / "agri_valuation_audit.md",
    )

    paths = write_agri_valuation_exports()
    payload = json.loads(paths["agri_valuation"].read_text(encoding="utf-8"))
    assert payload["engine"] == "agri_fundamental_valuation"
    assert len(payload["instruments"]) == len(AGRI_VALUATION_MARKETS)
    assert paths["inventory_md"].exists()
    assert paths["audit_md"].exists()
    assert "Agriculture Valuation Data Inventory" in paths["inventory_md"].read_text(encoding="utf-8")
    assert "Agriculture Valuation Audit" in paths["audit_md"].read_text(encoding="utf-8")


def test_merge_agri_tags_pillar_without_fx_side_effects():
    fx_row = {"market": "Euro FX / 6E", "wired": True, "valuation_bias": "Undervalued", "model_id": "fx_carry_real_yield_v3"}
    agri_row = {"market": "Soybeans", "wired": False, "unavailable_reason": "no balance sheet"}
    doc = {
        "instruments": {"Euro FX / 6E": fx_row, "Soybeans": agri_row},
        "summary": {"wired_count": 1, "total_instruments": 2},
    }
    merged = merge_agri_into_valuation_latest(doc)
    assert merged["instruments"]["Euro FX / 6E"]["wired"] is True
    assert merged["instruments"]["Soybeans"]["valuation_pillar"] == "agri_fundamental"
    assert merged["summary"]["agri_wired_count"] == 0


def test_regression_model_with_synthetic_balance_sheet(tmp_path, monkeypatch):
    bs_dir = tmp_path / "agri_balance_sheet"
    bs_dir.mkdir()
    monkeypatch.setattr("hptl.valuation.agri_fundamental_valuation.BALANCE_SHEET_DIR", bs_dir)

    series = []
    for i in range(30):
        year = 2020 + i // 12
        month = (i % 12) + 1
        stu = 0.10 + i * 0.005
        series.append(
            {
                "date": f"{year}-{month:02d}-01",
                "stocks_to_use": stu,
                "ending_stocks": stu * 100,
                "total_use": 100.0,
            }
        )
    (bs_dir / "Soybeans.json").write_text(json.dumps({"series": series}), encoding="utf-8")

    def fake_price_on_date(market: str, target_date: str):
        idx = int(target_date[:4]) - 2020
        return 800.0 + idx * 12.0

    monkeypatch.setattr(
        "hptl.valuation.agri_fundamental_valuation._price_on_date",
        fake_price_on_date,
    )
    monkeypatch.setattr(
        "hptl.valuation.agri_fundamental_valuation._spot_price",
        lambda market: (1100.0, "test", 500),
    )

    val = compute_agri_valuation(market="Soybeans")
    assert val["wired"] is True
    assert val["fair_value"] is not None
    assert val["deviation_pct"] is not None
    assert val["model_id"] in (MODEL_ID_REGRESSION, MODEL_ID_PERCENTILE)


def test_inventory_and_audit_renderers():
    inv = build_data_inventory()
    md = render_data_inventory_md(inv)
    assert "Soybeans" in md
    assert "USDA WASDE / PSD" in md

    payload = build_all_agri_valuations()
    audit = render_agri_audit_md(payload)
    assert "Instruments unavailable" in audit
    assert "Soybeans" in audit
