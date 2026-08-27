"""Tests for FX valuation wiring in weekly refresh pipeline."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_refresh_fx_valuation_inputs_delegates(monkeypatch):
    from hptl.dashboard import weekly_refresh as wr

    calls: list[str] = []

    def fake_refresh():
        calls.append("refresh")
        return {"generated_at": "2026-06-28T12:00:00+00:00", "sources": []}

    monkeypatch.setattr(wr, "refresh_fx_futures_data", fake_refresh, raising=False)
    monkeypatch.setattr(
        "hptl.valuation.fx_futures_data_refresh.refresh_fx_futures_data",
        fake_refresh,
    )
    out = wr.refresh_fx_valuation_inputs()
    assert calls == ["refresh"]
    assert "generated_at" in out


def test_rebuild_pillar_exports_runs_fx_refresh_before_valuation(monkeypatch):
    from hptl.dashboard import weekly_refresh as wr

    order: list[str] = []

    monkeypatch.setattr(
        wr,
        "refresh_fx_valuation_inputs",
        lambda: order.append("fx_refresh") or {"generated_at": "2026-06-28T12:00:00+00:00"},
    )
    monkeypatch.setattr(
        "hptl.data_sources.metals_driver_ingest.ingest_gold_etf_holdings",
        lambda: order.append("gold_etf"),
    )
    monkeypatch.setattr(
        "hptl.data_sources.cb_gold_purchases_ingest.ingest_cb_gold_purchases",
        lambda: order.append("cb_gold"),
    )
    monkeypatch.setattr(
        "hptl.valuation.export.write_valuation_exports",
        lambda payload=None: order.append("valuation_exports") or {"public": Path("valuation_latest.json")},
    )
    monkeypatch.setattr("hptl.location.export.write_location_exports", lambda: order.append("location"))
    monkeypatch.setattr(
        "hptl.seasonality.export.build_seasonality_latest",
        lambda: order.append("seasonality_build") or {},
    )
    monkeypatch.setattr(
        "hptl.seasonality.export.write_seasonality_exports",
        lambda doc: order.append("seasonality_write"),
    )

    meta = wr.rebuild_pillar_exports()
    assert order.index("fx_refresh") < order.index("valuation_exports")
    assert meta.get("fx_refresh_at")
    assert meta.get("fx_valuation_exports_at")


def test_write_valuation_exports_regenerates_fx_artifacts(monkeypatch, tmp_path):
    from hptl.valuation import export as ve

    calls: list[str] = []

    monkeypatch.setattr(ve, "DATA_OUT", tmp_path / "data" / "valuation_latest.json")
    monkeypatch.setattr(ve, "PROCESSED_OUT", tmp_path / "processed" / "valuation_latest.json")
    monkeypatch.setattr(ve, "PUBLIC_OUT", tmp_path / "public" / "valuation_latest.json")
    monkeypatch.setattr(ve, "DIST_OUT", tmp_path / "dist" / "valuation_latest.json")

    futures_path = tmp_path / "public" / "currency_futures_ive_latest.json"
    v3_path = tmp_path / "public" / "fx_valuation_v3_latest.json"

    def fake_futures_export():
        calls.append("futures")
        futures_path.parent.mkdir(parents=True, exist_ok=True)
        futures_path.write_text(
            json.dumps(
                {
                    "generated_at": "2026-06-28T12:00:00+00:00",
                    "instruments": {
                        "Euro FX / 6E": {
                            "valuation_pct": 2.5,
                            "valuation_pct_raw": 2.5123,
                            "valuation_diagnostics": {
                                "spot_date": "2026-06-20",
                                "freshness_status": "stale",
                                "stale_reason": "stale CPI (EUR.cpi_yoy)",
                                "input_latest_dates": {"spot": "2026-06-20"},
                            },
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        return {"public_json": futures_path}

    def fake_v3_export():
        calls.append("v3")
        v3_path.write_text(
            json.dumps({"generated_at": "2026-06-28T12:00:00+00:00", "pairs": {}}),
            encoding="utf-8",
        )
        return {"public_json": v3_path, "audit_json": tmp_path / "audit.json"}

    monkeypatch.setattr(
        "hptl.valuation.currency_futures_ive_v1.write_currency_futures_ive_export",
        fake_futures_export,
    )
    monkeypatch.setattr("hptl.valuation.fx_v3_audit.write_fx_v3_audit_artifacts", fake_v3_export)
    monkeypatch.setattr(ve, "build_valuation_latest", lambda refresh_fx_v3=False: {"generated_at": "t", "instruments": {}})

    paths = ve.write_valuation_exports()
    assert calls == ["futures", "v3"]
    assert paths["currency_futures_public"] == futures_path
    assert paths["fx_v3_public"] == v3_path
    assert futures_path.exists()
    assert v3_path.exists()


def test_fx_valuation_diagnostics_include_stale_reason():
    from hptl.valuation.fx_valuation_diagnostics import build_fx_valuation_diagnostics

    diag = build_fx_valuation_diagnostics(
        valuation_date="2026-06-28",
        spot_date="2026-06-12",
        spot=1.15,
        fair_value=1.12,
        raw_gap_pct_unrounded=2.678,
        gap_pct_rounded=2.68,
        input_latest_dates={"spot": "2026-06-12", "EUR.y2": "2026-06-18"},
        cache_generated_at="2026-06-28T12:00:00+00:00",
        source_file="test.json",
        stale_inputs=["EUR.cpi_yoy"],
        price_stale=True,
    )
    assert diag["freshness_status"] == "stale"
    assert diag["inputs_stale"] is True
    assert diag["spot_date"] == "2026-06-12"
    assert diag["raw_gap_pct_unrounded"] == 2.678
    assert "stale spot" in (diag.get("stale_reason") or "")
    assert any("CPI" in w for w in diag.get("stale_warnings") or [])


def test_currency_futures_stale_keeps_valuation_visible():
    from hptl.valuation.currency_futures_ive_v1 import compute_futures_instrument

    block = compute_futures_instrument("DX")
    if block.get("model_status") == "DATA_STALE" and block.get("valuation_pct_raw") is not None:
        assert block.get("valuation_pct") is not None
        assert block.get("publishable") is True
        assert block.get("inputs_stale") is True
        diag = block.get("valuation_diagnostics") or {}
        assert diag.get("freshness_status") in ("stale", "fresh", "missing")
        assert "spot_date" in diag or diag.get("spot_date") is None
