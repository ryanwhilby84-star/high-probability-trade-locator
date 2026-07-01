"""Tests for Phase 1G-B FX futures data pipeline fixes."""
from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from hptl.fx.ecb_adapter import fetch as fetch_ecb
from hptl.fx.rate_adapter_base import CACHE_DIR
from hptl.prices.coverage import load_price_coverage, select_price_source
from hptl.prices.fred_prices import fetch_fred_instrument
from hptl.prices.price_store import merge_fetched_into_production
from hptl.valuation.currency_futures_ive_v1 import build_currency_futures_ive_export


def test_dx_fred_mapping_in_coverage():
    cov = load_price_coverage()
    assert "US Dollar Index / DX" in (cov.get("fred_supported") or [])
    assert select_price_source("US Dollar Index / DX", cov) == "fred"


def test_validated_symbols_publish_values():
    doc = build_currency_futures_ive_export()
    for sym in ("6A", "6C", "6J"):
        block = doc["by_symbol"][sym]
        assert block["model_status"] == "VALIDATED", block.get("blocker_reason")
        assert block["wired"] is True
        assert block["valuation_pct"] is not None
        assert block.get("blocker_reason") == "VALIDATED"


def test_blocked_symbols_have_precise_reasons_not_na():
    doc = build_currency_futures_ive_export()
    for sym in doc["by_symbol"]:
        block = doc["by_symbol"][sym]
        reason = block.get("blocker_reason") or ""
        if block.get("wired"):
            assert reason == "VALIDATED"
        else:
            assert reason not in ("", "N/A", "—", "Valuation unavailable.")
            assert block["model_status"] in ("DATA_STALE", "MODEL_INCOMPLETE", "DATA_MISSING")


def test_unsupported_instrument_does_not_overwrite_stored_history():
    existing = {
        "instrument_id": "US Dollar Index / DX",
        "price": {"mid": 120.0, "as_of": "2026-06-05"},
        "daily": [{"date": "2026-06-05", "close": 120.0}],
        "weekly": [],
        "error": None,
    }
    fetched = {
        "instrument_id": "US Dollar Index / DX",
        "price": None,
        "daily": [],
        "weekly": [],
        "range_52w": None,
        "history": None,
        "error": "unsupported_instrument",
    }
    merged, _ = merge_fetched_into_production(existing, fetched, fetched_via="none")
    assert len(merged["daily"]) == 1
    assert merged.get("error") is None


def test_ecb_live_and_history_caches_are_separate(tmp_path, monkeypatch):
    cache = tmp_path / "fx_rates"
    cache.mkdir(parents=True)
    monkeypatch.setattr("hptl.fx.rate_adapter_base.CACHE_DIR", cache)

    history_csv = io.StringIO()
    writer = csv.DictWriter(history_csv, fieldnames=["TIME_PERIOD", "OBS_VALUE"])
    writer.writeheader()
    for i in range(260):
        writer.writerow({"TIME_PERIOD": f"2025-{(i % 12) + 1:02d}-01", "OBS_VALUE": "2.5"})
    (cache / "eur_2y_history.txt").write_text(history_csv.getvalue(), encoding="utf-8")

    live_csv = "TIME_PERIOD,OBS_VALUE\n2026-06-18,2.55\n"
    (cache / "eur_2y_live.txt").write_text(live_csv, encoding="utf-8")

    from hptl.fx import fx_macro_history as hist

    series, src = hist.load_eur_y2_history()
    assert len(series) >= 260
    assert "2026-06-18" in series
    assert "history" in src or "eur_2y" in src


def test_ecb_adapter_writes_live_cache_only(monkeypatch, tmp_path):
    cache = tmp_path / "fx_rates"
    cache.mkdir(parents=True)
    monkeypatch.setattr("hptl.fx.rate_adapter_base.CACHE_DIR", cache)

    live_body = "TIME_PERIOD,OBS_VALUE\n2026-06-18,2.55\n"
    history_body = io.StringIO()
    w = csv.DictWriter(history_body, fieldnames=["TIME_PERIOD", "OBS_VALUE"])
    w.writeheader()
    w.writerow({"TIME_PERIOD": "2026-01-01", "OBS_VALUE": "2.0"})
    (cache / "eur_2y_history.txt").write_text(history_body.getvalue(), encoding="utf-8")

    def fake_fetch(url, cache_key, **kwargs):
        assert cache_key == "eur_2y_live"
        return live_body

    monkeypatch.setattr("hptl.fx.ecb_adapter.fetch_text", fake_fetch)
    rate = fetch_ecb()
    assert rate.y2.value is not None
    assert (cache / "eur_2y_live.txt").exists()
    hist = (cache / "eur_2y_history.txt").read_text(encoding="utf-8")
    assert hist.count("TIME_PERIOD") == 1 or "2026-01-01" in hist


def test_dx_fred_fetch_preserves_existing_bars(monkeypatch):
    from hptl.prices import fred_prices as fp

    existing_daily = [{"date": "2026-06-05", "close": 120.0831}]
    monkeypatch.setattr(
        fp,
        "load_instrument_record_internal",
        lambda iid: {"daily": existing_daily},
    )
    monkeypatch.setattr(
        fp,
        "fred_series_to_daily_bars",
        lambda sid, observation_start="2016-01-01": [
            {"date": "2026-06-05", "open": 119.5, "high": 119.5, "low": 119.5, "close": 119.5, "volume": None},
            {"date": "2026-06-12", "open": 119.5073, "high": 119.5073, "low": 119.5073, "close": 119.5073, "volume": None},
        ],
    )
    rec = fetch_fred_instrument("US Dollar Index / DX")
    assert rec.get("error") is None
    dates = [b["date"] for b in rec["daily"]]
    assert "2026-06-05" in dates
    assert "2026-06-12" in dates
    assert float(next(b["close"] for b in rec["daily"] if b["date"] == "2026-06-05")) == pytest.approx(120.0831, rel=1e-4)
