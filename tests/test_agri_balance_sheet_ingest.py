"""Tests for USDA PSD balance sheet ingest."""

from __future__ import annotations

import json

import pytest

from hptl.valuation.agri_balance_sheet_ingest import (
    _release_date,
    ingest_priority_balance_sheets,
    validate_balance_sheet,
)
from hptl.valuation.agri_fundamental_valuation import compute_agri_valuation
from hptl.valuation.usda_psd_client import parse_psd_rows

SAMPLE_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getDatabyCommodityResponse xmlns="http://www.fas.usda.gov/wsfaspsd/">
      <getDatabyCommodityResult>
        <Commodity>
          <Commodity_code>0440000</Commodity_code>
          <Country_Code>US</Country_Code>
          <Calendar_Year>2026</Calendar_Year>
          <Month>06</Month>
          <Market_Year>2025</Market_Year>
          <Attribute_Id>176</Attribute_Id>
          <Attribute_Description>Ending Stocks</Attribute_Description>
          <Value>54481.0</Value>
        </Commodity>
        <Commodity>
          <Commodity_code>0440000</Commodity_code>
          <Country_Code>US</Country_Code>
          <Calendar_Year>2026</Calendar_Year>
          <Month>06</Month>
          <Market_Year>2025</Market_Year>
          <Attribute_Id>125</Attribute_Id>
          <Attribute_Description>Total Consumption</Attribute_Description>
          <Value>333517.0</Value>
        </Commodity>
        <Commodity>
          <Commodity_code>0440000</Commodity_code>
          <Country_Code>BR</Country_Code>
          <Calendar_Year>2026</Calendar_Year>
          <Month>06</Month>
          <Market_Year>2025</Market_Year>
          <Attribute_Id>176</Attribute_Id>
          <Value>1.0</Value>
        </Commodity>
      </getDatabyCommodityResult>
    </getDatabyCommodityResponse>
  </soap:Body>
</soap:Envelope>"""


def test_release_date_maps_month_zero_to_july():
    assert _release_date("2001", "00") == "2001-07-01"
    assert _release_date("2026", "06") == "2026-06-01"


def test_parse_psd_rows_us_only():
    rows = parse_psd_rows(SAMPLE_XML, country_code="US")
    assert len(rows) == 2
    assert all(r["country_code"] == "US" for r in rows)
    assert {r["attribute_id"] for r in rows} == {"176", "125"}


def test_validate_balance_sheet_schema():
    doc = {
        "market": "Corn",
        "source": "USDA WASDE/PSD",
        "ingest_status": "ok",
        "series": [
            {
                "date": "2026-06-01",
                "marketing_year": "2025/26",
                "production": 100.0,
                "ending_stocks": 10.0,
                "total_use": 90.0,
                "stocks_to_use": 0.111111,
            }
        ],
    }
    v = validate_balance_sheet(doc)
    assert v["observation_count"] == 1
    assert v["stu_observation_count"] == 1
    assert v["latest_observation_date"] == "2026-06-01"


@pytest.mark.skipif(
    not __import__("os").environ.get("HPTL_RUN_USDA_LIVE"),
    reason="Set HPTL_RUN_USDA_LIVE=1 for live USDA SOAP ingest test",
)
def test_live_ingest_priority_markets(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "hptl.valuation.usda_psd_client.DEFAULT_CACHE_DIR",
        tmp_path / "raw",
    )
    monkeypatch.setattr(
        "hptl.valuation.agri_fundamental_valuation.BALANCE_SHEET_DIR",
        tmp_path / "processed",
    )
    report = ingest_priority_balance_sheets(force_refresh=True)
    assert report["markets_written"], report
    for market in ("Soybeans", "Wheat", "Corn"):
        row = report["instruments"][market]
        assert row["observation_count"] >= 12, market
        assert row["stu_observation_count"] >= 12, market
        path = tmp_path / "processed" / f"{market}.json"
        assert path.exists()
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert doc["source"] == "USDA WASDE/PSD"


@pytest.mark.skipif(
    not __import__("os").environ.get("HPTL_RUN_USDA_LIVE"),
    reason="Set HPTL_RUN_USDA_LIVE=1 for live USDA valuation wiring test",
)
def test_valuation_wires_after_ingest(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "hptl.valuation.usda_psd_client.DEFAULT_CACHE_DIR",
        tmp_path / "raw",
    )
    monkeypatch.setattr(
        "hptl.valuation.agri_fundamental_valuation.BALANCE_SHEET_DIR",
        tmp_path / "processed",
    )
    ingest_priority_balance_sheets(force_refresh=True)
    val = compute_agri_valuation(market="Corn")
    assert val.get("balance_sheet_observations", 0) >= 12
    if val.get("wired"):
        assert val.get("fair_value") is not None
        assert val.get("deviation_pct") is not None
    else:
        assert val.get("unavailable_reason")
