"""Workstation route contract — controlled ok / integrity_error, never unsafe JSON."""

from __future__ import annotations

import math

import pytest

from hptl.cot.workstation_route_payload import (
    JsonUnsafeError,
    audit_all_workstation_routes,
    build_workstation_route_payload,
    sanitize_for_json,
)
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS


def test_sanitize_rejects_nan():
    with pytest.raises(JsonUnsafeError):
        sanitize_for_json({"percentile": float("nan")})


def test_sanitize_rejects_infinity():
    with pytest.raises(JsonUnsafeError):
        sanitize_for_json({"v": float("inf")})


def test_sanitize_converts_datetime():
    from datetime import datetime, timezone

    out = sanitize_for_json({"t": datetime(2026, 7, 21, tzinfo=timezone.utc)})
    assert out["t"].startswith("2026-07-21")


def test_crude_oil_route_ok():
    body, status = build_workstation_route_payload("Crude Oil / CL")
    assert status == 200, body
    assert body["status"] == "ok"
    assert body["instrument_id"] == "Crude Oil / CL"
    assert body["report_date"]
    assert "workstation" in body
    latest = body["workstation"]["latest_week"]
    assert 0 <= float(latest["commercial"]["percentile"]) <= 100
    assert math.isfinite(float(latest["cross"]["comm_nc_spread"]))


def test_missing_market_returns_integrity_error_not_500():
    body, status = build_workstation_route_payload(
        "Crude Oil / CL",
        weekly_inspector={"markets": {}},
    )
    assert status == 422
    assert body["status"] == "integrity_error"
    assert body["stage"] == "derived_cot"
    assert body["missing_fields"]


def test_incomplete_week_returns_integrity_error():
    stub = {
        "markets": {
            "Crude Oil / CL": {
                "available": True,
                "rows": [
                    [
                        "2026-07-21",
                        [1, 1, 1, 1, None, None, None, None, None, 5, 10, 0],
                        [1, 1, 1, 1, 50, 0, 0, 0, 10, 2, 9, 0],
                        [1, 1, 1, 1, 50, 0, 0, 0, 10, 2, 9, 0],
                        [None, 50, 50, None, None, None, None, None, None, 4, 7],
                    ]
                ],
            }
        }
    }
    body, status = build_workstation_route_payload(
        "Crude Oil / CL",
        weekly_inspector=stub,
        cot_3y={"markets": {"Crude Oil / CL": {"series": [{"date": "2026-07-21"}]}}},
    )
    assert status == 422
    assert body["status"] == "integrity_error"
    assert any("percentile" in f for f in body["missing_fields"])


def test_all_26_routes_ok_or_controlled():
    report = audit_all_workstation_routes()
    assert report["summary"]["http_500"] == 0
    assert report["summary"]["total"] == len(LEGACY_COT_MARKETS)
    for row in report["instruments"]:
        assert row["http_status"] in (200, 422)
        assert row["response_status"] in ("ok", "integrity_error")
        assert row["payload_valid"] is True
    assert report["summary"]["http_200_ok"] == 26
    assert report["summary"]["http_422_integrity"] == 0
    assert report["summary"]["overall_status"] == "PASS"
