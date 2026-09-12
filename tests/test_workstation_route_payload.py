"""Workstation route contract — core COT hard-gates, derived gaps warn safely."""

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
    assert latest is not None
    assert 0 <= float(latest["commercial"]["percentile"]) <= 100
    assert math.isfinite(float(latest["cross"]["comm_nc_spread"]))
    assert body["workstation"]["derived_integrity"]["status"] == "ok"


def test_missing_derived_market_warns_but_keeps_valid_history():
    body, status = build_workstation_route_payload(
        "Crude Oil / CL",
        weekly_inspector={"markets": {}},
        cot_3y={
            "markets": {
                "Crude Oil / CL": {
                    "series": [{"date": "2026-07-21", "commercial_net": 1}]
                }
            }
        },
    )
    assert status == 200
    assert body["status"] == "ok"
    integrity = body["workstation"]["derived_integrity"]
    assert integrity["status"] == "warning"
    assert "weekly_inspector.market" in integrity["missing_fields"]
    assert body["workstation"]["historical_rows"] == 1


def test_incomplete_derived_week_warns_instead_of_blanking_history():
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
    assert status == 200
    assert body["status"] == "ok"
    integrity = body["workstation"]["derived_integrity"]
    assert integrity["status"] == "warning"
    assert any("percentile" in f for f in integrity["missing_fields"])


def test_non_finite_derived_snapshot_warns_instead_of_blanking_history():
    weekly = {
        "markets": {
            "Crude Oil / CL": {
                "available": True,
                "weeks": [
                    {
                        "date": "2026-07-21",
                        "commercial": {
                            "net": 1,
                            "weekly_change": 0,
                            "four_week_change": 0,
                            "twelve_week_change": 0,
                            "percentile": float("nan"),
                            "percentile_change_1w": None,
                            "percentile_change_4w": None,
                            "percentile_change_12w": None,
                            "percentile_observation_count": 1,
                            "direction": "unknown",
                            "temperature": "unknown",
                            "is_extreme": False,
                        },
                        "noncommercial": {
                            "net": 1,
                            "weekly_change": 0,
                            "four_week_change": 0,
                            "twelve_week_change": 0,
                            "percentile": 50,
                            "percentile_change_1w": 0,
                            "percentile_change_4w": 0,
                            "percentile_change_12w": 0,
                            "percentile_observation_count": 1,
                            "direction": "stable",
                            "temperature": "neutral",
                            "is_extreme": False,
                        },
                        "nonreportable": {
                            "net": 1,
                            "weekly_change": 0,
                            "four_week_change": 0,
                            "twelve_week_change": 0,
                            "percentile": 50,
                            "percentile_change_1w": 0,
                            "percentile_change_4w": 0,
                            "percentile_change_12w": 0,
                            "percentile_observation_count": 1,
                            "direction": "stable",
                            "temperature": "neutral",
                            "is_extreme": False,
                        },
                        "cross": {
                            "commercial_percentile": float("nan"),
                            "noncommercial_percentile": 50,
                            "nonreportable_percentile": 50,
                            "comm_nc_spread": 0,
                            "comm_nc_spread_percentile": 50,
                            "comm_nc_spread_change_1w": 0,
                            "comm_nc_spread_change_4w": 0,
                            "comm_nr_spread": 0,
                            "comm_nr_spread_percentile": 50,
                            "relationship": "mixed",
                            "flow": "stable",
                        },
                    }
                ],
            }
        }
    }
    body, status = build_workstation_route_payload(
        "Crude Oil / CL",
        weekly_inspector=weekly,
        cot_3y={"markets": {"Crude Oil / CL": {"series": [{"date": "2026-07-21"}]}}},
    )
    assert status == 200
    assert body["status"] == "ok"
    assert body["workstation"]["latest_week"] is None
    integrity = body["workstation"]["derived_integrity"]
    assert integrity["status"] == "warning"
    assert any("latest_week_json" in f for f in integrity["missing_fields"])


def test_missing_core_history_remains_hard_integrity_error():
    body, status = build_workstation_route_payload(
        "Crude Oil / CL",
        weekly_inspector={"markets": {}},
        cot_3y={"markets": {"Crude Oil / CL": {"series": []}}},
    )
    assert status == 422
    assert body["status"] == "integrity_error"
    assert body["stage"] == "core_cot_history"
    assert "cot_3y.series" in body["missing_fields"]


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
