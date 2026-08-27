"""Legacy COT reset — reconciliation, latest, audit, regression instruments."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from hptl.cot.legacy_cot import (
    CANONICAL_LEGACY_CODE,
    build_legacy_latest_and_audit,
    build_legacy_reconciliation,
    run_legacy_cot_reset,
)

REGRESSION = [
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Gold",
    "Silver",
    "Copper / HG",
    "Wheat",
    "Corn",
    "Soybeans",
    "Sugar",
    "Coffee",
    "Cocoa",
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
    "Euro FX / 6E",
    "British Pound / 6B",
    "Japanese Yen / 6J",
    "Australian Dollar / 6A",
    "Canadian Dollar / 6C",
    "NZ Dollar / 6N",
    "Swiss Franc / 6S",
    "Bitcoin",
    "US Dollar Index / DX",
]


@pytest.fixture(scope="module")
def legacy_bundle():
    return run_legacy_cot_reset(year=2026, weeks=13)


def test_all_cot_markets_in_reconciliation(legacy_bundle):
    recon = legacy_bundle["reconciliation"]
    assert recon["instrument_count"] == 25
    for iid in CANONICAL_LEGACY_CODE:
        assert iid in recon["instruments"]


def test_regression_instruments_pass(legacy_bundle):
    latest = legacy_bundle["latest"]
    for iid in REGRESSION:
        inst = latest["instruments"][iid]
        assert inst.get("mapping_status") == "PASS", f"{iid} not PASS"
        nc = inst["groups"]["noncommercials"]["weeks"]
        assert len(nc) >= 1
        w = nc[-1]
        assert w["long"] is not None and w["short"] is not None
        assert w["parser"] == "hptl.cot.legacy_cot"


def test_nq_legacy_noncommercial_not_tff_leveraged(legacy_bundle):
    latest = legacy_bundle["latest"]["instruments"]["NASDAQ / NQ"]
    assert latest["selected_cftc_code"] == "209742"
    nc = latest["groups"]["noncommercials"]["weeks"][-1]
    assert nc["long"] != 52861 or nc["short"] != 104540


def test_audit_dashboard_equals_raw(legacy_bundle):
    audit = legacy_bundle["audit"]
    for iid in REGRESSION:
        inst = audit["instruments"].get(iid)
        assert inst is not None
        assert inst.get("audit_pass") is True
        for c in inst["checks"]:
            if c.get("match") is not None:
                assert c["match"] is True
                assert c.get("dashboard_value") == c.get("raw_cftc_value")


def test_exports_exist(legacy_bundle):
    assert Path("data/legacy_cot_reconciliation.json").exists()
    assert Path("data/legacy_cot_latest.json").exists()
    assert Path("data/legacy_cot_audit.json").exists()
    assert Path("data/exports/legacy_cot_reset_report.json").exists()


def test_deliverable_report_counts(legacy_bundle):
    report = legacy_bundle["report"]
    assert report["total_instruments_checked"] == 24
    assert report["pass_count"] == 24
    assert report["fail_count"] == 0
    assert report["tests_passed"] is True
