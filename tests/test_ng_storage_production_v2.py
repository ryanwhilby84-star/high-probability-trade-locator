"""Tests for Natural Gas ng_storage_production_v2 wiring."""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from hptl.valuation.energy_ng_drivers import (
    _monthly_production_yoy,
    build_ng_driver_bundle,
)
from hptl.valuation.ng_storage_production_v2 import (
    MODEL_V1,
    MODEL_V2,
    REJECTED_PRODUCTION_TRANSFORMS,
    VALIDATED_DRIVERS_V2,
    build_natural_gas_valuation_document,
    compute_ng_storage_production_v2,
    production_yoy_freshness,
)

ROOT = Path(__file__).resolve().parents[1]


def test_monthly_yoy_is_point_in_time_safe() -> None:
    monthly = {
        "2023-01-15": 100.0,
        "2023-02-15": 101.0,
        "2024-01-15": 110.0,
        "2024-02-15": 99.0,
    }
    yoy = _monthly_production_yoy(monthly)
    assert "2023-01-15" not in yoy
    assert abs(yoy["2024-01-15"] - 10.0) < 1e-9
    assert abs(yoy["2024-02-15"] - (-100.0 * 2.0 / 101.0)) < 1e-9


def test_production_freshness_gates() -> None:
    ok = production_yoy_freshness(
        as_of_week="2026-07-30",
        observation_date="2026-05-15",
        yoy_value=2.1,
    )
    assert ok["usable"] is True

    stale = production_yoy_freshness(
        as_of_week="2026-07-30",
        observation_date="2026-01-15",
        yoy_value=2.1,
    )
    assert stale["usable"] is False
    assert "stale" in stale["reason"]

    missing = production_yoy_freshness(
        as_of_week="2026-07-30",
        observation_date="2026-05-15",
        yoy_value=None,
    )
    assert missing["usable"] is False

    proxy = production_yoy_freshness(
        as_of_week="2026-07-30",
        observation_date="2026-05-15",
        yoy_value=2.1,
        using_proxy=True,
    )
    assert proxy["usable"] is False


def test_v2_uses_exactly_storage_and_production_yoy() -> None:
    block = compute_ng_storage_production_v2()
    assert block.get("wired") is True
    assert block.get("production_transformation") == "production_yoy_pct"
    assert block.get("raw_level_used_in_fair_value") is False
    assert "dry_gas_production" not in (block.get("validated_drivers") or [])
    assert "dry_gas_production_level" not in (block.get("validated_drivers") or [])

    if block.get("active_model") == MODEL_V2:
        assert block["validated_drivers"] == VALIDATED_DRIVERS_V2
        feats = (block.get("regression") or {}).get("features") or {}
        assert set(feats) == set(VALIDATED_DRIVERS_V2)
        for rejected in REJECTED_PRODUCTION_TRANSFORMS:
            assert rejected not in feats


def test_rejected_transforms_absent_from_active_features() -> None:
    doc = build_natural_gas_valuation_document()
    inst = doc["instrument"]
    active = set(inst.get("active_features") or [])
    for rejected in (
        "raw_level",
        "seasonal_deviation",
        "trailing_zscore_156",
        "chg_4w",
        "chg_12w",
        "v1_fullsample_zscore",
        "dry_gas_production",
    ):
        assert rejected not in active
    assert inst.get("production_transformation") == "production_yoy_pct"
    assert doc["summary"]["raw_level_used_in_fair_value"] is False


def test_contribution_arithmetic_reconciles() -> None:
    block = compute_ng_storage_production_v2()
    contrib = block.get("contribution_breakdown") or {}
    assert contrib.get("reconciliation_ok") is True
    fair = contrib.get("reconstructed_fair_value")
    assert fair is not None
    assert abs(float(fair) - float(block["fair_value"])) < 1e-6
    intercept = contrib["intercept_log_contribution"]
    logs = [intercept] + [d["log_contribution"] for d in contrib.get("drivers") or []]
    # Rounded display fields; reconstructed fair / reconciliation_ok are authoritative.
    assert abs(sum(logs) - contrib["sum_log_contributions"]) < 1e-5
    assert abs(math.exp(contrib["reconstructed_log_fair"]) - fair) < 1e-3


def test_v1_benchmark_always_available() -> None:
    block = compute_ng_storage_production_v2()
    v1 = block.get("v1_benchmark") or {}
    assert v1.get("model_id") == MODEL_V1
    assert v1.get("fair_value") is not None
    assert block.get("v1_fair_value") == v1.get("fair_value")
    assert set(v1.get("validated_drivers") or []) == {"storage_surplus_bcf"}


def test_valid_production_activates_v2_when_fresh() -> None:
    bundle = build_ng_driver_bundle()
    yoy = bundle.features.get("production_yoy_pct") or []
    if not yoy or yoy[-1] is None:
        pytest.skip("production YoY unavailable in local cache")
    block = compute_ng_storage_production_v2()
    age = (block.get("v2_model") or {}).get("production_freshness", {}).get("age_days")
    if age is not None and age <= 100:
        assert block["active_model"] == MODEL_V2
        assert block["fallback_to_v1"] is False
        assert block["validated_drivers"] == VALIDATED_DRIVERS_V2


def test_stale_production_falls_back_to_v1(monkeypatch: pytest.MonkeyPatch) -> None:
    import hptl.valuation.ng_storage_production_v2 as mod

    monkeypatch.setattr(mod, "MAX_PRODUCTION_STALENESS_DAYS", 10)
    block = compute_ng_storage_production_v2()
    # With a 10-day cadence, May observation vs July tip must fall back.
    assert block["active_model"] == MODEL_V1
    assert block["fallback_to_v1"] is True
    assert block["validated_drivers"] == ["storage_surplus_bcf"]
    assert block["fair_value"] == block["v1_fair_value"]
    assert "FALLBACK" in " ".join(block.get("freshness_warnings") or [])


def test_no_forward_fill_past_usable_cadence_in_metadata() -> None:
    block = compute_ng_storage_production_v2()
    assert block.get("max_production_staleness_days") == 100
    assert block.get("production_source_cadence") == "monthly"
    # Tip YoY must reference a real observation date, not an invented week.
    if block.get("production_yoy_value") is not None:
        assert block.get("production_observation_date")


def test_payload_model_version_fields() -> None:
    doc = build_natural_gas_valuation_document()
    assert doc.get("version") == 4
    assert doc.get("valuation_phase") == "Validated Two-Driver Fair Value"
    assert doc.get("engine") in {MODEL_V1, MODEL_V2}
    assert "active_model" in doc
    inst = doc["instrument"]
    assert inst.get("headline")
    assert "Institutional Fair Value" not in str(inst.get("headline"))


def test_phase2_audit_preserved() -> None:
    audit = (
        ROOT
        / "data"
        / "audits"
        / "ng_driver_validation_phase2_production"
        / "phase2_production_validation.json"
    )
    assert audit.exists()
    block = compute_ng_storage_production_v2()
    assert "phase2_production_validation.json" in str(block.get("phase2_audit_ref") or "")


def test_standalone_export_module_isolated_from_cot() -> None:
    import hptl.valuation.energy_ng_valuation_export as export_mod
    import inspect

    src = inspect.getsource(export_mod)
    assert "ng_storage_production_v2" in src
    assert "run_weekly_cot" not in src
    assert "HPTL_SKIP_VALUATION" not in src
    assert "confluence" not in src.lower()
