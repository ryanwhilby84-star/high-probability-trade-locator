"""Natural Gas Valuation Workstation — historical PIT reconstruction tests."""

from __future__ import annotations

import ast
import math
from pathlib import Path

import pytest

from hptl.valuation.energy_ng_drivers import _asof_series_with_obs_date
from hptl.valuation.ng_valuation_workstation_history import (
    EVENT_COOLDOWN_WEEKS,
    FROZEN_V2_BETA_STORAGE,
    FROZEN_V2_BETA_YOY,
    FROZEN_V2_INTERCEPT,
    FROZEN_LABEL,
    MIN_TRAIN,
    WF_LABEL,
    apply_frozen_v2,
    apply_walk_forward,
    build_event_study,
    _bucket,
    _dev_pct,
    _fair,
    _fwd_return,
    _mfe_mae,
)

ROOT = Path(__file__).resolve().parents[1]
MOD_PATH = ROOT / "src" / "hptl" / "valuation" / "ng_valuation_workstation_history.py"


def _synth_rows(n: int = 200, *, start_price: float = 2.5) -> list[dict]:
    """Synthetic PIT-eligible weekly rows with strictly lagged obs dates."""
    from datetime import date, timedelta

    rows = []
    for i in range(n):
        d = date(2018, 1, 5) + timedelta(weeks=i)
        week = d.isoformat()
        storage_obs = (d - timedelta(days=2)).isoformat()
        prod_obs = (d - timedelta(days=45)).isoformat()
        surplus = 50.0 * math.sin(i / 11) + 10.0
        yoy = 2.0 * math.cos(i / 17)
        # Induce mild inverse relationship for walk-forward fit
        price = math.exp(
            1.2 - 0.0008 * surplus - 0.02 * yoy + 0.02 * math.sin(i / 9)
        )
        _ = start_price
        rows.append(
            {
                "model_week": week,
                "market_price": round(price, 6),
                "storage_surplus_bcf": round(surplus, 6),
                "storage_observation_date": storage_obs,
                "storage_as_of_date": storage_obs,
                "storage_age_days": 2,
                "production_yoy_pct": round(yoy, 6),
                "production_observation_date": prod_obs,
                "production_as_of_date": prod_obs,
                "production_age_days": 45,
                "inputs_available_as_of_week": True,
                "storage_ok": True,
                "production_ok": True,
                "quality_status": "OK",
                "active_model_version": "ng_storage_production_v2",
                "using_production_proxy": False,
            }
        )
    return rows


def test_point_in_time_storage_alignment_no_future() -> None:
    series = {"2024-01-04": 100.0, "2024-01-11": 110.0, "2024-01-18": 120.0}
    weeks = ["2024-01-05", "2024-01-12", "2024-01-19"]
    vals, obs = _asof_series_with_obs_date(series, weeks)
    assert vals["2024-01-05"] == 100.0
    assert obs["2024-01-05"] == "2024-01-04"
    assert vals["2024-01-12"] == 110.0
    assert obs["2024-01-12"] == "2024-01-11"
    # Never uses 2024-01-18 for 2024-01-12
    assert obs["2024-01-12"] < "2024-01-12"
    assert vals["2024-01-19"] == 120.0


def test_point_in_time_production_alignment_no_future() -> None:
    monthly = {"2023-11-15": 1.5, "2023-12-15": 2.0, "2024-01-15": 2.5}
    weeks = ["2023-12-01", "2024-01-05", "2024-01-12"]
    vals, obs = _asof_series_with_obs_date(monthly, weeks)
    assert obs["2023-12-01"] == "2023-11-15"
    assert vals["2023-12-01"] == 1.5
    assert obs["2024-01-05"] == "2023-12-15"
    # Jan 15 release not available on Jan 5 or Jan 12
    assert obs["2024-01-12"] == "2023-12-15"
    assert "2024-01-15" not in {obs[w] for w in weeks}


def test_no_future_data_leakage_in_walk_forward() -> None:
    rows = _synth_rows(MIN_TRAIN + 20)
    wf, meta = apply_walk_forward(rows)
    for m in meta:
        assert m["training_window"]["end"] < m["model_week"]
    for r in wf:
        tw = r.get("training_window")
        if r.get("fair_value") is not None and tw and tw.get("end"):
            assert tw["end"] < r["model_week"]


def test_frozen_versus_walk_forward_separation() -> None:
    rows = _synth_rows(MIN_TRAIN + 5)
    frozen = apply_frozen_v2(rows)
    wf, _ = apply_walk_forward(rows)
    frozen_ok = [r for r in frozen if r.get("fair_value") is not None]
    wf_ok = [r for r in wf if r.get("fair_value") is not None]
    assert len(frozen_ok) == len(rows)
    assert len(wf_ok) == 5  # only after MIN_TRAIN prior eligible weeks
    assert all(r["model_type"] == FROZEN_LABEL for r in frozen_ok)
    assert all(r["model_type"] == WF_LABEL for r in wf_ok)
    # Frozen coefficients are constant tip values
    for r in frozen_ok:
        assert r["coefficients"]["intercept"] == FROZEN_V2_INTERCEPT
        assert r["coefficients"]["storage_surplus_bcf"] == FROZEN_V2_BETA_STORAGE
        assert r["coefficients"]["production_yoy_pct"] == FROZEN_V2_BETA_YOY
    # Walk-forward coefficients differ from frozen tip (except by chance)
    assert any(
        r["coefficients"]["intercept"] != FROZEN_V2_INTERCEPT for r in wf_ok
    )


def test_fair_value_arithmetic() -> None:
    s, y = 172.6, 2.1
    fair = _fair(FROZEN_V2_INTERCEPT, FROZEN_V2_BETA_STORAGE, FROZEN_V2_BETA_YOY, s, y)
    expected = math.exp(
        FROZEN_V2_INTERCEPT
        + FROZEN_V2_BETA_STORAGE * s
        + FROZEN_V2_BETA_YOY * y
    )
    assert abs(fair - expected) < 1e-12


def test_deviation_calculation() -> None:
    assert abs(_dev_pct(110.0, 100.0) - 10.0) < 1e-12
    assert abs(_dev_pct(90.0, 100.0) - (-10.0)) < 1e-12


def test_valuation_bucket_assignment() -> None:
    assert _bucket(-20) == "materially_undervalued"
    assert _bucket(-15) == "materially_undervalued"
    assert _bucket(-10) == "undervalued"
    assert _bucket(0) == "near_fair"
    assert _bucket(10) == "overvalued"
    assert _bucket(15) == "materially_overvalued"
    assert _bucket(None) is None


def test_forward_return_calculations() -> None:
    prices = [100.0, 102.0, 98.0, 110.0]
    assert abs(_fwd_return(prices, 0, 1) - 2.0) < 1e-12
    assert abs(_fwd_return(prices, 0, 2) - (-2.0)) < 1e-12
    assert abs(_fwd_return(prices, 0, 3) - 10.0) < 1e-12
    assert _fwd_return(prices, 2, 2) is None


def test_mfe_mae_calculations() -> None:
    prices = [100.0, 105.0, 95.0, 110.0]
    mfe, mae = _mfe_mae(prices, 0, 3)
    assert abs(mfe - 10.0) < 1e-12
    assert abs(mae - (-5.0)) < 1e-12


def test_event_cooldown_logic() -> None:
    # Build synthetic valuation path with prolonged undervalued episode
    rows = []
    from datetime import date, timedelta

    buckets = (
        ["near_fair"] * 5
        + ["undervalued"] * 8  # prolonged episode → 1 event
        + ["near_fair"] * 4  # cooldown
        + ["undervalued"] * 2  # second event
    )
    for i, b in enumerate(buckets):
        d = date(2020, 1, 3) + timedelta(weeks=i)
        rows.append(
            {
                "model_week": d.isoformat(),
                "market_price": 2.0 + 0.01 * i,
                "fair_value": 2.5,
                "valuation_bucket": b,
                "deviation_pct": -10.0 if b == "undervalued" else 0.0,
            }
        )
    study = build_event_study(rows, cooldown=EVENT_COOLDOWN_WEEKS)
    und_events = [
        e for e in study["events_sample"] if e["bucket"] == "undervalued"
    ]
    # With cooldown=4, first entry at week index 5, second after 4 weeks outside
    assert len(und_events) == 2
    assert study["cooldown_weeks"] == 4


def test_unavailable_and_fallback_periods() -> None:
    rows = _synth_rows(3)
    rows[1]["inputs_available_as_of_week"] = False
    rows[1]["storage_ok"] = True
    rows[1]["production_ok"] = False
    rows[1]["quality_status"] = "FALLBACK_V1_ELIGIBLE"
    rows[1]["production_yoy_pct"] = None
    rows[2]["inputs_available_as_of_week"] = False
    rows[2]["storage_ok"] = False
    rows[2]["production_ok"] = False
    rows[2]["quality_status"] = "UNAVAILABLE"
    rows[2]["storage_surplus_bcf"] = None
    rows[2]["production_yoy_pct"] = None

    frozen = apply_frozen_v2(rows)
    assert frozen[0]["fair_value"] is not None
    assert frozen[1]["fair_value"] is None
    assert frozen[1]["quality_status"] == "FALLBACK_V1_ELIGIBLE"
    assert frozen[2]["fair_value"] is None
    assert frozen[2]["quality_status"] == "UNAVAILABLE"


def test_module_has_no_cot_or_stage4_imports() -> None:
    tree = ast.parse(MOD_PATH.read_text(encoding="utf-8"))
    banned_substrings = (
        "cot",
        "stage4",
        "stage_4",
        "scanner",
        "seasonality",
        "weekly_inspector",
        "analyst_intelligence",
    )
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name.lower() for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            mod = (node.module or "").lower()
            imported.append(mod)
            imported.extend(alias.name.lower() for alias in node.names)
    for name in imported:
        for ban in banned_substrings:
            assert ban not in name, f"banned import fragment {ban!r} in {name!r}"


def test_build_outputs_exist_and_published_flags() -> None:
    """Smoke: durable outputs from prior build; do not re-run full build in CI unit path."""
    series = ROOT / "data" / "audits" / "ng_valuation_workstation" / "ng_valuation_historical_series.json"
    public = ROOT / "web-dashboard" / "public" / "data" / "ng_valuation_workstation_latest.json"
    if not series.exists() or not public.exists():
        pytest.skip("workstation outputs not built yet")
    import json

    doc = json.loads(public.read_text(encoding="utf-8"))
    assert doc.get("published_model_id") == "ng_storage_production_v2"
    assert doc.get("research_only") is True
    assert doc.get("verdict", {}).get("verdict")
    cov = doc.get("coverage") or {}
    assert cov.get("n_weeks", 0) > 100
    assert cov.get("n_walkforward_fair_values", 0) > 0
    # Frozen and walk-forward blocks are distinct per week
    week = next(w for w in doc["weeks"] if w.get("walk_forward", {}).get("fair_value") is not None)
    assert week["frozen_v2"]["model_type"] == FROZEN_LABEL
    assert week["walk_forward"]["model_type"] == WF_LABEL
