"""Unit tests for NG Driver Validation Phase 3 (LNG)."""

from __future__ import annotations

from hptl.valuation.ng_driver_validation_phase3_lng import (
    TRANSFORM_SPECS,
    _promotion_decision,
    document_lng_dataset,
)


def test_lng_dataset_document_has_required_fields() -> None:
    ds = document_lng_dataset()
    assert ds.get("series_id") == "N9133US2"
    assert ds.get("frequency") == "monthly"
    assert ds.get("expected_economic_sign") == "positive"
    assert (ds.get("history_available") or {}).get("n_observations", 0) > 100
    assert ds.get("current_observation_date")
    assert "revisions_policy" in ds
    assert "point_in_time_safety" in ds


def test_transform_specs_are_individual_and_include_required_set() -> None:
    ids = {t[0] for t in TRANSFORM_SPECS}
    assert {
        "raw_level",
        "yoy_pct",
        "seasonal_deviation",
        "trailing_zscore_156",
        "chg_4w",
        "chg_12w",
    }.issubset(ids)
    # All non-leaky expected signs are positive for LNG
    for tid, _lab, sign in TRANSFORM_SPECS:
        if tid != "v1_fullsample_zscore":
            assert sign == "positive"


def test_promotion_requires_all_gates_including_regime() -> None:
    v2 = {"oos_rmse": 0.36}
    candidate_fail = {
        "features": ["storage_surplus_bcf", "production_yoy_pct", "lng__raw_level"],
        "coefficients": {
            "storage_surplus_bcf": -0.0005,
            "production_yoy_pct": -0.02,
            "lng__raw_level": -0.01,  # wrong sign
        },
        "oos_rmse": 0.355,
        "coefficient_stability": {"lng__raw_level": {"sign_flip": True}},
    }
    dm = {"ok": True, "p_value_one_sided": 0.4, "mean_loss_diff": 0.0001}
    regime = {"not_single_regime": False, "reason": "early_only"}
    dec = _promotion_decision(
        transform_id="raw_level",
        leaky=False,
        candidate=candidate_fail,
        v2_baseline=v2,
        dm_vs_v2=dm,
        regime=regime,
    )
    assert dec["promote"] is False

    candidate_ok = {
        "features": ["storage_surplus_bcf", "production_yoy_pct", "lng__yoy_pct"],
        "coefficients": {
            "storage_surplus_bcf": -0.0005,
            "production_yoy_pct": -0.02,
            "lng__yoy_pct": 0.01,
        },
        "oos_rmse": 0.36 * 0.97,  # >2% improvement
        "coefficient_stability": {"lng__yoy_pct": {"sign_flip": False}},
    }
    dm_ok = {"ok": True, "p_value_one_sided": 0.02, "mean_loss_diff": 0.001}
    regime_ok = {"not_single_regime": True}
    dec_ok = _promotion_decision(
        transform_id="yoy_pct",
        leaky=False,
        candidate=candidate_ok,
        v2_baseline=v2,
        dm_vs_v2=dm_ok,
        regime=regime_ok,
    )
    assert dec_ok["promote"] is True
    assert dec_ok["recommendation"] == "Promote"
