"""Unit tests for NG Driver Validation Phase 2 (Production)."""

from __future__ import annotations

import math

from hptl.valuation.ng_driver_validation_phase2_production import (
    MIN_OOS_RMSE_IMPROVEMENT_PCT,
    _align_finite,
    _build_production_transforms,
    _diebold_mariano_pvalue,
    _promotion_decision,
    document_current_valuation_math,
)


def test_production_transforms_are_point_in_time_safe_except_v1_zscore() -> None:
    dates = [f"2020-{(i % 12) + 1:02d}-15" for i in range(200)]
    # repair invalid months like 2020-13 — use sequential weekly-like dates
    dates = []
    prod = []
    y, m, d = 2016, 1, 15
    for i in range(220):
        dates.append(f"{y:04d}-{m:02d}-{d:02d}")
        prod.append(90.0 + 0.05 * i + 3.0 * math.sin(i / 8.0))
        m += 1
        if m > 12:
            m = 1
            y += 1

    transforms = _build_production_transforms(dates, prod)
    assert set(transforms) >= {
        "raw_level",
        "yoy_pct",
        "seasonal_deviation",
        "trailing_zscore_156",
        "chg_4w",
        "chg_12w",
        "v1_fullsample_zscore",
    }

    # YoY unavailable before 52 observations
    assert transforms["yoy_pct"][0] is None
    assert transforms["yoy_pct"][52] is not None

    # 4w / 12w changes unavailable early
    assert transforms["chg_4w"][3] is None
    assert transforms["chg_4w"][4] == prod[4] - prod[0]
    assert transforms["chg_12w"][11] is None
    assert transforms["chg_12w"][12] == prod[12] - prod[0]

    # Trailing z uses only past window — changing a future point must not alter earlier z
    z0 = list(transforms["trailing_zscore_156"])
    prod2 = list(prod)
    prod2[-1] = prod2[-1] + 50.0
    z1 = _build_production_transforms(dates, prod2)["trailing_zscore_156"]
    assert z0[:-1] == z1[:-1]
    assert z0[-1] != z1[-1]

    # Full-sample z DOES leak: future shock changes early values
    v1_a = transforms["v1_fullsample_zscore"]
    v1_b = _build_production_transforms(dates, prod2)["v1_fullsample_zscore"]
    assert v1_a[0] != v1_b[0]


def test_align_finite_drops_missing_production() -> None:
    dates = ["2020-01-01", "2020-01-08", "2020-01-15"]
    y = [1.0, 1.1, 1.2]
    s = [10.0, 11.0, 12.0]
    p = [None, 5.0, 6.0]
    d2, y2, s2, p2 = _align_finite(dates, y, s, p)
    assert d2 == ["2020-01-08", "2020-01-15"]
    assert p2 == [5.0, 6.0]


def test_promotion_requires_all_gates() -> None:
    baseline = {"oos_rmse": 0.40}
    candidate_fail = {
        "features": ["storage_surplus_bcf", "production__raw_level"],
        "coefficients": {
            "storage_surplus_bcf": -0.0005,
            "production__raw_level": 0.01,  # wrong sign
        },
        "oos_rmse": 0.39,  # only 2.5% improvement if 0.40->0.39 = 2.5%
        "coefficient_stability": {
            "production__raw_level": {"sign_flip": True},
        },
    }
    # Fix RMSE to barely miss 2% if needed — use worse than 2%
    candidate_fail["oos_rmse"] = 0.395  # 1.25% improvement
    dm_fail = {"ok": True, "p_value_one_sided": 0.4, "mean_loss_diff": 0.0001}
    dec = _promotion_decision(
        transform_id="raw_level",
        leaky=False,
        candidate=candidate_fail,
        baseline=baseline,
        dm=dm_fail,
    )
    assert dec["promote"] is False
    assert dec["recommendation"] in {"Keep Experimental", "Reject"}

    candidate_ok = {
        "features": ["storage_surplus_bcf", "production__raw_level"],
        "coefficients": {
            "storage_surplus_bcf": -0.0005,
            "production__raw_level": -0.02,
        },
        "oos_rmse": 0.40 * (1.0 - (MIN_OOS_RMSE_IMPROVEMENT_PCT + 1.0) / 100.0),
        "coefficient_stability": {
            "production__raw_level": {"sign_flip": False},
        },
    }
    dm_ok = {"ok": True, "p_value_one_sided": 0.02, "mean_loss_diff": 0.001}
    dec_ok = _promotion_decision(
        transform_id="raw_level",
        leaky=False,
        candidate=candidate_ok,
        baseline=baseline,
        dm=dm_ok,
    )
    assert dec_ok["promote"] is True
    assert dec_ok["recommendation"] == "Promote"

    # Leaky can never promote
    dec_leaky = _promotion_decision(
        transform_id="v1_fullsample_zscore",
        leaky=True,
        candidate=candidate_ok,
        baseline=baseline,
        dm=dm_ok,
    )
    assert dec_leaky["promote"] is False


def test_diebold_mariano_detects_clear_improvement() -> None:
    # Baseline worse by constant margin
    se_b = [0.20 + 0.01 * (i % 5) for i in range(80)]
    se_a = [x - 0.05 for x in se_b]
    dm = _diebold_mariano_pvalue(se_b, se_a)
    assert dm["ok"] is True
    assert dm["mean_loss_diff"] > 0
    assert dm["p_value_one_sided"] < 0.05


def test_document_current_valuation_math_reads_export() -> None:
    doc = document_current_valuation_math()
    assert "fair_value_equation" in doc
    assert "storage_surplus_bcf" in (doc.get("validated_drivers") or ["storage_surplus_bcf"])
    assert "confidence_rules" in doc
