"""Phase 4 walk-forward validation tests."""

from __future__ import annotations

from hptl.cot.intelligence_phase4_validation import (
    MIN_OOS_N_VALIDATED,
    _binom_pvalue_two_sided,
    _wilson_ci,
    build_chronological_folds,
    classify_family,
    fold_stability,
)


def test_wilson_ci_and_binom_sane():
    lo, hi = _wilson_ci(35, 56)
    assert lo is not None and hi is not None
    assert 0.0 <= lo < hi <= 1.0
    p = _binom_pvalue_two_sided(28, 56)
    assert p is not None and 0.5 < p <= 1.0
    p2 = _binom_pvalue_two_sided(45, 56)
    assert p2 is not None and p2 < 0.01


def test_folds_are_chronological_and_non_overlapping_tests():
    samples = []
    for y in range(2016, 2026):
        for m in (1, 4, 7, 10):
            samples.append(
                {
                    "date": f"{y}-{m:02d}-15",
                    "price_study_eligible": True,
                    "market": "Gold",
                }
            )
    folds = build_chronological_folds(samples)
    assert len(folds) >= 2
    for f in folds:
        assert f["train_end"] < f["test_end"]
        assert f["test_start"] == f["train_end"]


def test_classify_prefers_insufficient_over_false_validated():
    oos = {
        "n_markets": 2,
        "by_market": {"Gold": 5, "Silver": 3},
        "fwd_4w": {
            "n": 8,
            "pct_positive": 62.5,
            "median_return_pct": 1.0,
            "binom_pvalue_vs_50": 0.3,
            "wilson_ci_positive": [0.3, 0.85],
        },
    }
    phase3 = {
        "outcomes": {
            "fwd_4w": {"pct_positive": 62.0, "median_return_pct": 1.1},
        }
    }
    result = classify_family(
        oos=oos,
        phase3=phase3,
        folds=[],
        lomo={"applicable": False},
        lao={"applicable": False},
        stability={"same_side_of_50": None, "dominated_by_one_fold": None, "folds": []},
    )
    assert result["classification"] == "PROMISING / MONITOR"
    assert result["classification"] != "VALIDATED"


def test_classify_failed_on_reversal():
    oos = {
        "n_markets": 5,
        "by_market": {"a": 4, "b": 4, "c": 4, "d": 4},
        "fwd_4w": {
            "n": 16,
            "pct_positive": 31.0,
            "median_return_pct": -1.2,
            "binom_pvalue_vs_50": 0.2,
            "wilson_ci_positive": [0.12, 0.55],
        },
    }
    phase3 = {
        "outcomes": {"fwd_4w": {"pct_positive": 62.0, "median_return_pct": 1.0}},
    }
    result = classify_family(
        oos=oos,
        phase3=phase3,
        folds=[],
        lomo={"applicable": False},
        lao={"applicable": False},
        stability={
            "same_side_of_50": True,
            "dominated_by_one_fold": False,
            "folds": [
                {"fold_id": "WF1", "n": 8, "pct_positive": 30},
                {"fold_id": "WF2", "n": 8, "pct_positive": 32},
            ],
        },
    )
    assert result["classification"] == "FAILED"
    assert result["reversed_vs_phase3"] is True


def test_fold_stability_detects_split():
    stab = fold_stability(
        [
            {
                "fold_id": "WF1",
                "outcomes": {"fwd_4w": {"n": 10, "pct_positive": 70}},
            },
            {
                "fold_id": "WF2",
                "outcomes": {"fwd_4w": {"n": 10, "pct_positive": 30}},
            },
        ]
    )
    assert stab["same_side_of_50"] is False


def test_validated_gate_requires_uncertainty():
    assert MIN_OOS_N_VALIDATED >= 15
