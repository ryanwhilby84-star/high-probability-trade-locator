"""Tests for Gold standalone driver ranking gate (research only)."""

from __future__ import annotations

from hptl.valuation.gold_standalone_driver_ranking import (
    CANDIDATES,
    SCORE_KEEP,
    SCORE_MAYBE,
    _keep_decision,
    _standalone_score,
    render_markdown,
)


def test_candidates_cover_requested_tier1():
    ids = {c["id"] for c in CANDIDATES}
    assert "us_10y_real_yield" in ids
    assert "dxy_ice" in ids or "broad_usd" in ids
    assert "us_10y_yield" in ids
    assert "breakeven_10y" in ids
    assert "fed_funds" in ids


def test_keep_decision_thresholds():
    assert _keep_decision(92, True, False) == "Keep"
    assert _keep_decision(61, True, True) == "Maybe"
    assert _keep_decision(44, True, False) == "Reject"
    assert _keep_decision(80, False, False) == "Maybe"
    assert SCORE_KEEP == 70.0
    assert SCORE_MAYBE == 50.0


def test_standalone_score_caps_and_sign_weight():
    good = _standalone_score(
        {
            "signs_ok": True,
            "oos_r2": 0.35,
            "oos_rmse": 0.2,
            "naive_oos_rmse": 0.4,
            "coef_sign_flip": False,
        },
        {"quality_score": 10.0},
    )
    bad = _standalone_score(
        {
            "signs_ok": False,
            "oos_r2": -0.2,
            "oos_rmse": 0.5,
            "naive_oos_rmse": 0.4,
            "coef_sign_flip": True,
        },
        {"quality_score": 0.0},
    )
    assert good["standalone_score"] >= SCORE_KEEP
    assert bad["standalone_score"] < SCORE_MAYBE
    assert good["score_parts"]["economic_sign"] == 25.0


def test_markdown_mentions_rank_before_combine():
    md = render_markdown(
        {
            "generated_at": "t",
            "panel": {"n_weeks": 1, "start": "a", "end": "b"},
            "ranking_table": [],
            "dataset_quality": [],
            "incremental_combinations": [],
            "best_combination": {},
            "recommendation": {"status": "X", "narrative": "n"},
            "runtime_sec": 0.1,
        }
    )
    assert "rank every variable standalone" in md.lower() or "Ranking table" in md
    assert "Published Gold valuation was not modified" in md
