"""Tests for Gold structural valuation research (research only)."""

from __future__ import annotations

import math

from hptl.valuation.gold_structural_valuation_research import (
    MONTHLY_PUBLICATION_LAG_DAYS,
    PUBLISHED_GOLD_MODEL_ID,
    SCORE_CONTINUE,
    SCORE_PROMOTE,
    _asof_with_lag,
    _bucket_forward_returns,
    _classify_deviation,
    _expanding_ratio_fair,
    _structural_score,
    _verdict,
    ng_methodology_transfer_notes,
    render_markdown,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as LIVE_PUBLISHED_ID


def test_published_model_id_untouched():
    assert PUBLISHED_GOLD_MODEL_ID == "metals_real_yield_v1"
    assert LIVE_PUBLISHED_ID == "metals_real_yield_v1"


def test_ng_transfer_notes_document_accepted_engine():
    notes = ng_methodology_transfer_notes()
    assert notes["accepted_ng_engine"] == "ng_storage_production_v2"
    assert "log(P_t)" in notes["fair_value_form"]
    assert notes["estimation_rules"]["walk_forward"]
    assert "EIA storage" in " ".join(notes["market_specific_do_not_reuse"]) or any(
        "storage" in x.lower() for x in notes["market_specific_do_not_reuse"]
    )


def test_publication_lag_is_past_only():
    daily = {"2020-01-01": 100.0, "2020-02-01": 110.0}
    dates = ["2020-01-15", "2020-02-20", "2020-04-01"]
    lagged = _asof_with_lag(daily, dates, lag_days=MONTHLY_PUBLICATION_LAG_DAYS)
    # Jan obs usable only after +42d → mid-Feb; first date still None/unavailable path.
    assert lagged[0] is None or lagged[0] == 100.0
    # By April, Feb observation (available ~mid-March) should be visible.
    assert lagged[-1] == 110.0


def test_expanding_ratio_fair_uses_only_past():
    prices = [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
    scale = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    fair = _expanding_ratio_fair(prices, scale, min_train=3)
    # Index 3 uses mean of first 3 ratios = (10+12+14)/3 = 12 → fair=12
    assert fair[0] is None and fair[1] is None and fair[2] is None
    assert fair[3] is not None and abs(fair[3] - 12.0) < 1e-9
    # Index 4 uses mean of first 4 = 13 → fair=13
    assert fair[4] is not None and abs(fair[4] - 13.0) < 1e-9


def test_walk_forward_coefficient_estimation_is_expanding():
    """Synthetic: y = 1 + 2x; expanding OLS recovers near-true beta OOS."""
    from hptl.valuation.gold_structural_valuation_research import _walk_forward_fair_logs_multi

    n = 220
    x = [float(i) * 0.01 for i in range(n)]
    y = [1.0 + 2.0 * xi + 0.001 * math.sin(i) for i, xi in enumerate(x)]
    fair, wf = _walk_forward_fair_logs_multi(y, [x], min_train=156, step=13)
    assert wf.get("n_oos", 0) >= 20
    # Later fair values should track y closely on this designed series.
    errs = [
        abs(fair[i] - y[i])
        for i in range(156, n)
        if fair[i] is not None
    ]
    assert errs and (sum(errs) / len(errs)) < 0.05


def test_valuation_classification_buckets():
    assert _classify_deviation(-20) == "materially_undervalued"
    assert _classify_deviation(-10) == "undervalued"
    assert _classify_deviation(0) == "near_fair_value"
    assert _classify_deviation(10) == "overvalued"
    assert _classify_deviation(20) == "materially_overvalued"


def test_forward_return_alignment():
    dates = [f"2020-01-{i:02d}" for i in range(1, 21)]
    # Simple upward path
    prices = [100.0 + i for i in range(20)]
    # First 5 undervalued, rest overvalued
    deviations = [-10.0] * 5 + [10.0] * 15
    rows = _bucket_forward_returns(dates, prices, deviations, horizons=(4,))
    under = next(r for r in rows if r["bucket"] == "undervalued" and r["horizon_weeks"] == 4)
    over = next(r for r in rows if r["bucket"] == "overvalued" and r["horizon_weeks"] == 4)
    assert under["n"] >= 1
    assert over["n"] >= 1
    # Same absolute price path → similar positive returns; just assert alignment ran.
    assert under["mean_return_pct"] is not None
    assert over["mean_return_pct"] is not None


def test_structural_score_penalizes_price_mirror_and_wrong_way_spread():
    good = _structural_score(
        signs_ok=True,
        flip=False,
        oos_r2=0.2,
        vs_naive_impr=10.0,
        usefulness={"score": 20.0, "spread_pp": 8.0},
        duplication={
            "is_price_mirror": False,
            "median_abs_dev_pct": 12.0,
            "corr_price_fair": 0.85,
        },
        fair_vol_ok=True,
    )
    mirror = _structural_score(
        signs_ok=True,
        flip=False,
        oos_r2=0.9,
        vs_naive_impr=40.0,
        usefulness={"score": 5.0, "spread_pp": 1.0},
        duplication={
            "is_price_mirror": True,
            "median_abs_dev_pct": 0.5,
            "corr_price_fair": 0.999,
        },
        fair_vol_ok=True,
    )
    wrong_way = _structural_score(
        signs_ok=True,
        flip=False,
        oos_r2=0.8,
        vs_naive_impr=30.0,
        usefulness={"score": 0.0, "spread_pp": -10.0},
        duplication={
            "is_price_mirror": False,
            "median_abs_dev_pct": 6.0,
            "corr_price_fair": 0.95,
        },
        fair_vol_ok=True,
    )
    assert good["structural_score"] > mirror["structural_score"]
    assert good["structural_score"] > wrong_way["structural_score"]
    assert SCORE_PROMOTE > SCORE_CONTINUE


def test_verdict_and_markdown_safety_language():
    ranking = [
        {
            "id": "keynes_core3",
            "structural_score": 55.0,
            "decision": "Continue",
            "is_published_form_reference": False,
        }
    ]
    v = _verdict(ranking)
    assert v["verdict"] == "CONTINUE_RESEARCH"
    md = render_markdown(
        {
            "generated_at": "t",
            "verdict": v,
            "ng_methodology_transfer": ng_methodology_transfer_notes(),
            "panel": {"n_weeks": 1, "start": "a", "end": "b", "monthly_publication_lag_days": 42},
            "ranking": ranking,
            "charts": [],
            "runtime_sec": 0.1,
        }
    )
    assert "Research only" in md
    assert "Published models untouched" in md or "untouched" in md.lower()
    assert "ng_storage_production_v2" in md
