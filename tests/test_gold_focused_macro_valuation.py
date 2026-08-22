"""Tests for Gold focused macro valuation (research only)."""

from __future__ import annotations

from hptl.valuation.gold_focused_macro_valuation import (
    CB_PUBLICATION_LAG_DAYS,
    MIN_TRAIN,
    PUBLISHED_GOLD_MODEL_ID,
    _asof_with_lag,
    _classify_deviation,
    _forward_bucket_stats,
    _select_transforms,
    _sign_bounds,
    _zscore_past,
    render_markdown,
)
from hptl.valuation.metals_valuation_v1 import MODEL_ID as LIVE_ID


def test_no_production_mutation_constants():
    assert PUBLISHED_GOLD_MODEL_ID == "metals_real_yield_v1"
    assert LIVE_ID == "metals_real_yield_v1"


def test_past_only_zscore_uses_history_before_t():
    xs = [float(i) for i in range(200)]
    z = _zscore_past(xs, 104)
    assert z[103] is None
    assert z[104] is not None
    # Point uses only prior window, not future
    window = xs[0:104]
    mu = sum(window) / len(window)
    import math

    sd = math.sqrt(sum((v - mu) ** 2 for v in window) / len(window))
    assert abs(float(z[104]) - (xs[104] - mu) / sd) < 1e-9


def test_publication_lag_asof_join():
    daily = {"2020-03-31": 100.0, "2020-06-30": 200.0}
    dates = ["2020-04-10", "2020-07-01", "2020-10-01"]
    lagged = _asof_with_lag(daily, dates, lag_days=CB_PUBLICATION_LAG_DAYS)
    assert lagged[0] is None  # Q1 not yet published
    assert lagged[-1] == 200.0


def test_sign_constrained_ols_respects_bounds():
    from hptl.valuation.gold_focused_macro_valuation import _constrained_ols_slopes

    n = 200
    dxy = [0.01 * i for i in range(n)]
    infl = [0.02 * (n - i) for i in range(n)]
    y = [-0.5 * dxy[i] + 0.3 * infl[i] for i in range(n)]
    beta, r2 = _constrained_ols_slopes(y, [dxy, infl], ["dxy", "inflation"])
    assert beta
    assert beta[0] <= 0  # dxy
    assert beta[1] >= 0  # inflation
    assert r2 is not None and r2 > 0.5
    lo, hi = _sign_bounds(["dxy", "inflation"])
    assert hi[1] == 0.0 and lo[2] == 0.0


def test_forward_return_alignment_and_buckets():
    from datetime import date, timedelta

    d0 = date(2015, 1, 2)
    dates = [(d0 + timedelta(weeks=i)).isoformat() for i in range(60)]
    prices = [1000.0 + i * 2 for i in range(60)]
    deviations = [-20.0] * 15 + [0.0] * 20 + [20.0] * 25
    rows = _forward_bucket_stats(dates, prices, deviations, horizons=(4,))
    under = next(r for r in rows if r["bucket"] == "materially_undervalued" and r["horizon_weeks"] == 4)
    assert under["n"] >= 1
    assert under["n_episodes"] >= 1
    assert _classify_deviation(-20) == "materially_undervalued"


def test_transform_selection_is_small_and_deterministic():
    n = 200
    raw = {
        "dxy": [100.0 + i * 0.01 for i in range(n)],
        "us2y": [1.0 + 0.01 * i for i in range(n)],
        "us30y": [3.0 + 0.005 * i for i in range(n)],
        "real10y": [0.5 + 0.002 * i for i in range(n)],
        "inflation": [2.0 for _ in range(n)],
        "cb_demand": [None] * 50 + [10.0 + i for i in range(n - 50)],
    }
    t1, c1 = _select_transforms(raw)
    t2, c2 = _select_transforms(raw)
    assert c1 == c2
    assert set(c1) == {"dxy", "us2y", "us30y", "real10y", "inflation", "cb_demand"}
    assert t1["dxy"][180] == t2["dxy"][180]


def test_markdown_safety_language():
    md = render_markdown(
        {
            "generated_at": "t",
            "verdict": {"verdict": "USEFUL_BUT_RESEARCH", "best_model": "B_no_cb", "narrative": "n"},
            "panel": {"n_weeks": 1, "start": "a", "end": "b", "gold": {}, "real_yield": {}, "cb": {}},
            "transform_choice": {"dxy": "z"},
            "ranking": [],
            "runtime_sec": 0.1,
        }
    )
    assert "Research only" in md
    assert MIN_TRAIN == 156
