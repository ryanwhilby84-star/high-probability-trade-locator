"""Phase 3 portfolio intelligence — deterministic metric tests."""

from __future__ import annotations

import pytest

from hptl.portfolio_intelligence.config import EXPOSURE_CLUSTER_ABS_THRESHOLD
from hptl.portfolio_intelligence.metrics import (
    classify_pair_strength,
    compute_portfolio_intelligence,
    diversification_score,
    duplication_score,
    effective_independent_trades,
    quadratic_form,
    risk_weights,
)
from hptl.portfolio_intelligence.service import enrich_basket_with_portfolio_intelligence
from hptl.trade_basket.service import build_trade_basket_payload


def _pair(a, da, b, db, raw, adj):
    return {
        "trade_a_instrument_id": a,
        "trade_a_direction": da,
        "trade_b_instrument_id": b,
        "trade_b_direction": db,
        "raw_correlation": raw,
        "direction_adjusted_correlation": adj,
        "frequency": "daily",
        "lookback": 60,
        "overlapping_return_count": 60,
    }


def test_threshold_is_configurable_constant():
    assert EXPOSURE_CLUSTER_ABS_THRESHOLD == 0.60


def test_example_a_two_perfectly_correlated():
    """Example A: two trades, ρ_adj = 1, equal risk → Neff=1, D=0, U=100."""
    trades = [
        {"instrument_id": "A", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "B", "direction": "LONG", "risk_percent": 1.0},
    ]
    pairs = [_pair("A", "LONG", "B", "LONG", 1.0, 1.0)]
    intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
    assert intel["effective_independent_trades"] == 1.0
    assert intel["diversification_score"] == 0.0
    assert intel["duplication_score"] == 100.0
    assert intel["largest_exposure_cluster"]["size"] == 2
    assert intel["highest_correlated_pair"]["direction_adjusted_correlation"] == 1.0
    assert intel["lowest_correlated_pair"]["direction_adjusted_correlation"] == 1.0


def test_example_b_three_uncorrelated():
    """Example B: three trades, all ρ_adj = 0 → Neff=3, D=100, U=0."""
    trades = [
        {"instrument_id": "A", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "B", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "C", "direction": "LONG", "risk_percent": 1.0},
    ]
    pairs = [
        _pair("A", "LONG", "B", "LONG", 0.0, 0.0),
        _pair("A", "LONG", "C", "LONG", 0.0, 0.0),
        _pair("B", "LONG", "C", "LONG", 0.0, 0.0),
    ]
    intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
    assert intel["effective_independent_trades"] == 3.0
    assert intel["diversification_score"] == 100.0
    assert intel["duplication_score"] == 0.0
    # Each trade is its own cluster when below threshold
    assert all(c["size"] == 1 for c in intel["exposure_clusters"])


def test_example_c_five_mixed():
    """Example C: five mixed correlations — intermediate Neff."""
    trades = [
        {"instrument_id": f"T{i}", "direction": "LONG", "risk_percent": 1.0}
        for i in range(5)
    ]
    # Build pairs with moderate overlap
    pairs = []
    ids = [f"T{i}" for i in range(5)]
    rhos = {
        frozenset(("T0", "T1")): 0.9,
        frozenset(("T0", "T2")): 0.85,
        frozenset(("T1", "T2")): 0.8,
        frozenset(("T0", "T3")): 0.1,
        frozenset(("T0", "T4")): 0.05,
        frozenset(("T1", "T3")): 0.12,
        frozenset(("T1", "T4")): 0.08,
        frozenset(("T2", "T3")): 0.1,
        frozenset(("T2", "T4")): 0.05,
        frozenset(("T3", "T4")): 0.2,
    }
    for i in range(5):
        for j in range(i + 1, 5):
            rho = rhos[frozenset((ids[i], ids[j]))]
            pairs.append(_pair(ids[i], "LONG", ids[j], "LONG", rho, rho))
    intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
    assert intel["diagnostics"]["q"] == pytest.approx(0.46)
    assert intel["effective_independent_trades"] == 2.2
    assert intel["diversification_score"] == 29.3
    assert intel["duplication_score"] == 70.7
    assert intel["largest_exposure_cluster"]["size"] == 3  # T0-T1-T2 linked at ≥0.60
    assert intel["largest_risk_concentration"] == pytest.approx(0.6)
    assert intel["highest_correlated_pair"]["direction_adjusted_correlation"] == 0.9
    assert intel["lowest_correlated_pair"]["direction_adjusted_correlation"] == 0.05
    assert intel["highest_correlated_pair"]["classification"]["strength"] == "Very High"
    assert intel["lowest_correlated_pair"]["classification"]["strength"] == "Minimal"


def test_neff_clamped_to_trade_count_even_with_hedges():
    """Hedging can make raw 1/Q > n; reported Neff must still ≤ n."""
    trades = [
        {"instrument_id": "A", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "B", "direction": "SHORT", "risk_percent": 1.0},
    ]
    # Strong positive raw → LONG/SHORT ⇒ adjusted −1 → Q can be near 0
    pairs = [_pair("A", "LONG", "B", "SHORT", 1.0, -1.0)]
    intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
    assert intel["effective_independent_trades"] == 2.0  # clamped to n


def test_five_highly_correlated_fx_like():
    trades = [
        {"instrument_id": f"FX{i}", "direction": "LONG", "risk_percent": 1.0}
        for i in range(5)
    ]
    pairs = []
    for i in range(5):
        for j in range(i + 1, 5):
            pairs.append(_pair(f"FX{i}", "LONG", f"FX{j}", "LONG", 0.95, 0.95))
    intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
    assert intel["diversification_score"] < 20
    assert intel["duplication_score"] > 80
    assert intel["effective_independent_trades"] <= 1.5
    assert intel["largest_exposure_cluster"]["size"] == 5


def test_five_unrelated_markets_high_diversification():
    trades = [
        {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "Corn", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "Bitcoin", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "Coffee", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "Swiss Franc / 6S", "direction": "LONG", "risk_percent": 1.0},
    ]
    # Near-zero adjusted correlations
    pairs = []
    ids = [t["instrument_id"] for t in trades]
    for i in range(5):
        for j in range(i + 1, 5):
            pairs.append(_pair(ids[i], "LONG", ids[j], "LONG", 0.02, 0.02))
    intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
    assert intel["diversification_score"] > 90
    assert intel["duplication_score"] < 10
    assert intel["effective_independent_trades"] >= 4.5


def test_pair_classification_negative_uses_strength_bands():
    c = classify_pair_strength(-0.85)
    assert c["strength"] == "Very High"
    assert c["relationship"] == "negative"


def test_terminology_largest_exposure_cluster():
    trades = [
        {"instrument_id": "A", "direction": "LONG", "risk_percent": 2.0},
        {"instrument_id": "B", "direction": "LONG", "risk_percent": 1.0},
    ]
    pairs = [_pair("A", "LONG", "B", "LONG", 0.7, 0.7)]
    intel = compute_portfolio_intelligence(trades=trades, pairs=pairs)
    assert "largest_exposure_cluster" in intel
    assert "largest_correlated_group" not in intel
    assert intel["largest_exposure_cluster"]["risk_percent_sum"] == pytest.approx(3.0)
    assert intel["largest_risk_concentration"] == pytest.approx(1.0)


def test_custom_cluster_threshold():
    trades = [
        {"instrument_id": "A", "direction": "LONG", "risk_percent": 1.0},
        {"instrument_id": "B", "direction": "LONG", "risk_percent": 1.0},
    ]
    pairs = [_pair("A", "LONG", "B", "LONG", 0.5, 0.5)]
    low = compute_portfolio_intelligence(
        trades=trades, pairs=pairs, exposure_cluster_threshold=0.60
    )
    high = compute_portfolio_intelligence(
        trades=trades, pairs=pairs, exposure_cluster_threshold=0.40
    )
    assert low["largest_exposure_cluster"]["size"] == 1
    assert high["largest_exposure_cluster"]["size"] == 2


def test_enrichment_preserves_phase2a_fields():
    basket = {
        "status": "ok",
        "populated_trade_count": 2,
        "pair_count": 1,
        "phase": "2A",
        "trades": [
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "LONG", "risk_percent": 1.0},
        ],
        "pairs": [_pair("Gold", "LONG", "Silver", "LONG", 0.86, 0.86)],
        "risk_percent_affects_calculations": False,
    }
    out = enrich_basket_with_portfolio_intelligence(basket)
    assert out["phase"] == "2A"
    assert out["risk_percent_affects_calculations"] is False
    assert out["pairs"][0]["direction_adjusted_correlation"] == 0.86
    assert "portfolio_intelligence" in out
    assert out["portfolio_intelligence"]["engine"] == "portfolio_intelligence_v3"
    assert "Largest Exposure Cluster" not in str(out)  # key uses snake_case
    assert out["portfolio_intelligence"]["largest_exposure_cluster"] is not None


def test_live_gold_silver_enrichment_unchanged_raw():
    """Phase 1/2 values untouched when Phase 3 is attached."""
    payload = build_trade_basket_payload(
        frequency="daily",
        lookback=60,
        trades=[
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Silver", "direction": "SHORT", "risk_percent": 1.0},
        ],
    )
    enriched = enrich_basket_with_portfolio_intelligence(payload)
    assert enriched["status"] == "ok"
    pair = enriched["pairs"][0]
    assert pair["raw_correlation"] == pytest.approx(0.857895, abs=1e-6)
    assert pair["direction_adjusted_correlation"] == pytest.approx(-0.857895, abs=1e-6)
    pi = enriched["portfolio_intelligence"]
    assert pi["status"] == "ok"
    assert 1.0 <= pi["effective_independent_trades"] <= 2.0
    assert pi["explanations"]


def test_diversification_duplication_complement():
    assert duplication_score(diversification_score(3.0, 5)) == pytest.approx(
        100.0 - diversification_score(3.0, 5)
    )


def test_quadratic_and_weights_basics():
    w = risk_weights([1.0, 1.0, 2.0])
    assert w == pytest.approx([0.25, 0.25, 0.5])
    c = [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    assert quadratic_form(w, c) == pytest.approx(sum(x * x for x in w))
    assert effective_independent_trades(1.0, 5) == 1.0
    assert effective_independent_trades(0.01, 5) == 5.0  # clamped
