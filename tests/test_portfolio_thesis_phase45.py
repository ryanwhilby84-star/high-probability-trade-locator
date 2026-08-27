"""Phase 4.5 Portfolio Thesis Summary — presentation assembly only."""

from __future__ import annotations

from hptl.portfolio_intelligence.service import enrich_basket_with_portfolio_intelligence
from hptl.trade_basket.currency_exposure import enrich_basket_with_currency_exposure
from hptl.trade_basket.portfolio_thesis import (
    build_portfolio_thesis,
    enrich_basket_with_portfolio_thesis,
)


def _aud_short_payload():
    return {
        "status": "ok",
        "phase": "2A",
        "populated_trade_count": 2,
        "pair_count": 1,
        "trades": [
            {"instrument_id": "AUD/NZD", "direction": "SHORT", "risk_percent": 1.0},
            {"instrument_id": "AUD/CHF", "direction": "SHORT", "risk_percent": 1.0},
        ],
        "pairs": [
            {
                "trade_a_instrument_id": "AUD/NZD",
                "trade_a_direction": "SHORT",
                "trade_b_instrument_id": "AUD/CHF",
                "trade_b_direction": "SHORT",
                "raw_correlation": 0.34,
                "direction_adjusted_correlation": 0.34,
            }
        ],
    }


def test_aud_short_thesis_example():
    out = enrich_basket_with_portfolio_thesis(
        enrich_basket_with_currency_exposure(
            enrich_basket_with_portfolio_intelligence(_aud_short_payload())
        )
    )
    thesis = out["portfolio_thesis"]
    assert thesis["status"] == "ok"
    assert thesis["no_new_calculations"] is True
    assert thesis["primary_thesis"] == "Australian Dollar Weakness"
    assert thesis["supporting_trades"] == ["AUD/NZD SHORT", "AUD/CHF SHORT"]
    assert thesis["risk_concentration"]["primary_exposure"] == "AUD SHORT"
    assert thesis["risk_concentration"]["shared_by_trades"] == 2
    assert thesis["risk_concentration"]["share_of_planned_risk_display"] == "50%"
    assert thesis["correlation_interpretation"]["adjusted_correlation_display"] == "+0.34"
    # 0.34 falls in existing Low band (0.20–0.39), not Moderate (0.40–0.59).
    assert thesis["correlation_interpretation"]["strength"] == "Low"
    assert "Low positive relationship" in thesis["correlation_interpretation"]["interpretation"]
    text = " ".join(thesis["portfolio_interpretation"])
    assert "two trades" in text
    assert "NZD" in text and "CHF" in text
    assert "not duplicates" in text


def test_thesis_skipped_without_fx():
    payload = {
        "status": "ok",
        "trades": [
            {"instrument_id": "Gold", "direction": "LONG", "risk_percent": 1.0},
            {"instrument_id": "Corn", "direction": "SHORT", "risk_percent": 1.0},
        ],
        "pairs": [
            {
                "trade_a_instrument_id": "Gold",
                "trade_a_direction": "LONG",
                "trade_b_instrument_id": "Corn",
                "trade_b_direction": "SHORT",
                "raw_correlation": 0.1,
                "direction_adjusted_correlation": -0.1,
            }
        ],
    }
    out = enrich_basket_with_portfolio_thesis(
        enrich_basket_with_currency_exposure(
            enrich_basket_with_portfolio_intelligence(payload)
        )
    )
    assert out["portfolio_thesis"]["status"] == "skipped"


def test_build_does_not_mutate_phase_fields():
    base = enrich_basket_with_currency_exposure(
        enrich_basket_with_portfolio_intelligence(_aud_short_payload())
    )
    pairs_before = base["pairs"][0]["direction_adjusted_correlation"]
    neff_before = base["portfolio_intelligence"]["effective_independent_trades"]
    aud_before = base["currency_exposure"]["currencies"][0]["net_exposure"]
    out = enrich_basket_with_portfolio_thesis(base)
    assert out["pairs"][0]["direction_adjusted_correlation"] == pairs_before
    assert out["portfolio_intelligence"]["effective_independent_trades"] == neff_before
    assert out["currency_exposure"]["currencies"][0]["net_exposure"] == aud_before
    assert build_portfolio_thesis(out)["status"] == "ok"
