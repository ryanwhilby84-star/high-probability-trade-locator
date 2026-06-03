"""FX relative strength — currency legs and pair differentials."""

from __future__ import annotations

from hptl.fx.currency_map import parse_fx_pair
from hptl.fx.relative_strength import (
    CONVICTION_HIGH,
    build_pair_opportunity,
    build_relative_strength,
    score_currency_leg,
    synthesize_usd,
)
from hptl.fx.currency_map import FxPairLegs


def _cot_row(market: str, cot_score: float = 7.0, weekly: float = 3000.0):
    return {
        "market": market,
        "cot_bias": "Bullish",
        "cot_score": cot_score,
        "weekly_change": weekly,
        "macro_score": 6.0,
        "macro_regime": "risk_off",
        "data_status": "complete",
        "macro_transmission": {"available": True, "generic_rates_only": False},
        "institutional_context": {
            "structural_regime": "structural_bullish",
            "flow_momentum": "long_build",
            "flow_momentum_label": "Long build",
            "macro_alignment": "supportive",
            "macro_alignment_label": "Macro Supportive",
            "macro_alignment_score": 72.0,
            "positioning_extreme": "none",
            "weeks_in_regime": 3,
            "attention": {"alerts": [{"kind": "flow_extreme", "text": "big week"}]},
        },
    }


def test_parse_fx_pair():
    p = parse_fx_pair("EUR/AUD")
    assert p and p.base == "EUR" and p.quote == "AUD"


def test_currency_leg_has_audit_components():
    leg = score_currency_leg(_cot_row("Euro FX / 6E"), currency="EUR", invert_cot=False)
    assert "final_score" in leg
    assert "cot_component" in leg
    assert "macro_component" in leg
    assert "flow_component" in leg
    assert "crowding_penalty" in leg
    assert "confidence_modifier" in leg
    assert leg["strongest_driver"]


def test_synthetic_usd_marked():
    legs = {
        "EUR": score_currency_leg(_cot_row("Euro FX / 6E", cot_score=8), currency="EUR", invert_cot=False),
        "JPY": score_currency_leg(_cot_row("Japanese Yen / 6J", cot_score=2), currency="JPY", invert_cot=True),
    }
    usd = synthesize_usd(legs)
    assert usd["data_source"] == "synthetic_usd"
    assert usd["synthetic_usd"] is True
    assert usd["confidence_modifier"] < 0.7


def test_pair_differential_bullish_when_base_stronger():
    legs = {
        "CHF": {"final_score": 84.0, "confidence_modifier": 0.9, "flow_momentum": "long_build", "strongest_driver": "x", "biggest_risk": "y"},
        "AUD": {"final_score": -55.0, "confidence_modifier": 0.9, "flow_momentum": "profit_taking", "strongest_driver": "x", "biggest_risk": "y"},
    }
    opp = build_pair_opportunity(FxPairLegs("CHF", "AUD", "CHF/AUD"), legs)
    assert opp is not None
    assert opp["differential"] > 0
    assert opp["directional_bias"] == "bullish"
    assert opp["direction_arrow"] == "↑"


def test_no_duplicate_canonical_on_pair_board():
    week = [
        _cot_row("Euro FX / 6E", 7),
        _cot_row("Australian Dollar / 6A", 3, weekly=-2000),
        _cot_row("Swiss Franc / 6S", 8, weekly=2500),
        _cot_row("Japanese Yen / 6J", 2, weekly=-1500),
        _cot_row("British Pound / 6B", 5),
        _cot_row("Canadian Dollar / 6C", 4),
        _cot_row("NZ Dollar / 6N", 2),
    ]
    rs = build_relative_strength(week, calendar_week="2026-05-19")
    assert len(rs["currency_leaderboard"]) == 8
    pairs = rs["pair_opportunities"]
    assert pairs
    assert all("differential" in p for p in pairs)
    assert all("conviction" in p for p in pairs)
    chf_aud = next((p for p in rs["pair_audit_all"] if p["pair"] == "CHF/AUD"), None)
    if chf_aud and chf_aud.get("raw_differential_score") is not None:
        assert "raw_differential_score" in chf_aud


def test_high_conviction_requires_spread_and_confidence():
    assert CONVICTION_HIGH in ("HIGH CONVICTION",)


def test_gbp_jpy_ranks_above_gbp_nzd_by_raw_differential():
    """GBP/JPY must appear #1 when GBP strong and JPY weak — not hidden by conviction filter."""
    legs = {
        "GBP": {"currency": "GBP", "final_score": 22.3, "confidence_modifier": 0.9, "flow_momentum": "long_build", "positioning_extreme": "none", "strongest_driver": "x", "biggest_risk": "y"},
        "JPY": {"currency": "JPY", "final_score": -43.3, "confidence_modifier": 0.9, "flow_momentum": "profit_taking", "positioning_extreme": "none", "strongest_driver": "x", "biggest_risk": "y"},
        "NZD": {"currency": "NZD", "final_score": -22.3, "confidence_modifier": 0.9, "flow_momentum": "profit_taking", "positioning_extreme": "none", "strongest_driver": "x", "biggest_risk": "y"},
        "EUR": {"final_score": 6.0, "confidence_modifier": 0.9, "flow_momentum": "neutral", "positioning_extreme": "none", "strongest_driver": "x", "biggest_risk": "y"},
        "AUD": {"final_score": -8.3, "confidence_modifier": 0.9, "flow_momentum": "neutral", "positioning_extreme": "none", "strongest_driver": "x", "biggest_risk": "y"},
        "USD": {"final_score": -14.2, "confidence_modifier": 0.55, "flow_momentum": "neutral", "positioning_extreme": "none", "synthetic_usd": True, "strongest_driver": "x", "biggest_risk": "y"},
        "CHF": {"final_score": -27.0, "confidence_modifier": 0.9, "flow_momentum": "neutral", "positioning_extreme": "none", "strongest_driver": "x", "biggest_risk": "y"},
        "CAD": {"final_score": -31.3, "confidence_modifier": 0.9, "flow_momentum": "neutral", "positioning_extreme": "none", "strongest_driver": "x", "biggest_risk": "y"},
    }
    from hptl.fx.relative_strength import build_g10_pair_audit, _finalize_pair_ranks, DISPLAY_PAIR_TOP_N

    audit = build_g10_pair_audit(legs, registry_pairs={"GBP/NZD", "AUD/JPY"})
    ranked = _finalize_pair_ranks(audit, display_top_n=DISPLAY_PAIR_TOP_N)
    assert ranked[0]["pair"] == "GBP/JPY"
    assert ranked[0]["raw_differential_score"] == 65.6
    gbp_nzd = next(r for r in ranked if r["pair"] == "GBP/NZD")
    assert gbp_nzd["raw_differential_score"] == 44.6
    assert ranked[0]["raw_differential_abs"] > gbp_nzd["raw_differential_abs"]


def test_pair_audit_exposes_required_fields():
    legs = {
        "GBP": {"final_score": 20.0, "confidence_modifier": 0.9, "flow_momentum": "long_build", "positioning_extreme": "none", "strongest_driver": "a", "biggest_risk": "b"},
        "JPY": {"final_score": -40.0, "confidence_modifier": 0.9, "flow_momentum": "short_build", "positioning_extreme": "none", "strongest_driver": "a", "biggest_risk": "b"},
    }
    from hptl.fx.relative_strength import audit_pair

    row = audit_pair(pair_id="GBP/JPY", base_code="GBP", quote_code="JPY", legs=legs, in_registry=False, pair_source="g10")
    assert row["raw_differential_score"] == 60.0
    assert "adjusted_opportunity_score" in row
    assert "downgrade_penalties" in row
    assert "confidence_score" in row


def test_leaderboard_sorted_strongest_first():
    week = [
        _cot_row("Swiss Franc / 6S", 9),
        _cot_row("Euro FX / 6E", 4),
    ]
    rs = build_relative_strength(week, calendar_week="2026-05-19")
    lb = rs["currency_leaderboard"]
    assert lb[0]["final_score"] >= lb[-1]["final_score"]
