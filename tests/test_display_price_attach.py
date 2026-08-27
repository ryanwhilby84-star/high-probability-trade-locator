"""Tests for display-only live vs weekly price attachment."""

from __future__ import annotations

from hptl.valuation.display_price_attach import attach_display_price_context


def test_attach_display_price_uses_live_for_valuation_display():
    block = {
        "spot_price": 4016.955,
        "fair_value": 3800.0,
        "deviation_pct": 5.71,
        "ive": {"current_price": 4016.955, "valuation_pct": 5.71},
    }
    quote = {
        "historical_ohlc_source": "oanda:XAU_USD",
        "latest_completed_ohlc_date": "2026-06-28",
        "latest_completed_ohlc_close": 4016.955,
        "live_price": 4029.5,
        "live_price_source": "oanda:XAU_USD",
        "live_price_as_of": "2026-06-30T12:00:00Z",
    }
    out = attach_display_price_context(block, quote)
    assert out["spot_price"] == 4016.955
    assert out["deviation_pct"] == 5.71
    assert out["model_spot_price"] == 4016.955
    assert out["live_price"] == 4029.5
    assert out["valuation_price_used"] == 4029.5
    assert out["valuation_price_source"] == "live/latest"
    assert out["display_valuation_pct"] == round(100 * (4029.5 - 3800) / 3800, 2)
    assert out["ive"]["display_current_price"] == 4029.5


def test_attach_display_price_falls_back_to_weekly():
    block = {"spot_price": 4016.955, "fair_value": 3800.0, "deviation_pct": 5.71}
    out = attach_display_price_context(block, {"historical_ohlc_source": "oanda:XAU_USD"})
    assert out["valuation_price_used"] == 4016.955
    assert out["valuation_price_source"] == "weekly_close_fallback"
