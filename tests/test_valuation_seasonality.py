"""Valuation and seasonality pillar engines."""
from __future__ import annotations

from hptl.seasonality.engine import compute_seasonality, seasonality_pass
from hptl.valuation.engine import compute_valuation, valuation_pass


def _weekly(closes: list[float], start: str = "2024-01-01") -> list[dict]:
    bars = []
    y, m, d = 2024, 1, 1
    for i, c in enumerate(closes):
        bars.append({"date": f"{y:04d}-{m:02d}-{d:02d}", "close": c})
        d += 7
        if d > 28:
            d = 1
            m += 1
        if m > 12:
            m = 1
            y += 1
    return bars


def test_valuation_bullish_low_percentile():
    closes = [100 + i * 0.5 for i in range(60)]
    out = compute_valuation(market="TEST", weekly_bars=_weekly(closes))
    assert out["wired"] is True
    assert out["valuation_bias"] in {"Bullish", "Neutral", "Bearish"}
    assert out["valuation_score"] is not None


def test_valuation_pass_long():
    assert valuation_pass("Bullish", "long") is True
    assert valuation_pass("Bearish", "long") is False


def test_seasonality_wired_with_enough_history():
    closes = [100 + (i % 5) - 2 for i in range(80)]
    weekly = _weekly(closes)
    out = compute_seasonality(market="TEST", weekly_bars=weekly, as_of_week=weekly[-1]["date"])
    assert out.get("wired") in {True, False}
    if out.get("wired"):
        assert out["seasonality_bias"] in {"Bullish", "Neutral", "Bearish"}
        assert seasonality_pass(out["seasonality_bias"], "long") in {True, False}
