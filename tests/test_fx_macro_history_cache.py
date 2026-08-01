"""Regression: RBA workbook parse must not repeat inside Stage-4 valuation loops."""
from __future__ import annotations

import os

import pytest


def test_currency_histories_parses_rba_workbook_once_per_process() -> None:
    from hptl.fx import fx_macro_history as hist

    hist.clear_fx_macro_history_caches()
    assert hist.rba_workbook_parse_count() == 0

    # First load parses aud_f1 + aud_f2 once each (2 workbook parses max).
    h1 = hist.currency_histories()
    first = hist.rba_workbook_parse_count()
    assert first > 0
    assert first <= 2
    assert "AUD" in h1

    # Repeated calls in a market-week loop must reuse the process cache — zero new parses.
    for _ in range(25):
        hist.currency_histories()
        hist.load_aud_rba_history()
    assert hist.rba_workbook_parse_count() == first


def test_compute_fx_market_v3_reuses_cached_histories_no_extra_rba_parses() -> None:
    from hptl.fx import fx_macro_history as hist
    from hptl.valuation.fx_carry_real_yield_v3 import compute_fx_market_v3

    hist.clear_fx_macro_history_caches()
    # Warm cache once.
    hist.currency_histories()
    baseline = hist.rba_workbook_parse_count()

    # Simulate Stage-4 style repeated FX pillar calls with valuation enabled.
    prev = os.environ.pop("HPTL_SKIP_VALUATION", None)
    try:
        for week in ("2026-07-07", "2026-07-14", "2026-07-21", "2026-07-28"):
            for market in (
                "Australian Dollar / 6A",
                "Euro FX / 6E",
                "British Pound / 6B",
                "Japanese Yen / 6J",
            ):
                compute_fx_market_v3(market, as_of_week=week)
    finally:
        if prev is not None:
            os.environ["HPTL_SKIP_VALUATION"] = prev

    assert hist.rba_workbook_parse_count() == baseline


def test_skip_valuation_gate_avoids_fx_histories_path(monkeypatch: pytest.MonkeyPatch) -> None:
    from hptl.fx import fx_macro_history as hist
    from hptl.valuation.engine import compute_valuation

    hist.clear_fx_macro_history_caches()
    monkeypatch.setenv("HPTL_SKIP_VALUATION", "1")

    calls = {"n": 0}
    original = hist.currency_histories

    def _counted() -> dict:
        calls["n"] += 1
        return original()

    monkeypatch.setattr(hist, "currency_histories", _counted)
    # Also patch the import path used by fx_carry if somehow reached.
    monkeypatch.setattr("hptl.fx.fx_rate_history_loaders.currency_histories", _counted)

    out = compute_valuation(market="Australian Dollar / 6A", as_of_week="2026-07-28")
    assert out.get("valuation_phase") == "SKIPPED"
    assert calls["n"] == 0
    assert hist.rba_workbook_parse_count() == 0
