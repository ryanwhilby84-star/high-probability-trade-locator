"""Regression: HIST_CONTEXT_DUAL must not emit generic N/A for COT-mapped markets."""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from datetime import date, timedelta

import pandas as pd
import pytest

from hptl.confluence.build_decision_table import (
    _build_expanding_historical_stats,
    _build_full_loaded_historical_stats,
    _cot_hist_diagnostic_markets,
    _normalize_cot_report_dates_naive,
    _print_dual_hist_context_console,
)
from hptl.markets.instrument_registry import TARGET_MARKETS, cot_mapped_ids


def _synthetic_cot(markets: list[str], n_weeks: int = 12) -> pd.DataFrame:
    start = date(2024, 1, 2)
    rows = []
    for market in markets:
        for i in range(n_weeks):
            d = start + timedelta(weeks=i)
            rows.append(
                {
                    "market": market,
                    "cot_report_date": pd.Timestamp(d),
                    "long_value": 10_000.0 + i * 10,
                    "short_value": 8_000.0 + i * 5,
                    "net_value": 2_000.0 + i * 5,
                    "open_interest": 50_000.0,
                }
            )
    return pd.DataFrame(rows)


def test_hist_diagnostic_markets_are_cot_mapped_not_full_universe():
    diag = _cot_hist_diagnostic_markets()
    assert diag == list(cot_mapped_ids())
    assert "NZD/USD" not in diag
    assert "NZ Dollar / 6N" in diag
    assert "Gold" in diag
    assert len(diag) < len(TARGET_MARKETS)


def test_cot_mapped_markets_with_rows_have_hist_diagnostics():
    markets = ["Gold", "NZ Dollar / 6N", "Wheat", "Euro FX / 6E"]
    cot = _normalize_cot_report_dates_naive(_synthetic_cot(markets, n_weeks=20))
    hist_exp = _normalize_cot_report_dates_naive(_build_expanding_historical_stats(cot))
    hist_full = _normalize_cot_report_dates_naive(_build_full_loaded_historical_stats(cot))
    assert not hist_exp.empty and not hist_full.empty
    merged = cot.merge(hist_exp, on=["market", "cot_report_date"], how="left")
    merged = merged.merge(hist_full, on=["market", "cot_report_date"], how="left")

    for market in markets:
        sub = merged.loc[merged["market"] == market].sort_values("cot_report_date")
        assert not sub.empty, market
        tail = sub.iloc[-1]
        assert pd.notna(tail["full_loaded_rows_used"]), market
        assert pd.notna(tail["expanding_rows_used"]), market
        assert float(tail["full_loaded_rows_used"]) == len(sub)
        assert float(tail["expanding_rows_used"]) == len(sub)
        assert pd.notna(tail["full_loaded_earliest_report_date"])
        assert pd.notna(tail["full_loaded_latest_report_date"])
        assert pd.notna(tail["expanding_earliest_report_date"])
        assert pd.notna(tail["expanding_latest_report_date"])


def test_dual_hist_console_prints_real_values_for_cot_markets():
    markets = ["Gold", "NZ Dollar / 6N", "Wheat"]
    cot = _normalize_cot_report_dates_naive(_synthetic_cot(markets, n_weeks=15))
    hist_exp = _normalize_cot_report_dates_naive(_build_expanding_historical_stats(cot))
    hist_full = _normalize_cot_report_dates_naive(_build_full_loaded_historical_stats(cot))
    merged = cot.merge(hist_exp, on=["market", "cot_report_date"], how="left")
    merged = merged.merge(hist_full, on=["market", "cot_report_date"], how="left")

    buf = io.StringIO()
    with redirect_stdout(buf):
        _print_dual_hist_context_console(merged)
    text = buf.getvalue()

    assert "NZD/USD" not in text
    assert "AUD/USD" not in text
    for market in markets:
        assert f"market='{market}'" in text
        line = next(l for l in text.splitlines() if f"market='{market}'" in l)
        assert "full_loaded_rows_used=15" in line
        assert "expanding_rows_used=15" in line
        assert "full_loaded_date_range=" in line
        assert "expanding_date_range=" in line
        assert "full_loaded_rows_used=N/A" not in line
        assert "status=hist_diagnostics_missing" not in line


def test_dual_hist_console_explicit_reason_when_fields_missing():
    """Price-eligible / COT-mapped markets must not collapse to generic N/A."""
    markets = ["Gold"]
    cot = _normalize_cot_report_dates_naive(_synthetic_cot(markets, n_weeks=8))
    # Deliberately skip hist merge — columns absent
    buf = io.StringIO()
    with redirect_stdout(buf):
        _print_dual_hist_context_console(cot)
    text = buf.getvalue()
    line = next(l for l in text.splitlines() if "market='Gold'" in l)
    assert "status=hist_diagnostics_missing" in line
    assert "reason=" in line
    assert "full_loaded_rows_used=N/A" not in line
    assert "cot_rows_loaded=8" in line


def test_dual_hist_console_explicit_reason_when_mapped_market_absent():
    # Mapped market with no rows in frame
    cot = _normalize_cot_report_dates_naive(_synthetic_cot(["Gold"], n_weeks=5))
    buf = io.StringIO()
    with redirect_stdout(buf):
        _print_dual_hist_context_console(cot)
    text = buf.getvalue()
    # NZ Dollar is mapped but absent from this frame
    line = next(l for l in text.splitlines() if "market='NZ Dollar / 6N'" in l)
    assert "status=no_cot_rows" in line
    assert "reason=instrument_in_cot_mapped_set_but_absent_from_loaded_master" in line
    assert "full_loaded_rows_used=N/A" not in line
