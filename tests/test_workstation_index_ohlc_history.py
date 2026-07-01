"""Tests for workstation index OHLC history (visualization layer)."""

from __future__ import annotations

from hptl.prices.workstation_index_ohlc_history import _filter_daily_bars, _is_real_ohlc


def test_filter_daily_bars_rejects_flat_rows():
    bars = [
        {"date": "2017-01-03", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        {"date": "2017-01-04", "open": 100.0, "high": 105.0, "low": 99.0, "close": 104.0},
    ]
    filtered, stats = _filter_daily_bars(bars)
    assert len(filtered) == 1
    assert stats["rejected_flat_rows"] == 1
    assert stats["accepted_rows"] == 1
    assert _is_real_ohlc(100, 105, 99, 104)


def test_filter_daily_bars_respects_window_start():
    bars = [
        {"date": "2016-12-20", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
        {"date": "2017-01-03", "open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5},
    ]
    from datetime import date

    filtered, _ = _filter_daily_bars(bars, window_start=date(2017, 1, 3))
    assert len(filtered) == 1
    assert filtered[0]["date"] == "2017-01-03"
