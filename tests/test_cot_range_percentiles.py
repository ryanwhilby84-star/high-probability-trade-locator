"""Regression checks for point-in-time COT min-max range percentiles."""
from hptl.cot.positioning_research_engine import _pct, _rolling_percentiles


def test_range_extremes_and_middle():
    assert _pct([-100, 0, 100], -100) == 0.0
    assert _pct([-100, 0, 100], 100) == 100.0
    assert _pct([-100, 0, 100], 80) == 90.0


def test_flat_or_insufficient_history_is_unavailable():
    assert _pct([5], 5) is None
    assert _pct([5, 5], 5) is None


def test_rolling_window_excludes_old_extreme():
    values = [-100, 0, 50, 100]
    assert _rolling_percentiles(values, 4)[-1] == 100.0
    assert _rolling_percentiles(values, 2)[-1] == 100.0
    assert _rolling_percentiles([100, -100, 0, 50], 3)[-1] == 100.0


def test_no_future_lookahead():
    values = [-100, 0, 100, 1000]
    assert _rolling_percentiles(values, 3)[2] == 100.0
