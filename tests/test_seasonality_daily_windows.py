"""Regression checks for daily seasonal windows without external market feeds."""
from datetime import date, timedelta
from hptl.seasonality_workstation.daily_windows import scan_daily_windows


def test_daily_windows_detect_repeatable_rally():
    rows = []
    for year in range(2008, 2026):
        start = date(year, 1, 1)
        for n in range(365):
            d = start + timedelta(days=n)
            if d.year != year or d.weekday() >= 5:
                continue
            # Consistent March rise, with a different base price each year.
            price = 100 + (year - 2008) * 2 + max(0, min(n - 59, 60)) * 0.35
            rows.append((d.isoformat(), price))
    windows = scan_daily_windows(rows, asof_year=2026, lookback_years=15)
    assert windows, 'Expected at least one qualifying recurring window'
    assert any(w['direction'] == 'BUY' for w in windows)
    for w in windows:
        assert w['wins'] <= w['sample_years'] <= 15
        assert len(w['outcomes']) == w['sample_years']
        assert w['validation'] == 'IN_SAMPLE_ONLY'


def test_daily_windows_reject_insufficient_history():
    rows = [(date(2023, 1, 1).isoformat(), 100),
            (date(2023, 3, 1).isoformat(), 120)]
    assert scan_daily_windows(rows, asof_year=2026) == []
