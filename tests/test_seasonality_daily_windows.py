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


def test_new_year_window_uses_following_january_prices():
    rows = []
    for year in range(2009, 2026):
        for month, day, px in [(12, 1, 100), (12, 15, 105),
                               (12, 29, 110), (12, 31, 110)]:
            rows.append((date(year, month, day).isoformat(), px))
        # The following January price belongs to the following calendar year.
        rows.append((date(year, 1, 5).isoformat(), 120))
        rows.append((date(year, 1, 12).isoformat(), 120))
    windows = scan_daily_windows(rows, asof_year=2026, lookback_years=15,
                                 min_years=10, min_hit_rate=0.7)
    crossing = [w for w in windows if w['start'] >= '12-01' and w['end'] < '03-01']
    assert crossing, 'Expected valid year-crossing windows'
    assert any(w['direction'] == 'BUY' for w in crossing)
