"""Day-by-day historical calendar window discovery from verified daily closes.

Uses exact per-year entry/exit closes (first session on or after a calendar
date), excludes incomplete observations, and reports in-sample outcomes.
"""
from __future__ import annotations

from bisect import bisect_left
from datetime import date, timedelta
from statistics import mean, median


def scan_daily_windows(daily, *, asof_year, lookback_years=15,
                       min_years=10, min_hit_rate=0.7, limit=24):
    """Discover long/short seasonal windows; return auditable year outcomes."""
    years = range(asof_year - lookback_years, asof_year)
    by_year = {}
    for d, close in daily:
        try:
            dt = date.fromisoformat(str(d)[:10])
            px = float(close)
        except (ValueError, TypeError):
            continue
        if dt.year in years and px > 0:
            by_year.setdefault(dt.year, []).append((dt, px))
    indexed = {}
    for y, rows in by_year.items():
        rows.sort()
        indexed[y] = ([d for d, _ in rows], [p for _, p in rows])
    base = date(2000, 1, 1)  # leap year; Feb 29 candidate intentionally skipped
    offsets = [i for i in range(366) if (base + timedelta(days=i)).month != 2
               or (base + timedelta(days=i)).day != 29]
    candidates = []
    # One-calendar-day entry resolution; exit horizons 1-12 weeks.
    for offset in offsets:
        start = base + timedelta(days=offset)
        for duration in (7, 14, 21, 28, 35, 42, 56, 70, 84):
            end = start + timedelta(days=duration)
            observations = []
            for y, (dates, prices) in indexed.items():
                try:
                    entry = date(y, start.month, start.day)
                    exit_ = date(y + (end.year - base.year), end.month, end.day)
                except ValueError:
                    continue
                i = bisect_left(dates, entry)
                j = bisect_left(dates, exit_)
                if i >= len(dates) or j >= len(dates):
                    continue
                # No stale price substitution across missing market sessions.
                if (dates[i] - entry).days > 4 or (dates[j] - exit_).days > 4:
                    continue
                if dates[j] <= dates[i] or prices[i] <= 0:
                    continue
                r = prices[j] / prices[i] - 1
                if abs(r) > 0.75:  # avoid obvious contract discontinuities
                    continue
                observations.append((y, r))
            if len(observations) < min_years:
                continue
            for direction in ('BUY', 'SELL'):
                sign = 1 if direction == 'BUY' else -1
                outcomes = [(y, sign * r) for y, r in observations]
                wins = sum(r > 0 for _, r in outcomes)
                hit = wins / len(outcomes)
                med = median(r for _, r in outcomes)
                if hit < min_hit_rate or med <= 0:
                    continue
                candidates.append({
                    'direction': direction,
                    'start': start.strftime('%m-%d'),
                    'end': end.strftime('%m-%d'),
                    'calendar_days': duration,
                    'wins': wins,
                    'sample_years': len(outcomes),
                    'hit_rate': round(hit, 4),
                    'median_return_pct': round(med * 100, 3),
                    'average_return_pct': round(mean(r for _, r in outcomes) * 100, 3),
                    'worst_return_pct': round(min(r for _, r in outcomes) * 100, 3),
                    'recent_5y_wins': sum(r > 0 for y, r in outcomes if y >= asof_year - 5),
                    'recent_5y_n': sum(y >= asof_year - 5 for y, _ in outcomes),
                    'outcomes': [{'year': y, 'return_pct': round(r * 100, 3)}
                                 for y, r in outcomes],
                    'validation': 'IN_SAMPLE_ONLY',
                })
    candidates.sort(key=lambda w: (-w['hit_rate'], -w['sample_years'],
                                   -w['median_return_pct']))
    selected = []
    def day_index(md):
        m, d = map(int, md.split('-'))
        return (date(2000, m, d) - base).days
    for w in candidates:
        a = day_index(w['start'])
        b = a + w['calendar_days']
        # Suppress near-duplicate windows within each direction.
        duplicate = any(
            k['direction'] == w['direction'] and
            max(0, min(b, day_index(k['start']) + k['calendar_days']) -
                max(a, day_index(k['start']))) /
            min(w['calendar_days'], k['calendar_days']) >= .75
            for k in selected
        )
        if not duplicate:
            selected.append(w)
        if len(selected) >= limit:
            break
    return selected
