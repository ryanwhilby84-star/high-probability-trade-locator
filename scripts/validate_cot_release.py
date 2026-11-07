"""Fail closed unless every published COT market belongs to one validated week."""
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

MEASURE = 'net_positioning_rolling_156_week_percentile'


def expected_week(now=None):
    now = now or datetime.now(timezone.utc)
    day = now.date()
    expected = day - timedelta(days=(day.weekday() - 1) % 7)
    # Tuesday observations are published Friday at 15:30 America/New_York.
    from zoneinfo import ZoneInfo
    release = datetime.combine(expected + timedelta(days=3), datetime.min.time(), ZoneInfo('America/New_York')).replace(hour=15, minute=30)
    if now < release:
        expected -= timedelta(days=7)
    return expected.isoformat()


def validate(inspector, gate, expected):
    if inspector.get('engine') != 'weekly_inspector_flow' or inspector.get('measure') != MEASURE:
        raise ValueError('Incompatible Weekly Inspector source or measure')
    markets = inspector.get('markets') or {}
    if len(markets) != 26:
        raise ValueError(f'Expected 26 markets, found {len(markets)}')
    for name, market in markets.items():
        rows = market.get('rows') or []
        if not market.get('available') or not rows or rows[-1][0] != expected:
            raise ValueError(f'{name}: missing or stale report; expected {expected}')
        dates = [r[0] for r in rows]
        if dates != sorted(set(dates)):
            raise ValueError(f'{name}: duplicate or unordered history')
        for group in rows[-1][1:4]:
            if len(group) < 16 or any(isinstance(group[i], bool) or not isinstance(group[i], (int, float)) or not math.isfinite(group[i]) for i in (0, 1, 4)):
                raise ValueError(f'{name}: missing participant net, change or percentile')
            for i in (4, 12, 13, 14, 15):
                if group[i] is not None and not 0 <= group[i] <= 100:
                    raise ValueError(f'{name}: invalid percentile')
    if gate.get('report_date') != expected or gate.get('checked_count') != 26 or gate.get('passed_count') != 26 or gate.get('failed_count') != 0 or gate.get('error') or gate.get('exit_code') != 0:
        raise ValueError('Source integrity gate does not certify all 26 markets for ' + expected)
    return expected


def main():
    inspector = json.loads(Path('web-dashboard/public/data/cot_weekly_inspector_latest.json').read_text())
    gate = json.loads(Path('data/cot_weekly_integrity_gate_latest.json').read_text())
    week = validate(inspector, gate, expected_week())
    print(f'PASS: all 26 Inspector markets and source integrity checks agree on {week}')


if __name__ == '__main__':
    main()
