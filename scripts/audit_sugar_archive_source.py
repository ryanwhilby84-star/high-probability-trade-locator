#!/usr/bin/env python3
"""Reconcile every stored sugar return against the original SB contract ZIP.

No provider calls or dataset writes. Usage:
python scripts/audit_sugar_archive_source.py RAW.zip [ARCHIVE.json]
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path

MONTHS = dict(zip('FGHJKMNQUVXZ', range(1, 13)))


def _date(value: str) -> date:
    if '-' in value:
        return date.fromisoformat(value)
    if '/' in value:
        return datetime.strptime(value, '%m/%d/%Y').date()
    year = int(value[:2])
    return date(1900 + year if year >= 50 else 2000 + year, int(value[2:4]), int(value[4:6]))


def audit_archive(raw_zip: Path, archive: dict) -> dict:
    contracts = {}
    maturities = {}
    raw_count = 0
    with zipfile.ZipFile(raw_zip) as source:
        for filename in source.namelist():
            if not filename.lower().endswith('.txt'):
                continue
            code = Path(filename).stem.upper()
            year = int(code[2:4])
            maturities[code] = (1900 + year if year >= 50 else 2000 + year, MONTHS[code[4]])
            records = {}
            for row in csv.reader(io.StringIO(source.read(filename).decode('utf-8-sig'))):
                if not row or row[0].strip().lower() == 'date':
                    continue
                if len(row) != 7:
                    raise ValueError(f'Invalid source row: {filename}')
                current = _date(row[0]).isoformat()
                numbers = tuple(float(value) for value in row[1:])
                if not all(math.isfinite(value) for value in numbers) or min(numbers[:4]) <= 0 or min(numbers[4:]) < 0:
                    raise ValueError(f'Invalid source prices/volume: {filename}, {current}')
                if current in records and records[current] != numbers:
                    raise ValueError(f'Conflicting source duplicate: {filename}, {current}')
                records[current] = numbers
                raw_count += 1
            contracts[code] = records
    source_dates = sorted({d for records in contracts.values() for d in records})
    previous_session = dict(zip(source_dates[1:], source_dates))
    failures = []
    prior_contract = None
    prior_date = ''
    switches = 0
    flagged = 0
    for current, previous, stored_return, eligible, contract in archive['rows']:
        records = contracts.get(contract, {})
        if current <= prior_date or previous_session.get(current) != previous:
            failures.append(f'{current}: duplicate/order/previous-session discrepancy')
        prior_date = current
        if current not in records or previous not in records:
            failures.append(f'{current}: missing same-contract source closes')
            continue
        prior_close, prior_volume = records[previous][3:5]
        close, volume = records[current][3:5]
        expected = close / prior_close - 1
        if not isinstance(stored_return, (int, float)) or not math.isclose(stored_return, expected, rel_tol=1e-12, abs_tol=1e-12):
            failures.append(f'{current}: return does not reconcile to {contract} source closes')
        expected_eligible = int(volume > 0 and prior_volume > 0 and abs(expected) <= .10)
        flagged += 1 - expected_eligible
        if eligible != expected_eligible:
            failures.append(f'{current}: quality flag disagrees with source volume/return')
        maturity = maturities[contract]
        if maturity <= (_date(current).year, _date(current).month):
            failures.append(f'{current}: delivery/expired contract used')
        available = [code for code, rows in contracts.items() if current in rows and previous in rows and maturities[code] > (_date(current).year, _date(current).month) and (prior_contract is None or maturities[code] >= maturities[prior_contract])]
        if available and maturity != min(maturities[code] for code in available):
            failures.append(f'{current}: selected contract differs from fixed calendar roll policy')
        if prior_contract and contract != prior_contract:
            switches += 1
            if maturity < maturities[prior_contract]:
                failures.append(f'{current}: contract maturity moves backwards')
        prior_contract = contract
    if len(archive['rows']) != len(source_dates) - 1:
        failures.append('Return session count differs from source sessions minus entry close')
    if archive.get('first_date') != source_dates[0] or archive.get('last_date') != source_dates[-1]:
        failures.append('Archive coverage metadata differs from source')
    return {'passed': not failures, 'source_sha256': hashlib.sha256(raw_zip.read_bytes()).hexdigest(), 'contracts': len(contracts), 'raw_rows': raw_count, 'source_sessions': len(source_dates), 'returns_checked': len(archive['rows']), 'contract_switches_checked': switches, 'quality_flagged_intervals': flagged, 'failures': failures}


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[1]
    raw_path = Path(sys.argv[1])
    json_path = Path(sys.argv[2]) if len(sys.argv) > 2 else root / 'web-dashboard/public/data/sugar_archive_daily.json'
    report = audit_archive(raw_path, json.loads(json_path.read_text(encoding='utf-8')))
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report['passed'] else 1)
