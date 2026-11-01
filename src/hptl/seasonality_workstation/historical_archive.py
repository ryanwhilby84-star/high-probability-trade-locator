"""Gap-aware historical futures archive engine.

Builds a research-grade continuous return series without crossing contract rolls,
missing sessions, or unverified provider seams. No interpolation or backfilling.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import re
import zipfile
from collections import defaultdict
from pathlib import Path

MONTH_CODES = dict(zip("FGHJKMNQUVXZ", range(1, 13)))
CONTRACT_RE = re.compile(r"([A-Z]+)(\\d{2})([FGHJKMNQUVXZ])$", re.I)


def parse_date(value: str) -> dt.date:
    value = value.strip()
    if "-" in value:
        return dt.date.fromisoformat(value[:10])
    if "/" in value:
        return dt.datetime.strptime(value, "%m/%d/%Y").date()
    if len(value) == 6 and value.isdigit():
        year = int(value[:2])
        return dt.date(1900 + year if year >= 50 else 2000 + year,
                       int(value[2:4]), int(value[4:6]))
    raise ValueError(f"Unrecognised date: {value!r}")


def read_contract_archive(path: str | Path) -> dict:
    """Read the seven-column TurtleTrader-style ZIP; reject ambiguous duplicates."""
    contracts = {}
    with zipfile.ZipFile(path) as archive:
        for filename in sorted(archive.namelist()):
            if not filename.lower().endswith(".txt"):
                continue
            code = Path(filename).stem.upper()
            match = CONTRACT_RE.fullmatch(code)
            if not match:
                raise ValueError(f"Invalid contract code: {code}")
            year = int(match.group(2))
            maturity = (1900 + year if year >= 50 else 2000 + year,
                        MONTH_CODES[match.group(3)])
            rows = {}
            for fields in csv.reader(io.StringIO(archive.read(filename).decode("utf-8-sig"))):
                if not fields or fields[0].strip().lower() == "date":
                    continue
                if len(fields) != 7:
                    raise ValueError(f"{code}: expected 7 fields, got {len(fields)}")
                day = parse_date(fields[0])
                values = tuple(map(float, fields[1:]))
                if not all(math.isfinite(v) for v in values):
                    raise ValueError(f"{code}: non-finite value on {day}")
                if min(values[:4]) <= 0 or min(values[4:]) < 0:
                    raise ValueError(f"{code}: invalid price/volume on {day}")
                if day in rows and rows[day] != values:
                    raise ValueError(f"{code}: conflicting duplicate on {day}")
                rows[day] = values
            if rows:
                contracts[code] = (maturity, rows)
    if not contracts:
        raise ValueError("No contract files found")
    return contracts


def build_returns(contracts: dict, *, max_calendar_gap_days: int = 7) -> dict:
    """Calendar-roll using same-contract prior-session close.

    Large date gaps become breaks; no return is calculated across them.
    """
    dates = sorted({day for _, rows in contracts.values() for day in rows})
    previous_code = None
    results, breaks, rolls = [], [], []
    for previous_day, day in zip(dates, dates[1:]):
        gap = (day - previous_day).days
        if gap > max_calendar_gap_days:
            breaks.append({"after": previous_day.isoformat(), "before": day.isoformat(),
                           "calendar_days": gap, "reason": "missing_sessions"})
            previous_code = None
            continue
        candidates = []
        for code, (maturity, rows) in contracts.items():
            if maturity <= (day.year, day.month):
                continue
            if previous_code and maturity < contracts[previous_code][0]:
                continue
            if previous_day in rows and day in rows:
                candidates.append((maturity, code))
        if not candidates:
            breaks.append({"after": previous_day.isoformat(), "before": day.isoformat(),
                           "calendar_days": gap, "reason": "no_paired_contract"})
            previous_code = None
            continue
        _, code = min(candidates)
        _, rows = contracts[code]
        old_close, close = rows[previous_day][3], rows[day][3]
        daily_return = close / old_close - 1
        if previous_code and code != previous_code:
            rolls.append({"date": day.isoformat(), "from": previous_code, "to": code})
        results.append({"date": day.isoformat(), "previous_session": previous_day.isoformat(),
                        "contract": code, "daily_return": daily_return,
                        "close": close, "volume": rows[day][4],
                        "strict_eligible": bool(rows[day][4] > 0 and
                                                rows[previous_day][4] > 0 and
                                                abs(daily_return) <= .10)})
        previous_code = code
    return {"returns": results, "breaks": breaks, "rolls": rolls,
            "first_source_date": dates[0].isoformat(),
            "last_source_date": dates[-1].isoformat()}


def audit_coverage(series: list[dict], *, through_year: int | None = None) -> dict:
    """Report yearly coverage, never treating calendar presence as completeness."""
    by_year = defaultdict(list)
    for row in series:
        by_year[int(row["date"][:4])].append(row)
    if not by_year:
        return {"years": [], "missing_years": [], "yearly": []}
    last = through_year or max(by_year)
    years = list(range(min(by_year), last + 1))
    return {"years": sorted(by_year),
            "missing_years": [year for year in years if year not in by_year],
            "yearly": [{"year": year, "sessions": len(by_year[year]),
                        "first": by_year[year][0]["date"] if by_year[year] else None,
                        "last": by_year[year][-1]["date"] if by_year[year] else None}
                       for year in years]}


def export_archive(source: str | Path, destination: str | Path) -> dict:
    contracts = read_contract_archive(source)
    result = build_returns(contracts)
    result["coverage"] = audit_coverage(result["returns"])
    result["source_file"] = Path(source).name
    result["contract_count"] = len(contracts)
    Path(destination).parent.mkdir(parents=True, exist_ok=True)
    Path(destination).write_text(json.dumps(result, separators=(",", ":")))
    return result
