"""Automatic seasonal calendar scanner using the near-perfect Bernd methodology.

This is deliberately separate from daily_window_edge.py.  It freezes the exact
cross-market methodology that reproduced 5/6 original Bernd benchmarks exactly
(JPY was one win short) before crude was added as a seventh external benchmark.

Frozen semantics:
- displayed start date: shift -1 calendar day, resolve NEXT available session,
  then move -1 available session;
- displayed end date: shift 0 calendar days, resolve PREVIOUS available session,
  then move -2 available sessions;
- 15 completed historical years;
- direction is inferred from the sign that wins most often.

The scanner contains no benchmark dates.  Benchmark dates live only in the
validation script and are used after discovery to measure rediscovery quality.
"""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from hptl.seasonality_workstation.returns import load_daily_closes

LOOKBACK_YEARS = 15
START_SHIFT_DAYS = -1
END_SHIFT_DAYS = 0
ENTRY_OFFSET_SESSIONS = -1
EXIT_OFFSET_SESSIONS = -2
MIN_WINDOW_DAYS = 10
MAX_WINDOW_DAYS = 100
CALENDAR_STEP_DAYS = 1
MIN_HIT_RATE = 0.60


@dataclass(frozen=True)
class Market:
    dates: tuple[date, ...]
    price: dict[date, float]
    source: str | None


def prepare_market(instrument_id: str) -> tuple[Market | None, str | None]:
    daily, source, error = load_daily_closes(instrument_id)
    if error or not daily:
        return None, error or "no_daily_history"

    by_date: dict[date, float] = {}
    for raw_date, raw_close in daily:
        try:
            d = date.fromisoformat(str(raw_date)[:10])
            px = float(raw_close)
        except (TypeError, ValueError):
            continue
        if math.isfinite(px) and px > 0:
            by_date[d] = px

    if not by_date:
        return None, "no_usable_daily_history"

    dates = tuple(sorted(by_date))
    return Market(dates=dates, price=by_date, source=source), None


def _next(dates: tuple[date, ...], target: date) -> date | None:
    i = bisect_left(dates, target)
    return dates[i] if i < len(dates) else None


def _previous(dates: tuple[date, ...], target: date) -> date | None:
    i = bisect_right(dates, target) - 1
    return dates[i] if i >= 0 else None


def _move_sessions(dates: tuple[date, ...], current: date, offset: int) -> date | None:
    i = bisect_left(dates, current)
    if i >= len(dates) or dates[i] != current:
        return None
    j = i + offset
    return dates[j] if 0 <= j < len(dates) else None


def _safe_date(year: int, md: tuple[int, int]) -> date:
    month, day = md
    try:
        return date(year, month, day)
    except ValueError:
        return date(year, month, 28)


def resolve_displayed_window(
    market: Market,
    start_year: int,
    start_md: tuple[int, int],
    end_md: tuple[int, int],
) -> tuple[date | None, date | None]:
    crosses_year = end_md <= start_md
    end_year = start_year + 1 if crosses_year else start_year

    displayed_start = _safe_date(start_year, start_md)
    displayed_end = _safe_date(end_year, end_md)

    start = _next(market.dates, displayed_start + timedelta(days=START_SHIFT_DAYS))
    end = _previous(market.dates, displayed_end + timedelta(days=END_SHIFT_DAYS))
    if start is None or end is None:
        return None, None

    start = _move_sessions(market.dates, start, ENTRY_OFFSET_SESSIONS)
    end = _move_sessions(market.dates, end, EXIT_OFFSET_SESSIONS)
    return start, end


def evaluate_window(
    market: Market,
    start_md: tuple[int, int],
    end_md: tuple[int, int],
    years: list[int],
) -> dict[str, Any] | None:
    samples: list[dict[str, Any]] = []
    for year in years:
        start, end = resolve_displayed_window(market, year, start_md, end_md)
        if start is None or end is None or end <= start:
            continue
        ret = (market.price[end] / market.price[start] - 1.0) * 100.0
        samples.append({"year": year, "start": start.isoformat(), "end": end.isoformat(), "return_pct": ret})

    if len(samples) != len(years):
        return None

    returns = [float(x["return_pct"]) for x in samples]
    up = sum(x > 0 for x in returns)
    down = sum(x < 0 for x in returns)
    direction = "bullish" if up >= down else "bearish"
    wins = up if direction == "bullish" else down
    directional = returns if direction == "bullish" else [-x for x in returns]

    return {
        "start_md": start_md,
        "end_md": end_md,
        "direction": direction,
        "wins": wins,
        "n": len(samples),
        "hit_rate": wins / len(samples),
        "average_return_pct": sum(returns) / len(returns),
        "directional_average_pct": sum(directional) / len(directional),
        "samples": samples,
    }


def _md_from_offset(offset: int) -> tuple[int, int]:
    d = date(2000, 1, 1) + timedelta(days=offset % 366)
    return d.month, d.day


def _circular_distance(a: int, b: int) -> int:
    raw = abs(a - b)
    return min(raw, 366 - raw)


def _md_offset(md: tuple[int, int]) -> int:
    return (date(2000, *md) - date(2000, 1, 1)).days


def discover_windows(
    instrument_id: str,
    *,
    asof: date | None = None,
    min_hit_rate: float = MIN_HIT_RATE,
    min_window_days: int = MIN_WINDOW_DAYS,
    max_window_days: int = MAX_WINDOW_DAYS,
    max_results: int = 30,
) -> dict[str, Any]:
    market, error = prepare_market(instrument_id)
    if market is None:
        return {"status": "error", "instrument_id": instrument_id, "error": error, "windows": []}

    anchor = asof or market.dates[-1]
    years = list(range(anchor.year - LOOKBACK_YEARS, anchor.year))
    candidates: list[dict[str, Any]] = []

    for start_offset in range(0, 366, CALENDAR_STEP_DAYS):
        start_md = _md_from_offset(start_offset)
        for length in range(min_window_days, max_window_days + 1):
            end_md = _md_from_offset(start_offset + length)
            result = evaluate_window(market, start_md, end_md, years)
            if result is None or result["hit_rate"] < min_hit_rate:
                continue
            result["window_days"] = length
            # Consistency is primary; magnitude breaks ties rather than dominating.
            result["score"] = result["hit_rate"] * 100.0 + min(max(result["directional_average_pct"], 0.0), 9.99)
            candidates.append(result)

    candidates.sort(key=lambda x: (-x["wins"], -x["score"], -x["directional_average_pct"], x["window_days"]))

    # Collapse one-day variants of the same move, while keeping enough candidates
    # to test whether known external dates are rediscovered naturally.
    selected: list[dict[str, Any]] = []
    for candidate in candidates:
        cs = _md_offset(candidate["start_md"])
        ce = (cs + int(candidate["window_days"])) % 366
        duplicate = False
        for existing in selected:
            if existing["direction"] != candidate["direction"]:
                continue
            es = _md_offset(existing["start_md"])
            ee = (es + int(existing["window_days"])) % 366
            if _circular_distance(cs, es) <= 5 and _circular_distance(ce, ee) <= 5:
                duplicate = True
                break
        if not duplicate:
            selected.append(candidate)
        if len(selected) >= max_results:
            break

    return {
        "status": "ok",
        "engine": "bernd_method_scanner_v1",
        "instrument_id": instrument_id,
        "source": market.source,
        "asof": anchor.isoformat(),
        "methodology": {
            "lookback_years": LOOKBACK_YEARS,
            "start_shift_days": START_SHIFT_DAYS,
            "start_mode": "next",
            "entry_offset_sessions": ENTRY_OFFSET_SESSIONS,
            "end_shift_days": END_SHIFT_DAYS,
            "end_mode": "previous",
            "exit_offset_sessions": EXIT_OFFSET_SESSIONS,
        },
        "candidate_count": len(candidates),
        "windows": selected,
    }
