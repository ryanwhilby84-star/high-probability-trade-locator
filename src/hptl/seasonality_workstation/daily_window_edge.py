"""Parallel 15Y daily seasonal-window discovery engine.

This module is deliberately separate from the existing weekly/roadmap seasonality
engine. It discovers repeatable calendar windows directly from canonical daily
closes and reports their historical pay rate without COT or other filters.

The engine does not contain Bernd benchmark dates and does not fit parameters to
known answers. Benchmarks belong in validation scripts only.
"""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from hptl.seasonality_workstation.returns import load_daily_closes

LOOKBACK_YEARS = 15
MIN_HIT_RATE = 0.60
MIN_WINDOW_DAYS = 10
MAX_WINDOW_DAYS = 100
START_STEP_DAYS = 2
END_STEP_DAYS = 2


@dataclass(frozen=True)
class DailyMarket:
    dates: tuple[date, ...]
    price: dict[date, float]
    source: str | None


def _prepare(instrument_id: str) -> tuple[DailyMarket | None, str | None]:
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
    return DailyMarket(dates=dates, price=by_date, source=source), None


def _next_available(dates: tuple[date, ...], target: date) -> date | None:
    i = bisect_left(dates, target)
    return dates[i] if i < len(dates) else None


def _previous_available(dates: tuple[date, ...], target: date) -> date | None:
    i = bisect_right(dates, target) - 1
    return dates[i] if i >= 0 else None


def _calendar_date(year: int, month: int, day: int) -> date:
    # Feb 29 candidates are represented as Feb 28 in non-leap years.
    try:
        return date(year, month, day)
    except ValueError:
        return date(year, month, 28)


def _window_samples(
    market: DailyMarket,
    start_md: tuple[int, int],
    end_md: tuple[int, int],
    years: list[int],
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    crosses_year = end_md <= start_md

    for start_year in years:
        end_year = start_year + 1 if crosses_year else start_year
        target_start = _calendar_date(start_year, *start_md)
        target_end = _calendar_date(end_year, *end_md)

        # Literal calendar-window semantics: first available session on/after the
        # displayed start, last available session on/before the displayed end.
        s = _next_available(market.dates, target_start)
        e = _previous_available(market.dates, target_end)
        if s is None or e is None or e <= s:
            continue

        ret = market.price[e] / market.price[s] - 1.0
        samples.append(
            {
                "year": start_year,
                "start": s.isoformat(),
                "end": e.isoformat(),
                "return_pct": round(ret * 100.0, 4),
            }
        )

    return samples


def evaluate_daily_window(
    instrument_id: str,
    start_md: tuple[int, int],
    end_md: tuple[int, int],
    *,
    asof: date | None = None,
    lookback_years: int = LOOKBACK_YEARS,
) -> dict[str, Any]:
    """Evaluate one displayed calendar window over completed historical years."""
    market, error = _prepare(instrument_id)
    if market is None:
        return {"status": "error", "instrument_id": instrument_id, "error": error}

    anchor = asof or market.dates[-1]
    years = list(range(anchor.year - lookback_years, anchor.year))
    samples = _window_samples(market, start_md, end_md, years)
    if len(samples) != lookback_years:
        return {
            "status": "insufficient_history",
            "instrument_id": instrument_id,
            "n": len(samples),
            "required": lookback_years,
        }

    up = sum(float(x["return_pct"]) > 0 for x in samples)
    down = sum(float(x["return_pct"]) < 0 for x in samples)
    direction = "bullish" if up >= down else "bearish"
    wins = up if direction == "bullish" else down
    signed = [float(x["return_pct"]) for x in samples]
    directional = signed if direction == "bullish" else [-x for x in signed]

    return {
        "status": "ok",
        "instrument_id": instrument_id,
        "source": market.source,
        "lookback_years": lookback_years,
        "start_md": start_md,
        "end_md": end_md,
        "direction": direction,
        "wins": wins,
        "n": len(samples),
        "hit_rate": wins / len(samples),
        "average_return_pct": sum(signed) / len(signed),
        "directional_average_pct": sum(directional) / len(directional),
        "samples": samples,
    }


def _md_from_offset(offset: int) -> tuple[int, int]:
    d = date(2000, 1, 1) + timedelta(days=offset % 366)
    return d.month, d.day


def discover_daily_windows(
    instrument_id: str,
    *,
    asof: date | None = None,
    lookback_years: int = LOOKBACK_YEARS,
    min_hit_rate: float = MIN_HIT_RATE,
    min_window_days: int = MIN_WINDOW_DAYS,
    max_window_days: int = MAX_WINDOW_DAYS,
    max_results: int = 20,
) -> dict[str, Any]:
    """Search the calendar for strong 15Y daily windows without benchmark hints.

    Candidates are sampled every two calendar days to keep the first-pass scanner
    cheap enough for cross-market validation. Overlapping near-duplicates are
    collapsed so the result is a shortlist of distinct seasonal opportunities.
    """
    market, error = _prepare(instrument_id)
    if market is None:
        return {"status": "error", "instrument_id": instrument_id, "error": error, "windows": []}

    anchor = asof or market.dates[-1]
    years = list(range(anchor.year - lookback_years, anchor.year))
    candidates: list[dict[str, Any]] = []

    for start_offset in range(0, 366, START_STEP_DAYS):
        start_md = _md_from_offset(start_offset)
        for length in range(min_window_days, max_window_days + 1, END_STEP_DAYS):
            end_md = _md_from_offset(start_offset + length)
            samples = _window_samples(market, start_md, end_md, years)
            if len(samples) != lookback_years:
                continue

            returns = [float(x["return_pct"]) for x in samples]
            up = sum(x > 0 for x in returns)
            down = sum(x < 0 for x in returns)
            direction = "bullish" if up >= down else "bearish"
            wins = up if direction == "bullish" else down
            hit_rate = wins / lookback_years
            if hit_rate < min_hit_rate:
                continue

            directional = returns if direction == "bullish" else [-x for x in returns]
            avg_directional = sum(directional) / len(directional)
            # Ranking rewards repeatability first, then meaningful magnitude.
            score = hit_rate * 100.0 + min(max(avg_directional, 0.0), 10.0)
            candidates.append(
                {
                    "start_md": start_md,
                    "end_md": end_md,
                    "window_days": length,
                    "direction": direction,
                    "wins": wins,
                    "n": lookback_years,
                    "hit_rate": round(hit_rate, 4),
                    "average_return_pct": round(sum(returns) / len(returns), 4),
                    "directional_average_pct": round(avg_directional, 4),
                    "score": round(score, 4),
                }
            )

    candidates.sort(key=lambda x: (-x["score"], -x["wins"], -abs(x["directional_average_pct"])))

    # Collapse highly similar windows. We want opportunities, not dozens of
    # one-day variants of the same seasonal move.
    selected: list[dict[str, Any]] = []
    for candidate in candidates:
        s0 = (date(2000, *candidate["start_md"]) - date(2000, 1, 1)).days
        e0 = s0 + int(candidate["window_days"])
        duplicate = False
        for existing in selected:
            if existing["direction"] != candidate["direction"]:
                continue
            s1 = (date(2000, *existing["start_md"]) - date(2000, 1, 1)).days
            e1 = s1 + int(existing["window_days"])
            if abs(s0 - s1) <= 7 and abs(e0 - e1) <= 7:
                duplicate = True
                break
        if not duplicate:
            selected.append(candidate)
        if len(selected) >= max_results:
            break

    return {
        "status": "ok",
        "engine": "daily_window_edge_v1",
        "instrument_id": instrument_id,
        "source": market.source,
        "asof": anchor.isoformat(),
        "lookback_years": lookback_years,
        "min_hit_rate": min_hit_rate,
        "windows": selected,
        "candidate_count": len(candidates),
    }
