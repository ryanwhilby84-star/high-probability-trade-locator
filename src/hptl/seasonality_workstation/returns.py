"""Daily → weekly closes and return series for seasonality."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def iso_week(date: str) -> tuple[int, int]:
    dt = datetime.strptime(str(date)[:10], "%Y-%m-%d")
    cal = dt.isocalendar()
    week = int(cal.week)
    if week > 52:
        week = 52
    return int(cal.year), week


def weekly_closes_from_daily(daily: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """Last trading day close of each ISO week."""
    buckets: dict[tuple[int, int], tuple[str, float]] = {}
    for d, c in daily:
        y, w = iso_week(d)
        prev = buckets.get((y, w))
        if prev is None or d >= prev[0]:
            buckets[(y, w)] = (d, c)
    return [buckets[k] for k in sorted(buckets.keys())]


def weekly_return_rows(weekly: list[tuple[str, float]]) -> list[dict[str, Any]]:
    """One row per week with return from prior week."""
    rows: list[dict[str, Any]] = []
    for i, (d, c) in enumerate(weekly):
        y, w = iso_week(d)
        ret = None
        if i > 0 and weekly[i - 1][1] > 0:
            ret = c / weekly[i - 1][1] - 1.0
        rows.append(
            {
                "date": d,
                "close": c,
                "iso_year": y,
                "iso_week": w,
                "return": ret,
            }
        )
    return rows


def load_daily_closes(instrument_id: str) -> tuple[list[tuple[str, float]], str | None, str | None]:
    """Load dense daily closes from the canonical price store / timeline."""
    from hptl.prices.canonical_timeline import build_canonical_timeline

    tl = build_canonical_timeline(instrument_id, apply_supplements=False)
    closes = list(tl.daily_closes()) if tl is not None else []
    if not closes:
        # Fallback: raw store record
        from hptl.prices.price_store import load_price_store

        instruments = (load_price_store().get("instruments") or {})
        rec = instruments.get(instrument_id)
        if not rec:
            return [], None, "missing_price_store_record"
        daily_raw = rec.get("daily") or []
        out: list[tuple[str, float]] = []
        for b in daily_raw:
            d = str(b.get("date") or "")[:10]
            try:
                c = float(b.get("close"))
            except (TypeError, ValueError):
                continue
            if d and c == c and c > 0:
                out.append((d, c))
        out.sort(key=lambda t: t[0])
        source = str((rec.get("price_scale") or {}).get("source") or "price_store")
        return out, source, None

    source = getattr(tl, "canonical_source", None) or "canonical_timeline"
    return closes, source, None
