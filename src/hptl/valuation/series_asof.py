"""Time-series as-of lookup for valuation panels (no FX pair model logic)."""
from __future__ import annotations

from typing import Any


def value_as_of(daily_map: dict[str, float], iso_date: str) -> float | None:
    """Last observation on or before iso_date."""
    if not daily_map:
        return None
    best: str | None = None
    for d in daily_map:
        if d <= iso_date and (best is None or d > best):
            best = d
    return daily_map.get(best) if best else None


def daily_map_from_rows(rows: list[dict[str, Any]], *, date_key: str = "date", value_key: str = "value") -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        d = str(row.get(date_key) or "")[:10]
        v = row.get(value_key)
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if d:
            out[d] = f
    return out
