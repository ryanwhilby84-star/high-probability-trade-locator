"""Freshness / staleness helpers for Macro Hub fields."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def age_days(as_of: str | None, *, reference: date | None = None) -> int | None:
    d = _parse_date(as_of)
    if d is None:
        return None
    ref = reference or datetime.now(timezone.utc).date()
    return max(0, (ref - d).days)


def freshness_status(
    as_of: str | None,
    *,
    stale_after_days: int,
    reference: date | None = None,
) -> dict[str, Any]:
    """Return {status, as_of, age_days} where status is fresh|stale|missing."""
    if not as_of:
        return {"status": "missing", "as_of": None, "age_days": None}
    days = age_days(as_of, reference=reference)
    if days is None:
        return {"status": "missing", "as_of": as_of, "age_days": None}
    status = "fresh" if days <= stale_after_days else "stale"
    return {"status": status, "as_of": as_of, "age_days": days}
