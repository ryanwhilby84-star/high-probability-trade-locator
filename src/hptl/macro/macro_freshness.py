"""Freshness classification for the macro data layer.

Pure functions only (no I/O, no network) so they are trivially unit-testable and
safe to import from anywhere. The freshness band is driven by *refresh age* — the
number of days since a series was last successfully fetched from FRED — not by the
observation latency (which is intrinsic to a series' publication cadence).

Bands (per Stage B spec):
    0-7 days   -> live
    8-30 days  -> cached
    31-90 days -> stale
    >90 days   -> warning
No usable data at all -> missing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

LIVE_MAX_DAYS = 7
CACHED_MAX_DAYS = 30
STALE_MAX_DAYS = 90

# Canonical status vocabulary surfaced to the dashboard.
STATUS_LIVE = "live"
STATUS_CACHED = "cached"
STATUS_STALE = "stale"
STATUS_WARNING = "warning"
STATUS_MISSING = "missing"
STATUS_UNKNOWN = "unknown"

STATUS_ORDER: tuple[str, ...] = (
    STATUS_LIVE,
    STATUS_CACHED,
    STATUS_STALE,
    STATUS_WARNING,
    STATUS_MISSING,
    STATUS_UNKNOWN,
)


def parse_iso(ts: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp into an aware UTC datetime (or None)."""
    if not ts or not isinstance(ts, str):
        return None
    raw = ts.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_days_from(ts: Any, *, now: datetime | None = None) -> int | None:
    """Whole days between an ISO timestamp and now (None if unparseable)."""
    dt = parse_iso(ts)
    if dt is None:
        return None
    ref = now or datetime.now(timezone.utc)
    delta = ref - dt
    return max(0, int(delta.total_seconds() // 86400))


def latency_days_from_date(date_str: Any, *, now: datetime | None = None) -> int | None:
    """Whole days between an observation date (YYYY-MM-DD) and today."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        d = datetime.strptime(date_str.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    ref = now or datetime.now(timezone.utc)
    return max(0, int((ref - d).total_seconds() // 86400))


def band_for_age(age_days: int | None) -> str:
    """Map a refresh age (in days) to a freshness band."""
    if age_days is None:
        return STATUS_UNKNOWN
    if age_days <= LIVE_MAX_DAYS:
        return STATUS_LIVE
    if age_days <= CACHED_MAX_DAYS:
        return STATUS_CACHED
    if age_days <= STALE_MAX_DAYS:
        return STATUS_STALE
    return STATUS_WARNING


def data_status(*, available: bool, refresh_age_days: int | None, has_data: bool) -> str:
    """Resolve the surfaced ``data_status`` for a macro relationship map.

    - No usable data (not available and nothing cached) -> ``missing``.
    - Available -> freshness band from refresh age.
    """
    if not available and not has_data:
        return STATUS_MISSING
    if not available:
        # Carried-over / degraded but we still have a renderable dataset.
        return band_for_age(refresh_age_days)
    return band_for_age(refresh_age_days)


def status_label(status: str) -> str:
    return {
        STATUS_LIVE: "Live",
        STATUS_CACHED: "Cached",
        STATUS_STALE: "Stale",
        STATUS_WARNING: "Warning",
        STATUS_MISSING: "Missing",
        STATUS_UNKNOWN: "Unknown",
    }.get(status, status.title() if status else "Unknown")


def empty_status_counts() -> dict[str, int]:
    return {s: 0 for s in STATUS_ORDER}
