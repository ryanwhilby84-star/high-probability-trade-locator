"""Shared constants and lookback definitions for Seasonality Workstation."""

from __future__ import annotations

LOOKBACKS: tuple[tuple[str, int | None], ...] = (
    ("5Y", 5),
    ("10Y", 10),
    ("15Y", 15),
    ("20Y", 20),
    ("FULL", None),
)

DEFAULT_LOOKBACK = "15Y"
FORWARD_WEEKS = 12
TRIM_FRACTION = 0.10
MIN_YEARS_FOR_PASS = 5
MIN_WEEKS_PER_YEAR = 40
MAX_GAP_DAYS = 14
MAX_SINGLE_DAY_RETURN = 0.35  # 35% absolute daily move flags discontinuity
TURN_HALF_WINDOW_WEEKS = 2
TURN_FOLLOW_WEEKS = 8
ENGINE_VERSION = "seasonality_robust_weekly_v2"
