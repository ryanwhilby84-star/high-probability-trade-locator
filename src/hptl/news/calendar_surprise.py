"""Actual vs forecast/previous surprise labels for calendar rows (not trade signals)."""
from __future__ import annotations

import math
from typing import Any


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _scale(forecast: float | None, previous: float | None, actual: float | None) -> float:
    parts = [abs(forecast or 0), abs(previous or 0), abs(actual or 0), 1.0]
    return max(parts)


def _classify_direction(delta: float, scale: float) -> str:
    rel = abs(delta) / scale
    if rel < 0.02:
        return "inline"
    return "beat" if delta > 0 else "miss"


def _classify_magnitude(delta: float, scale: float) -> str:
    rel = abs(delta) / scale
    if rel < 0.02:
        return "small"
    if rel < 0.08:
        return "medium"
    return "large"


def compute_surprise_fields(
    *,
    actual: float | None,
    forecast: float | None,
    previous: float | None,
) -> dict[str, Any]:
    """Return surprise metadata for dashboard/API consumers."""
    a = _num(actual)
    f = _num(forecast)
    p = _num(previous)

    if a is None and f is None and p is None:
        return {
            "surprise_vs_forecast": None,
            "surprise_vs_previous": None,
            "direction_vs_forecast": None,
            "direction_vs_previous": None,
            "magnitude_vs_forecast": None,
            "magnitude_vs_previous": None,
            "data_quality": "missing_actual",
        }

    if a is None:
        dq = "missing_actual"
        if f is None and p is None:
            dq = "missing_actual"
        return {
            "surprise_vs_forecast": None,
            "surprise_vs_previous": None,
            "direction_vs_forecast": None,
            "direction_vs_previous": None,
            "magnitude_vs_forecast": None,
            "magnitude_vs_previous": None,
            "data_quality": dq,
        }

    scale = _scale(f, p, a)
    out: dict[str, Any] = {"data_quality": "complete"}

    if f is not None:
        d = a - f
        out["surprise_vs_forecast"] = d
        out["direction_vs_forecast"] = _classify_direction(d, scale)
        out["magnitude_vs_forecast"] = _classify_magnitude(d, scale)
    else:
        out["surprise_vs_forecast"] = None
        out["direction_vs_forecast"] = None
        out["magnitude_vs_forecast"] = None
        out["data_quality"] = "missing_forecast"

    if p is not None:
        d2 = a - p
        out["surprise_vs_previous"] = d2
        out["direction_vs_previous"] = _classify_direction(d2, scale)
        out["magnitude_vs_previous"] = _classify_magnitude(d2, scale)
    else:
        out["surprise_vs_previous"] = None
        out["direction_vs_previous"] = None
        out["magnitude_vs_previous"] = None

    if f is None and a is not None:
        out["data_quality"] = "missing_forecast"
    elif f is not None and a is not None:
        out["data_quality"] = "complete"

    return out
