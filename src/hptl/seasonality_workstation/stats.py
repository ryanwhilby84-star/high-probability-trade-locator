"""Robust statistics for seasonal return buckets."""

from __future__ import annotations

import math
from typing import Any

from hptl.seasonality_workstation.models import TRIM_FRACTION


def _finite(xs: list[float]) -> list[float]:
    return [x for x in xs if x is not None and math.isfinite(x)]


def trimmed_mean(xs: list[float], fraction: float = TRIM_FRACTION) -> float | None:
    vals = sorted(_finite(xs))
    n = len(vals)
    if n == 0:
        return None
    k = int(n * fraction)
    if 2 * k >= n:
        return sum(vals) / n
    core = vals[k : n - k] if k else vals
    return sum(core) / len(core)


def bucket_stats(returns: list[float]) -> dict[str, Any]:
    vals = _finite(returns)
    n = len(vals)
    if n == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "trimmed_mean": None,
            "q25": None,
            "q75": None,
            "std": None,
            "positive_frequency": None,
            "dispersion": None,
        }
    ordered = sorted(vals)
    mean = sum(vals) / n
    median = ordered[n // 2] if n % 2 else 0.5 * (ordered[n // 2 - 1] + ordered[n // 2])
    q25 = ordered[max(0, int(0.25 * (n - 1)))]
    q75 = ordered[min(n - 1, int(0.75 * (n - 1)))]
    var = sum((v - mean) ** 2 for v in vals) / n
    std = math.sqrt(var)
    pos = sum(1 for v in vals if v > 0) / n
    return {
        "n": n,
        "mean": mean,
        "median": median,
        "trimmed_mean": trimmed_mean(vals),
        "q25": q25,
        "q75": q75,
        "std": std,
        "positive_frequency": pos,
        "dispersion": std,
    }
