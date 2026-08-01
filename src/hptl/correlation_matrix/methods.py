"""Correlation methods.

Phase 1 implements Pearson only. Spearman / Kendall can register later
without changing the matrix engine architecture.
"""

from __future__ import annotations

import math
from typing import Protocol


class CorrelationMethod(Protocol):
    name: str

    def correlate(self, x: list[float], y: list[float]) -> float | None:
        """Return r in [-1, 1], or None if undefined / insufficient."""
        ...


class PearsonMethod:
    """Sample Pearson correlation (divide by n-1)."""

    name = "pearson"

    def correlate(self, x: list[float], y: list[float]) -> float | None:
        n = len(x)
        if n != len(y) or n < 2:
            return None
        mx = sum(x) / n
        my = sum(y) / n
        num = 0.0
        dx2 = 0.0
        dy2 = 0.0
        for a, b in zip(x, y):
            da = a - mx
            db = b - my
            num += da * db
            dx2 += da * da
            dy2 += db * db
        if dx2 <= 0.0 or dy2 <= 0.0:
            return None
        r = num / math.sqrt(dx2 * dy2)
        # Numerical guard — keep within theoretical bounds.
        if r > 1.0:
            r = 1.0
        elif r < -1.0:
            r = -1.0
        if not math.isfinite(r):
            return None
        return r


_METHODS: dict[str, CorrelationMethod] = {
    PearsonMethod.name: PearsonMethod(),
}


def get_method(name: str) -> CorrelationMethod:
    key = str(name or "").strip().lower()
    if key not in _METHODS:
        raise ValueError(f"Unsupported correlation method: {name!r}")
    return _METHODS[key]


def register_method(method: CorrelationMethod) -> None:
    """Extension point for Spearman / Kendall (Phase 1 unused)."""
    _METHODS[method.name] = method


def available_methods() -> list[str]:
    return sorted(_METHODS.keys())
