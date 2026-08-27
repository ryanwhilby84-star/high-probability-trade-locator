"""Pairwise date alignment for return series.

Only common dates are used. No forward filling. No fabricated values.
"""

from __future__ import annotations


def align_returns_pairwise(
    a: list[tuple[str, float]],
    b: list[tuple[str, float]],
) -> tuple[list[float], list[float], list[str]]:
    """Inner-join two return series by date. NaNs / non-finite dropped."""
    if not a or not b:
        return [], [], []

    map_a: dict[str, float] = {}
    for d, r in a:
        try:
            v = float(r)
        except (TypeError, ValueError):
            continue
        if v == v:
            map_a[str(d)[:10]] = v

    map_b: dict[str, float] = {}
    for d, r in b:
        try:
            v = float(r)
        except (TypeError, ValueError):
            continue
        if v == v:
            map_b[str(d)[:10]] = v

    dates = sorted(set(map_a.keys()) & set(map_b.keys()))
    xa = [map_a[d] for d in dates]
    xb = [map_b[d] for d in dates]
    return xa, xb, dates


def take_last_n(
    x: list[float],
    y: list[float],
    dates: list[str],
    n: int,
) -> tuple[list[float], list[float], list[str]]:
    """Keep the most recent n aligned observations."""
    if n <= 0:
        return [], [], []
    if len(x) <= n:
        return list(x), list(y), list(dates)
    return x[-n:], y[-n:], dates[-n:]
