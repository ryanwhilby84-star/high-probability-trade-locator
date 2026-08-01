"""Statistical correlation matrix engine (UI-independent).

Phase 1: Pearson only. Daily / weekly percentage returns.
Lookbacks are open-ended — pass any positive int; defaults include 20/60/120/252.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

from hptl.correlation_matrix.alignment import align_returns_pairwise, take_last_n
from hptl.correlation_matrix.methods import get_method
from hptl.correlation_matrix.prices import load_closes_for_correlation
from hptl.correlation_matrix.returns import ReturnFrequency, returns_for_frequency

logger = logging.getLogger(__name__)

ENGINE_VERSION = "correlation_matrix_v1"
DEFAULT_LOOKBACKS: tuple[int, ...] = (20, 60, 120, 252)
DEFAULT_FREQUENCY: ReturnFrequency = "daily"
DEFAULT_METHOD = "pearson"


def default_universe() -> list[str]:
    from hptl.markets.instrument_registry import LEGACY_COT_MARKETS

    return list(LEGACY_COT_MARKETS)


def _round_corr(r: float | None) -> float | None:
    if r is None:
        return None
    return round(float(r), 6)


def pair_correlation(
    returns_a: list[tuple[str, float]],
    returns_b: list[tuple[str, float]],
    *,
    lookback: int,
    method_name: str = DEFAULT_METHOD,
) -> tuple[float | None, dict[str, Any]]:
    """Correlate two return series after pairwise date alignment."""
    method = get_method(method_name)
    xa, xb, dates = align_returns_pairwise(returns_a, returns_b)
    overlap = len(dates)
    if overlap < lookback:
        return None, {
            "status": "insufficient_overlap",
            "overlap": overlap,
            "required": lookback,
        }
    xa, xb, dates = take_last_n(xa, xb, dates, lookback)
    r = method.correlate(xa, xb)
    if r is None:
        return None, {
            "status": "undefined",
            "overlap": len(dates),
            "required": lookback,
            "reason": "zero_variance_or_non_finite",
        }
    return _round_corr(r), {
        "status": "ok",
        "overlap": len(dates),
        "required": lookback,
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
    }


def build_correlation_matrix(
    *,
    instrument_ids: Sequence[str] | None = None,
    frequency: ReturnFrequency = DEFAULT_FREQUENCY,
    lookback: int = 60,
    method: str = DEFAULT_METHOD,
    return_series: dict[str, list[tuple[str, float]]] | None = None,
) -> dict[str, Any]:
    """Build a full square correlation matrix.

    Parameters
    ----------
    instrument_ids:
        Canonical HPTL IDs. Defaults to LEGACY_COT_MARKETS.
    frequency:
        ``daily`` or ``weekly`` percentage returns.
    lookback:
        Number of most-recent *aligned* return observations required.
        Any positive integer is accepted (extensible beyond the UI presets).
    method:
        Correlation method name (Phase 1: ``pearson`` only).
    return_series:
        Optional precomputed ``{id: [(date, return), ...]}`` for tests /
        reuse. When omitted, prices are loaded from the canonical store.
    """
    if lookback <= 0:
        raise ValueError("lookback must be a positive integer")
    if frequency not in ("daily", "weekly"):
        raise ValueError(f"Unsupported frequency: {frequency!r}")

    get_method(method)  # fail fast on unknown method
    ids = list(instrument_ids) if instrument_ids is not None else default_universe()
    warnings: list[str] = []
    series: dict[str, list[tuple[str, float]]] = {}
    price_meta: dict[str, Any] = {}

    if return_series is not None:
        for iid in ids:
            series[iid] = list(return_series.get(iid) or [])
            if not series[iid]:
                warnings.append(f"{iid}: no_return_series")
                logger.warning("correlation data quality: %s no_return_series", iid)
    else:
        for iid in ids:
            closes, meta = load_closes_for_correlation(iid)
            price_meta[iid] = meta
            if not closes:
                series[iid] = []
                msg = f"{iid}: missing_price_data ({meta.get('error') or 'empty'})"
                warnings.append(msg)
                logger.warning("correlation data quality: %s", msg)
                continue
            rets = returns_for_frequency(closes, frequency)
            series[iid] = rets
            if len(rets) < lookback:
                msg = f"{iid}: short_history returns={len(rets)} required={lookback}"
                warnings.append(msg)
                logger.warning("correlation data quality: %s", msg)

    n = len(ids)
    matrix: list[list[float | None]] = [[None] * n for _ in range(n)]
    pair_meta: dict[str, Any] = {}

    for i, a in enumerate(ids):
        ra = series.get(a) or []
        if not ra:
            continue
        matrix[i][i] = 1.0
        for j in range(i + 1, n):
            b = ids[j]
            rb = series.get(b) or []
            if not rb:
                continue
            r, meta = pair_correlation(ra, rb, lookback=lookback, method_name=method)
            key = f"{a}||{b}"
            pair_meta[key] = meta
            if r is None:
                msg = (
                    f"pair {a} × {b}: {meta.get('status')} "
                    f"overlap={meta.get('overlap')} required={lookback}"
                )
                warnings.append(msg)
                logger.warning("correlation data quality: %s", msg)
                matrix[i][j] = None
                matrix[j][i] = None
            else:
                matrix[i][j] = r
                matrix[j][i] = r

    return {
        "status": "ok",
        "engine": ENGINE_VERSION,
        "method": method,
        "frequency": frequency,
        "lookback": lookback,
        "instruments": ids,
        "matrix": matrix,
        "warnings": warnings,
        "pair_meta": pair_meta,
        "price_meta": price_meta,
        "available_lookbacks": list(DEFAULT_LOOKBACKS),
        "available_frequencies": ["daily", "weekly"],
        "available_methods": [method],
    }


def validate_matrix_payload(payload: dict[str, Any]) -> list[str]:
    """Structural validation — returns list of error strings (empty = pass)."""
    errors: list[str] = []
    ids = payload.get("instruments") or []
    matrix = payload.get("matrix") or []
    n = len(ids)
    if len(matrix) != n:
        errors.append(f"matrix_row_count={len(matrix)} expected={n}")
        return errors
    for i, row in enumerate(matrix):
        if len(row) != n:
            errors.append(f"matrix_col_count row={i} cols={len(row)} expected={n}")
            continue
        diag = row[i]
        if diag is not None and abs(float(diag) - 1.0) > 1e-9:
            errors.append(f"diagonal_not_one i={i} value={diag}")
        for j, v in enumerate(row):
            if v is None:
                continue
            fv = float(v)
            if fv < -1.0 - 1e-12 or fv > 1.0 + 1e-12:
                errors.append(f"out_of_bounds [{i},{j}]={fv}")
            other = matrix[j][i]
            if other is None:
                errors.append(f"asymmetry_null [{i},{j}]={fv} vs [{j},{i}]=null")
            elif abs(float(other) - fv) > 1e-9:
                errors.append(f"asymmetry [{i},{j}]={fv} vs [{j},{i}]={other}")
    return errors
