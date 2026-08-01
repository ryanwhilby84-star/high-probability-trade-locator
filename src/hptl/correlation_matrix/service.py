"""Correlation Matrix service — UI-facing payload builder.

Keeps calculation in the engine; this layer only shapes API responses.
"""

from __future__ import annotations

from typing import Any, Sequence

from hptl.correlation_matrix.engine import (
    DEFAULT_FREQUENCY,
    DEFAULT_LOOKBACKS,
    DEFAULT_METHOD,
    ENGINE_VERSION,
    build_correlation_matrix,
    default_universe,
    validate_matrix_payload,
)
from hptl.correlation_matrix.returns import ReturnFrequency


def build_correlation_matrix_payload(
    *,
    frequency: ReturnFrequency | str = DEFAULT_FREQUENCY,
    lookback: int = 60,
    method: str = DEFAULT_METHOD,
    instrument_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build the Phase 1 workstation payload.

    No portfolio, scoring, COT, seasonality, or recommendations.
    """
    try:
        lb = int(lookback)
    except (TypeError, ValueError):
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_lookback",
            "message": f"lookback must be a positive integer, got {lookback!r}",
        }
    if lb <= 0:
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_lookback",
            "message": "lookback must be a positive integer",
        }

    freq = str(frequency or DEFAULT_FREQUENCY).strip().lower()
    if freq not in ("daily", "weekly"):
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_frequency",
            "message": f"frequency must be daily or weekly, got {frequency!r}",
        }

    try:
        result = build_correlation_matrix(
            instrument_ids=instrument_ids,
            frequency=freq,  # type: ignore[arg-type]
            lookback=lb,
            method=str(method or DEFAULT_METHOD).strip().lower(),
        )
    except ValueError as exc:
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_request",
            "message": str(exc)[:400],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "builder_crash",
            "message": str(exc)[:400],
        }

    errors = validate_matrix_payload(result)
    if errors:
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "matrix_validation_failed",
            "message": "Built matrix failed structural validation.",
            "validation_errors": errors[:50],
            "warnings": result.get("warnings") or [],
        }

    # Compact UI payload — omit bulky pair/price meta from default response.
    return {
        "status": "ok",
        "engine": result["engine"],
        "method": result["method"],
        "frequency": result["frequency"],
        "lookback": result["lookback"],
        "instruments": result["instruments"],
        "matrix": result["matrix"],
        "warnings": result["warnings"],
        "available_lookbacks": result["available_lookbacks"],
        "available_frequencies": result["available_frequencies"],
        "available_methods": result["available_methods"],
        "universe": "LEGACY_COT_MARKETS",
        "universe_size": len(result["instruments"]),
        "display": {
            "value_decimals": 2,
            "color_scale": {
                "negative": "deep_red",
                "zero": "neutral",
                "positive": "green",
            },
        },
    }


def get_instrument_correlation_map(
    instrument_ids: Sequence[str],
    *,
    frequency: ReturnFrequency | str = DEFAULT_FREQUENCY,
    lookback: int = 60,
    method: str = DEFAULT_METHOD,
) -> dict[str, Any]:
    """Phase 1 service helper for basket / pair consumers.

    Returns the full engine matrix **plus** ``pair_meta`` (overlap counts).
    Does not recalculate Pearson outside ``build_correlation_matrix``.
    """
    ids = [str(x) for x in instrument_ids if x]
    # Preserve order, de-dupe
    seen: set[str] = set()
    ordered: list[str] = []
    for iid in ids:
        if iid in seen:
            continue
        seen.add(iid)
        ordered.append(iid)

    try:
        lb = int(lookback)
    except (TypeError, ValueError):
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_lookback",
            "message": f"lookback must be a positive integer, got {lookback!r}",
        }
    if lb <= 0:
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_lookback",
            "message": "lookback must be a positive integer",
        }

    freq = str(frequency or DEFAULT_FREQUENCY).strip().lower()
    if freq not in ("daily", "weekly"):
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_frequency",
            "message": f"frequency must be daily or weekly, got {frequency!r}",
        }

    if not ordered:
        return {
            "status": "ok",
            "engine": ENGINE_VERSION,
            "method": method,
            "frequency": freq,
            "lookback": lb,
            "instruments": [],
            "matrix": [],
            "pair_meta": {},
            "warnings": [],
        }

    try:
        result = build_correlation_matrix(
            instrument_ids=ordered,
            frequency=freq,  # type: ignore[arg-type]
            lookback=lb,
            method=str(method or DEFAULT_METHOD).strip().lower(),
        )
    except ValueError as exc:
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "invalid_request",
            "message": str(exc)[:400],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "error",
            "engine": ENGINE_VERSION,
            "error": "builder_crash",
            "message": str(exc)[:400],
        }

    return {
        "status": "ok",
        "engine": result["engine"],
        "method": result["method"],
        "frequency": result["frequency"],
        "lookback": result["lookback"],
        "instruments": result["instruments"],
        "matrix": result["matrix"],
        "pair_meta": result.get("pair_meta") or {},
        "warnings": result.get("warnings") or [],
    }


def lookup_raw_correlation(
    corr_map: dict[str, Any],
    instrument_a: str,
    instrument_b: str,
) -> dict[str, Any]:
    """Read one pair from a ``get_instrument_correlation_map`` result."""
    ids = list(corr_map.get("instruments") or [])
    matrix = corr_map.get("matrix") or []
    try:
        i = ids.index(instrument_a)
        j = ids.index(instrument_b)
    except ValueError:
        return {
            "status": "missing_instrument",
            "raw_correlation": None,
            "overlapping_return_count": None,
        }
    if i >= len(matrix) or j >= len(matrix[i]):
        return {
            "status": "missing_matrix_cell",
            "raw_correlation": None,
            "overlapping_return_count": None,
        }
    raw = matrix[i][j]
    key = f"{instrument_a}||{instrument_b}"
    key_rev = f"{instrument_b}||{instrument_a}"
    pair_meta = corr_map.get("pair_meta") or {}
    meta = pair_meta.get(key) or pair_meta.get(key_rev) or {}
    if raw is None:
        return {
            "status": meta.get("status") or "missing_correlation",
            "raw_correlation": None,
            "overlapping_return_count": meta.get("overlap"),
            "pair_meta": meta,
        }
    try:
        fv = float(raw)
    except (TypeError, ValueError):
        return {
            "status": "non_finite",
            "raw_correlation": None,
            "overlapping_return_count": meta.get("overlap"),
        }
    if fv != fv:  # NaN
        return {
            "status": "non_finite",
            "raw_correlation": None,
            "overlapping_return_count": meta.get("overlap"),
        }
    return {
        "status": "ok",
        "raw_correlation": fv,
        "overlapping_return_count": meta.get("overlap")
        if meta.get("overlap") is not None
        else corr_map.get("lookback"),
        "pair_meta": meta,
    }


def build_correlation_matrix_payload_from_args(
    frequency: str = "daily",
    lookback: str | int = 60,
) -> dict[str, Any]:
    """CLI helper."""
    return build_correlation_matrix_payload(
        frequency=frequency,
        lookback=int(lookback),
        instrument_ids=default_universe(),
    )


# Re-export presets for docs / UI
LOOKBACK_PRESETS = DEFAULT_LOOKBACKS
