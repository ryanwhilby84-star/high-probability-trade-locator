"""Shared FX valuation freshness / pipeline diagnostics for dashboard exports."""
from __future__ import annotations

from datetime import date
from typing import Any, Literal

DEFAULT_STALE_DAYS = 14

FreshnessStatus = Literal["fresh", "stale", "missing"]


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def build_stale_warnings(
    *,
    stale_inputs: list[str] | None = None,
    missing_inputs: list[str] | None = None,
    price_stale: bool = False,
    input_latest_dates: dict[str, str | None] | None = None,
) -> list[str]:
    """Human-readable stale-input warnings for dashboard display."""
    warnings: list[str] = []
    stale_inputs = stale_inputs or []
    missing_inputs = missing_inputs or []
    input_latest_dates = input_latest_dates or {}

    if price_stale:
        spot = input_latest_dates.get("spot")
        warnings.append(
            f"stale spot / futures price (as-of {spot})" if spot else "stale spot / futures price"
        )

    seen: set[str] = set()
    for key in stale_inputs + missing_inputs:
        low = key.lower()
        if "y2" in low and "y2" not in seen:
            seen.add("y2")
            warnings.append(f"stale 2Y yield ({key})")
        elif "y10" in low and "y10" not in seen:
            seen.add("y10")
            warnings.append(f"stale 10Y yield ({key})")
        elif "policy" in low and "policy" not in seen:
            seen.add("policy")
            warnings.append(f"stale policy rate ({key})")
        elif "cpi" in low and "cpi" not in seen:
            seen.add("cpi")
            warnings.append(f"stale CPI ({key})")

    dxy = input_latest_dates.get("dxy")
    if dxy and _parse_date(dxy):
        warnings.append(f"DXY / broad USD proxy as-of {dxy}")

    return warnings


def build_stale_reason(warnings: list[str]) -> str | None:
    if not warnings:
        return None
    return "; ".join(warnings)


def assess_input_freshness(
    *,
    input_latest_dates: dict[str, str | None],
    stale_inputs: list[str] | None = None,
    missing_inputs: list[str] | None = None,
    price_stale: bool = False,
    max_stale_days: int = DEFAULT_STALE_DAYS,
    reference: date | None = None,
) -> FreshnessStatus:
    stale_inputs = stale_inputs or []
    missing_inputs = missing_inputs or []
    if missing_inputs:
        return "missing"
    if price_stale or stale_inputs:
        return "stale"
    ref = reference or date.today()
    for as_of in input_latest_dates.values():
        d = _parse_date(as_of)
        if d is None:
            continue
        if (ref - d).days > max_stale_days:
            return "stale"
    return "fresh"


def build_fx_valuation_diagnostics(
    *,
    valuation_date: str | None,
    spot_date: str | None,
    spot: float | None,
    fair_value: float | None,
    raw_gap_pct_unrounded: float | None,
    gap_pct_rounded: float | None,
    input_latest_dates: dict[str, str | None],
    cache_generated_at: str,
    source_file: str,
    stale_inputs: list[str] | None = None,
    missing_inputs: list[str] | None = None,
    price_stale: bool = False,
    max_stale_days: int = DEFAULT_STALE_DAYS,
) -> dict[str, Any]:
    stale_warnings = build_stale_warnings(
        stale_inputs=stale_inputs,
        missing_inputs=missing_inputs,
        price_stale=price_stale,
        input_latest_dates=input_latest_dates,
    )
    freshness = assess_input_freshness(
        input_latest_dates=input_latest_dates,
        stale_inputs=stale_inputs,
        missing_inputs=missing_inputs,
        price_stale=price_stale,
        max_stale_days=max_stale_days,
    )
    stale_reason = build_stale_reason(stale_warnings)
    return {
        "valuation_date": valuation_date,
        "spot_date": spot_date,
        "input_latest_dates": {k: v for k, v in input_latest_dates.items() if v},
        "spot": spot,
        "fair_value": fair_value,
        "raw_gap_pct_unrounded": raw_gap_pct_unrounded,
        "gap_pct_rounded": gap_pct_rounded,
        "cache_generated_at": cache_generated_at,
        "source_file": source_file,
        "freshness_status": freshness,
        "stale_inputs": list(stale_inputs or []),
        "missing_inputs": list(missing_inputs or []),
        "stale_warnings": stale_warnings,
        "stale_reason": stale_reason,
        "price_stale": price_stale,
        "inputs_stale": freshness == "stale",
    }
