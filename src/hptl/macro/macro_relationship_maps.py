"""Build ``macro_relationship_maps`` for all dashboard-tracked macro relationship markets.

Does not touch COT. Each market maps to one primary price vs driver pair (FRED).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.macro import macro_freshness
from hptl.macro.fred_relationship_pair import (
    DEFAULT_OBS_START,
    RelationshipProfile,
    build_relationship_payload,
)

# Canonical public artifact (also mirrored to dist by dashboard_export).
MACRO_MAPS_PUBLIC_PATH = Path("web-dashboard/public/data/macro_relationship_maps_latest.json")


def _skip_live_feeds() -> bool:
    """Weekly COT job sets HPTL_SKIP_LIVE_FEEDS=1; macro/FRED refresh is a separate job.

    Under Stage B this only forces the FRED client into *cache-only* mode (no
    network) — it no longer blanks the maps. Maps are rebuilt from the persistent
    cache, and a non-destructive merge preserves any previously-valid map.
    """
    return str(os.environ.get("HPTL_SKIP_LIVE_FEEDS", "")).strip().lower() in {"1", "true", "yes"}


def _load_previous_maps() -> dict[str, Any]:
    """Read the last published macro_relationship_maps (for non-destructive merge)."""
    path = MACRO_MAPS_PUBLIC_PATH
    if not path.exists():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    maps = doc.get("macro_relationship_maps")
    return maps if isinstance(maps, dict) else {}


def merge_macro_relationship_maps(
    new_maps: dict[str, Any],
    previous_maps: dict[str, Any] | None,
) -> dict[str, Any]:
    """Non-destructive merge: a failed/unavailable refresh never overwrites a
    previously-valid macro relationship map.

    For each market:
      * If the new build is available -> use it (fresh).
      * Else if a previous available map exists -> carry it over, recompute its
        ``data_status`` from its stored ``last_successful_refresh`` age, and tag it
        ``carried_over`` with the new failure reason.
      * Else -> keep the new (error/missing) payload, classified as ``missing``.
    """
    previous_maps = previous_maps or {}
    now_iso = datetime.now(timezone.utc).isoformat()
    out: dict[str, Any] = {}
    for market, new_payload in new_maps.items():
        prev_payload = previous_maps.get(market)
        if isinstance(new_payload, dict) and new_payload.get("available") is True:
            out[market] = new_payload
            continue
        if isinstance(prev_payload, dict) and prev_payload.get("available") is True:
            carried = dict(prev_payload)
            age = macro_freshness.age_days_from(carried.get("last_successful_refresh"))
            carried["data_status"] = macro_freshness.data_status(
                available=True, refresh_age_days=age, has_data=True
            )
            carried["freshness_band"] = macro_freshness.band_for_age(age)
            carried["refresh_age_days"] = age
            carried["carried_over"] = True
            carried["last_refresh_error"] = (
                new_payload.get("error") if isinstance(new_payload, dict) else "refresh failed"
            )
            carried["last_refresh_error_at"] = now_iso
            out[market] = carried
            continue
        # No usable data anywhere -> missing.
        payload = dict(new_payload) if isinstance(new_payload, dict) else {"available": False, "market": market}
        payload["data_status"] = macro_freshness.STATUS_MISSING
        payload["last_refresh_error_at"] = now_iso
        out[market] = payload
    return out

# Markets explicitly requested for expansion (subset of TARGET_MARKETS).
MACRO_RELATIONSHIP_MARKETS: tuple[str, ...] = (
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
    "Gold",
    "Silver",
    "Copper / HG",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Coffee",
    "Cocoa",
    "Wheat",
    "Corn",
    "Soybeans",
)


def _profiles() -> dict[str, RelationshipProfile]:
    """Primary driver per market — FRED-backed where series support cadence."""
    obs_m = "2005-01-01"
    obs_q = "2000-01-01"
    return {
        "NASDAQ / NQ": {
            "market": "NASDAQ / NQ",
            "price_fred_id": "NASDAQCOM",
            "price_display": "Nasdaq Composite",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "daily",
            "observation_start": DEFAULT_OBS_START,
            "rolling_primary": 20,
            "rolling_secondary": 30,
            "rolling_tertiary": 60,
        },
        "S&P 500 / ES": {
            "market": "S&P 500 / ES",
            "price_fred_id": "SP500",
            "price_display": "S&P 500",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "daily",
            "observation_start": DEFAULT_OBS_START,
            "rolling_primary": 20,
            "rolling_secondary": 30,
            "rolling_tertiary": 60,
        },
        "Dow / YM": {
            "market": "Dow / YM",
            "price_fred_id": "DJIA",
            "price_display": "Dow Jones Industrial Average",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "daily",
            "observation_start": DEFAULT_OBS_START,
            "rolling_primary": 20,
            "rolling_secondary": 30,
            "rolling_tertiary": 60,
        },
        "Gold": {
            "market": "Gold",
            "price_fred_id": "IR14270",
            "price_display": "Gold (import price index)",
            "driver_fred_id": "DTWEXBGS",
            "driver_id": "dxy",
            "driver_display": "Broad US dollar index",
            "driver_is_yield": False,
            "cadence": "monthly",
            "observation_start": obs_m,
            "rolling_primary": 6,
            "rolling_secondary": 9,
            "rolling_tertiary": 12,
        },
        "Silver": {
            "market": "Silver",
            "price_fred_id": "WPU102301",
            "price_display": "Silver (PPI proxy)",
            "driver_fred_id": "IR14270",
            "driver_id": "gold",
            "driver_display": "Gold (import price index)",
            "driver_is_yield": False,
            "cadence": "monthly",
            "observation_start": obs_m,
            "rolling_primary": 6,
            "rolling_secondary": 9,
            "rolling_tertiary": 12,
        },
        "Copper / HG": {
            "market": "Copper / HG",
            "price_fred_id": "PCOPPUSDM",
            "price_display": "Copper import price index",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "monthly",
            "observation_start": obs_m,
            "rolling_primary": 6,
            "rolling_secondary": 9,
            "rolling_tertiary": 12,
        },
        "Crude Oil / CL": {
            "market": "Crude Oil / CL",
            "price_fred_id": "DCOILWTICO",
            "price_display": "WTI crude oil",
            "driver_fred_id": "DTWEXBGS",
            "driver_id": "dxy",
            "driver_display": "Broad US dollar index",
            "driver_is_yield": False,
            "cadence": "daily",
            "observation_start": DEFAULT_OBS_START,
            "rolling_primary": 20,
            "rolling_secondary": 30,
            "rolling_tertiary": 60,
        },
        "Natural Gas / NG": {
            "market": "Natural Gas / NG",
            "price_fred_id": "DHHNGSP",
            "price_display": "Henry Hub natural gas",
            "driver_fred_id": "DCOILWTICO",
            "driver_id": "wti",
            "driver_display": "WTI crude oil",
            "driver_is_yield": False,
            "cadence": "daily",
            "observation_start": DEFAULT_OBS_START,
            "rolling_primary": 20,
            "rolling_secondary": 30,
            "rolling_tertiary": 60,
        },
        "Coffee": {
            "market": "Coffee",
            "price_fred_id": "PCOFFOTMUSDM",
            "price_display": "Coffee import price index",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "monthly",
            "observation_start": obs_m,
            "rolling_primary": 6,
            "rolling_secondary": 9,
            "rolling_tertiary": 12,
        },
        "Cocoa": {
            "market": "Cocoa",
            "price_fred_id": "PCOCOUSDM",
            "price_display": "Cocoa import price index",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "monthly",
            "observation_start": obs_m,
            "rolling_primary": 6,
            "rolling_secondary": 9,
            "rolling_tertiary": 12,
        },
        "Wheat": {
            "market": "Wheat",
            "price_fred_id": "PWHEAMTUSDM",
            "price_display": "Wheat import price index",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "monthly",
            "observation_start": obs_m,
            "rolling_primary": 6,
            "rolling_secondary": 9,
            "rolling_tertiary": 12,
        },
        "Corn": {
            "market": "Corn",
            "price_fred_id": "PMAIZMTUSDM",
            "price_display": "Corn import price index",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "monthly",
            "observation_start": obs_m,
            "rolling_primary": 6,
            "rolling_secondary": 9,
            "rolling_tertiary": 12,
        },
        "Soybeans": {
            "market": "Soybeans",
            "price_fred_id": "PSOYBUSDQ",
            "price_display": "Soybean import price index",
            "driver_fred_id": "DGS10",
            "driver_id": "dgs10",
            "driver_display": "US 10Y Treasury yield",
            "driver_is_yield": True,
            "cadence": "quarterly",
            "observation_start": obs_q,
            "rolling_primary": 5,
            "rolling_secondary": 8,
            "rolling_tertiary": 12,
        },
    }


def build_all_macro_relationship_maps(
    previous_maps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return ``{ market: payload }`` for JSON export.

    Stage B behaviour:
      * Each market is always built. The FRED client serves from the persistent
        cache when ``HPTL_SKIP_LIVE_FEEDS=1`` (cache-only, no network) and fetches
        live (with retry/backoff + cache fallback) otherwise.
      * A non-destructive merge against the previously published maps guarantees a
        transient outage can never blank a map that was valid before.
    """
    out: dict[str, Any] = {}
    profiles = _profiles()
    for market in MACRO_RELATIONSHIP_MARKETS:
        prof = profiles.get(market)
        if not prof:
            out[market] = {
                "available": False,
                "market": market,
                "driver_id": "unknown",
                "error": "No macro relationship profile configured for this market.",
                "data_status": macro_freshness.STATUS_MISSING,
            }
            continue
        try:
            out[market] = build_relationship_payload(prof)
        except Exception as exc:
            out[market] = {
                "available": False,
                "market": market,
                "driver_id": str(prof.get("driver_id", "unknown")),
                "error": f"{type(exc).__name__}: {exc}",
                "data_status": macro_freshness.STATUS_MISSING,
            }

    if previous_maps is None:
        previous_maps = _load_previous_maps()
    return merge_macro_relationship_maps(out, previous_maps)
