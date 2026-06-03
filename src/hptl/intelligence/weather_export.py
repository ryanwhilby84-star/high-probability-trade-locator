"""OpenWeather bundle export for dashboard ``weather_latest.json``."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT, get_openweather_api_key
from hptl.intelligence.weather_adapter import (
    WEATHER_ENABLED_MARKETS,
    WEATHER_MARKET_REGIONS,
    fetch_weather_summaries,
)
from hptl.news.economic_calendar_provider import live_feeds_disabled


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def planned_locations() -> list[dict[str, Any]]:
    """All configured query points (before network)."""
    out: list[dict[str, Any]] = []
    for market in sorted(WEATHER_ENABLED_MARKETS):
        for region in WEATHER_MARKET_REGIONS.get(market) or ():
            out.append(
                {
                    "market": market,
                    "region": region.label,
                    "lat": region.lat,
                    "lon": region.lon,
                }
            )
    return out


def build_weather_bundle() -> dict[str, Any]:
    skip = live_feeds_disabled()
    key = get_openweather_api_key()
    locations = planned_locations()

    doc: dict[str, Any] = {
        "wired": False,
        "skip_live_feeds": skip,
        "openweather_api_key_detected": bool(key),
        "provider": "none",
        "message": "",
        "fetched_at": _now_iso(),
        "locations_queried": locations,
        "regions_queried": [loc["region"] for loc in locations],
        "forecast_records_loaded": 0,
        "markets": {},
    }

    if skip:
        doc["message"] = "Live feeds skipped (HPTL_SKIP_LIVE_FEEDS is set)"
        return doc
    if not key:
        doc["message"] = "OPENWEATHER_API_KEY not configured"
        return doc

    doc["provider"] = "openweather"
    total = 0
    markets_doc: dict[str, Any] = {}

    for market in sorted(WEATHER_ENABLED_MARKETS):
        rows, status = fetch_weather_summaries(market)
        regions_loaded = [str(r.get("region") or "") for r in rows if r.get("region")]
        markets_doc[market] = {
            "status": status,
            "regions_loaded": regions_loaded,
            "summaries": rows,
        }
        total += len(rows)

    doc["markets"] = markets_doc
    doc["forecast_records_loaded"] = total
    doc["wired"] = total > 0
    if not doc["wired"]:
        doc["message"] = "OpenWeather key present but no forecast rows returned"
    return doc


def default_export_path() -> Path:
    return PROJECT_ROOT / "web-dashboard" / "public" / "data" / "weather_latest.json"


def write_weather_latest_export(path: Path | None = None) -> Path:
    doc = build_weather_bundle()
    p = path or default_export_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return p
