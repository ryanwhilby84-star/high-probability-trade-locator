"""OpenWeather forecast summaries for weather-sensitive futures markets."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from hptl.config import get_openweather_api_key, get_settings
from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED
from hptl.intelligence.weather_analysis import (
    analyze_forecast,
    build_trader_summary,
    parse_forecast_slices,
    weather_importance,
)

# Markets with live OpenWeather wiring in the environment feed.
WEATHER_ENABLED_MARKETS = frozenset(
    {
        "Natural Gas / NG",
        "Wheat",
        "Corn",
        "Soybeans",
        "Coffee",
        "Cocoa",
    }
)

CROP_BELT_REGIONS = frozenset(
    {
        "Kansas",
        "Oklahoma",
        "Nebraska",
        "Texas",
        "Kansas City HRW",
        "Iowa crop belt",
        "Illinois belt",
        "São Paulo arabica belt",
        "Vietnam robusta (Đắk Lắk proxy)",
        "Abidjan / Ivory Coast",
        "Accra / Ghana",
    }
)


@dataclass(frozen=True)
class WeatherRegion:
    label: str
    lat: float
    lon: float


WEATHER_MARKET_REGIONS: dict[str, tuple[WeatherRegion, ...]] = {
    "Natural Gas / NG": (
        WeatherRegion("Chicago", 41.88, -87.63),
        WeatherRegion("New York", 40.71, -74.01),
        WeatherRegion("Houston", 29.76, -95.37),
        WeatherRegion("Dallas", 32.78, -96.80),
    ),
    "Wheat": (
        WeatherRegion("Kansas", 38.50, -98.50),
        WeatherRegion("Oklahoma", 35.47, -97.52),
        WeatherRegion("Nebraska", 41.26, -95.94),
        WeatherRegion("Texas", 32.78, -96.80),
    ),
    "Corn": (
        WeatherRegion("Iowa crop belt", 41.59, -93.62),
        WeatherRegion("Chicago futures hub", 41.88, -87.63),
    ),
    "Soybeans": (
        WeatherRegion("Illinois belt", 40.11, -88.24),
        WeatherRegion("Paranaguá export (proxy)", -25.52, -48.51),
    ),
    "Coffee": (
        WeatherRegion("São Paulo arabica belt", -22.91, -43.17),
        WeatherRegion("Vietnam robusta (Đắk Lắk proxy)", 12.67, 108.05),
    ),
    "Cocoa": (
        WeatherRegion("Abidjan / Ivory Coast", 5.36, -4.01),
        WeatherRegion("Accra / Ghana", 5.56, -0.20),
    ),
}


def _fetch_forecast_payload(lat: float, lon: float, api_key: str) -> dict[str, Any] | None:
    settings = get_settings()
    params = {
        "lat": f"{lat:.4f}",
        "lon": f"{lon:.4f}",
        "units": "imperial",
        "appid": api_key,
    }
    url = f"https://api.openweathermap.org/data/2.5/forecast?{urlencode(params)}"
    try:
        req = Request(url, headers={"User-Agent": "hptl-intelligence/1.0"})
        with urlopen(req, timeout=settings.request_timeout_seconds) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
        if isinstance(body, dict) and body.get("cod") not in (None, "200", 200):
            return None
        return body if isinstance(body, dict) else None
    except OSError:
        return None
    except ValueError:
        return None


def _summarize_region(region: WeatherRegion, payload: dict[str, Any]) -> dict[str, Any] | None:
    slices = parse_forecast_slices(payload)
    if not slices:
        return None
    sig = analyze_forecast(
        region.label,
        slices,
        is_crop_belt=region.label in CROP_BELT_REGIONS,
    )
    summary = build_trader_summary(region.label, sig, slices)
    if not summary:
        return None
    return {
        "region": region.label,
        "summary": summary,
        "importance": weather_importance(sig),
        "risk_tags": list(sig.tags),
        "signals": {
            "heatwave": sig.heatwave,
            "cold_snap": sig.cold_snap,
            "storm": sig.storm,
            "heavy_precip": sig.heavy_precip,
            "dry_spell": sig.dry_spell,
            "temp_anomaly_high": sig.temp_anomaly_high,
            "temp_anomaly_low": sig.temp_anomaly_low,
        },
        "raw_payload": {
            "city": payload.get("city"),
            "cnt": payload.get("cnt"),
            "forecast_window_start": slices[0].dt.isoformat() if slices else None,
        },
    }


def fetch_weather_summaries(market: str) -> tuple[list[dict[str, Any]], str]:
    """Return normalized weather rows for ``market``; empty if API key missing."""
    if market not in WEATHER_ENABLED_MARKETS:
        return [], SOURCE_NOT_CONFIGURED

    key = get_openweather_api_key()
    regions = WEATHER_MARKET_REGIONS.get(market)
    if not regions:
        return [], SOURCE_NOT_CONFIGURED
    if not key:
        return [], SOURCE_NOT_CONFIGURED

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for region in regions:
        payload = _fetch_forecast_payload(region.lat, region.lon, key)
        if not payload:
            continue
        block = _summarize_region(region, payload)
        if not block:
            continue
        rows.append(
            {
                "region": block["region"],
                "summary": block["summary"],
                "importance": block["importance"],
                "risk_tags": block["risk_tags"],
                "signals": block["signals"],
                "fetched_at": fetched_at,
                "source": "OpenWeather",
                "provider": "openweather",
                "raw_payload": block["raw_payload"],
            }
        )
    status = "openweather" if rows else "openweather:no_data"
    return rows, status
