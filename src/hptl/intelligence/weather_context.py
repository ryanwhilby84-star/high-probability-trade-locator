"""Dashboard weather context: Wheat and Natural Gas regions via OpenWeather."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from hptl.config import get_openweather_api_key, get_settings
from hptl.intelligence.weather_adapter import WeatherRegion
from hptl.intelligence.weather_analysis import (
    analyze_forecast,
    build_trader_summary,
    parse_forecast_slices,
    weather_importance,
)
from hptl.intelligence.weather_interpretation import (
    aggregate_weekly_weather_bias,
    interpret_weather_region,
)

WEATHER_CONTEXT_MARKETS = frozenset({"Wheat", "Natural Gas / NG"})

_CROP_REGIONS = frozenset({"Kansas", "Oklahoma", "Nebraska", "Texas"})

WEATHER_CONTEXT_REGIONS: dict[str, tuple[WeatherRegion, ...]] = {
    "Wheat": (
        WeatherRegion("Kansas", 38.50, -98.50),
        WeatherRegion("Oklahoma", 35.47, -97.52),
        WeatherRegion("Nebraska", 41.26, -95.94),
        WeatherRegion("Texas", 32.78, -96.80),
    ),
    "Natural Gas / NG": (
        WeatherRegion("Chicago", 41.88, -87.63),
        WeatherRegion("New York", 40.71, -74.01),
        WeatherRegion("Houston", 29.76, -95.37),
        WeatherRegion("Dallas", 32.78, -96.80),
    ),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_forecast_payload(lat: float, lon: float, api_key: str) -> tuple[dict[str, Any] | None, str | None]:
    """Return ``(payload, error)`` — error is set when the request or API response fails."""
    settings = get_settings()
    params = {
        "lat": f"{lat:.4f}",
        "lon": f"{lon:.4f}",
        "units": "imperial",
        "appid": api_key,
    }
    url = f"https://api.openweathermap.org/data/2.5/forecast?{urlencode(params)}"
    try:
        req = Request(url, headers={"User-Agent": "hptl-weather-context/1.0"})
        with urlopen(req, timeout=settings.request_timeout_seconds) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace")[:200]
        except OSError:
            pass
        return None, f"HTTP {exc.code}: {exc.reason}" + (f" — {detail}" if detail else "")
    except OSError as exc:
        return None, str(exc)
    except ValueError as exc:
        return None, f"Invalid JSON response: {exc}"

    if not isinstance(body, dict):
        return None, "OpenWeather returned non-object JSON"
    cod = body.get("cod")
    if cod not in (None, "200", 200):
        msg = body.get("message") or body.get("message_key") or "unknown"
        return None, f"OpenWeather cod={cod}: {msg}"
    return body, None


def build_context_record(
    region: WeatherRegion,
    market: str,
    *,
    payload: dict[str, Any] | None,
    error: str | None,
    fetched_at: str,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "region": region.label,
        "market": market,
        "lat": region.lat,
        "lon": region.lon,
        "temperature_f": None,
        "temperature_display": None,
        "precipitation_mm_24h": None,
        "precipitation_display": None,
        "forecast_summary": None,
        "importance": None,
        "timestamp": fetched_at,
        "fetched_at": fetched_at,
        "provider": "openweather",
        "ok": False,
        "error": error,
    }
    if error or not payload:
        base["error"] = error or "No forecast data returned"
        return base

    slices = parse_forecast_slices(payload)
    if not slices:
        base["error"] = "Forecast list empty in OpenWeather response"
        return base

    sig = analyze_forecast(
        region.label,
        slices,
        is_crop_belt=region.label in _CROP_REGIONS,
    )
    summary = build_trader_summary(region.label, sig, slices)
    window_start = slices[0].dt.isoformat() if slices else fetched_at

    temp_display = None
    if sig.min_temp_f is not None and sig.max_temp_f is not None:
        temp_display = f"{sig.min_temp_f:.0f}–{sig.max_temp_f:.0f}°F (48h)"
    elif slices[0].temp_f is not None:
        temp_display = f"{slices[0].temp_f:.0f}°F"

    precip_mm = round(sig.rain_24h_mm, 1)
    if sig.rain_24h_mm >= 0.1:
        precip_display = f"{precip_mm:.1f} mm / 24h"
    else:
        precip_display = "Trace / none (24h)"

    signals = {
        "heatwave": sig.heatwave,
        "cold_snap": sig.cold_snap,
        "storm": sig.storm,
        "heavy_precip": sig.heavy_precip,
        "dry_spell": sig.dry_spell,
        "temp_anomaly_high": sig.temp_anomaly_high,
        "temp_anomaly_low": sig.temp_anomaly_low,
    }
    importance = weather_importance(sig)
    interpretation = interpret_weather_region(
        market,
        region=region.label,
        importance=importance,
        signals=signals,
        precip_mm_24h=precip_mm,
    )

    base.update(
        {
            "temperature_f": {
                "min": sig.min_temp_f,
                "max": sig.max_temp_f,
                "current": slices[0].temp_f,
            },
            "temperature_display": temp_display,
            "precipitation_mm_24h": precip_mm,
            "precipitation_display": precip_display,
            "forecast_summary": summary,
            "importance": importance,
            "risk_tags": list(sig.tags),
            "signals": signals,
            "interpretation": interpretation,
            "timestamp": window_start,
            "ok": True,
            "error": None,
        }
    )
    return base


def fetch_market_context(market: str, *, api_key: str | None = None) -> tuple[list[dict[str, Any]], str]:
    """Fetch all context regions for ``market``; always one record per configured region."""
    regions = WEATHER_CONTEXT_REGIONS.get(market)
    if not regions:
        return [], "not_configured"

    key = (api_key or get_openweather_api_key()).strip()
    if not key:
        fetched_at = _now_iso()
        err = "OPENWEATHER_API_KEY not set"
        return [build_context_record(r, market, payload=None, error=err, fetched_at=fetched_at) for r in regions], err

    fetched_at = _now_iso()
    rows: list[dict[str, Any]] = []
    for region in regions:
        payload, err = fetch_forecast_payload(region.lat, region.lon, key)
        rows.append(build_context_record(region, market, payload=payload, error=err, fetched_at=fetched_at))

    ok_count = sum(1 for r in rows if r.get("ok"))
    status = "openweather" if ok_count == len(rows) else ("openweather:partial" if ok_count else "openweather:error")
    return rows, status


def build_weather_context_bundle(*, respect_skip_live: bool = True) -> dict[str, Any]:
    from hptl.news.economic_calendar_provider import live_feeds_disabled

    skip = respect_skip_live and live_feeds_disabled()
    key = get_openweather_api_key()
    fetched_at = _now_iso()

    doc: dict[str, Any] = {
        "wired": False,
        "skip_live_feeds": skip,
        "openweather_api_key_detected": bool(key),
        "provider": "none",
        "error": None,
        "fetched_at": fetched_at,
        "forecast_records_loaded": 0,
        "forecast_records_ok": 0,
        "locations_queried": [],
        "markets": {},
    }

    for market in sorted(WEATHER_CONTEXT_MARKETS):
        for region in WEATHER_CONTEXT_REGIONS.get(market) or ():
            doc["locations_queried"].append(
                {"market": market, "region": region.label, "lat": region.lat, "lon": region.lon}
            )

    if skip:
        doc["error"] = "HPTL_SKIP_LIVE_FEEDS is enabled — weather fetch skipped"
        for market in sorted(WEATHER_CONTEXT_MARKETS):
            rows = [
                build_context_record(r, market, payload=None, error=doc["error"], fetched_at=fetched_at)
                for r in WEATHER_CONTEXT_REGIONS.get(market) or ()
            ]
            doc["markets"][market] = {"status": "skipped", "records": rows}
        return doc

    if not key:
        doc["error"] = "OPENWEATHER_API_KEY not set in environment"
        for market in sorted(WEATHER_CONTEXT_MARKETS):
            rows = [
                build_context_record(r, market, payload=None, error=doc["error"], fetched_at=fetched_at)
                for r in WEATHER_CONTEXT_REGIONS.get(market) or ()
            ]
            doc["markets"][market] = {"status": "no_key", "records": rows}
        return doc

    doc["provider"] = "openweather"
    total = 0
    ok_total = 0
    for market in sorted(WEATHER_CONTEXT_MARKETS):
        rows, status = fetch_market_context(market, api_key=key)
        weekly = aggregate_weekly_weather_bias(market, rows)
        doc["markets"][market] = {
            "status": status,
            "records": rows,
            "weekly_bias": weekly.get("bias"),
            "weekly_bias_line": weekly.get("summary_line"),
        }
        total += len(rows)
        ok_total += sum(1 for r in rows if r.get("ok"))

    doc["forecast_records_loaded"] = total
    doc["forecast_records_ok"] = ok_total
    doc["wired"] = ok_total > 0
    if ok_total < total:
        doc["error"] = f"{total - ok_total} of {total} region fetches failed — see per-region error fields"
    return doc
