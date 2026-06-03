"""Derive trader-readable weather risk tags from OpenWeather 3h forecast slices (factual only)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class RegionClimate:
    """Approximate mid-month normals (°F) for anomaly context — not a price forecast."""

    typical_high_f: float
    typical_low_f: float


# Month-agnostic mid-season normals per hub (conservative, for anomaly wording only).
REGION_CLIMATE: dict[str, RegionClimate] = {
    "Chicago": RegionClimate(78, 58),
    "New York": RegionClimate(76, 58),
    "Houston": RegionClimate(86, 58),
    "Dallas": RegionClimate(84, 60),
    "Kansas": RegionClimate(82, 62),
    "Oklahoma": RegionClimate(84, 62),
    "Nebraska": RegionClimate(80, 58),
    "Texas": RegionClimate(86, 62),
    "Henry Hub / Gulf Coast": RegionClimate(86, 58),
    "Chicago demand hub": RegionClimate(78, 58),
    "Kansas City HRW": RegionClimate(82, 62),
    "Chicago futures hub": RegionClimate(78, 58),
    "Iowa crop belt": RegionClimate(80, 60),
    "Illinois belt": RegionClimate(80, 60),
    "Paranaguá export (proxy)": RegionClimate(82, 64),
    "São Paulo arabica belt": RegionClimate(79, 58),
    "Vietnam robusta (Đắk Lắk proxy)": RegionClimate(88, 72),
    "Abidjan / Ivory Coast": RegionClimate(88, 74),
    "Accra / Ghana": RegionClimate(88, 75),
}


@dataclass
class ForecastSlice:
    dt: datetime
    temp_f: float | None
    temp_min_f: float | None
    temp_max_f: float | None
    weather_id: int | None
    description: str
    rain_mm_3h: float
    snow_mm_3h: float
    pop: float | None  # probability of precipitation 0–1


@dataclass
class WeatherRiskSignals:
    heatwave: bool = False
    cold_snap: bool = False
    storm: bool = False
    heavy_precip: bool = False
    dry_spell: bool = False
    temp_anomaly_high: bool = False
    temp_anomaly_low: bool = False
    tags: list[str] = field(default_factory=list)
    max_temp_f: float | None = None
    min_temp_f: float | None = None
    rain_24h_mm: float = 0.0
    anomaly_high_f: float | None = None
    anomaly_low_f: float | None = None


def _to_float(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        v = float(x)
        return v if v == v else None  # NaN check
    except (TypeError, ValueError):
        return None


def parse_forecast_slices(payload: dict[str, Any], *, max_slices: int = 40) -> list[ForecastSlice]:
    items = payload.get("list")
    if not isinstance(items, list):
        return []
    out: list[ForecastSlice] = []
    for row in items[:max_slices]:
        if not isinstance(row, dict):
            continue
        main = row.get("main") if isinstance(row.get("main"), dict) else {}
        rain = row.get("rain") if isinstance(row.get("rain"), dict) else {}
        snow = row.get("snow") if isinstance(row.get("snow"), dict) else {}
        w = row.get("weather")
        wid: int | None = None
        desc = ""
        if isinstance(w, list) and w and isinstance(w[0], dict):
            try:
                wid = int(w[0].get("id") or 0)
            except (TypeError, ValueError):
                wid = None
            desc = str(w[0].get("description") or "").strip()
        ts = row.get("dt")
        try:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc) if ts is not None else datetime.now(timezone.utc)
        except (TypeError, ValueError, OSError):
            dt = datetime.now(timezone.utc)
        out.append(
            ForecastSlice(
                dt=dt,
                temp_f=_to_float(main.get("temp")),
                temp_min_f=_to_float(main.get("temp_min")),
                temp_max_f=_to_float(main.get("temp_max")),
                weather_id=wid,
                description=desc,
                rain_mm_3h=_to_float(rain.get("3h")) or 0.0,
                snow_mm_3h=_to_float(snow.get("3h")) or 0.0,
                pop=_to_float(row.get("pop")),
            )
        )
    return out


def _is_storm(wid: int | None, rain_mm: float, desc: str) -> bool:
    if wid is not None and 200 <= wid < 300:
        return True
    if rain_mm >= 15.0:
        return True
    d = desc.lower()
    return any(k in d for k in ("thunder", "storm", "tornado", "hurricane"))


def analyze_forecast(
    region_label: str,
    slices: list[ForecastSlice],
    *,
    is_crop_belt: bool = False,
) -> WeatherRiskSignals:
    """Flag objective conditions from API fields + static regional normals."""
    climate = REGION_CLIMATE.get(region_label, RegionClimate(75, 55))
    sig = WeatherRiskSignals()

    if not slices:
        return sig

    temps: list[float] = []
    mins: list[float] = []
    maxs: list[float] = []
    rain_chunks: list[float] = []
    storm_hit = False
    heavy_rain_hit = False

    for sl in slices[:16]:  # ~48h window
        if sl.temp_f is not None:
            temps.append(sl.temp_f)
        if sl.temp_min_f is not None:
            mins.append(sl.temp_min_f)
        if sl.temp_max_f is not None:
            maxs.append(sl.temp_max_f)
        precip = sl.rain_mm_3h + sl.snow_mm_3h
        rain_chunks.append(precip)
        if _is_storm(sl.weather_id, precip, sl.description):
            storm_hit = True
        if precip >= 8.0:
            heavy_rain_hit = True

    sig.max_temp_f = max(maxs or temps or [None])
    sig.min_temp_f = min(mins or temps or [None])
    sig.rain_24h_mm = sum(rain_chunks[:8])

    if sig.max_temp_f is not None:
        delta_hi = sig.max_temp_f - climate.typical_high_f
        if delta_hi >= 12:
            sig.temp_anomaly_high = True
            sig.anomaly_high_f = delta_hi
        if sig.max_temp_f >= climate.typical_high_f + 8:
            sig.heatwave = True

    if sig.min_temp_f is not None:
        delta_lo = climate.typical_low_f - sig.min_temp_f
        if delta_lo >= 12:
            sig.temp_anomaly_low = True
            sig.anomaly_low_f = delta_lo
        if sig.min_temp_f <= climate.typical_low_f - 10:
            sig.cold_snap = True

    sig.storm = storm_hit
    sig.heavy_precip = heavy_rain_hit or sig.rain_24h_mm >= 20.0

    # Drought watch: crop regions, negligible rain across full 5-day window
    if is_crop_belt and len(slices) >= 20:
        total_rain = sum(s.rain_mm_3h + s.snow_mm_3h for s in slices)
        if total_rain < 1.0:
            sig.dry_spell = True

    if sig.storm:
        sig.tags.append("storms")
    if sig.heatwave:
        sig.tags.append("heatwave")
    if sig.cold_snap:
        sig.tags.append("cold snap")
    if sig.heavy_precip:
        sig.tags.append("heavy precipitation")
    elif sig.rain_24h_mm >= 10.0:
        sig.tags.append("precipitation anomaly")
    if sig.dry_spell:
        sig.tags.append("drought watch")
    if sig.temp_anomaly_high:
        sig.tags.append("temperature anomaly (warm)")
    if sig.temp_anomaly_low:
        sig.tags.append("temperature anomaly (cold)")

    return sig


def build_trader_summary(region_label: str, sig: WeatherRiskSignals, slices: list[ForecastSlice]) -> str:
    """One compact line: region + active risk tags + observed range (no price bias)."""
    parts: list[str] = [region_label]

    if sig.tags:
        parts.append("; ".join(sig.tags[:4]))

    range_bits: list[str] = []
    if sig.min_temp_f is not None and sig.max_temp_f is not None:
        range_bits.append(f"{sig.min_temp_f:.0f}–{sig.max_temp_f:.0f}°F next 48h")
    elif slices and slices[0].temp_f is not None:
        range_bits.append(f"~{slices[0].temp_f:.0f}°F now")

    if sig.rain_24h_mm >= 1.0:
        range_bits.append(f"~{sig.rain_24h_mm:.0f} mm precip / 24h")

    if sig.anomaly_high_f is not None and sig.temp_anomaly_high:
        range_bits.append(f"+{sig.anomaly_high_f:.0f}°F vs typical high")
    if sig.anomaly_low_f is not None and sig.temp_anomaly_low:
        range_bits.append(f"−{sig.anomaly_low_f:.0f}°F vs typical low")

    if range_bits:
        parts.append(" · ".join(range_bits))
    elif slices and slices[0].description:
        parts.append(slices[0].description)

    return " — ".join(parts[:2]) if len(parts) > 1 else parts[0]


def weather_importance(sig: WeatherRiskSignals) -> str:
    if sig.storm or (sig.temp_anomaly_high and sig.heatwave) or (sig.temp_anomaly_low and sig.cold_snap):
        return "high"
    if sig.heatwave or sig.cold_snap or sig.heavy_precip or sig.dry_spell:
        return "medium"
    return "low"
