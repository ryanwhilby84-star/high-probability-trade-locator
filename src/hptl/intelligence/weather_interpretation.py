"""Translate OpenWeather signals into commodity impact (no price forecasts)."""
from __future__ import annotations

from typing import Any

from hptl.intelligence.weather_analysis import WeatherRiskSignals


def _interp(
    *,
    crop_impact: str,
    crop_label: str,
    price_impact: str,
    price_label: str,
    confidence: str,
    badge: str,
    reason: str,
) -> dict[str, str]:
    return {
        "crop_impact": crop_impact,
        "crop_impact_label": crop_label,
        "price_impact": price_impact,
        "price_impact_label": price_label,
        "confidence": confidence,
        "badge": badge,
        "reason": reason,
    }


def interpret_weather_region(
    market: str,
    *,
    region: str,
    importance: str,
    signals: WeatherRiskSignals | dict[str, Any],
    precip_mm_24h: float,
) -> dict[str, str]:
    """Return interpretation dict for one region (Wheat or Natural Gas / NG)."""
    if isinstance(signals, dict):
        s = signals
        cold = bool(s.get("cold_snap"))
        heat = bool(s.get("heatwave"))
        storm = bool(s.get("storm"))
        heavy = bool(s.get("heavy_precip"))
        dry = bool(s.get("dry_spell"))
        warm_anom = bool(s.get("temp_anomaly_high"))
        cold_anom = bool(s.get("temp_anomaly_low"))
    else:
        cold = signals.cold_snap
        heat = signals.heatwave
        storm = signals.storm
        heavy = signals.heavy_precip
        dry = signals.dry_spell
        warm_anom = signals.temp_anomaly_high
        cold_anom = signals.temp_anomaly_low

    imp = (importance or "low").lower()
    conf = "high" if imp == "high" else ("medium" if imp == "medium" else "low")

    if market == "Wheat":
        return _interpret_wheat(
            region=region,
            cold=cold,
            heat=heat,
            storm=storm,
            heavy=heavy,
            dry=dry,
            warm_anom=warm_anom,
            cold_anom=cold_anom,
            precip_mm=precip_mm_24h,
            base_conf=conf,
        )
    if market == "Natural Gas / NG":
        return _interpret_nat_gas(
            region=region,
            cold=cold,
            heat=heat,
            storm=storm,
            heavy=heavy,
            warm_anom=warm_anom,
            cold_anom=cold_anom,
            precip_mm=precip_mm_24h,
            base_conf=conf,
        )
    return _interp(
        crop_impact="neutral",
        crop_label="Neutral",
        price_impact="neutral",
        price_label="Neutral",
        confidence="low",
        badge="amber",
        reason="Weather context not mapped for this market.",
    )


def _interpret_wheat(
    *,
    region: str,
    cold: bool,
    heat: bool,
    storm: bool,
    heavy: bool,
    dry: bool,
    warm_anom: bool,
    cold_anom: bool,
    precip_mm: float,
    base_conf: str,
) -> dict[str, str]:
    if cold and dry:
        return _interp(
            crop_impact="bad",
            crop_label="Stress risk",
            price_impact="bullish",
            price_label="Bullish wheat",
            confidence="high" if base_conf != "low" else "medium",
            badge="red",
            reason=f"{region}: cold/dry conditions can threaten crop quality and yield.",
        )
    if cold or cold_anom:
        return _interp(
            crop_impact="bad",
            crop_label="Stress risk",
            price_impact="bullish",
            price_label="Bullish wheat",
            confidence=base_conf,
            badge="red",
            reason=f"{region}: cold snap risk to emerging or filling crops.",
        )
    if dry:
        return _interp(
            crop_impact="bad",
            crop_label="Dryness risk",
            price_impact="bullish",
            price_label="Bullish wheat",
            confidence=base_conf,
            badge="red",
            reason=f"{region}: limited rainfall raises moisture-stress concern.",
        )
    if heat or warm_anom:
        return _interp(
            crop_impact="bad",
            crop_label="Heat stress",
            price_impact="bullish",
            price_label="Bullish wheat",
            confidence=base_conf,
            badge="red",
            reason=f"{region}: heat can stress yields during sensitive growth windows.",
        )
    if storm or (heavy and precip_mm >= 25.0):
        return _interp(
            crop_impact="mixed",
            crop_label="Mixed (flood risk)",
            price_impact="mixed",
            price_label="Mixed wheat",
            confidence="medium",
            badge="amber",
            reason=f"{region}: heavy rain/storms can help soil moisture but raise flooding/damage risk.",
        )
    if heavy or precip_mm >= 10.0:
        return _interp(
            crop_impact="good",
            crop_label="Moisture supportive",
            price_impact="bearish",
            price_label="Neutral/Bearish wheat",
            confidence=base_conf,
            badge="green",
            reason=f"{region}: rainfall can improve crop conditions unless totals become excessive.",
        )
    if precip_mm >= 0.1:
        return _interp(
            crop_impact="good",
            crop_label="Moisture adequate",
            price_impact="neutral",
            price_label="Neutral wheat",
            confidence="low",
            badge="green",
            reason=f"{region}: light precipitation is generally crop-supportive.",
        )
    return _interp(
        crop_impact="neutral",
        crop_label="Neutral",
        price_impact="neutral",
        price_label="Neutral wheat",
        confidence="low",
        badge="amber",
        reason=f"{region}: no dominant stress signal in the current forecast window.",
    )


def _interpret_nat_gas(
    *,
    region: str,
    cold: bool,
    heat: bool,
    storm: bool,
    heavy: bool,
    warm_anom: bool,
    cold_anom: bool,
    precip_mm: float,
    base_conf: str,
) -> dict[str, str]:
    if cold or cold_anom:
        return _interp(
            crop_impact="neutral",
            crop_label="Heating demand firm",
            price_impact="bullish",
            price_label="Bullish nat gas",
            confidence="high" if base_conf != "low" else "medium",
            badge="red",
            reason=f"{region}: colder outlook lifts residential/commercial heating demand.",
        )
    if heat or warm_anom:
        return _interp(
            crop_impact="neutral",
            crop_label="Cooling demand elevated",
            price_impact="bullish",
            price_label="Bullish nat gas (power burn)",
            confidence=base_conf,
            badge="red",
            reason=f"{region}: heat can lift power-sector gas burn for cooling.",
        )
    if storm or (heavy and precip_mm >= 15.0):
        return _interp(
            crop_impact="neutral",
            crop_label="Mixed (supply/logistics)",
            price_impact="mixed",
            price_label="Mixed nat gas",
            confidence="medium",
            badge="amber",
            reason=f"{region}: storms can disrupt Gulf production/flows — direction depends on damage vs demand loss.",
        )
    if not cold and not heat and precip_mm < 5.0:
        return _interp(
            crop_impact="neutral",
            crop_label="Mild demand backdrop",
            price_impact="bearish",
            price_label="Bearish nat gas",
            confidence="low",
            badge="green",
            reason=f"{region}: mild conditions reduce heating/cooling demand versus extremes.",
        )
    return _interp(
        crop_impact="neutral",
        crop_label="Neutral demand",
        price_impact="neutral",
        price_label="Neutral nat gas",
        confidence="low",
        badge="amber",
        reason=f"{region}: weather not extreme enough for a clear demand bias.",
    )


def aggregate_weekly_weather_bias(market: str, records: list[dict[str, Any]]) -> dict[str, str]:
    """Combine regional price_impact into one weekly bias line (conservative)."""
    ok = [r for r in records if r.get("ok") and isinstance(r.get("interpretation"), dict)]
    if not ok:
        return {
            "bias": "Mixed",
            "summary_line": f"Weather bias this week: Mixed for {market}.",
        }

    bulls = [r for r in ok if r["interpretation"].get("price_impact") == "bullish"]
    bears = [r for r in ok if r["interpretation"].get("price_impact") == "bearish"]
    mixed = [r for r in ok if r["interpretation"].get("price_impact") == "mixed"]

    bias = "Mixed"
    if bulls and bears:
        bias = "Mixed"
    elif len(bulls) >= 2 and not bears:
        bias = "Bullish"
    elif len(bears) >= 2 and not bulls:
        bias = "Bearish"
    elif len(mixed) >= 2 and not bulls and not bears:
        bias = "Mixed"
    elif len(bulls) == 1 and not bears and not mixed:
        high = bulls[0]["interpretation"].get("confidence") == "high"
        bias = "Bullish" if high else "Mixed"
    elif len(bears) == 1 and not bulls and not mixed:
        high = bears[0]["interpretation"].get("confidence") == "high"
        bias = "Bearish" if high else "Mixed"
    elif not bulls and not bears and not mixed:
        bias = "Neutral"

    return {
        "bias": bias,
        "summary_line": f"Weather bias this week: {bias} for {market}.",
    }
