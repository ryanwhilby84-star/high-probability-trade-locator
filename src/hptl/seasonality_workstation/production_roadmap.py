"""Production Seasonal Roadmap built from volatility-normalised DAILY returns.

The plotted path is entirely data-derived. Historical daily returns are first
normalised by each year's robust daily volatility, aggregated by trading-day
ordinal with a cross-year median, then rescaled to the market's recent robust
daily volatility. No SMA, spline interpolation, synthetic noise or COT input.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from math import isfinite
from typing import Any

from hptl.seasonality_workstation.indexed_seasonality import (
    MIN_BARS_PER_YEAR,
    _date_axis_for_trading_days,
    _trading_day_index_for_asof,
    calendar_doy,
    complete_year_bars,
    load_daily_closes_for_seasonality,
)
from hptl.seasonality_workstation.seasonal_roadmap import historical_horizon_stats
from hptl.seasonality_workstation.seasonal_texture import (
    build_texture_profile,
    compound_texture,
    recent_daily_scale,
)

METHOD_VERSION = "volatility_normalised_daily_texture_v4"
METHOD_NAME = "median_normalised_daily_return_rescaled_to_recent_volatility"
PRODUCT_NAME = "Seasonal Roadmap"
NO_EDGE = "NO RELIABLE SEASONAL EDGE"
EDGE = "RELIABLE SEASONAL EDGE"


def _num(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if isfinite(value) else None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _reliability(research: dict[str, Any], block: dict[str, Any]) -> dict[str, Any]:
    """Transparent evidence gate; separate from plotted path amplitude."""
    n_years = int(block.get("sample_size") or 0)
    h8 = (block.get("forward_horizons") or {}).get("8w") or {}
    h8_n = int(h8.get("n") or 0)
    mean8 = _num(h8.get("mean_return"))
    median8 = _num(h8.get("median_return"))
    pos8 = _num(h8.get("positive_frequency"))
    dispersion8 = _num(h8.get("dispersion"))
    agreement = _num((research.get("lookback_agreement") or {}).get("score"))
    walk = research.get("walk_forward") or {}
    wf_hit = _num(walk.get("hit_rate"))
    wf_n = int(walk.get("n") or 0)
    integrity_pass = (research.get("integrity") or {}).get("status") == "PASS"

    sample_score = _clamp01(n_years / 15.0)
    agreement_score = 0.5 if agreement is None else _clamp01(agreement)
    oos_score = 0.0 if wf_hit is None or wf_n <= 0 else _clamp01((wf_hit - 0.50) / 0.25)
    dispersion_score = 0.4 if dispersion8 is None else 1.0 - _clamp01(abs(dispersion8) / 0.08)
    direction_score = 0.0 if pos8 is None else _clamp01(abs(pos8 - 0.5) * 2.0)
    if mean8 is None or median8 is None:
        outlier_score = 0.4
    else:
        scale = max(abs(dispersion8 or 0.0), 0.005)
        outlier_score = 1.0 - _clamp01(abs(mean8 - median8) / (1.5 * scale))
    integrity_score = 1.0 if integrity_pass else 0.0
    composite = (
        0.20 * sample_score + 0.15 * agreement_score + 0.20 * oos_score
        + 0.15 * dispersion_score + 0.10 * direction_score
        + 0.10 * outlier_score + 0.10 * integrity_score
    )

    reasons: list[str] = []
    if not integrity_pass:
        reasons.append("data_integrity_not_pass")
    if n_years < 7:
        reasons.append(f"sample_years_below_7:{n_years}")
    if h8_n < 7:
        reasons.append(f"8w_horizon_samples_below_7:{h8_n}")
    if wf_n < 5:
        reasons.append(f"walk_forward_samples_below_5:{wf_n}")
    if wf_hit is None or wf_hit < 0.50:
        reasons.append("walk_forward_directional_hit_rate_below_50pct")
    if agreement is not None and agreement < 0.60:
        reasons.append("lookback_direction_agreement_below_60pct")
    if composite < 0.55:
        reasons.append(f"composite_below_0.55:{composite:.3f}")

    reliable = not reasons
    return {
        "score": round(composite, 3),
        "label": "HIGH" if composite >= 0.72 else "MEDIUM" if composite >= 0.55 else "LOW",
        "reliable": reliable,
        "verdict": EDGE if reliable else NO_EDGE,
        "reasons": reasons,
        "factors": {
            "sample_size": round(sample_score, 3),
            "lookback_agreement": round(agreement_score, 3),
            "out_of_sample_stability": round(oos_score, 3),
            "dispersion": round(dispersion_score, 3),
            "directional_consistency": round(direction_score, 3),
            "outlier_independence": round(outlier_score, 3),
            "data_integrity": round(integrity_score, 3),
        },
    }


def _load_daily_for_research(research: dict[str, Any]) -> list[tuple[str, float]]:
    supplied = research.get("_daily_closes")
    if supplied:
        return sorted((str(d)[:10], float(c)) for d, c in supplied if float(c) > 0)
    instrument_id = research.get("instrument_id") or research.get("price_instrument_id")
    if not instrument_id:
        return []
    daily, _meta = load_daily_closes_for_seasonality(str(instrument_id))
    return sorted((str(d)[:10], float(c)) for d, c in daily if float(c) > 0)


def build_production_roadmap(research: dict[str, Any]) -> dict[str, Any]:
    selected = research.get("selected_lookback") or "15Y"
    block = (research.get("lookbacks") or {}).get(selected) or {}
    anchor = research.get("anchor") or {}
    asof = str(anchor.get("date") or "")[:10]
    anchor_price = _num(anchor.get("price"))
    daily = _load_daily_for_research(research)
    if not block or not asof or anchor_price is None or anchor_price <= 0 or not daily:
        return {"available": False, "reason": "missing_daily_texture_inputs"}

    daily = [(d, c) for d, c in daily if d <= asof]
    lookback_years = None if selected == "FULL" else int(str(selected).rstrip("Y"))
    if lookback_years is None:
        lookback_years = max(
            5,
            int(anchor.get("iso_year") or date.today().year) - int(daily[0][0][:4]),
        )
    years = complete_year_bars(
        daily,
        asof=asof,
        lookback_years=lookback_years,
        min_bars=MIN_BARS_PER_YEAR,
    )
    if len(years) < 5:
        return {
            "available": False,
            "reason": "insufficient_complete_years",
            "sample_size": len(years),
        }

    target_scale = recent_daily_scale(daily, asof=asof)
    if target_scale is None or target_scale <= 0:
        return {"available": False, "reason": "missing_recent_volatility_scale"}

    profile, d_len = build_texture_profile(years, target_scale=target_scale)
    if not profile or d_len < MIN_BARS_PER_YEAR:
        return {"available": False, "reason": "insufficient_daily_texture_profile"}

    index = compound_texture(profile)
    asof_td = min(max(1, _trading_day_index_for_asof(daily, asof)), d_len)
    base = index[asof_td - 1]
    if base <= 0:
        return {"available": False, "reason": "invalid_texture_anchor"}
    prices = [anchor_price * (v / base) for v in index]

    axis = _date_axis_for_trading_days(daily, asof=asof, d_len=d_len)
    asof_date = date.fromisoformat(asof)
    cursor = asof_date
    full_year: list[dict[str, Any]] = []
    for i, (px, st) in enumerate(zip(prices, profile)):
        td = i + 1
        if td < asof_td:
            segment = "historical"
            dt = axis[i] if i < len(axis) else asof
        elif td == asof_td:
            segment = "today"
            dt = asof
            px = anchor_price
        else:
            segment = "forward"
            cursor += timedelta(days=1)
            while cursor.weekday() >= 5:
                cursor += timedelta(days=1)
            dt = cursor.isoformat()

        seasonal_return = _num(st.get("seasonal_return"))
        full_year.append(
            {
                "trading_day": td,
                "date": dt,
                "doy": calendar_doy(date.fromisoformat(dt)),
                "price": float(anchor_price) if segment == "today" else round(float(px), 6),
                "index": round(float(index[i]), 8),
                "segment": segment,
                "sample_count": int(st.get("n") or 0),
                "seasonal_return": seasonal_return,
                # Compatibility alias for older tooltips/audits; same v4 return.
                "trimmed_mean_return": seasonal_return,
                "normalised_median_return": _num(st.get("normalised_median")),
                "directional_agreement": _num(st.get("directional_agreement")),
                "positive_frequency": _num(st.get("positive_frequency")),
                "typical_abs_normalised_move": _num(st.get("typical_abs_normalised_move")),
                "target_daily_scale": float(target_scale),
            }
        )

    stats = historical_horizon_stats(years, asof=asof, daily=daily)
    reliability = _reliability(research, block)
    return {
        "available": True,
        "asof": asof,
        "asof_price": anchor_price,
        "anchor_price": anchor_price,
        "asof_trading_day": asof_td,
        "sample_years": sorted(years.keys()),
        "sample_size": len(years),
        "target_daily_scale": float(target_scale),
        "unsmoothed": {
            "full_year": full_year,
            "historical": [p for p in full_year if p["segment"] in {"historical", "today"}],
            "forward": [p for p in full_year if p["segment"] in {"today", "forward"}],
        },
        "smoothed": None,
        "smooth_window": None,
        "forecast_stats": stats,
        "reliability": reliability,
        "method": {
            "version": METHOD_VERSION,
            "name": METHOD_NAME,
            "product": PRODUCT_NAME,
            "lookback_years": lookback_years,
            "alignment": "trading_day_of_year",
            "historical_normalisation": "each_year_daily_returns_divided_by_year_median_absolute_daily_return",
            "aggregation": "cross_year_median_normalised_daily_return",
            "volatility_rescale": "recent_60_observation_median_absolute_daily_return",
            "compounding": "C_d = C_(d-1) * (1 + normalised_median_d * current_robust_daily_scale)",
            "rebase": "current_trading_day_pinned_to_current_price",
            "interpolation": "none_in_payload",
            "smooth": None,
            "smooth_optional": False,
            "synthetic_noise": False,
            "units": "price",
            "forecast_stats_source": "historical_asof_to_horizon_daily_returns",
            "cot_dependency": "none",
        },
    }


def _direction_from_roadmap(roadmap: dict[str, Any]) -> str:
    row = (roadmap.get("forecast_stats") or {}).get("12w") or {}
    mean = _num(row.get("mean"))
    median = _num(row.get("median"))
    bull = _num(row.get("bullish_frequency"))
    bear = _num(row.get("bearish_frequency"))
    if mean is not None and median is not None and bull is not None and mean > 0 and median > 0 and bull > 0.5:
        return "Bullish"
    if mean is not None and median is not None and bear is not None and mean < 0 and median < 0 and bear > 0.5:
        return "Bearish"
    return "Neutral"


def apply_production_seasonality(research: dict[str, Any]) -> dict[str, Any]:
    if research.get("status") != "ok":
        return research
    out = deepcopy(research)
    roadmap = build_production_roadmap(out)
    if not roadmap.get("available"):
        return out

    out["seasonal_roadmap"] = roadmap
    out["monthly_roadmap"] = roadmap
    out["reliability"] = roadmap.get("reliability")
    seasonality = out.setdefault("seasonality", {})
    seasonality["primary"] = METHOD_VERSION
    seasonality["seasonal_roadmap"] = roadmap
    seasonality["monthly_roadmap"] = roadmap

    defaults = out.setdefault("display_defaults", {})
    defaults["primary_chart"] = "seasonal_roadmap"
    defaults["seasonal_view"] = "roadmap"
    defaults["methodology_label"] = "Volatility-Normalised Daily Seasonal Roadmap"
    defaults["roadmap_smoothed"] = False

    weekly = out.get("weekly_roadmap") or {}
    comparison = weekly.get("comparison") or {}
    if comparison:
        comparison["monthly_roadmap_direction"] = _direction_from_roadmap(roadmap)
        comparison["monthly_label"] = "Daily Seasonal Roadmap"
    return out
