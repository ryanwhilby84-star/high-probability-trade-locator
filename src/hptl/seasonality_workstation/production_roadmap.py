"""Production seasonality roadmap built from robust ISO weekly returns.

This module is the final presentation adapter for the Seasonality Workstation.
It deliberately does not recompute COT or mix any COT state into seasonality.
The underlying research engine already produces week-by-week robust return
statistics for every lookback; this module turns the selected lookback into the
canonical production roadmap contract used by the UI.

Key rules
---------
* one genuine observation per ISO week (1..52)
* 10% trimmed-mean weekly return path from the research engine
* no interpolation, synthetic wiggles, or display SMA in the payload
* current price is the rebase anchor
* horizon statistics remain historical as-of -> horizon observations
* reliability is explicit, multi-factor, and can refuse an edge
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any

METHOD_VERSION = "robust_weekly_returns_v2"
METHOD_NAME = "trimmed_mean_iso_weekly_returns_compounded"
PRODUCT_NAME = "Seasonal Roadmap"
NO_EDGE = "NO RELIABLE SEASONAL EDGE"
EDGE = "RELIABLE SEASONAL EDGE"


def _num(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value == value else None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _week_date(iso_year: int, week: int) -> str:
    """Friday label for an ISO week; week 53 is intentionally not produced."""
    return date.fromisocalendar(int(iso_year), min(max(int(week), 1), 52), 5).isoformat()


def _forecast_stats(block: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    horizons = block.get("forward_horizons") or {}
    for label in ("4w", "8w", "12w", "26w", "48w"):
        row = horizons.get(label) or {}
        mean = _num(row.get("mean_return"))
        median = _num(row.get("median_return"))
        positive = _num(row.get("positive_frequency"))
        n = int(row.get("n") or 0)
        dispersion = _num(row.get("dispersion"))
        out[label] = {
            "weeks": int(label[:-1]),
            "n": n,
            "mean": mean,
            "median": median,
            "mean_pct": None if mean is None else round(mean * 100.0, 3),
            "median_pct": None if median is None else round(median * 100.0, 3),
            "bullish_frequency": positive,
            "bearish_frequency": None if positive is None else round(1.0 - positive, 6),
            "dispersion": dispersion,
            "source": "historical_asof_to_horizon_weekly_returns",
            "not_from_roadmap_amplitude": True,
        }
    return out


def _reliability(research: dict[str, Any], block: dict[str, Any]) -> dict[str, Any]:
    """Transparent production reliability gate; never turns weak data into an edge."""
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
    if wf_hit is None or wf_n <= 0:
        oos_score = 0.0
    else:
        # 50% directional hit-rate is neutral; 75%+ earns full OOS credit.
        oos_score = _clamp01((wf_hit - 0.50) / 0.25)
    dispersion_score = (
        0.4 if dispersion8 is None else 1.0 - _clamp01(abs(dispersion8) / 0.08)
    )
    direction_score = 0.0 if pos8 is None else _clamp01(abs(pos8 - 0.5) * 2.0)
    if mean8 is None or median8 is None:
        outlier_score = 0.4
    else:
        scale = max(abs(dispersion8 or 0.0), 0.005)
        outlier_score = 1.0 - _clamp01(abs(mean8 - median8) / (1.5 * scale))
    integrity_score = 1.0 if integrity_pass else 0.0

    composite = (
        0.20 * sample_score
        + 0.15 * agreement_score
        + 0.20 * oos_score
        + 0.15 * dispersion_score
        + 0.10 * direction_score
        + 0.10 * outlier_score
        + 0.10 * integrity_score
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
        "evidence": {
            "sample_years": n_years,
            "horizon_8w_n": h8_n,
            "horizon_8w_mean": mean8,
            "horizon_8w_median": median8,
            "horizon_8w_positive_frequency": pos8,
            "horizon_8w_dispersion": dispersion8,
            "lookback_agreement": agreement,
            "walk_forward_hit_rate": wf_hit,
            "walk_forward_n": wf_n,
            "integrity_status": (research.get("integrity") or {}).get("status"),
        },
    }


def build_production_roadmap(research: dict[str, Any]) -> dict[str, Any]:
    selected = research.get("selected_lookback")
    block = (research.get("lookbacks") or {}).get(selected) or {}
    anchor = research.get("anchor") or {}
    anchor_week = int(anchor.get("iso_week") or 1)
    iso_year = int(anchor.get("iso_year") or date.today().isocalendar().year)
    anchor_price = _num(anchor.get("price"))
    if not block or anchor_price is None or anchor_price <= 0:
        return {"available": False, "reason": "missing_return_roadmap_inputs"}

    aligned = ((block.get("price_aligned") or {}).get("trimmed_mean") or {})
    index_path = ((block.get("index_paths") or {}).get("trimmed_mean") or {})
    week_stats = block.get("week_stats") or {}
    full_year: list[dict[str, Any]] = []
    for week in range(1, 53):
        price = _num(aligned.get(str(week)))
        idx = _num(index_path.get(str(week)))
        st = week_stats.get(str(week)) or {}
        if week < anchor_week:
            segment = "historical"
        elif week == anchor_week:
            segment = "today"
            price = anchor_price
        else:
            segment = "forward"
        full_year.append(
            {
                "trading_day": week,  # legacy UI key; one observation now means one ISO week
                "iso_week": week,
                "date": _week_date(iso_year, week),
                "price": None if price is None else round(price, 6),
                "index": None if idx is None else round(idx, 8),
                "segment": segment,
                "sample_count": int(st.get("n") or 0),
                "median_return": _num(st.get("median")),
                "trimmed_mean_return": _num(st.get("trimmed_mean")),
                "positive_frequency": _num(st.get("positive_frequency")),
                "dispersion": _num(st.get("dispersion")),
            }
        )

    reliability = _reliability(research, block)
    return {
        "available": True,
        "asof": anchor.get("date"),
        "asof_price": anchor_price,
        "anchor_price": anchor_price,
        "asof_trading_day": anchor_week,
        "asof_iso_week": anchor_week,
        "sample_years": block.get("sample_years") or [],
        "sample_size": int(block.get("sample_size") or 0),
        "unsmoothed": {
            "full_year": full_year,
            "historical": [p for p in full_year if p["segment"] in {"historical", "today"}],
            "forward": [p for p in full_year if p["segment"] in {"today", "forward"}],
        },
        # Deliberately absent: production defaults must not silently bind an SMA curve.
        "smoothed": None,
        "smooth_window": None,
        "forecast_stats": _forecast_stats(block),
        "reliability": reliability,
        "method": {
            "version": METHOD_VERSION,
            "name": METHOD_NAME,
            "product": PRODUCT_NAME,
            "lookback_years": None if selected == "FULL" else int(str(selected).rstrip("Y")),
            "alignment": "iso_calendar_week_1_to_52",
            "aggregation": "10pct_trimmed_mean_weekly_return",
            "compounding": "C_w = C_(w-1) * (1 + robust_return_w)",
            "rebase": "current_iso_week_pinned_to_current_price",
            "interpolation": "none_in_payload",
            "smooth": None,
            "smooth_optional": False,
            "missing_week_rule": "do_not_bridge_missing_week_returns",
            "units": "price",
            "forecast_stats_source": "historical_asof_to_horizon_weekly_returns",
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
    """Return a payload copy with robust weekly-return roadmap as production primary."""
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
    defaults["methodology_label"] = "Robust Weekly-Return Seasonal Roadmap"
    defaults["roadmap_smoothed"] = False

    weekly = out.get("weekly_roadmap") or {}
    comparison = weekly.get("comparison") or {}
    if comparison:
        comparison["monthly_roadmap_direction"] = _direction_from_roadmap(roadmap)
        comparison["monthly_label"] = "Robust Seasonal Roadmap"

    return out
