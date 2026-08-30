"""Route payload builder for Seasonality Workstation API."""

from __future__ import annotations

from typing import Any

from hptl.seasonality_workstation.engine import build_seasonality_research
from hptl.seasonality_workstation.models import DEFAULT_LOOKBACK, ENGINE_VERSION
from hptl.seasonality_workstation.production_roadmap import apply_production_seasonality


def build_seasonality_workstation_payload(
    instrument_id: str,
    *,
    lookback: str = DEFAULT_LOOKBACK,
) -> dict[str, Any]:
    research = build_seasonality_research(
        instrument_id,
        lookback=lookback,
        fail_on_integrity=True,
    )
    if research.get("status") != "ok":
        return {
            "status": "integrity_error" if research.get("error") == "integrity_failed" else "error",
            "instrument_id": instrument_id,
            "engine": ENGINE_VERSION,
            "lookback": lookback,
            "error": research.get("error"),
            "message": research.get("message") or research.get("error"),
            "integrity": research.get("integrity"),
            # Separate contract keys — Monthly remains unavailable on integrity FAIL.
            "monthly_roadmap": research.get("monthly_roadmap"),
            "weekly_roadmap": research.get("weekly_roadmap"),
            "seasonal_roadmap": research.get("seasonal_roadmap"),
        }

    # Production presentation contract: use the engine's robust ISO-week return
    # statistics as the canonical roadmap. Legacy indexed / mean-return products
    # remain available as explicit alternate views, not silent primary fallbacks.
    research = apply_production_seasonality(research)

    return {
        "status": "ok",
        "instrument_id": instrument_id,
        "price_instrument_id": research.get("price_instrument_id"),
        "price_identity": research.get("price_identity"),
        "engine": ENGINE_VERSION,
        "report_date": research.get("report_date"),
        "exchange": research.get("exchange"),
        "selected_lookback": research.get("selected_lookback"),
        "available_lookbacks": research.get("available_lookbacks"),
        "sample_size": research.get("sample_size"),
        "confidence": research.get("confidence"),
        "reliability": research.get("reliability"),
        "data_quality": research.get("data_quality"),
        "integrity": research.get("integrity"),
        "anchor": research.get("anchor"),
        "price_series": research.get("price_series"),
        "normalised_seasonality": research.get("normalised_seasonality"),
        "seasonal_price_path": research.get("seasonal_price_path"),
        "seasonal_roadmap": research.get("seasonal_roadmap"),
        "monthly_roadmap": research.get("monthly_roadmap"),
        "weekly_roadmap": research.get("weekly_roadmap"),
        "walk_forward": research.get("walk_forward"),
        "seasonality": research.get("seasonality"),
        "lookback_agreement": research.get("lookback_agreement"),
        "turning_windows": research.get("turning_windows"),
        "stats_panel": research.get("stats_panel"),
        "advanced": research.get("advanced"),
        "display_defaults": research.get("display_defaults"),
        # Compact lookback comparison for audits / UI switcher
        "lookback_summaries": {
            k: {
                "sample_size": v.get("sample_size"),
                "forward_horizons": v.get("forward_horizons"),
                "turning_window_count": len(v.get("turning_windows") or []),
            }
            for k, v in (research.get("lookbacks") or {}).items()
        },
    }
