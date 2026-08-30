"""Route payload builder for Seasonality Workstation API."""

from __future__ import annotations

from typing import Any

from hptl.seasonality_workstation.engine import build_seasonality_research
from hptl.seasonality_workstation.models import DEFAULT_LOOKBACK, ENGINE_VERSION
from hptl.seasonality_workstation.production_roadmap import apply_production_seasonality
from hptl.seasonality_workstation.returns import (
    load_daily_closes,
    weekly_closes_from_daily,
    weekly_return_rows,
)
from hptl.seasonality_workstation.validation import robust_weekly_leave_one_year_out


def _attach_robust_walk_forward(research: dict[str, Any], instrument_id: str) -> None:
    """Replace legacy OOS evidence with validation of the production return model."""
    selected = str(research.get("selected_lookback") or DEFAULT_LOOKBACK)
    block = (research.get("lookbacks") or {}).get(selected) or {}
    years = list(block.get("sample_years") or [])
    anchor_week = int((research.get("anchor") or {}).get("iso_week") or 0)
    asof = str((research.get("anchor") or {}).get("date") or "")[:10]
    price_id = str(research.get("price_instrument_id") or instrument_id)

    if not years or not anchor_week:
        research["robust_walk_forward"] = {
            "method": "leave_one_year_out_robust_weekly_direction",
            "lookback": selected,
            "horizon_weeks": 8,
            "hit_rate": None,
            "hits": 0,
            "n": 0,
            "outcomes": [],
            "reason": "missing_validation_inputs",
        }
        return

    daily, _source, error = load_daily_closes(price_id)
    if error or not daily:
        research["robust_walk_forward"] = {
            "method": "leave_one_year_out_robust_weekly_direction",
            "lookback": selected,
            "horizon_weeks": 8,
            "hit_rate": None,
            "hits": 0,
            "n": 0,
            "outcomes": [],
            "reason": error or "no_daily_history",
        }
        return

    if asof:
        daily = [(d, c) for d, c in daily if str(d)[:10] <= asof]
    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)
    robust = robust_weekly_leave_one_year_out(
        rows,
        years=years,
        anchor_week=anchor_week,
        lookback=selected,
        horizon=8,
    )
    research["robust_walk_forward"] = robust
    # The production payload's generic walk_forward key now refers to the model
    # actually shown on screen. Preserve the old validation separately for audit.
    research["legacy_walk_forward"] = research.get("walk_forward")
    research["walk_forward"] = robust


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

    # Production reliability must validate the same robust weekly-return model
    # that is plotted, not the legacy indexed-year model.
    _attach_robust_walk_forward(research, instrument_id)

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
        "robust_walk_forward": research.get("robust_walk_forward"),
        "legacy_walk_forward": research.get("legacy_walk_forward"),
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
