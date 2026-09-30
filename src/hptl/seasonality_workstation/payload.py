"""Route payload builder for Seasonality Workstation API."""

from __future__ import annotations

from typing import Any

from hptl.seasonality_workstation.engine import build_seasonality_research
from hptl.seasonality_workstation.lookback import build_reliable_seasonal_lookback
from hptl.seasonality_workstation.models import DEFAULT_LOOKBACK, ENGINE_VERSION
from hptl.seasonality_workstation.production_roadmap import apply_production_seasonality
from hptl.seasonality_workstation.returns import (
    load_daily_closes,
    weekly_closes_from_daily,
    weekly_return_rows,
)
from hptl.seasonality_workstation.validation import (
    robust_forward_horizon_stats,
    robust_lookback_agreement,
    robust_weekly_leave_one_year_out,
)


def _attach_robust_validation(research: dict[str, Any], instrument_id: str) -> None:
    """Attach validation/statistics for the exact production weekly-return model."""
    selected = str(research.get("selected_lookback") or DEFAULT_LOOKBACK)
    lookbacks = research.get("lookbacks") or {}
    block = lookbacks.get(selected) or {}
    years = list(block.get("sample_years") or [])
    anchor_week = int((research.get("anchor") or {}).get("iso_week") or 0)
    asof = str((research.get("anchor") or {}).get("date") or "")[:10]
    price_id = str(research.get("price_instrument_id") or instrument_id)

    empty = {
        "method": "leave_one_year_out_robust_weekly_direction",
        "lookback": selected,
        "horizon_weeks": 8,
        "hit_rate": None,
        "hits": 0,
        "n": 0,
        "outcomes": [],
    }
    if not years or not anchor_week:
        research["robust_walk_forward"] = {**empty, "reason": "missing_validation_inputs"}
        research["legacy_walk_forward"] = research.get("walk_forward")
        research["walk_forward"] = research["robust_walk_forward"]
        return

    daily, _source, error = load_daily_closes(price_id)
    if error or not daily:
        research["robust_walk_forward"] = {**empty, "reason": error or "no_daily_history"}
        research["legacy_walk_forward"] = research.get("walk_forward")
        research["walk_forward"] = research["robust_walk_forward"]
        return

    if asof:
        daily = [(d, c) for d, c in daily if str(d)[:10] <= asof]
    weekly = weekly_closes_from_daily(daily)
    rows = weekly_return_rows(weekly)

    # Replace the engine's old non-wrapping forward horizon summaries with the
    # production model's exact historical observations, including week 52 -> 1.
    for _label, lb in lookbacks.items():
        sample_years = list((lb or {}).get("sample_years") or [])
        if not sample_years:
            continue
        lb["forward_horizons"] = robust_forward_horizon_stats(
            rows,
            years=sample_years,
            anchor_week=anchor_week,
            horizons=(1, 2, 4, 8, 12),
        )

    research["lookback_agreement"] = robust_lookback_agreement(
        lookbacks,
        anchor_week=anchor_week,
        horizon=8,
    )
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


def _attach_reliable_seasonal_lookback(
    research: dict[str, Any], instrument_id: str
) -> None:
    """Attach the auditable same-ISO-week lookback and feed its stats to the UI.

    The displayed Seasonal Roadmap remains a separate path model. Only the
    horizon statistics on its side panel are replaced with actual historical
    same-week outcomes so the visual path cannot manufacture apparent edge.
    """
    selected = str(research.get("selected_lookback") or DEFAULT_LOOKBACK)
    lookbacks = research.get("lookbacks") or {}
    block = lookbacks.get(selected) or {}
    years = list(block.get("sample_years") or [])
    anchor = research.get("anchor") or {}
    anchor_week = int(anchor.get("iso_week") or 0)
    asof = str(anchor.get("date") or "")[:10]
    price_id = str(research.get("price_instrument_id") or instrument_id)

    if not years or not anchor_week:
        research["seasonal_lookback"] = {
            "available": False,
            "lookback": selected,
            "anchor_week": anchor_week,
            "reason": "missing_sample_years_or_anchor_week",
        }
        return

    daily, source, error = load_daily_closes(price_id)
    if error or not daily:
        research["seasonal_lookback"] = {
            "available": False,
            "lookback": selected,
            "anchor_week": anchor_week,
            "reason": error or "no_daily_history",
            "price_source": source,
        }
        return

    # Critical anti-lookahead gate: nothing later than the workstation anchor is
    # allowed into the analogue engine, even when the canonical store contains it.
    if asof:
        daily = [(d, c) for d, c in daily if str(d)[:10] <= asof]
    rows = weekly_return_rows(weekly_closes_from_daily(daily))
    lookback_result = build_reliable_seasonal_lookback(
        rows,
        years=years,
        anchor_week=anchor_week,
        lookback=selected,
    )
    lookback_result["price_source"] = source
    lookback_result["asof"] = asof or None
    research["seasonal_lookback"] = lookback_result

    if not lookback_result.get("available"):
        return

    # The right-hand Seasonal Roadmap panel is the live, visible route. Make it
    # consume the exact same audited observations rather than separate legacy
    # horizon maths. The roadmap line itself is intentionally left untouched.
    forecast_stats = lookback_result.get("forecast_stats") or {}
    roadmap = research.get("seasonal_roadmap") or {}
    if roadmap.get("available"):
        roadmap["forecast_stats"] = forecast_stats
        roadmap["lookback_audit"] = lookback_result

    monthly = research.get("monthly_roadmap") or {}
    if monthly.get("available"):
        monthly["forecast_stats"] = forecast_stats
        monthly["lookback_audit"] = lookback_result

    seasonality = research.get("seasonality") or {}
    nested = seasonality.get("seasonal_roadmap") or {}
    if nested.get("available"):
        nested["forecast_stats"] = forecast_stats
        nested["lookback_audit"] = lookback_result


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

    # Production reliability/statistics must validate the same robust weekly
    # return model that is plotted, not the legacy indexed-year model.
    _attach_robust_validation(research, instrument_id)

    # Production presentation contract: robust ISO-week returns are canonical.
    # Legacy indexed / mean-return products remain explicit alternate views.
    research = apply_production_seasonality(research)

    # Build the final auditable same-week analogue study after the roadmap so
    # its horizon statistics cannot be overwritten by presentation transforms.
    _attach_reliable_seasonal_lookback(research, instrument_id)

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
        "seasonal_lookback": research.get("seasonal_lookback"),
        "walk_forward": research.get("walk_forward"),
        "robust_walk_forward": research.get("robust_walk_forward"),
        "legacy_walk_forward": research.get("legacy_walk_forward"),
        "seasonality": research.get("seasonality"),
        "lookback_agreement": research.get("lookback_agreement"),
        "turning_windows": research.get("turning_windows"),
        "stats_panel": research.get("stats_panel"),
        "advanced": research.get("advanced"),
        "display_defaults": research.get("display_defaults"),
        "lookback_summaries": {
            k: {
                "sample_size": v.get("sample_size"),
                "forward_horizons": v.get("forward_horizons"),
                "turning_window_count": len(v.get("turning_windows") or []),
            }
            for k, v in (research.get("lookbacks") or {}).items()
        },
    }
