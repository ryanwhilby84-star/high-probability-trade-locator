"""Route payload builder for Seasonality Workstation API."""

from __future__ import annotations

from typing import Any

from hptl.seasonality_workstation.engine import build_seasonality_research
from hptl.seasonality_workstation.integrity import audit_daily_series_for_lookback
from hptl.seasonality_workstation.models import DEFAULT_LOOKBACK, ENGINE_VERSION, LOOKBACKS
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
from hptl.seasonality_workstation.weekly_roadmap import build_weekly_roadmap


def _lookback_years(label: str) -> int | None:
    for name, years in LOOKBACKS:
        if name == label:
            return years
    return 15


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

    for _label, lb in lookbacks.items():
        sample_years = list((lb or {}).get("sample_years") or [])
        if not sample_years:
            continue
        lb["forward_horizons"] = robust_forward_horizon_stats(
            rows,
            years=sample_years,
            anchor_week=anchor_week,
            horizons=(4, 8, 12),
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
    research["legacy_walk_forward"] = research.get("walk_forward")
    research["walk_forward"] = robust


def _apply_scoped_integrity(
    research: dict[str, Any],
    instrument_id: str,
    lookback: str,
) -> tuple[dict[str, Any], list[tuple[str, float]]]:
    """Gate only the price history that can affect the selected lookback.

    The engine still computes full-history diagnostics for auditability, but a
    defect from the 1990s cannot blank a 15Y workstation. FULL remains a genuine
    full-history gate.
    """
    price_id = str(research.get("price_instrument_id") or instrument_id)
    daily, source, error = load_daily_closes(price_id)
    if error or not daily:
        return {
            "instrument_id": price_id,
            "status": "FAIL",
            "issues": [error or "no_daily_history"],
            "warnings": [],
            "source": source,
        }, []

    anchor = str((research.get("anchor") or {}).get("date") or daily[-1][0])[:10]
    scoped = audit_daily_series_for_lookback(
        price_id,
        daily,
        source=source,
        lookback_years=_lookback_years(lookback),
        asof=anchor,
    )
    return scoped, daily


def build_seasonality_workstation_payload(
    instrument_id: str,
    *,
    lookback: str = DEFAULT_LOOKBACK,
) -> dict[str, Any]:
    # Do not let the engine's full-history audit pre-empt the selected-lookback
    # contract. We apply the correct scoped gate immediately afterwards.
    research = build_seasonality_research(
        instrument_id,
        lookback=lookback,
        fail_on_integrity=False,
    )
    if research.get("status") != "ok":
        return {
            "status": "error",
            "instrument_id": instrument_id,
            "engine": ENGINE_VERSION,
            "lookback": lookback,
            "error": research.get("error"),
            "message": research.get("message") or research.get("error"),
            "integrity": research.get("integrity"),
            "monthly_roadmap": research.get("monthly_roadmap"),
            "weekly_roadmap": research.get("weekly_roadmap"),
            "seasonal_roadmap": research.get("seasonal_roadmap"),
        }

    scoped_integrity, daily = _apply_scoped_integrity(research, instrument_id, lookback)
    research["integrity"] = scoped_integrity
    research["data_quality"] = scoped_integrity.get("data_quality")

    if scoped_integrity.get("status") != "PASS":
        return {
            "status": "integrity_error",
            "instrument_id": instrument_id,
            "engine": ENGINE_VERSION,
            "lookback": lookback,
            "error": "integrity_failed",
            "message": (
                "Seasonality Workstation refused to compute — selected-lookback price integrity FAIL: "
                + ", ".join(scoped_integrity.get("issues") or [])
            ),
            "integrity": scoped_integrity,
            "monthly_roadmap": None,
            "weekly_roadmap": build_weekly_roadmap(
                daily,
                asof=str((research.get("anchor") or {}).get("date") or "")[:10] or None,
                lookback_years=_lookback_years(lookback) or 15,
                integrity=scoped_integrity,
            ) if daily else None,
            "seasonal_roadmap": None,
        }

    # Rebuild Weekly Roadmap with the selected-lookback audit. This prevents an
    # out-of-scope legacy defect from leaving a false red gate inside an otherwise
    # valid workstation.
    if daily:
        weekly = build_weekly_roadmap(
            daily,
            asof=str((research.get("anchor") or {}).get("date") or "")[:10] or None,
            lookback_years=_lookback_years(lookback) or 15,
            integrity=scoped_integrity,
            seasonal_roadmap=research.get("seasonal_roadmap"),
        )
        research["weekly_roadmap"] = weekly
        if isinstance(research.get("seasonality"), dict):
            research["seasonality"]["weekly_roadmap"] = weekly

    _attach_robust_validation(research, instrument_id)
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
        "lookback_summaries": {
            k: {
                "sample_size": v.get("sample_size"),
                "forward_horizons": v.get("forward_horizons"),
                "turning_window_count": len(v.get("turning_windows") or []),
            }
            for k, v in (research.get("lookbacks") or {}).items()
        },
    }
