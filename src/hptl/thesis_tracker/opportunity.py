"""Opportunity summary — action, rank, and card payload for Thesis Tracker UI."""
from __future__ import annotations

from typing import Any

from hptl.thesis_tracker.alignment import (
    alignment_summary,
    display_instrument_name,
    evaluate_pillars,
    _effective_direction,
)
from hptl.thesis_tracker.conviction import compute_trend
from hptl.thesis_tracker.models import STATUS_COMPLETED, STATUS_INVALIDATED, norm_status
from hptl.thesis_tracker.snapshot import load_records, snapshot_from_record

ACTION_HIGH = "HIGH ATTENTION"
ACTION_PAY = "PAY ATTENTION"
ACTION_WATCH = "WATCH"
ACTION_NONE = "NO EDGE"
ACTION_CLOSED = "CLOSED"

_ACTION_WEIGHT = {
    ACTION_HIGH: 50,
    ACTION_PAY: 35,
    ACTION_WATCH: 15,
    ACTION_NONE: 0,
    ACTION_CLOSED: -100,
}


def _latest_snap(thesis: dict[str, Any]) -> dict[str, Any]:
    snaps = thesis.get("snapshots") or []
    if not snaps:
        return {}
    return snaps[-1] if isinstance(snaps[-1], dict) else {}


def _hydrate_snap(
    snap: dict[str, Any],
    market: str,
    *,
    include_pillars: bool = True,
) -> dict[str, Any]:
    """Fill retail/zone/valuation/seasonality from confluence when missing on stored snapshots."""
    needs_hydrate = (
        not snap.get("zone_focus")
        or snap.get("retail_net") is None
        or (include_pillars and not snap.get("valuation_bias"))
        or (include_pillars and not snap.get("seasonality_bias"))
    )
    if not needs_hydrate:
        return snap
    rec = _find_confluence_record(market, snap.get("week") or snap.get("cot_report_date"))
    if not rec:
        return snap
    fresh = snapshot_from_record(rec)
    out = dict(snap)
    hydrate_keys = ["zone_focus", "retail_net", "retail_long", "retail_short"]
    if include_pillars:
        hydrate_keys.extend(
            [
                "valuation_bias",
                "valuation_score",
                "valuation_reason",
                "valuation_wired",
                "seasonality_bias",
                "seasonality_score",
                "seasonality_reason",
                "seasonality_wired",
            ]
        )
    for key in hydrate_keys:
        if out.get(key) is None and fresh.get(key) is not None:
            out[key] = fresh[key]
    return out


def _find_confluence_record(market: str, week: str | None) -> dict[str, Any] | None:
    records = load_records()
    hits = [r for r in records if str(r.get("market") or "").strip() == market]
    if not hits:
        return None
    if week:
        w = str(week)[:10]
        for r in hits:
            d = str(r.get("date") or r.get("cot_report_date") or "")[:10]
            if d == w:
                return r
    hits.sort(key=lambda r: str(r.get("date") or r.get("cot_report_date") or ""))
    return hits[-1]


def _derive_action(
    *,
    alignment_pass: int,
    alignment_total: int,
    trend: str,
    status: str,
) -> str:
    st = norm_status(status)
    if st in {STATUS_INVALIDATED, STATUS_COMPLETED}:
        return ACTION_CLOSED
    if alignment_pass >= 5 and trend != "deteriorating":
        return ACTION_HIGH
    if alignment_pass >= 4:
        return ACTION_PAY
    if alignment_pass >= 3 and trend == "improving":
        return ACTION_PAY
    if alignment_pass >= 3:
        return ACTION_WATCH
    return ACTION_NONE


def _rank_score(alignment_pass: int, action: str, cot_score: float | None) -> int:
    w = _ACTION_WEIGHT.get(action, 0)
    cot = min(_num(cot_score) or 0.0, 10.0) if _num(cot_score) is not None else 0.0
    return int(alignment_pass * 20 + w + cot * 2)


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _action_key(action: str) -> str:
    return action.lower().replace(" ", "_")


def _summary_card(pillars: list[dict[str, Any]], market: str) -> dict[str, Any]:
    by_id = {p["pillar"]: p for p in pillars}

    def _row(pid: str) -> dict[str, Any]:
        p = by_id.get(pid) or {}
        return {"state": p.get("state"), "score_display": p.get("score_display")}

    return {
        "instrument_display": display_instrument_name(market),
        "valuation": _row("valuation"),
        "institutions": _row("institutions"),
        "retail": _row("retail"),
        "seasonality": _row("seasonality"),
        "location": _row("location"),
    }


def build_opportunity(thesis: dict[str, Any], *, include_pillars: bool = True) -> dict[str, Any]:
    market = str(thesis.get("market") or "").strip()
    snap = _hydrate_snap(_latest_snap(thesis), market, include_pillars=include_pillars)
    direction = _effective_direction(str(thesis.get("direction_bias") or "neutral"), snap)
    trend = str(thesis.get("conviction_trend") or compute_trend(thesis.get("snapshots") or []))

    pillars = evaluate_pillars(snap, direction=direction)
    align = alignment_summary(pillars)

    status = norm_status(thesis.get("status"))
    if thesis.get("archived"):
        action = ACTION_CLOSED
    elif status in {STATUS_INVALIDATED, STATUS_COMPLETED}:
        action = ACTION_CLOSED
    else:
        action = _derive_action(
            alignment_pass=align["pass"],
            alignment_total=align["total"],
            trend=trend,
            status=status,
        )

    why = [
        {
            "pillar": p["pillar"],
            "label": p["label"],
            "pass": p.get("pass"),
            "wired": p.get("wired"),
            "state": p.get("state"),
            "detail": p.get("one_line"),
        }
        for p in pillars
    ]

    rank = _rank_score(align["pass"], action, _num(snap.get("cot_score")))

    headline = _action_headline(action, align["label"], market)

    return {
        "alignment": {**align, "pillars": pillars},
        "action": action,
        "action_key": _action_key(action),
        "rank_score": rank,
        "direction": direction,
        "summary": _summary_card(pillars, market),
        "why": why,
        "headline": headline,
    }


def _action_headline(action: str, alignment_label: str, market: str) -> str:
    name = display_instrument_name(market)
    if action == ACTION_CLOSED:
        return f"{name} — closed / not actionable."
    if action == ACTION_NONE:
        return f"{name} — {alignment_label} alignment — no institutional edge."
    return f"{name} — {alignment_label} alignment — {action.lower()}."
