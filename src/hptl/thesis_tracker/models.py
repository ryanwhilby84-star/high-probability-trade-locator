"""Thesis Tracker data model: status state machine + normalization.

Pure data shaping only — no scoring or pipeline logic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

# Status state machine (req 1). Stored == displayed for simplicity.
STATUS_DISCOVERED = "DISCOVERED"
STATUS_DEVELOPING = "DEVELOPING"
STATUS_READY = "READY"
STATUS_ACTIVE = "ACTIVE"
STATUS_INVALIDATED = "INVALIDATED"
STATUS_COMPLETED = "COMPLETED"

# Ordered progression (for the status stepper / validation of forward moves).
STATUS_FLOW: tuple[str, ...] = (
    STATUS_DISCOVERED,
    STATUS_DEVELOPING,
    STATUS_READY,
    STATUS_ACTIVE,
    STATUS_COMPLETED,
)
TERMINAL_STATUSES = frozenset({STATUS_COMPLETED, STATUS_INVALIDATED})
ALL_STATUSES = frozenset(STATUS_FLOW) | {STATUS_INVALIDATED}

# Legacy status values migrated to the 6-state model.
STATUS_ALIASES = {
    "LIMIT ORDER SET": STATUS_READY,
    "LIMIT_ORDER_SET": STATUS_READY,
    "ACTIVE TRADE": STATUS_ACTIVE,
    "ACTIVE_TRADE": STATUS_ACTIVE,
}

STATUS_DEFINITIONS = {
    STATUS_DISCOVERED: "Initial signal detected — worth monitoring.",
    STATUS_DEVELOPING: "Conditions improving — thesis forming.",
    STATUS_READY: "Multiple factors aligned — limit-order preparation justified.",
    STATUS_ACTIVE: "Trade entered / order placed.",
    STATUS_INVALIDATED: "Thesis broken — conditions deteriorated.",
    STATUS_COMPLETED: "Trade closed — thesis finished.",
}

DIRECTIONS = frozenset({"long", "short", "neutral"})

# Snapshot fields that exist in the pipeline today vs. placeholders (req 6 / 13).
SNAPSHOT_REAL_FIELDS = (
    "cot_bias",
    "cot_score",
    "long_value",
    "short_value",
    "net_value",
    "one_week_net_change",
    "four_week_net_change",
    "positioning_state",
    "macro_regime",
    "macro_score",
    "structural_score",
    "structural_conviction",
    "priority_score",
    "zone_focus",
    "retail_long",
    "retail_short",
    "retail_net",
    "valuation_bias",
    "valuation_score",
    "valuation_reason",
    "valuation_wired",
    "seasonality_bias",
    "seasonality_score",
    "seasonality_reason",
    "seasonality_wired",
)
# Legacy placeholder keys (retail_positioning_score superseded by retail_net + bias).
SNAPSHOT_PLACEHOLDER_FIELDS: tuple[str, ...] = ()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_thesis_id() -> str:
    return str(uuid4())


def num(v: Any) -> float | None:
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n == n else None  # drop NaN


def norm_status(value: Any, *, default: str = STATUS_DISCOVERED) -> str:
    s = str(value or "").strip().upper()
    if s in STATUS_ALIASES:
        return STATUS_ALIASES[s]
    if s in ALL_STATUSES:
        return s
    # tolerate snake/space variants
    s2 = s.replace("_", " ")
    if s2 in STATUS_ALIASES:
        return STATUS_ALIASES[s2]
    return s2 if s2 in ALL_STATUSES else default


def normalize_snapshot(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize one weekly snapshot. Placeholder fields are forced to null."""
    if not isinstance(data, dict):
        data = {}
    out: dict[str, Any] = {
        "week": str(data.get("week") or "").strip(),
        "cot_report_date": str(data.get("cot_report_date") or "").strip() or None,
        "captured_at": str(data.get("captured_at") or now_iso()),
        "cot_bias": str(data.get("cot_bias") or "").strip() or None,
        "cot_score": num(data.get("cot_score")),
        "long_value": num(data.get("long_value")),
        "short_value": num(data.get("short_value")),
        "net_value": num(data.get("net_value")),
        "one_week_net_change": num(data.get("one_week_net_change")),
        "four_week_net_change": num(data.get("four_week_net_change")),
        "positioning_state": str(data.get("positioning_state") or "").strip() or None,
        "macro_regime": str(data.get("macro_regime") or "").strip() or None,
        "macro_score": num(data.get("macro_score")),
        "structural_score": num(data.get("structural_score")),
        "structural_conviction": str(data.get("structural_conviction") or "").strip() or None,
        "priority_score": num(data.get("priority_score")),
        "zone_focus": str(data.get("zone_focus") or "").strip() or None,
        "retail_long": num(data.get("retail_long")),
        "retail_short": num(data.get("retail_short")),
        "retail_net": num(data.get("retail_net")),
        "valuation_bias": str(data.get("valuation_bias") or "").strip() or None,
        "valuation_score": num(data.get("valuation_score")),
        "valuation_reason": str(data.get("valuation_reason") or "").strip() or None,
        "valuation_wired": bool(data.get("valuation_wired", False)),
        "seasonality_bias": str(data.get("seasonality_bias") or "").strip() or None,
        "seasonality_score": num(data.get("seasonality_score")),
        "seasonality_reason": str(data.get("seasonality_reason") or "").strip() or None,
        "seasonality_wired": bool(data.get("seasonality_wired", False)),
        "retail_positioning_score": num(data.get("retail_positioning_score")),
        # computed (set by conviction layer; preserved if already present)
        "conviction_score": num(data.get("conviction_score")),
        "conviction_components_present": list(data.get("conviction_components_present") or []),
    }
    return out


def normalize_log_entry(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "week": str(data.get("week") or "").strip() or None,
        "auto": bool(data.get("auto", False)),
        "text": str(data.get("text") or "").strip(),
        "created_at": str(data.get("created_at") or now_iso()),
    }


def normalize_thesis(data: dict[str, Any], *, source: str = "manual") -> dict[str, Any]:
    """Validate + normalize a thesis document (without recomputing derived fields)."""
    if not isinstance(data, dict):
        raise ValueError("Thesis payload must be a JSON object")
    market = str(data.get("market") or "").strip()
    if not market:
        raise ValueError("market is required")

    direction = str(data.get("direction_bias") or "neutral").strip().lower()
    if direction not in DIRECTIONS:
        direction = "neutral"

    snapshots = [normalize_snapshot(s) for s in (data.get("snapshots") or []) if isinstance(s, dict)]
    snapshots.sort(key=lambda s: str(s.get("week") or ""))
    log = [normalize_log_entry(e) for e in (data.get("evolution_log") or []) if isinstance(e, dict)]
    log.sort(key=lambda e: str(e.get("week") or e.get("created_at") or ""))

    outcome = data.get("outcome")
    if outcome is not None and not isinstance(outcome, dict):
        outcome = None

    return {
        "thesis_id": str(data.get("thesis_id") or "").strip() or new_thesis_id(),
        "market": market,
        "symbol": str(data.get("symbol") or "").strip(),
        "asset_class": str(data.get("asset_class") or "").strip() or None,
        "status": norm_status(data.get("status")),
        "direction_bias": direction,
        "created_at": str(data.get("created_at") or now_iso()),
        "created_week": str(data.get("created_week") or "").strip() or None,
        "source": str(data.get("source") or source).strip(),
        "archived": bool(data.get("archived", False)),
        "archived_at": str(data.get("archived_at")) if data.get("archived_at") else None,
        "outcome": outcome,
        "summary_manual": (str(data.get("summary_manual")).strip() if data.get("summary_manual") else None),
        "tags": [str(t).strip() for t in (data.get("tags") or []) if str(t).strip()],
        "snapshots": snapshots,
        "evolution_log": log,
        # derived (filled by store._recompute_derived)
        "summary_auto": str(data.get("summary_auto") or ""),
        "age_weeks": int(data.get("age_weeks") or 0),
        "conviction_current": num(data.get("conviction_current")),
        "conviction_trend": str(data.get("conviction_trend") or "stable"),
        "last_update_week": str(data.get("last_update_week") or "").strip() or None,
    }
