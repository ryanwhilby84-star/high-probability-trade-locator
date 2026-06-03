"""Build weekly thesis snapshots from already-exported confluence records.

Read-only: this reads ``confluence_history_latest.json`` records and copies the
numbers that already exist. It never recomputes COT / macro / structural scores.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from hptl.config import PROJECT_ROOT
from hptl.thesis_tracker.conviction import annotate_conviction
from hptl.thesis_tracker.models import normalize_snapshot, now_iso

CONFLUENCE_EXPORT = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "confluence_history_latest.json"


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, str):
        s = v.strip().lower()
        if not s or s in {"n/a", "nan", "null", "none", "—"}:
            return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n == n else None


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"n/a", "nan", "null", "none"}:
        return None
    return s


def load_records(path: Path | None = None) -> list[dict[str, Any]]:
    src = path or CONFLUENCE_EXPORT
    if not src.exists():
        return []
    raw = json.loads(src.read_text(encoding="utf-8"))
    rows = raw.get("records") if isinstance(raw, dict) else None
    return [r for r in (rows or []) if isinstance(r, dict)]


def record_week(record: dict[str, Any]) -> str:
    return _str(record.get("date")) or _str(record.get("cot_report_date")) or _str(record.get("latest_report_date")) or ""


def snapshot_from_record(record: dict[str, Any]) -> dict[str, Any]:
    """Translate one confluence record into a normalized weekly snapshot."""
    inst = record.get("institutional_context") if isinstance(record.get("institutional_context"), dict) else {}
    attention = inst.get("attention") if isinstance(inst.get("attention"), dict) else {}

    groups = record.get("cot_positioning_groups") if isinstance(record.get("cot_positioning_groups"), dict) else {}
    nr = groups.get("nonreportable") if isinstance(groups.get("nonreportable"), dict) else {}
    tactical = inst.get("tactical") if isinstance(inst.get("tactical"), dict) else {}
    zone = _str(record.get("zone_focus")) or _str(tactical.get("zone_focus"))

    week = record_week(record)
    market = str(record.get("market") or "").strip()
    pillar_keys = (
        "valuation_bias",
        "valuation_score",
        "valuation_reason",
        "valuation_wired",
        "seasonality_bias",
        "seasonality_score",
        "seasonality_reason",
        "seasonality_wired",
    )
    if record.get("valuation_bias") is not None or record.get("seasonality_bias") is not None:
        pillars = {k: record.get(k) for k in pillar_keys}
    elif week and market:
        from hptl.pillars.confluence_attach import pillar_fields_for_market_week

        pillars = pillar_fields_for_market_week(market, week)
    else:
        pillars = {}

    snap = normalize_snapshot(
        {
            "week": record_week(record),
            "cot_report_date": _str(record.get("cot_report_date")) or _str(record.get("latest_report_date")),
            "captured_at": now_iso(),
            "cot_bias": _str(record.get("cot_bias")) or _str(record.get("final_calculated_cot_bias")),
            "cot_score": _num(record.get("cot_score")) if _num(record.get("cot_score")) is not None
            else _num(record.get("final_calculated_cot_score")),
            "long_value": _num(record.get("long_value")),
            "short_value": _num(record.get("short_value")),
            "net_value": _num(record.get("net_value")),
            "one_week_net_change": _num(record.get("one_week_net_change")),
            "four_week_net_change": _num(record.get("four_week_net_change")),
            "positioning_state": _str(record.get("positioning_state")),
            "macro_regime": _str(record.get("macro_regime")) or _str(record.get("macro_signal")),
            "macro_score": _num(record.get("macro_score")),
            "structural_score": _num(inst.get("structural_score")),
            "structural_conviction": _str(inst.get("structural_conviction")),
            "priority_score": _num(attention.get("priority_score")),
            "zone_focus": zone,
            "retail_long": _num(nr.get("long")),
            "retail_short": _num(nr.get("short")),
            "retail_net": _num(nr.get("net")),
            **pillars,
        }
    )
    return snap


def market_history(
    records: Iterable[dict[str, Any]],
    market: str,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Ordered, de-duplicated weekly snapshots for one market (oldest -> newest)."""
    by_week: dict[str, dict[str, Any]] = {}
    for r in records:
        if str(r.get("market") or "").strip() != market:
            continue
        week = record_week(r)
        if not week:
            continue
        # later record for the same week wins (more enriched)
        by_week[week] = snapshot_from_record(r)
    ordered = [by_week[w] for w in sorted(by_week)]
    if limit is not None and limit > 0:
        ordered = ordered[-limit:]
    annotate_conviction(ordered)
    return ordered


def latest_snapshot_for_market(records: Iterable[dict[str, Any]], market: str) -> dict[str, Any] | None:
    hist = market_history(records, market, limit=1)
    return hist[-1] if hist else None
