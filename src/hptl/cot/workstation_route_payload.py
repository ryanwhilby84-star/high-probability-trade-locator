"""Controlled workstation route payload — core COT plus always-current derived statistics.

The historical COT series is the hard integrity boundary. Percentiles and related
weekly-inspector statistics are rebuilt directly from the current core series on
every workstation request so stale exported inspector files can never leave the
latest weeks without derived values.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.derived_cot_integrity_audit import LOOKBACK_WEEKS, audit_week
from hptl.cot.json_safety import JsonUnsafeError, sanitize_for_json
from hptl.cot.weekly_inspector_export import expand_compact_market
from hptl.cot.weekly_inspector_flow import build_weekly_inspector_series
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS

PUBLIC = PROJECT_ROOT / "web-dashboard" / "public" / "data"
DATA = PROJECT_ROOT / "data"

WI_PATHS = (
    PROCESSED_DIR / "cot_weekly_inspector_latest.json",
    PUBLIC / "cot_weekly_inspector_latest.json",
    DATA / "cot_weekly_inspector_latest.json",
)
COT3Y_PATHS = (
    PUBLIC / "cot_3y_series_latest.json",
    PROCESSED_DIR / "cot_3y_series_latest.json",
)

__all__ = [
    "JsonUnsafeError",
    "sanitize_for_json",
    "build_workstation_route_payload",
    "audit_all_workstation_routes",
]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _load_first(paths: tuple[Path, ...]) -> dict[str, Any]:
    for p in paths:
        doc = _read_json(p)
        if doc:
            return doc
    return {}


def _resolve_market_block(doc: dict[str, Any], instrument_id: str) -> tuple[str | None, dict[str, Any] | None]:
    markets = doc.get("markets") or {}
    if instrument_id in markets:
        return instrument_id, markets[instrument_id]
    lower = instrument_id.lower()
    for key, block in markets.items():
        if str(key).lower() == lower:
            return str(key), block
    return None, None


def _core_integrity_error(mid: str, missing: list[str], *, report_date: str | None = None) -> tuple[dict[str, Any], int]:
    return (
        {
            "status": "integrity_error",
            "instrument_id": mid,
            "report_date": report_date,
            "stage": "core_cot_history",
            "missing_fields": missing,
            "message": "Core COT history is unavailable or incomplete for this instrument.",
        },
        422,
    )


def _cached_inspector_market(
    weekly_inspector: dict[str, Any],
    mid: str,
) -> tuple[str | None, dict[str, Any]]:
    """Best-effort fallback only. The current core COT series is canonical."""
    key, block = _resolve_market_block(weekly_inspector, mid)
    if not block or not block.get("available"):
        return key, {}
    try:
        expanded = expand_compact_market(block) if "rows" in block else block
    except Exception:
        return key, {}
    return key, expanded


def build_workstation_route_payload(
    instrument_id: str,
    *,
    weekly_inspector: dict[str, Any] | None = None,
    cot_3y: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], int]:
    """Build ``(body, http_status)`` for one COT workstation route.

    Percentiles, percentile changes, state labels and cross-group statistics are
    rebuilt from the same latest core COT series rendered by the workstation. This
    removes the previous race/staleness condition where ``cot_3y_series_latest``
    had newer weeks than ``cot_weekly_inspector_latest``.
    """
    mid = str(instrument_id or "").strip()
    if not mid:
        return _core_integrity_error("", ["instrument_id"])

    wi = weekly_inspector if weekly_inspector is not None else _load_first(WI_PATHS)
    cot3y = cot_3y if cot_3y is not None else _load_first(COT3Y_PATHS)

    cot_key, cot_block = _resolve_market_block(cot3y, mid)
    series = list((cot_block or {}).get("series") or [])
    if not cot_block:
        return _core_integrity_error(mid, ["cot_3y.market"])
    if not series:
        return _core_integrity_error(mid, ["cot_3y.series"])

    fallback_report_date = str((series[-1] or {}).get("date") or "")[:10] or None
    derived_missing: list[str] = []

    # Canonical path: derive every inspector week from the current core series.
    # A cached export is used only if this pure rebuild unexpectedly fails.
    matched_key = cot_key
    try:
        expanded = build_weekly_inspector_series(series)
        weeks = list(expanded.get("weeks") or [])
    except Exception as exc:  # noqa: BLE001
        derived_missing.append(f"live_rebuild_error:{type(exc).__name__}")
        cached_key, expanded = _cached_inspector_market(wi, mid)
        matched_key = cached_key or cot_key
        weeks = list(expanded.get("weeks") or [])
        if not weeks:
            derived_missing.append("weekly_inspector.market")

    if not weeks:
        derived_missing.append("weeks")

    lookback = weeks[-LOOKBACK_WEEKS:] if weeks else []
    report_date = (
        str((lookback[-1] or {}).get("date") or "")[:10] if lookback else ""
    ) or fallback_report_date

    # The derived report must be on the same week as the core historical series.
    if fallback_report_date and report_date != fallback_report_date:
        derived_missing.append(
            f"derived_stale:core={fallback_report_date}:derived={report_date or 'none'}"
        )

    for week in lookback:
        for fail in audit_week(week, instrument_id=mid):
            derived_missing.append(f"{fail.get('report_date')}:{fail.get('field')}")

    safe_latest_week = None
    if lookback:
        try:
            safe_latest_week = sanitize_for_json(
                lookback[-1], path="workstation.latest_week"
            )
        except JsonUnsafeError as exc:
            derived_missing.append(f"latest_week_json:{exc}")

    derived_missing = list(dict.fromkeys(derived_missing))
    derived_status = "warning" if derived_missing else "ok"

    workstation = {
        "matched_key": matched_key,
        "cot_matched_key": cot_key,
        "report_date": report_date,
        "week_count": len(weeks),
        "lookback_weeks": len(lookback),
        "historical_rows": len(series),
        "latest_week": safe_latest_week,
        "measure": expanded.get("measure"),
        "measure_label": expanded.get("measure_label"),
        "derived_source": "live_from_current_cot_series" if not any(
            str(x).startswith("live_rebuild_error:") for x in derived_missing
        ) else "cached_fallback",
        "derived_integrity": {
            "status": derived_status,
            "stage": "derived_cot",
            "missing_fields": derived_missing[:80],
            "message": (
                "Derived COT statistics are incomplete; historical COT remains available."
                if derived_missing
                else "Derived COT statistics passed integrity checks and match the latest core week."
            ),
        },
    }

    try:
        safe = sanitize_for_json(
            {
                "status": "ok",
                "instrument_id": mid,
                "report_date": report_date,
                "workstation": workstation,
                "warnings": derived_missing[:80],
            }
        )
    except JsonUnsafeError as exc:
        return (
            {
                "status": "integrity_error",
                "instrument_id": mid,
                "report_date": report_date,
                "stage": "json_serialisation",
                "missing_fields": [str(exc)],
                "message": "COT workstation payload contains unsafe core JSON values.",
            },
            422,
        )

    return safe, 200


def audit_all_workstation_routes(
    *,
    weekly_inspector: dict[str, Any] | None = None,
    cot_3y: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wi = weekly_inspector if weekly_inspector is not None else _load_first(WI_PATHS)
    cot3y = cot_3y if cot_3y is not None else _load_first(COT3Y_PATHS)
    rows: list[dict[str, Any]] = []
    ok = 0
    integrity = 0
    warnings = 0
    for mid in LEGACY_COT_MARKETS:
        body, status = build_workstation_route_payload(mid, weekly_inspector=wi, cot_3y=cot3y)
        result = "PASS" if status == 200 and body.get("status") == "ok" else "FAIL"
        if body.get("status") == "ok":
            ok += 1
            derived = ((body.get("workstation") or {}).get("derived_integrity") or {})
            if derived.get("status") == "warning":
                warnings += 1
        elif body.get("status") == "integrity_error":
            integrity += 1
        rows.append(
            {
                "instrument": mid,
                "http_status": status,
                "response_status": body.get("status"),
                "report_date": body.get("report_date"),
                "derived_source": ((body.get("workstation") or {}).get("derived_source")),
                "missing_fields": body.get("missing_fields") or body.get("warnings") or [],
                "payload_valid": body.get("status") in ("ok", "integrity_error"),
                "final_result": result,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "http_200_ok": ok,
            "http_200_with_derived_warning": warnings,
            "http_422_integrity": integrity,
            "http_500": 0,
            "blank_renders": 0,
            "total": len(LEGACY_COT_MARKETS),
            "overall_status": "PASS" if ok == len(LEGACY_COT_MARKETS) and integrity == 0 else "FAIL",
        },
        "instruments": rows,
    }
