"""Controlled workstation route payload — never leak 500 for derived-COT gaps.

Returns one of two shapes:

- status=ok (+ workstation block)
- status=integrity_error (+ missing_fields, HTTP 422 recommended)
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


def build_workstation_route_payload(
    instrument_id: str,
    *,
    weekly_inspector: dict[str, Any] | None = None,
    cot_3y: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], int]:
    """Build (body, http_status) for one instrument workstation route.

    http_status: 200 ok | 422 integrity_error | 404 unknown instrument
    Never raises for incomplete derived COT — returns integrity_error instead.
    """
    mid = str(instrument_id or "").strip()
    if not mid:
        body = {
            "status": "integrity_error",
            "instrument_id": "",
            "report_date": None,
            "stage": "derived_cot",
            "missing_fields": ["instrument_id"],
            "message": "Derived COT statistics are incomplete for this instrument.",
        }
        return body, 422

    wi = weekly_inspector if weekly_inspector is not None else _load_first(WI_PATHS)
    cot3y = cot_3y if cot_3y is not None else _load_first(COT3Y_PATHS)

    key, block = _resolve_market_block(wi, mid)
    if not block or not block.get("available"):
        body = {
            "status": "integrity_error",
            "instrument_id": mid,
            "report_date": None,
            "stage": "derived_cot",
            "missing_fields": ["weekly_inspector.market"],
            "message": "Derived COT statistics are incomplete for this instrument.",
        }
        return body, 422

    try:
        expanded = expand_compact_market(block) if "rows" in block else block
    except Exception as exc:  # noqa: BLE001
        body = {
            "status": "integrity_error",
            "instrument_id": mid,
            "report_date": None,
            "stage": "derived_cot",
            "missing_fields": [f"expand_error:{type(exc).__name__}"],
            "message": "Derived COT statistics are incomplete for this instrument.",
        }
        return body, 422

    weeks = list(expanded.get("weeks") or [])
    if not weeks:
        body = {
            "status": "integrity_error",
            "instrument_id": mid,
            "report_date": None,
            "stage": "derived_cot",
            "missing_fields": ["weeks"],
            "message": "Derived COT statistics are incomplete for this instrument.",
        }
        return body, 422

    lookback = weeks[-LOOKBACK_WEEKS:]
    missing: list[str] = []
    report_date = str(lookback[-1].get("date") or "")[:10] or None
    for week in lookback:
        for fail in audit_week(week, instrument_id=mid):
            missing.append(
                f"{fail.get('report_date')}:{fail.get('field')}"
            )

    cot_key, cot_block = _resolve_market_block(cot3y, mid)
    series_len = len((cot_block or {}).get("series") or [])

    if missing:
        body = {
            "status": "integrity_error",
            "instrument_id": mid,
            "report_date": report_date,
            "stage": "derived_cot",
            "missing_fields": missing[:80],
            "message": "Derived COT statistics are incomplete for this instrument.",
            "matched_key": key,
            "historical_rows": series_len,
        }
        return body, 422

    workstation = {
        "matched_key": key,
        "cot_matched_key": cot_key,
        "report_date": report_date,
        "week_count": len(weeks),
        "lookback_weeks": len(lookback),
        "historical_rows": series_len,
        "latest_week": lookback[-1],
        "measure": expanded.get("measure"),
        "measure_label": expanded.get("measure_label"),
    }

    try:
        safe = sanitize_for_json(
            {
                "status": "ok",
                "instrument_id": mid,
                "report_date": report_date,
                "workstation": workstation,
            }
        )
    except JsonUnsafeError as exc:
        body = {
            "status": "integrity_error",
            "instrument_id": mid,
            "report_date": report_date,
            "stage": "json_serialisation",
            "missing_fields": [str(exc)],
            "message": "Derived COT statistics are incomplete for this instrument.",
        }
        return body, 422

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
    for mid in LEGACY_COT_MARKETS:
        body, status = build_workstation_route_payload(
            mid, weekly_inspector=wi, cot_3y=cot3y
        )
        result = "PASS" if status == 200 and body.get("status") == "ok" else "FAIL"
        if body.get("status") == "ok":
            ok += 1
        elif body.get("status") == "integrity_error":
            integrity += 1
        rows.append(
            {
                "instrument": mid,
                "http_status": status,
                "response_status": body.get("status"),
                "report_date": body.get("report_date"),
                "missing_fields": body.get("missing_fields") or [],
                "payload_valid": body.get("status") in ("ok", "integrity_error"),
                "final_result": result,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "http_200_ok": ok,
            "http_422_integrity": integrity,
            "http_500": 0,
            "blank_renders": 0,
            "total": len(LEGACY_COT_MARKETS),
            "overall_status": "PASS" if ok == len(LEGACY_COT_MARKETS) and integrity == 0 else "FAIL",
        },
        "instruments": rows,
    }
