"""COT Pipeline Health Monitor — Phase 2A bulletproof CFTC ingestion."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.confluence.build_decision_table import OUT_PATH as CONFLUENCE_PATH
from hptl.cot.cot_failures import FAILURES_JSON, read_cot_failures
from hptl.cot.report_dates import get_latest_local_report_date, probe_cftc_latest_report_date
from hptl.cot.weekly_run_log import WEEKLY_JSON_HISTORY, WEEKLY_JSON_LATEST

# Phase 2A canonical path (Task 1)
HEALTH_JSON = DATA_DIR / "audits" / "cot_pipeline_health.json"
PUBLIC_HEALTH_JSON = PROJECT_ROOT / "web-dashboard/public/data/cot_pipeline_health.json"
RELIABILITY_MD = DATA_DIR / "audits/cot_pipeline_reliability_report.md"

# Legacy alias kept for imports elsewhere
LEGACY_HEALTH_JSON = DATA_DIR / "cot_pipeline_health.json"

HEALTHY_MAX_DAYS = 10
WARNING_MAX_DAYS = 17


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def compute_days_stale(latest: date | None, expected: date | None, *, reference: date | None = None) -> int:
    """Days between stored latest report and expected CFTC report."""
    ref = reference or date.today()
    if expected is None:
        if latest is None:
            return 999
        return max(0, (ref - latest).days)
    if latest is None:
        return max(0, (expected - ref).days) if expected > ref else 999
    return max(0, (expected - latest).days)


def status_from_health(
    *,
    days_stale: int,
    download_success: bool,
    ingest_success: bool,
    pipeline_error: str | None = None,
) -> str:
    if pipeline_error or not download_success or not ingest_success:
        return "FAILURE"
    if days_stale >= 18:
        return "FAILURE"
    if days_stale >= 11:
        return "WARNING"
    return "HEALTHY"


def dashboard_message(
    *,
    status: str,
    latest_report_date: str | None,
    days_stale: int,
    download_success: bool,
    ingest_success: bool,
) -> str:
    if status == "HEALTHY":
        return f"Latest COT: {latest_report_date or '—'}"
    if not download_success or not ingest_success:
        return "No successful CFTC update"
    if status == "WARNING":
        return f"Data {days_stale} days stale"
    if status == "FAILURE":
        if days_stale >= 18:
            return f"Data {days_stale} days stale — no successful CFTC update"
        return "No successful CFTC update"
    return f"COT status: {status}"


def _read_weekly_latest() -> dict[str, Any]:
    path = Path(WEEKLY_JSON_LATEST)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _recent_download_failures(limit: int = 20) -> list[dict[str, Any]]:
    failures = read_cot_failures(limit=limit)
    return [f for f in failures if f.get("failure_type") in {"download", "parse", "download_validation"}]


def _recent_ingest_failures(limit: int = 20) -> list[dict[str, Any]]:
    failures = read_cot_failures(limit=limit)
    return [f for f in failures if f.get("failure_type") in {"ingest_validation", "ingest"}]


def _confluence_cot_status() -> dict[str, Any]:
    path = Path(CONFLUENCE_PATH)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload.get("cot_feed_status") or {})


def build_cot_pipeline_health(
    *,
    probe_cftc: bool = True,
    run_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build Phase 2A health document."""
    now = datetime.now(timezone.utc).isoformat()
    ctx = run_context or {}
    weekly = _read_weekly_latest()

    local_ts = get_latest_local_report_date()
    local_iso = local_ts.strftime("%Y-%m-%d") if local_ts is not None else None

    cftc_iso: str | None = ctx.get("expected_report_date") or ctx.get("latest_cftc_report_date")
    probe_error: str | None = None
    if probe_cftc and not cftc_iso:
        try:
            probe = probe_cftc_latest_report_date()
            cftc_iso = probe.latest_report_date.strftime("%Y-%m-%d") if probe.latest_report_date is not None else None
        except Exception as exc:
            probe_error = f"{type(exc).__name__}: {exc}"

    conf = _confluence_cot_status()
    export_week = conf.get("latest_export_cot_week") or weekly.get("export_latest_cot_week") or local_iso
    latest_report = str(export_week or local_iso or "")[:10] or None
    expected_report = str(cftc_iso or "")[:10] or None

    latest_d = _parse_date(latest_report)
    expected_d = _parse_date(expected_report)
    days_stale = compute_days_stale(latest_d, expected_d)

    download_success = bool(ctx.get("download_success", weekly.get("download_success", True)))
    ingest_success = bool(ctx.get("ingest_success", weekly.get("ingest_success", True)))
    if weekly.get("exit_code", 0) not in (0, None) and weekly.get("error"):
        if weekly.get("update_needed") and not weekly.get("update_performed"):
            download_success = False
    if ctx.get("pipeline_error") or weekly.get("error"):
        err = str(ctx.get("pipeline_error") or weekly.get("error") or "")
        if "download" in err.lower() or "export failed" in err.lower():
            download_success = False
        if "ingest" in err.lower() or "master rebuild" in err.lower():
            ingest_success = False

    pipeline_error = ctx.get("pipeline_error") or weekly.get("error")
    status = status_from_health(
        days_stale=days_stale,
        download_success=download_success,
        ingest_success=ingest_success,
        pipeline_error=pipeline_error if status_from_health.__name__ else None,
    )
    # Recompute with pipeline_error
    status = status_from_health(
        days_stale=days_stale,
        download_success=download_success,
        ingest_success=ingest_success,
        pipeline_error=str(pipeline_error) if pipeline_error else None,
    )

    is_stale = status in {"WARNING", "FAILURE"} or days_stale > HEALTHY_MAX_DAYS
    message = dashboard_message(
        status=status,
        latest_report_date=latest_report,
        days_stale=days_stale,
        download_success=download_success,
        ingest_success=ingest_success,
    )

    health: dict[str, Any] = {
        "phase": "Phase 2A COT Pipeline Health",
        "generated_at": now,
        "last_check": now,
        # Task 1 required fields
        "status": status,
        "latest_report_date": latest_report,
        "expected_report_date": expected_report,
        "days_stale": days_stale,
        "download_success": download_success,
        "ingest_success": ingest_success,
        # Extended observability
        "latest_local_cot_week": local_iso,
        "latest_export_cot_week": export_week,
        "latest_cftc_report_week": expected_report,
        "probe_error": probe_error,
        "pipeline_error": pipeline_error,
        "download_failures": _recent_download_failures(),
        "ingest_failures": _recent_ingest_failures(),
        "missing_markets": ctx.get("missing_markets") or weekly.get("markets_missing") or [],
        "last_run": {
            "run_timestamp_utc": weekly.get("run_timestamp_utc"),
            "update_performed": weekly.get("update_performed"),
            "update_needed": weekly.get("update_needed"),
            "exit_code": weekly.get("exit_code"),
            "error": weekly.get("error"),
        },
        "validation": {
            "download": ctx.get("download_validation"),
            "ingest": ctx.get("ingest_validation"),
        },
        "never_use_stale_positioning": is_stale,
        "is_stale": is_stale,
        "warning_message": message if status != "HEALTHY" else None,
        "dashboard_message": message,
        "failures_log": str(FAILURES_JSON),
        "weekly_log": str(WEEKLY_JSON_LATEST),
        "weekly_history": str(WEEKLY_JSON_HISTORY),
    }
    return health


def write_cot_pipeline_health(
    *,
    probe_cftc: bool = True,
    run_context: dict[str, Any] | None = None,
) -> dict[str, Path]:
    health = build_cot_pipeline_health(probe_cftc=probe_cftc, run_context=run_context)
    payload = json.dumps(health, indent=2)
    for path in (HEALTH_JSON, PUBLIC_HEALTH_JSON, LEGACY_HEALTH_JSON):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")

    lines = [
        "# COT Pipeline Reliability Report (Phase 2A)",
        "",
        f"Generated: {health['last_check']}",
        "",
        f"**Status:** {health['status']}",
        f"**Latest stored:** {health['latest_report_date']}",
        f"**Expected CFTC:** {health['expected_report_date']}",
        f"**Days stale:** {health['days_stale']}",
        f"**Download OK:** {health['download_success']}",
        f"**Ingest OK:** {health['ingest_success']}",
        "",
        f"> {health['dashboard_message']}",
    ]
    if health.get("pipeline_error"):
        lines.append(f"\nPipeline error: {health['pipeline_error']}")
    if health.get("missing_markets"):
        lines.append(f"\nMissing markets: {', '.join(health['missing_markets'])}")
    RELIABILITY_MD.parent.mkdir(parents=True, exist_ok=True)
    RELIABILITY_MD.write_text("\n".join(lines), encoding="utf-8")

    return {
        "health_json": HEALTH_JSON,
        "public_json": PUBLIC_HEALTH_JSON,
        "report_md": RELIABILITY_MD,
    }


def write_health_from_pipeline_result(result: Any) -> dict[str, Path]:
    """Write health JSON from a CotPipelineResult + attached validation metadata."""
    ctx: dict[str, Any] = {
        "expected_report_date": getattr(result, "latest_cftc_report_date", None),
        "latest_cftc_report_date": getattr(result, "latest_cftc_report_date", None),
        "download_success": getattr(result, "download_success", True),
        "ingest_success": getattr(result, "ingest_success", True),
        "pipeline_error": getattr(result, "error", None),
        "missing_markets": getattr(result, "markets_missing", None) or [],
        "download_validation": getattr(result, "download_validation", None),
        "ingest_validation": getattr(result, "ingest_validation", None),
    }
    if getattr(result, "exit_code", 0) != 0 and getattr(result, "error", None):
        ctx["pipeline_error"] = result.error
    return write_cot_pipeline_health(probe_cftc=False, run_context=ctx)
