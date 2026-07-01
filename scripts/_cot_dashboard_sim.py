"""Simulate dashboard COT warning visibility (mirrors ScannerPage + AppShell)."""
from __future__ import annotations


def scanner_warning(cot_feed_status: dict | None, cot_pipeline_health: dict | None) -> str | None:
    cot_stale = bool(
        (cot_feed_status or {}).get("is_stale")
        or (cot_pipeline_health or {}).get("is_stale")
        or (
            (cot_pipeline_health or {}).get("status")
            and cot_pipeline_health.get("status") != "HEALTHY"
        )
    )
    if not cot_stale:
        return None
    if cot_pipeline_health and cot_pipeline_health.get("status"):
        msg = cot_pipeline_health.get("dashboard_message") or cot_pipeline_health.get("warning_message")
        return f"COT {cot_pipeline_health['status']} — {msg or 'COT data requires attention'}"
    return "COT stale (legacy feed)"


def appshell_badge(cot_feed_status: dict | None, cot_pipeline_health: dict | None) -> str | None:
    if cot_pipeline_health and cot_pipeline_health.get("status"):
        msg = cot_pipeline_health.get("dashboard_message") or cot_pipeline_health.get("warning_message") or ""
        return f"COT: {cot_pipeline_health['status']} — {msg}".strip(" —")
    if (cot_feed_status or {}).get("is_stale") or (cot_pipeline_health or {}).get("is_stale"):
        return "COT stale"
    return None


def main() -> int:
    scenarios = {
        "missing_download": {
            "status": "FAILURE",
            "download_success": False,
            "ingest_success": True,
            "is_stale": True,
            "dashboard_message": "No successful CFTC update",
        },
        "corrupt_file": {
            "status": "FAILURE",
            "download_success": False,
            "ingest_success": True,
            "is_stale": True,
            "dashboard_message": "No successful CFTC update",
        },
        "stale_report": {
            "status": "WARNING",
            "download_success": True,
            "ingest_success": True,
            "is_stale": True,
            "days_stale": 14,
            "dashboard_message": "Data 14 days stale",
        },
        "parser_failure": {
            "status": "FAILURE",
            "download_success": False,
            "ingest_success": True,
            "is_stale": True,
            "dashboard_message": "No successful CFTC update",
            "warning_message": "Parser failed on CFTC ZIP",
        },
        "healthy": {
            "status": "HEALTHY",
            "download_success": True,
            "ingest_success": True,
            "is_stale": False,
            "dashboard_message": "Latest COT: 2026-06-09",
        },
    }

    print("=== Dashboard Failure Simulation ===")
    all_fail_visible = True
    healthy_hidden = True
    for name, health in scenarios.items():
        feed = {"is_stale": name == "stale_report"}
        banner = scanner_warning(feed, health)
        badge = appshell_badge(feed, health)
        visible = banner is not None or (
            badge is not None and health.get("status") != "HEALTHY"
        )
        if name == "healthy":
            healthy_hidden = banner is None and badge is not None and health["status"] == "HEALTHY"
        elif not visible:
            all_fail_visible = False
        print(f"[{name}]")
        print(f"  Scanner inline: {banner or '(hidden)'}")
        print(f"  AppShell badge:  {badge or '(hidden)'}")
        print(f"  Visible: {visible}")
        print()

    print(f"All failure scenarios visible: {all_fail_visible}")
    print(f"Healthy shows badge only (no stale banner): {healthy_hidden}")
    return 0 if all_fail_visible and healthy_hidden else 1


if __name__ == "__main__":
    raise SystemExit(main())
