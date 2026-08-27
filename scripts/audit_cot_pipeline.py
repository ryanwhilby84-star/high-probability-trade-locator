#!/usr/bin/env python3
"""Phase 2A — COT pipeline audit (health, staleness, failures)."""
from __future__ import annotations

import json
import sys
from datetime import date

from hptl.cot.cot_failures import FAILURES_JSON, read_cot_failures
from hptl.cot.pipeline_health import (
    HEALTH_JSON,
    build_cot_pipeline_health,
    compute_days_stale,
    status_from_health,
    write_cot_pipeline_health,
)
from hptl.cot.report_dates import get_latest_local_report_date, probe_cftc_latest_report_date


def main() -> int:
    probe = None
    probe_err = None
    try:
        probe = probe_cftc_latest_report_date()
        expected = probe.latest_report_date.strftime("%Y-%m-%d") if probe.latest_report_date is not None else None
    except Exception as exc:
        expected = None
        probe_err = str(exc)

    local = get_latest_local_report_date()
    latest = local.strftime("%Y-%m-%d") if local is not None else None

    paths = write_cot_pipeline_health(probe_cftc=probe is None)
    health = build_cot_pipeline_health(probe_cftc=probe is None)

    print("=== COT Pipeline Audit (Phase 2A) ===")
    print(f"Health JSON:     {paths['health_json']}")
    print(f"Dashboard JSON:  {paths['public_json']}")
    print(f"Reliability MD:  {paths['report_md']}")
    print(f"Failures log:    {FAILURES_JSON}")
    print()
    print(f"Latest available (CFTC):  {health.get('expected_report_date') or expected or probe_err or '—'}")
    print(f"Latest stored (local):    {health.get('latest_local_cot_week') or latest or '—'}")
    print(f"Latest export week:       {health.get('latest_export_cot_week') or '—'}")
    print(f"Days stale:               {health.get('days_stale')}")
    print(f"Health status:            {health.get('status')}")
    print(f"Download success:         {health.get('download_success')}")
    print(f"Ingest success:           {health.get('ingest_success')}")
    print(f"Missing markets:          {', '.join(health.get('missing_markets') or []) or '—'}")
    print()
    dl_fail = health.get("download_failures") or []
    ig_fail = health.get("ingest_failures") or []
    print(f"Recent download failures: {len(dl_fail)}")
    for f in dl_fail[-5:]:
        print(f"  - [{f.get('timestamp', '')[:19]}] {f.get('source')}: {f.get('error')}")
    print(f"Recent ingest failures:   {len(ig_fail)}")
    for f in ig_fail[-5:]:
        print(f"  - [{f.get('timestamp', '')[:19]}] {f.get('source')}: {f.get('error')}")
    print()
    print(f"Dashboard message: {health.get('dashboard_message')}")

    if health.get("status") == "FAILURE":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
