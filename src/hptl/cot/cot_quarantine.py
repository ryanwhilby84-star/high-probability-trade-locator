"""COT weekly integrity quarantine — instruments that failed source-truth or lineage gates."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

QUARANTINE_PATH = Path("data/cot_quarantine_latest.json")
PUBLIC_QUARANTINE_PATH = Path("web-dashboard/public/data/cot_quarantine_latest.json")


def load_quarantine_doc() -> dict[str, Any]:
    if not QUARANTINE_PATH.exists():
        return {}
    try:
        return json.loads(QUARANTINE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def quarantined_instrument_ids(*, report_date: str | None = None) -> set[str]:
    """Active quarantine set for the latest gate run (optionally filtered by report date)."""
    doc = load_quarantine_doc()
    if report_date and str(doc.get("report_date") or "")[:10] != str(report_date)[:10]:
        return set()
    ids = doc.get("quarantined_instruments") or doc.get("instruments") or []
    if isinstance(ids, dict):
        return {k for k, v in ids.items() if v}
    return {str(x) for x in ids if x}


def is_quarantined(instrument_id: str, *, report_date: str | None = None) -> bool:
    return instrument_id in quarantined_instrument_ids(report_date=report_date)


def write_quarantine(
    *,
    report_date: str,
    failed: list[dict[str, Any]],
    passed_count: int,
    checked_count: int,
    gate_run_at: str | None = None,
) -> dict[str, Any]:
    """Persist quarantine list and mirror to dashboard public data."""
    failed_ids = [str(f.get("instrument") or f.get("instrument_id") or "") for f in failed]
    failed_ids = [x for x in failed_ids if x]
    payload = {
        "version": 1,
        "generated_at": gate_run_at or datetime.now(timezone.utc).isoformat(),
        "report_date": report_date,
        "checked_count": checked_count,
        "passed_count": passed_count,
        "failed_count": len(failed_ids),
        "quarantined_instruments": failed_ids,
        "instruments": {
            fid: {
                "instrument": fid,
                "source_truth_status": next(
                    (f.get("source_truth_status") for f in failed if f.get("instrument") == fid),
                    None,
                ),
                "lineage_status": next(
                    (f.get("lineage_status") for f in failed if f.get("instrument") == fid),
                    None,
                ),
                "first_divergence_layer": next(
                    (f.get("first_divergence_layer") for f in failed if f.get("instrument") == fid),
                    None,
                ),
                "reasons": next((f.get("reasons") for f in failed if f.get("instrument") == fid), []),
            }
            for fid in failed_ids
        },
    }
    for path in (QUARANTINE_PATH, PUBLIC_QUARANTINE_PATH):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def clear_quarantine(*, report_date: str, checked_count: int) -> dict[str, Any]:
    return write_quarantine(
        report_date=report_date,
        failed=[],
        passed_count=checked_count,
        checked_count=checked_count,
    )
