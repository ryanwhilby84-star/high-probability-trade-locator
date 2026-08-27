"""Rolling COT pipeline failure log — Phase 2A."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR

FAILURES_JSON = DATA_DIR / "audits" / "cot_failures.json"
MAX_ENTRIES = 200


def _load_doc() -> dict[str, Any]:
    if not FAILURES_JSON.exists():
        return {"schema_version": 1, "failures": []}
    try:
        doc = json.loads(FAILURES_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema_version": 1, "failures": []}
    if not isinstance(doc.get("failures"), list):
        doc["failures"] = []
    return doc


def log_cot_failure(
    *,
    failure_type: str,
    source: str,
    error: str,
    retry_result: str | None = None,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append one failure record; keep rolling history."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "failure_type": failure_type,
        "source": source,
        "error": error,
        "retry_result": retry_result,
        "detail": detail or {},
    }
    doc = _load_doc()
    failures: list[dict[str, Any]] = doc["failures"]
    failures.append(entry)
    doc["failures"] = failures[-MAX_ENTRIES:]
    doc["last_updated"] = entry["timestamp"]
    doc["failure_count"] = len(doc["failures"])
    FAILURES_JSON.parent.mkdir(parents=True, exist_ok=True)
    FAILURES_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return entry


def read_cot_failures(*, limit: int = 50) -> list[dict[str, Any]]:
    doc = _load_doc()
    failures = doc.get("failures") or []
    return list(failures[-limit:])
