"""Persistent logs for scheduled weekly COT runs."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import EXPORTS_DIR

WEEKLY_TEXT_LOG = EXPORTS_DIR / "weekly_cot_update.log"
WEEKLY_JSON_LATEST = EXPORTS_DIR / "weekly_cot_update_latest.json"
WEEKLY_JSON_HISTORY = EXPORTS_DIR / "weekly_cot_update_history.jsonl"


def _ensure_exports_dir() -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def persist_weekly_run(
    payload: dict[str, Any],
    *,
    human_lines: list[str],
) -> tuple[Path, Path]:
    """Append text log, overwrite latest JSON, append one JSONL history line."""
    _ensure_exports_dir()

    stamp = payload.get("run_timestamp_utc") or datetime.now(timezone.utc).isoformat()
    block = [
        "",
        "=" * 72,
        f"HPTL WEEKLY COT UPDATE @ {stamp}",
        "=" * 72,
        *human_lines,
        f"exit_code: {payload.get('exit_code')}",
        "=" * 72,
    ]
    with WEEKLY_TEXT_LOG.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(block) + "\n")

    WEEKLY_JSON_LATEST.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with WEEKLY_JSON_HISTORY.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, separators=(",", ":")) + "\n")

    return WEEKLY_TEXT_LOG, WEEKLY_JSON_LATEST
