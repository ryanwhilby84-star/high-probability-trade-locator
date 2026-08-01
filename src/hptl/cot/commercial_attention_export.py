"""Export Commercial Attention Engine V1 to dashboard JSON paths."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.commercial_attention_engine import build_commercial_attention
from hptl.cot.legacy_cot_loader import load_legacy_cot_document, legacy_cot_latest_path

CANONICAL_PATH = PROCESSED_DIR / "cot_commercial_attention_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_commercial_attention_latest.json"
DIST_PATH = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "cot_commercial_attention_latest.json"
DATA_PATH = PROJECT_ROOT / "data" / "cot_commercial_attention_latest.json"


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_commercial_attention_export(
    *,
    as_of: str | None = None,
    legacy_path: Path | None = None,
) -> dict[str, Any]:
    doc = load_legacy_cot_document(str(legacy_path) if legacy_path else None)
    payload = build_commercial_attention(doc, as_of=as_of)
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    payload["legacy_cot_source"] = str(legacy_path or legacy_cot_latest_path())

    for path in (CANONICAL_PATH, PUBLIC_PATH, DATA_PATH):
        _write(path, payload)
    if DIST_PATH.parent.is_dir():
        try:
            shutil.copy2(PUBLIC_PATH, DIST_PATH)
        except OSError:
            _write(DIST_PATH, payload)
    return payload


def main() -> int:
    payload = run_commercial_attention_export()
    summary = payload.get("summary") or {}
    board = payload.get("attention_board") or []
    print(
        f"Commercial attention V1 — week {payload.get('source_week')} | "
        f"scanned={summary.get('instruments_scanned')} eligible={summary.get('eligible')} "
        f"events={summary.get('with_events')} HIGH={summary.get('high_attention')}"
    )
    for i, row in enumerate(board[:12], start=1):
        print(
            f"  {i:2d}. {row['instrument']} [{row['attention_label']}] "
            f"pts={row['evidence_points']} events={', '.join(row.get('events') or [])}"
        )
    print(f"wrote {PUBLIC_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
