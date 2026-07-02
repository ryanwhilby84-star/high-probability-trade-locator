"""Write Macro Hub export to exports/, processed/, and dashboard public paths."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.macro_hub.config import CANONICAL_PATH, EXPORT_PATH, PUBLIC_PATH
from hptl.macro_hub.pool_builder import build_macro_hub_payload


def write_macro_hub_exports(payload: dict[str, Any]) -> Path:
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for path in (EXPORT_PATH, CANONICAL_PATH, PUBLIC_PATH):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return EXPORT_PATH


def run(*, allow_live: bool | None = None, cot_download: bool | None = None) -> Path:
    payload = build_macro_hub_payload(allow_live=allow_live, cot_download=cot_download)
    path = write_macro_hub_exports(payload)
    health = payload.get("source_health") or {}
    print(
        f"Wrote {path} (as_of={payload.get('as_of_date')}, "
        f"issues={health.get('issue_count', 0)}, missing={health.get('missing_count', 0)})"
    )
    return path


if __name__ == "__main__":
    run()
