"""Export FX setup ranking artifact."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.setup_ranking.fx_engine import build_fx_setup_ranking_payload


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


CANONICAL_PATH = PROCESSED_DIR / "fx_setup_ranking_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "fx_setup_ranking_latest.json"


def write_fx_setup_ranking_exports(payload: dict[str, Any]) -> Path:
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for path in (CANONICAL_PATH, PUBLIC_PATH):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def run(*, confluence_records: list[dict[str, Any]] | None = None) -> Path:
    if confluence_records is None:
        from hptl.config import PROJECT_ROOT

        conf_path = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "confluence_history_latest.json"
        doc = _load_json(conf_path) if conf_path.exists() else None
        if doc:
            confluence_records = doc.get("records") or []
    payload = build_fx_setup_ranking_payload(confluence_records=confluence_records)
    path = write_fx_setup_ranking_exports(payload)
    n = len(payload.get("filtered_opportunities") or [])
    top = (payload.get("filtered_opportunities") or [{}])[0]
    print(
        f"Wrote FX setup ranking: {path} "
        f"({len(payload.get('opportunities') or [])} pairs, {n} A+/A/B+; "
        f"top={top.get('pair')} {top.get('setup_score')} {top.get('grade')})"
    )
    return path


if __name__ == "__main__":
    run()
