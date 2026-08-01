"""Export COT Workstation Intelligence V2 (precomputed analogues + markers)."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.workstation_intelligence import build_workstation_intelligence

COT3Y_PATHS = (
    PROCESSED_DIR / "cot_3y_series_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json",
    PROJECT_ROOT / "data" / "cot_3y_series_latest.json",
)

CANONICAL_PATH = PROCESSED_DIR / "cot_workstation_intelligence_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_workstation_intelligence_latest.json"
DIST_PATH = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "cot_workstation_intelligence_latest.json"
DATA_PATH = PROJECT_ROOT / "data" / "cot_workstation_intelligence_latest.json"


def _load_cot3y() -> dict[str, Any]:
    for p in COT3Y_PATHS:
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    return {"markets": {}}


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_workstation_intelligence_export(*, cot3y: dict[str, Any] | None = None) -> dict[str, Any]:
    doc = cot3y if cot3y is not None else _load_cot3y()
    payload = build_workstation_intelligence(doc)
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()

    for path in (CANONICAL_PATH, PUBLIC_PATH, DATA_PATH):
        _write(path, payload)
    if DIST_PATH.parent.is_dir():
        try:
            shutil.copy2(PUBLIC_PATH, DIST_PATH)
        except OSError:
            _write(DIST_PATH, payload)
    return payload


def main() -> int:
    payload = run_workstation_intelligence_export()
    s = payload.get("summary") or {}
    print(
        f"Workstation Intelligence V2 — markets={s.get('markets_available')}/{s.get('markets_total')}"
    )
    # Representative sample across whatever markets are available (not a fixed subset).
    available = [
        (mid, m)
        for mid, m in sorted((payload.get("markets") or {}).items())
        if m.get("available")
    ]
    step = max(1, len(available) // 5) if available else 1
    for mid, m in available[::step][:6]:
        a = m.get("analogues") or {}
        print(
            f"  {mid}: week={m.get('source_week')} markers={len(m.get('markers') or [])} "
            f"analogues={a.get('independent_case_count')} quality={a.get('sample_quality')}"
        )
    print(f"wrote {PUBLIC_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
