"""Export Weekly Analysis JSON for the COT workstation.

Driven exclusively by the trajectory reasoning engine.
Does not call analyst_intelligence.py templates.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.trajectory_reasoning import build_trajectory_weekly_analysis

CANONICAL_PATH = PROCESSED_DIR / "cot_analyst_intelligence_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_analyst_intelligence_latest.json"
DIST_PATH = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "cot_analyst_intelligence_latest.json"
DATA_PATH = PROJECT_ROOT / "data" / "cot_analyst_intelligence_latest.json"

WI_PATHS = (
    PROCESSED_DIR / "cot_weekly_inspector_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_weekly_inspector_latest.json",
    PROJECT_ROOT / "data" / "cot_weekly_inspector_latest.json",
)
RESEARCH_PATHS = (
    PROCESSED_DIR / "cot_positioning_research_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_positioning_research_latest.json",
    PROJECT_ROOT / "data" / "cot_positioning_research_latest.json",
)
OHLC_PATHS = (
    PROCESSED_DIR / "workstation_ohlc_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json",
    PROJECT_ROOT / "data" / "workstation_ohlc_latest.json",
)


def _load_first(paths: tuple[Path, ...]) -> dict[str, Any]:
    for p in paths:
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    return {}


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_analyst_intelligence_export(
    *,
    weekly_inspector: dict[str, Any] | None = None,
    positioning_research: dict[str, Any] | None = None,
    workstation_ohlc: dict[str, Any] | None = None,
    skip_integrity_gate: bool = False,
) -> dict[str, Any]:
    wi = weekly_inspector if weekly_inspector is not None else _load_first(WI_PATHS)
    research = (
        positioning_research
        if positioning_research is not None
        else _load_first(RESEARCH_PATHS)
    )
    ohlc = workstation_ohlc if workstation_ohlc is not None else _load_first(OHLC_PATHS)
    if not skip_integrity_gate:
        from hptl.cot.derived_cot_integrity_audit import run_derived_cot_integrity_audit

        gate = run_derived_cot_integrity_audit(weekly_inspector=wi)
        if gate.get("summary", {}).get("overall_status") != "PASS":
            failing = [
                r["instrument"]
                for r in gate.get("instruments") or []
                if r.get("status") != "PASS"
            ]
            raise RuntimeError(
                "DERIVED COT INTEGRITY FAILED — Weekly Analysis blocked. "
                f"Failing: {', '.join(failing[:8])}"
                + ("…" if len(failing) > 8 else "")
            )
    payload = build_trajectory_weekly_analysis(
        weekly_inspector=wi,
        workstation_ohlc=ohlc,
        positioning_research=research,
    )
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
    payload = run_analyst_intelligence_export()
    s = payload.get("summary") or {}
    print(
        f"Trajectory Weekly Analysis — engine={payload.get('engine')} "
        f"markets={s.get('markets_available')}/{s.get('markets_total')}"
    )
    print(f"wrote {PUBLIC_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
