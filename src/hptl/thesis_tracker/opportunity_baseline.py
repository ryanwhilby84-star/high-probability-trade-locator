"""Capture 3-pillar opportunity rankings before valuation/seasonality are wired."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.thesis_tracker.alignment import alignment_summary, evaluate_pillars, _effective_direction
from hptl.thesis_tracker.opportunity import build_opportunity, _latest_snap, _hydrate_snap

BASELINE_PATH = PROJECT_ROOT / "data/opportunity_baseline_3pillar.json"
THESIS_EXPORT = PROJECT_ROOT / "web-dashboard/public/data/thesis_tracker_latest.json"

_PILLAR_STRIP_KEYS = (
    "valuation_bias",
    "valuation_score",
    "valuation_reason",
    "valuation_wired",
    "seasonality_bias",
    "seasonality_score",
    "seasonality_reason",
    "seasonality_wired",
)


def _strip_pillar_fields(snap: dict[str, Any]) -> dict[str, Any]:
    out = dict(snap)
    for k in _PILLAR_STRIP_KEYS:
        out.pop(k, None)
    return out


def evaluate_three_pillar_opportunity(thesis: dict[str, Any]) -> dict[str, Any]:
    """Recompute opportunity as if valuation/seasonality were not wired."""
    t = dict(thesis)
    snaps = t.get("snapshots") or []
    if snaps:
        last = _hydrate_snap(snaps[-1] if isinstance(snaps[-1], dict) else {}, str(t.get("market") or ""))
        t["snapshots"] = [*snaps[:-1], _strip_pillar_fields(last)]
    t.pop("opportunity", None)
    return build_opportunity(t, include_pillars=False)


def capture_baseline_from_export(path: Path | None = None) -> dict[str, Any]:
    src = path or THESIS_EXPORT
    doc = json.loads(src.read_text(encoding="utf-8"))
    theses = doc.get("theses") or []
    rows: list[dict[str, Any]] = []
    for th in theses:
        if not isinstance(th, dict):
            continue
        opp = evaluate_three_pillar_opportunity(th)
        align = opp.get("alignment") or {}
        rows.append(
            {
                "market": th.get("market"),
                "thesis_id": th.get("thesis_id"),
                "alignment_label": align.get("label"),
                "alignment_pass": align.get("pass"),
                "action": opp.get("action"),
                "rank_score": opp.get("rank_score"),
                "pillars_pass": {
                    p["pillar"]: p.get("pass")
                    for p in (align.get("pillars") or [])
                    if isinstance(p, dict)
                },
            }
        )
    rows.sort(key=lambda r: (-(r.get("rank_score") or 0), str(r.get("market") or "")))
    return {
        "version": 1,
        "mode": "3_pillar_stripped",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(src),
        "instruments": rows,
    }


def write_baseline(path: Path | None = None) -> Path:
    out = path or BASELINE_PATH
    payload = capture_baseline_from_export()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def main() -> int:
    p = write_baseline()
    n = len(json.loads(p.read_text(encoding="utf-8")).get("instruments") or [])
    print(f"Wrote {p} ({n} instruments)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
