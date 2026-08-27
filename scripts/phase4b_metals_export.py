#!/usr/bin/env python3
"""Patch metals blocks in valuation_latest.json without full 138-market export."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.config import PROJECT_ROOT, PROCESSED_DIR
from hptl.valuation.export import (
    DATA_OUT,
    DIST_OUT,
    METALS_PILLAR_ENGINE,
    PUBLIC_OUT,
    _sanitize_withheld_export_block,
)
from hptl.valuation.ive_adapter import attach_ive_to_export_block
from hptl.valuation.metals_valuation_v1 import METALS_MARKETS
from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation


def _load_doc(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def patch_metals_exports() -> None:
    paths = [ROOT / DATA_OUT, PROCESSED_DIR / "valuation_latest.json", PUBLIC_OUT, DIST_OUT]
    doc = _load_doc(paths[0])
    if not doc:
        doc = {"version": 3, "instruments": {}, "summary": {}}

    generated_at = datetime.now(timezone.utc).isoformat()
    instruments = dict(doc.get("instruments") or {})
    for market in METALS_MARKETS:
        block = compute_metals_institutional_valuation(market=market)
        block["valuation_pillar"] = METALS_PILLAR_ENGINE
        enriched = attach_ive_to_export_block(dict(block), market, generated_at=generated_at)
        instruments[market] = _sanitize_withheld_export_block(enriched)

    doc["instruments"] = instruments
    doc["generated_at"] = generated_at
    doc["metals_pillar_engine"] = METALS_PILLAR_ENGINE
    doc["valuation_phase"] = "4B"
    doc["note"] = (
        "Institutional Valuation Engine (IVE) Phase 4B export. "
        "Metal-specific fair value models. publish=true required for scanner display."
    )
    summary = dict(doc.get("summary") or {})
    summary["metals_wired_count"] = sum(1 for m in METALS_MARKETS if (instruments.get(m) or {}).get("wired"))
    summary["wired_count"] = sum(1 for v in instruments.values() if v.get("wired"))
    summary["unavailable_count"] = len(instruments) - summary["wired_count"]
    doc["summary"] = summary

    text = json.dumps(doc, indent=2, ensure_ascii=False)
    for path in paths:
        if path == DIST_OUT and not path.parent.parent.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(f"Wrote {path}")


if __name__ == "__main__":
    patch_metals_exports()
