"""Re-attach IVE contract to existing valuation_latest.json without recomputing models."""
from __future__ import annotations

import json
from pathlib import Path

from hptl.valuation.ive_adapter import attach_ive_to_export_block

ROOT = Path(__file__).resolve().parents[1]
PATHS = [
    ROOT / "data" / "valuation_latest.json",
    ROOT / "web-dashboard" / "public" / "data" / "valuation_latest.json",
]


def main() -> None:
    for path in PATHS:
        if not path.exists():
            print(f"skip {path}")
            continue
        doc = json.loads(path.read_text(encoding="utf-8"))
        gen = doc.get("generated_at") or ""
        for market, block in (doc.get("instruments") or {}).items():
            doc["instruments"][market] = attach_ive_to_export_block(dict(block), market, generated_at=gen)
        doc["ive_schema_version"] = 1
        doc["note"] = (
            "Institutional Valuation Engine (IVE) Phase 0 export. "
            "Fair value + auditable calculation breakdown. No confidence scores."
        )
        path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
        gold = doc["instruments"].get("Gold") or {}
        print(
            f"{path.name}: Gold status={gold.get('model_status')} "
            f"grade={gold.get('valuation_grade')} "
            f"no_conf={'confidence' not in gold}"
        )


if __name__ == "__main__":
    main()
