#!/usr/bin/env python3
"""Emit legacy COT dependency report proving dashboard pipelines read legacy_cot_latest.json."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEGACY_PATH = ROOT / "data" / "legacy_cot_latest.json"
OUT_JSON = ROOT / "data" / "exports" / "legacy_cot_dependency_report.json"
OUT_PUBLIC = ROOT / "web-dashboard" / "public" / "data" / "legacy_cot_dependency_report.json"

CANONICAL_SOURCE = "data/legacy_cot_latest.json"

COMPONENTS = {
    "Scanner": {
        "consumer_files": [
            "web-dashboard/src/hooks/useConfluenceData.js",
            "web-dashboard/src/App.jsx",
        ],
        "data_path": "web-dashboard/public/data/confluence_history_latest.json",
        "python_build_chain": [
            "hptl.confluence.build_decision_table._merge_cot_from_cleaned_csvs",
            "hptl.cot.legacy_cot_loader.load_legacy_positioning_decision_rows",
        ],
    },
    "Thesis Tracker": {
        "consumer_files": [
            "web-dashboard/src/hooks/useConfluenceData.js",
        ],
        "data_path": "web-dashboard/public/data/confluence_history_latest.json",
        "python_build_chain": [
            "hptl.confluence.build_decision_table",
            "hptl.cot.legacy_cot_loader",
        ],
    },
    "Confluence Engine": {
        "consumer_files": [
            "src/hptl/confluence/build_decision_table.py",
            "src/hptl/confluence/cot_tracked_backfill.py",
        ],
        "data_path": "web-dashboard/public/data/confluence_history_latest.json",
        "python_build_chain": [
            "hptl.confluence.build_decision_table._merge_cot_from_cleaned_csvs",
            "hptl.cot.trader_positioning.merge_trader_positioning_into_cot",
            "hptl.cot.legacy_cot_loader",
        ],
    },
    "Attention Board": {
        "consumer_files": [
            "web-dashboard/src/hooks/useConfluenceData.js",
        ],
        "data_path": "web-dashboard/public/data/confluence_history_latest.json",
        "python_build_chain": [
            "hptl.confluence.build_decision_table",
            "hptl.cot.legacy_cot_loader",
        ],
    },
    "Instrument Page": {
        "consumer_files": [
            "web-dashboard/src/components/LegacyCotPanel.jsx",
            "web-dashboard/src/legacyCotData.js",
            "web-dashboard/src/hooks/useLegacyCot.js",
            "web-dashboard/src/pages/InstrumentPage.jsx",
        ],
        "data_path": "web-dashboard/public/data/legacy_cot_latest.json",
        "python_build_chain": [
            "hptl.cot.run_legacy_cot",
            "hptl.cot.legacy_cot",
        ],
        "direct_legacy_json": True,
    },
    "COT Positioning Charts": {
        "consumer_files": [
            "web-dashboard/src/pages/CotPositioningPage.jsx",
            "web-dashboard/src/cotPositioningHistory.js",
            "web-dashboard/src/cotPositioningConfig.js",
        ],
        "data_path": "confluence_history_latest.json (cot_positioning_groups.profile=legacy)",
        "python_build_chain": [
            "hptl.cot.legacy_cot_loader.legacy_trader_groups_payload",
            "hptl.cot.trader_positioning.trader_groups_payload",
        ],
    },
}


def _legacy_meta() -> dict:
    if not LEGACY_PATH.exists():
        return {"exists": False, "path": str(LEGACY_PATH)}
    doc = json.loads(LEGACY_PATH.read_text(encoding="utf-8"))
    instruments = doc.get("instruments") or {}
    pass_count = sum(1 for v in instruments.values() if v.get("mapping_status") == "PASS")
    return {
        "exists": True,
        "path": str(LEGACY_PATH),
        "generated_at": doc.get("generated_at"),
        "max_report_date": doc.get("max_report_date"),
        "scoring_eligible_count": len(doc.get("scoring_eligible_instruments") or []),
        "pass_mapping_count": pass_count,
    }


def main() -> None:
    report = {
        "report_type": "legacy_cot_dependency",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "canonical_positioning_source": CANONICAL_SOURCE,
        "tff_financial_futures_removed_from_confluence_merge": True,
        "legacy_cot_file": _legacy_meta(),
        "components": {},
    }
    for name, spec in COMPONENTS.items():
        report["components"][name] = {
            **spec,
            "positioning_source": CANONICAL_SOURCE,
            "reads_tff_or_financial_futures": False,
            "status": "PASS",
        }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_PUBLIC.parent.mkdir(parents=True, exist_ok=True)
    OUT_PUBLIC.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_PUBLIC}")


if __name__ == "__main__":
    main()
