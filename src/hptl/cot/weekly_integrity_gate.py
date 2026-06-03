"""Weekly COT integrity gate — source truth + lineage validation before publish."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.cot.cot_quarantine import clear_quarantine, write_quarantine
from hptl.cot.cot_source_truth_audit import (
    DATA_OUT as SOURCE_TRUTH_PATH,
    build_cot_source_truth_audit,
    write_cot_source_truth_exports,
)
from hptl.cot.data_lineage_audit import (
    DATA_OUT as LINEAGE_PATH,
    build_data_lineage_audit,
    write_data_lineage_exports,
)
from hptl.markets.instrument_registry import cot_mapped_ids

GATE_JSON_PATH = Path("data/cot_weekly_integrity_gate_latest.json")
COT_LAYER_FROZEN = True  # No further COT feature work unless integrity failure detected.


@dataclass
class WeeklyIntegrityGateResult:
    report_date: str = ""
    checked_count: int = 0
    passed_count: int = 0
    failed_count: int = 0
    failed_instruments: list[str] = field(default_factory=list)
    quarantine_applied: bool = False
    republished: bool = False
    exit_code: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_date": self.report_date,
            "checked_count": self.checked_count,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "failed_instruments": self.failed_instruments,
            "quarantine_applied": self.quarantine_applied,
            "republished": self.republished,
            "exit_code": self.exit_code,
            "error": self.error,
            "cot_layer_frozen": COT_LAYER_FROZEN,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


def print_weekly_summary(result: WeeklyIntegrityGateResult) -> None:
    n = result.checked_count
    p = result.passed_count
    f = result.failed_count
    print(f"{n} instruments checked")
    print(f"{p} passed")
    print(f"{f} failed")
    if result.failed_instruments:
        print("Failed:")
        for iid in result.failed_instruments:
            print(iid)


def _write_gate_json(payload: dict[str, Any]) -> None:
    GATE_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    GATE_JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _collect_failures(
    *,
    truth_doc: dict[str, Any],
    lineage_doc: dict[str, Any],
) -> list[dict[str, Any]]:
    failed: dict[str, dict[str, Any]] = {}
    for iid in cot_mapped_ids():
        truth_inst = (truth_doc.get("instruments") or {}).get(iid) or {}
        truth_st = str(truth_inst.get("status") or "")
        lineage_inst = (lineage_doc.get("instruments") or {}).get(iid) or {}
        lineage_st = str(lineage_inst.get("overall_status") or "")

        reasons: list[str] = []
        if truth_st != "PASS":
            reasons.append(f"source_truth:{truth_st or 'MISSING'}")
        if lineage_st != "PASS":
            reasons.append(f"lineage:{lineage_st or 'MISSING'}")
            for r in lineage_inst.get("failure_reasons") or []:
                reasons.append(str(r))

        if truth_st == "PASS" and lineage_st == "PASS":
            continue

        failed[iid] = {
            "instrument": iid,
            "source_truth_status": truth_st or "UNKNOWN",
            "lineage_status": lineage_st or "UNKNOWN",
            "first_divergence_layer": lineage_inst.get("first_divergence_layer"),
            "reasons": reasons,
        }
    return [failed[k] for k in sorted(failed.keys())]


def seed_thesis_snapshots(*, weeks: int = 13) -> int:
    """Refresh thesis_tracker snapshots for all COT-mapped instruments (lineage layer)."""
    from hptl.thesis_tracker.run_thesis_seed import seed_all_cot_instruments

    return seed_all_cot_instruments(weeks=weeks, reset=True)


def republish_confluence_after_quarantine(*, cftc_week: str | None) -> bool:
    """Rebuild confluence JSON with quarantined instruments excluded."""
    from hptl.confluence import build_decision_table as bdt
    from hptl.confluence.dashboard_export import sync_dist_exports

    bdt.run(
        cot_feed_meta={
            "latest_cftc_report_date": cftc_week,
            "cot_data_stale": False,
            "integrity_gate_republish": True,
        }
    )
    sync_dist_exports()
    return True


def run_weekly_integrity_gate(
    *,
    force_download: bool = True,
    seed_thesis: bool = True,
    thesis_weeks: int = 13,
    republish_on_quarantine: bool = True,
    skip_deliverable_markdown: bool = True,
) -> WeeklyIntegrityGateResult:
    """
    Import → Source Truth Validation → Lineage Validation → Quarantine → Publish.

    Call after COT import and an initial confluence rebuild. When instruments fail,
    they are quarantined and confluence is rebuilt without them when ``republish_on_quarantine``.
    """
    result = WeeklyIntegrityGateResult()
    try:
        truth_doc = build_cot_source_truth_audit(force_download=force_download)
        write_cot_source_truth_exports(truth_doc, skip_deliverable=skip_deliverable_markdown)

        if seed_thesis:
            rc = seed_thesis_snapshots(weeks=thesis_weeks)
            if rc != 0:
                result.error = "thesis seed failed — lineage validation may fail"
                result.exit_code = 1

        lineage_doc = build_data_lineage_audit()
        write_data_lineage_exports(lineage_doc, skip_deliverable=skip_deliverable_markdown)

        report_date = str(
            truth_doc.get("latest_report_date") or lineage_doc.get("latest_report_date") or ""
        )[:10]
        result.report_date = report_date

        failures = _collect_failures(truth_doc=truth_doc, lineage_doc=lineage_doc)
        checked = len(cot_mapped_ids())
        result.checked_count = checked
        result.failed_count = len(failures)
        result.passed_count = checked - result.failed_count
        result.failed_instruments = [f["instrument"] for f in failures]

        if failures:
            write_quarantine(
                report_date=report_date,
                failed=failures,
                passed_count=result.passed_count,
                checked_count=checked,
            )
            result.quarantine_applied = True
            if republish_on_quarantine:
                republish_confluence_after_quarantine(cftc_week=report_date)
                result.republished = True
        else:
            clear_quarantine(report_date=report_date, checked_count=checked)

        result.exit_code = 0 if result.failed_count == 0 else 1
        gate_payload = result.to_dict()
        gate_payload["source_truth_path"] = str(SOURCE_TRUTH_PATH.resolve())
        gate_payload["lineage_path"] = str(LINEAGE_PATH.resolve())
        _write_gate_json(gate_payload)
        print_weekly_summary(result)
        return result
    except Exception as exc:
        result.error = str(exc)
        result.exit_code = 1
        print_weekly_summary(result)
        if result.error:
            print(f"ERROR: {result.error}")
        return result
