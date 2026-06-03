"""COT proof layer — verify dashboard values against raw Legacy CFTC rows."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from hptl.cot.legacy_cot import (
    CANONICAL_LEGACY_CODE,
    DATA_LATEST,
    _candidate_rows_for_instrument,
    _extract_legacy_position_row,
    _select_canonical_row,
    load_legacy_futures_only_dataframe,
)
from hptl.markets.instrument_registry import cot_mapped_ids

PARSER_NAME = "hptl.cot.cot_proof"
DATA_PROOF = Path("data/cot_proof_latest.json")
PUBLIC_PROOF = Path("web-dashboard/public/data/cot_proof_latest.json")
CONFLUENCE_PATH = Path("web-dashboard/public/data/confluence_history_latest.json")

GroupId = Literal["noncommercials", "commercials", "nonreportables"]
OverallStatus = Literal["PASS", "FAIL", "NEEDS_REVIEW"]

GROUP_LABELS: dict[GroupId, str] = {
    "noncommercials": "NC",
    "commercials": "Commercial",
    "nonreportables": "Non-Reportable",
}

RAW_KEYS: dict[GroupId, tuple[str, str]] = {
    "noncommercials": ("noncommercial_long", "noncommercial_short"),
    "commercials": ("commercial_long", "commercial_short"),
    "nonreportables": ("nonreportable_long", "nonreportable_short"),
}

CONFLUENCE_GROUP_KEYS: dict[GroupId, str] = {
    "noncommercials": "managed_money",
    "commercials": "commercial",
    "nonreportables": "nonreportable",
}


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if pd.notna(f) else None


def _values_match(a: float | None, b: float | None) -> bool | None:
    if a is None and b is None:
        return None
    if a is None or b is None:
        return False
    return abs(a - b) < 0.5


def _compare_metric(dashboard: float | None, raw: float | None) -> dict[str, Any]:
    match = _values_match(dashboard, raw)
    diff = None
    if dashboard is not None and raw is not None:
        diff = dashboard - raw
    return {
        "dashboard_value": dashboard,
        "raw_cftc_value": raw,
        "difference": diff,
        "match": match,
    }


def _group_status(metrics: dict[str, dict[str, Any]]) -> str:
    checks = [metrics.get("long", {}).get("match"), metrics.get("short", {}).get("match"), metrics.get("net", {}).get("match")]
    if any(c is False for c in checks):
        return "FAIL"
    if all(c is True for c in checks):
        return "PASS"
    return "FAIL"


def _latest_week(groups: dict[str, Any], group_id: GroupId) -> dict[str, Any] | None:
    weeks = (groups.get(group_id) or {}).get("weeks") or []
    if not weeks:
        return None
    return weeks[-1]


def _raw_group_values(raw_row: dict[str, Any], group_id: GroupId) -> tuple[float | None, float | None, float | None]:
    lk, sk = RAW_KEYS[group_id]
    long_v = _num(raw_row.get(lk))
    short_v = _num(raw_row.get(sk))
    net_v = (long_v - short_v) if long_v is not None and short_v is not None else None
    return long_v, short_v, net_v


def _legacy_dashboard_values(week: dict[str, Any] | None) -> tuple[float | None, float | None, float | None]:
    if not week:
        return None, None, None
    return _num(week.get("long")), _num(week.get("short")), _num(week.get("net"))


def _load_legacy_latest() -> dict[str, Any]:
    if not DATA_LATEST.exists():
        return {"instruments": {}, "scoring_eligible_instruments": []}
    return json.loads(DATA_LATEST.read_text(encoding="utf-8"))


def _load_confluence_records() -> list[dict[str, Any]]:
    if not CONFLUENCE_PATH.exists():
        return []
    doc = json.loads(CONFLUENCE_PATH.read_text(encoding="utf-8"))
    return list(doc.get("records") or [])


def _confluence_row_for_date(records: list[dict[str, Any]], instrument_id: str, report_date: str) -> dict[str, Any] | None:
    hits = [
        r
        for r in records
        if r.get("market") == instrument_id and str(r.get("cot_report_date") or r.get("date") or "")[:10] == report_date
    ]
    if not hits:
        return None
    hits.sort(key=lambda r: str(r.get("date") or ""))
    return hits[-1]


def _confluence_group_values(row: dict[str, Any] | None, group_id: GroupId) -> tuple[float | None, float | None, float | None]:
    if not row:
        return None, None, None
    groups = row.get("cot_positioning_groups") or {}
    key = CONFLUENCE_GROUP_KEYS[group_id]
    block = groups.get(key) if isinstance(groups, dict) else None
    if block and block.get("available"):
        long_v = _num(block.get("long"))
        short_v = _num(block.get("short"))
        net_v = _num(block.get("net"))
        if net_v is None and long_v is not None and short_v is not None:
            net_v = long_v - short_v
        return long_v, short_v, net_v
    if group_id == "noncommercials":
        long_v = _num(row.get("long_value"))
        short_v = _num(row.get("short_value"))
        net_v = _num(row.get("net_value"))
        if net_v is None and long_v is not None and short_v is not None:
            net_v = long_v - short_v
        return long_v, short_v, net_v
    return None, None, None


def _build_group_proof(
    *,
    group_id: GroupId,
    legacy_week: dict[str, Any] | None,
    raw_row: dict[str, Any],
    confluence_row: dict[str, Any] | None,
) -> dict[str, Any]:
    raw_l, raw_s, raw_n = _raw_group_values(raw_row, group_id)
    leg_l, leg_s, leg_n = _legacy_dashboard_values(legacy_week)
    conf_l, conf_s, conf_n = _confluence_group_values(confluence_row, group_id)

    legacy_metrics = {
        "long": _compare_metric(leg_l, raw_l),
        "short": _compare_metric(leg_s, raw_s),
        "net": _compare_metric(leg_n, raw_n),
    }
    confluence_metrics = {
        "long": _compare_metric(conf_l, raw_l),
        "short": _compare_metric(conf_s, raw_s),
        "net": _compare_metric(conf_n, raw_n),
    }

    legacy_status = _group_status(legacy_metrics)
    confluence_status = _group_status(confluence_metrics) if confluence_row else "N/A"

    if legacy_status == "PASS" and (confluence_status in ("PASS", "N/A")):
        group_status = "PASS"
    elif confluence_status == "FAIL":
        group_status = "FAIL"
    else:
        group_status = legacy_status

    return {
        "group_id": group_id,
        "label": GROUP_LABELS[group_id],
        "status": group_status,
        "legacy_panel": legacy_metrics,
        "confluence_headline": confluence_metrics,
        "raw_cftc": {"long": raw_l, "short": raw_s, "net": raw_n},
    }


def _overall_status(
    mapping_status: str,
    groups: dict[str, Any],
    mismatch_reasons: list[str],
) -> OverallStatus:
    if mapping_status == "NEEDS_MANUAL_REVIEW":
        return "NEEDS_REVIEW"
    if mapping_status == "FAIL":
        return "FAIL"
    if mismatch_reasons:
        return "FAIL"
    if all(groups[g]["status"] == "PASS" for g in GROUP_LABELS):
        return "PASS"
    return "FAIL"


def build_cot_proof(*, year: int | None = None, download: bool = True) -> dict[str, Any]:
    """Build proof JSON for every COT-mapped instrument at latest Legacy report date."""
    year = year or datetime.now(timezone.utc).year
    legacy_doc = _load_legacy_latest()
    confluence_records = _load_confluence_records()
    df, meta = load_legacy_futures_only_dataframe(year, download=download)

    instruments: dict[str, Any] = {}
    mismatch_all: list[str] = []
    counts = {"PASS": 0, "FAIL": 0, "NEEDS_REVIEW": 0}
    latest_report_date: str | None = None

    for iid in cot_mapped_ids():
        inst_legacy = (legacy_doc.get("instruments") or {}).get(iid) or {}
        mapping_status = str(inst_legacy.get("mapping_status") or "FAIL")
        groups_legacy = inst_legacy.get("groups") or {}

        candidates = _candidate_rows_for_instrument(df, iid)
        if candidates.empty:
            instruments[iid] = {
                "instrument_id": iid,
                "overall_status": "FAIL",
                "mapping_status": mapping_status,
                "mismatch_reasons": ["no_raw_cftc_rows_for_instrument"],
                "groups": {},
            }
            counts["FAIL"] += 1
            mismatch_all.append(f"{iid}: no_raw_cftc_rows_for_instrument")
            continue

        latest_ts = candidates["_report_date"].max()
        report_date = pd.Timestamp(latest_ts).strftime("%Y-%m-%d")
        if latest_report_date is None or report_date > latest_report_date:
            latest_report_date = report_date

        week_df = candidates[candidates["_report_date"] == latest_ts]
        canonical = _select_canonical_row(week_df, iid)
        if canonical is None:
            instruments[iid] = {
                "instrument_id": iid,
                "overall_status": "FAIL",
                "mapping_status": mapping_status,
                "mismatch_reasons": ["no_canonical_raw_row"],
                "groups": {},
            }
            counts["FAIL"] += 1
            mismatch_all.append(f"{iid}: no_canonical_raw_row")
            continue

        raw_row = _extract_legacy_position_row(canonical, meta, int(canonical.name))
        confluence_row = _confluence_row_for_date(confluence_records, iid, report_date)

        group_results: dict[str, Any] = {}
        mismatch_reasons: list[str] = []

        for group_id in GROUP_LABELS:
            legacy_week = _latest_week(groups_legacy, group_id)
            if legacy_week and str(legacy_week.get("report_date") or "")[:10] != report_date:
                legacy_week = None
            proof = _build_group_proof(
                group_id=group_id,
                legacy_week=legacy_week,
                raw_row=raw_row,
                confluence_row=confluence_row,
            )
            group_results[group_id] = proof

            for metrics_key in ("legacy_panel", "confluence_headline"):
                metrics = proof[metrics_key]
                for field in ("long", "short", "net"):
                    m = metrics.get(field) or {}
                    if m.get("match") is False:
                        mismatch_reasons.append(
                            f"{iid} {GROUP_LABELS[group_id]} {field}: "
                            f"{metrics_key}={m.get('dashboard_value')} raw={m.get('raw_cftc_value')} "
                            f"diff={m.get('difference')}"
                        )

        overall = _overall_status(mapping_status, group_results, mismatch_reasons)
        counts[overall] += 1
        if overall == "FAIL":
            mismatch_all.extend(mismatch_reasons[:6])

        instruments[iid] = {
            "instrument_id": iid,
            "report_date": report_date,
            "cftc_code": raw_row.get("cftc_market_code") or inst_legacy.get("selected_cftc_code"),
            "market_name": raw_row.get("market_name") or inst_legacy.get("selected_market_name"),
            "report_type": raw_row.get("report_type") or "legacy_futures_only",
            "source_file": raw_row.get("raw_source_file"),
            "source_row": raw_row.get("raw_source_row"),
            "mapping_status": mapping_status,
            "overall_status": overall,
            "column_status": {
                "NC": group_results["noncommercials"]["status"],
                "Commercial": group_results["commercials"]["status"],
                "Non-Reportable": group_results["nonreportables"]["status"],
            },
            "groups": group_results,
            "mismatch_reasons": mismatch_reasons,
            "confluence_present": confluence_row is not None,
            "confluence_trader_group_used": (confluence_row or {}).get("trader_group_used"),
            "confluence_positioning_source": (confluence_row or {}).get("positioning_source"),
        }

    total = len(cot_mapped_ids())
    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": PARSER_NAME,
        "canonical_dashboard_source": "data/legacy_cot_latest.json",
        "raw_cftc_source": "CFTC Legacy Futures Only (deacot annual.txt)",
        "confluence_export_path": str(CONFLUENCE_PATH),
        "latest_report_date": latest_report_date,
        "summary": {
            "total_instruments_checked": total,
            "pass_count": counts["PASS"],
            "fail_count": counts["FAIL"],
            "needs_review_count": counts["NEEDS_REVIEW"],
            "all_pass": counts["FAIL"] == 0 and counts["NEEDS_REVIEW"] == 0,
            "failed_instruments": [iid for iid, row in instruments.items() if row.get("overall_status") == "FAIL"],
            "needs_review_instruments": [
                iid for iid, row in instruments.items() if row.get("overall_status") == "NEEDS_REVIEW"
            ],
            "mismatch_reasons_sample": mismatch_all[:50],
        },
        "gate": {
            "trusted": counts["FAIL"] == 0 and counts["NEEDS_REVIEW"] == 0,
            "message": (
                "All COT-supported instruments PASS — safe to resume valuation/seasonality/scoring."
                if counts["FAIL"] == 0 and counts["NEEDS_REVIEW"] == 0
                else "BLOCKED: fix FAIL/NEEDS_REVIEW on #/cot-proof before valuation, seasonality, thesis, or scanner scoring."
            ),
        },
        "instruments": instruments,
    }
    return payload


def write_cot_proof_exports(payload: dict[str, Any]) -> dict[str, Path]:
    for path in (DATA_PROOF, PUBLIC_PROOF):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"proof": DATA_PROOF, "public": PUBLIC_PROOF}
