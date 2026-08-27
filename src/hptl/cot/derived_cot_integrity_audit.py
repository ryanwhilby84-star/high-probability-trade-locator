"""Derived COT integrity audit — Weekly Inspector contract for LEGACY_COT_MARKETS.

Proves that raw COT → expanding percentiles → movements → extremes/temperature →
cross-group features → inspector payload is complete for every instrument and
the latest 13 report weeks (latest + prior 12).

No warnings. No skipped instruments. Any missing required field → FAIL.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.weekly_inspector_export import expand_compact_market
from hptl.markets.canonical_identity import BY_ID
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, load_registry

DATA = PROJECT_ROOT / "data"
PUBLIC = PROJECT_ROOT / "web-dashboard" / "public" / "data"
OUT_JSON = DATA / "audits" / "derived_cot_integrity_audit.json"
OUT_MD = DATA / "audits" / "derived_cot_integrity_audit.md"
PUBLIC_JSON = PUBLIC / "derived_cot_integrity_audit.json"

WI_PATHS = (
    PROCESSED_DIR / "cot_weekly_inspector_latest.json",
    PUBLIC / "cot_weekly_inspector_latest.json",
    DATA / "cot_weekly_inspector_latest.json",
)
COT3Y_PATHS = (
    PUBLIC / "cot_3y_series_latest.json",
    PROCESSED_DIR / "cot_3y_series_latest.json",
)

LOOKBACK_WEEKS = 13  # latest + prior 12

GROUP_FIELDS = (
    "net",
    "weekly_change",
    "four_week_change",
    "twelve_week_change",
    "percentile",
    "percentile_change_1w",
    "percentile_change_4w",
    "percentile_change_12w",
    "percentile_observation_count",
    "temperature",
    "state_label",
    "direction",
    "direction_arrow",
)

CROSS_FIELDS = (
    "commercial_percentile",
    "noncommercial_percentile",
    "nonreportable_percentile",
    "comm_nc_spread",
    "comm_nc_spread_change_1w",
    "comm_nc_spread_change_4w",
    "comm_nr_spread",
    "relationship",
    "flow",
)

UNAVAILABLE_TOKENS = {"", "unavailable", "unknown", "Unavailable", "None"}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _load_first(paths: tuple[Path, ...]) -> dict[str, Any]:
    for p in paths:
        doc = _read_json(p)
        if doc:
            return doc
    return {}


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _require_finite(pack: dict[str, Any], key: str, *, loc: str, missing: list[str]) -> None:
    v = _finite(pack.get(key))
    if v is None:
        missing.append(f"{loc}.{key}")


def _require_token(pack: dict[str, Any], key: str, *, loc: str, missing: list[str]) -> None:
    v = pack.get(key)
    if v is None or str(v) in UNAVAILABLE_TOKENS:
        missing.append(f"{loc}.{key}")


def audit_week(week: dict[str, Any], *, instrument_id: str) -> list[dict[str, Any]]:
    """Return structured field failures for one inspector week."""
    failures: list[dict[str, Any]] = []
    date = str(week.get("date") or "")[:10]
    for group in ("commercial", "noncommercial", "nonreportable"):
        pack = week.get(group) or {}
        missing: list[str] = []
        for key in GROUP_FIELDS:
            if key in (
                "net",
                "weekly_change",
                "four_week_change",
                "twelve_week_change",
                "percentile",
                "percentile_change_1w",
                "percentile_change_4w",
                "percentile_change_12w",
                "percentile_observation_count",
            ):
                _require_finite(pack, key, loc=group, missing=missing)
            else:
                _require_token(pack, key, loc=group, missing=missing)
        pct = _finite(pack.get("percentile"))
        if pct is not None and (pct < 0 or pct > 100):
            missing.append(f"{group}.percentile_out_of_range")
        obs = _finite(pack.get("percentile_observation_count"))
        if obs is not None and obs < 0:
            missing.append(f"{group}.percentile_observation_count_negative")
        if str(pack.get("state_label") or "") in ("Unavailable", "unavailable"):
            missing.append(f"{group}.state_label")
        if str(pack.get("temperature") or "") in ("unknown", "Unavailable"):
            missing.append(f"{group}.temperature")
        for field in missing:
            failures.append(
                {
                    "instrument": instrument_id,
                    "report_date": date,
                    "participant_group": group,
                    "field": field,
                    "pipeline_stage": "weekly_inspector_payload",
                    "suspected_cause": "required derived field missing or invalid",
                    "value": pack.get(field.split(".", 1)[-1]),
                }
            )

    cross = week.get("cross") or {}
    cross_missing: list[str] = []
    for key in CROSS_FIELDS:
        if key in (
            "commercial_percentile",
            "noncommercial_percentile",
            "nonreportable_percentile",
            "comm_nc_spread",
            "comm_nc_spread_change_1w",
            "comm_nc_spread_change_4w",
            "comm_nr_spread",
        ):
            _require_finite(cross, key, loc="cross_group", missing=cross_missing)
        else:
            _require_token(cross, key, loc="cross_group", missing=cross_missing)

    c_pct = _finite(cross.get("commercial_percentile"))
    nc_pct = _finite(cross.get("noncommercial_percentile"))
    spread = _finite(cross.get("comm_nc_spread"))
    if c_pct is not None and nc_pct is not None and spread is not None:
        expected = c_pct - nc_pct
        if abs(spread - expected) > 0.05:
            cross_missing.append("cross_group.comm_nc_spread_mismatch")

    for field in cross_missing:
        failures.append(
            {
                "instrument": instrument_id,
                "report_date": date,
                "participant_group": "cross_group",
                "field": field,
                "pipeline_stage": "weekly_inspector_payload",
                "suspected_cause": "required cross-group field missing or inconsistent",
                "value": cross.get(field.split(".", 1)[-1]),
            }
        )
    return failures


def audit_identity(instrument_id: str) -> list[str]:
    issues: list[str] = []
    canon = BY_ID.get(instrument_id)
    reg = load_registry().get(instrument_id)
    if not canon:
        issues.append("missing_canonical_identity")
    if not reg:
        issues.append("missing_registry_row")
    if canon and reg:
        if reg.id != canon.instrument_id:
            issues.append(f"registry_id_mismatch:{reg.id!r}")
        if str(reg.cot_market_code or "") != str(canon.cftc_market_code or ""):
            issues.append(
                f"cftc_code_mismatch:registry={reg.cot_market_code!r} canonical={canon.cftc_market_code!r}"
            )
    return issues


def audit_instrument(
    instrument_id: str,
    *,
    wi_doc: dict[str, Any],
    cot3y_doc: dict[str, Any],
) -> dict[str, Any]:
    identity_issues = audit_identity(instrument_id)
    block = (wi_doc.get("markets") or {}).get(instrument_id)
    cot_block = (cot3y_doc.get("markets") or {}).get(instrument_id) or {}

    if not block:
        return {
            "instrument": instrument_id,
            "status": "FAIL",
            "latest_cot_week": None,
            "raw_cot_row_present": False,
            "historical_window_length": 0,
            "commercial_completeness": "FAIL",
            "noncommercial_completeness": "FAIL",
            "nonreportable_completeness": "FAIL",
            "cross_group_completeness": "FAIL",
            "inspector_completeness": "FAIL",
            "first_failing_stage": "weekly_inspector_missing_market",
            "failures": [
                {
                    "instrument": instrument_id,
                    "report_date": None,
                    "participant_group": "all",
                    "field": "markets[instrument_id]",
                    "pipeline_stage": "weekly_inspector_payload",
                    "suspected_cause": "instrument absent from inspector export",
                }
            ],
            "identity_issues": identity_issues,
        }

    expanded = expand_compact_market(block) if block.get("rows") else block
    weeks = list(expanded.get("weeks") or [])
    window = weeks[-LOOKBACK_WEEKS:] if weeks else []
    latest = str((window[-1] or {}).get("date") or "")[:10] if window else None

    cot_series = cot_block.get("series") or []
    cot_latest = str(cot_block.get("latest_date") or "")[:10] or (
        str((cot_series[-1] or {}).get("date") or "")[:10] if cot_series else None
    )

    failures: list[dict[str, Any]] = []
    for issue in identity_issues:
        failures.append(
            {
                "instrument": instrument_id,
                "report_date": latest,
                "participant_group": "identity",
                "field": issue,
                "pipeline_stage": "canonical_identity",
                "suspected_cause": "identity / join-key mismatch",
            }
        )

    if not window:
        failures.append(
            {
                "instrument": instrument_id,
                "report_date": None,
                "participant_group": "all",
                "field": "weeks",
                "pipeline_stage": "weekly_inspector_payload",
                "suspected_cause": "no inspector weeks",
            }
        )
    elif len(window) < LOOKBACK_WEEKS:
        failures.append(
            {
                "instrument": instrument_id,
                "report_date": latest,
                "participant_group": "all",
                "field": "lookback_weeks",
                "pipeline_stage": "historical_window",
                "suspected_cause": (
                    f"only {len(window)} weeks available; need {LOOKBACK_WEEKS} "
                    "(latest + prior 12)"
                ),
            }
        )

    if latest and cot_latest and latest != cot_latest:
        failures.append(
            {
                "instrument": instrument_id,
                "report_date": latest,
                "participant_group": "alignment",
                "field": "latest_week_mismatch",
                "pipeline_stage": "cot3y_vs_inspector",
                "suspected_cause": f"inspector latest {latest} != cot3y latest {cot_latest}",
            }
        )

    for week in window:
        failures.extend(audit_week(week, instrument_id=instrument_id))

    # Completeness rollups
    def _group_ok(group: str) -> str:
        return (
            "PASS"
            if not any(f.get("participant_group") == group for f in failures)
            else "FAIL"
        )

    status = "FAIL" if failures else "PASS"
    first_stage = failures[0]["pipeline_stage"] if failures else None

    return {
        "instrument": instrument_id,
        "status": status,
        "latest_cot_week": latest,
        "raw_cot_row_present": bool(cot_series) or bool(weeks),
        "historical_window_length": len(weeks),
        "lookback_weeks_audited": len(window),
        "commercial_completeness": _group_ok("commercial"),
        "noncommercial_completeness": _group_ok("noncommercial"),
        "nonreportable_completeness": _group_ok("nonreportable"),
        "cross_group_completeness": _group_ok("cross_group"),
        "inspector_completeness": status,
        "first_failing_stage": first_stage,
        "identity": {
            "canonical_id": instrument_id,
            "cftc_market_code": (BY_ID[instrument_id].cftc_market_code if instrument_id in BY_ID else None),
            "registry_oanda": (
                load_registry().get(instrument_id).oanda_symbol
                if load_registry().get(instrument_id)
                else None
            ),
            "issues": identity_issues,
        },
        "failures": failures,
        "crude_oil_trace_note": (
            "Backend payload complete; frontend must as-of-join Friday price weeks "
            "to Tuesday COT inspector weeks (resolveInspectorWeekForDate)."
            if instrument_id == "Crude Oil / CL" and status == "PASS"
            else None
        ),
    }


def run_derived_cot_integrity_audit(
    *,
    weekly_inspector: dict[str, Any] | None = None,
    cot_3y: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wi = weekly_inspector if weekly_inspector is not None else _load_first(WI_PATHS)
    cot3y = cot_3y if cot_3y is not None else _load_first(COT3Y_PATHS)

    instruments = [
        audit_instrument(mid, wi_doc=wi, cot3y_doc=cot3y) for mid in LEGACY_COT_MARKETS
    ]
    passed = [r for r in instruments if r["status"] == "PASS"]
    failed = [r for r in instruments if r["status"] == "FAIL"]
    overall = "PASS" if len(failed) == 0 and len(instruments) == len(LEGACY_COT_MARKETS) else "FAIL"

    return {
        "version": "derived_cot_integrity_audit_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_weeks": LOOKBACK_WEEKS,
        "summary": {
            "markets_total": len(instruments),
            "pass_count": len(passed),
            "fail_count": len(failed),
            "overall_status": overall,
            "gate_open": overall == "PASS",
        },
        "failing_instruments": [r["instrument"] for r in failed],
        "instruments": instruments,
        "crude_oil_root_cause": {
            "backend_payload": "complete for latest 13 weeks when present",
            "frontend_failure_mode": (
                "Price timeline dates (OANDA Friday weeks) did not exact-match "
                "inspector COT dates (Tuesday). Without as-of join, percentiles "
                "rendered as Unavailable."
            ),
            "fix": "resolveInspectorWeekForDate + integrity banner in WeeklyInspector",
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    lines = [
        "# Derived COT Integrity Audit",
        "",
        f"Generated: `{report.get('generated_at')}`",
        f"Lookback: latest + prior {int(report.get('lookback_weeks') or LOOKBACK_WEEKS) - 1} weeks "
        f"({report.get('lookback_weeks')} total)",
        "",
        "## Summary",
        "",
        f"- Total instruments: **{s.get('markets_total')}**",
        f"- PASS: **{s.get('pass_count')}**",
        f"- FAIL: **{s.get('fail_count')}**",
        f"- Gate open: **{s.get('gate_open')}**",
        "",
        "## Crude Oil trace note",
        "",
        f"- {(report.get('crude_oil_root_cause') or {}).get('backend_payload')}",
        f"- Frontend failure mode: {(report.get('crude_oil_root_cause') or {}).get('frontend_failure_mode')}",
        f"- Fix: {(report.get('crude_oil_root_cause') or {}).get('fix')}",
        "",
        "## Per instrument",
        "",
    ]
    for row in report.get("instruments") or []:
        lines.extend(
            [
                f"### {row.get('instrument')} — **{row.get('status')}**",
                "",
                f"- Latest COT week: `{row.get('latest_cot_week')}`",
                f"- Raw COT row present: `{row.get('raw_cot_row_present')}`",
                f"- Historical window length: `{row.get('historical_window_length')}`",
                f"- Lookback weeks audited: `{row.get('lookback_weeks_audited')}`",
                f"- Commercial completeness: `{row.get('commercial_completeness')}`",
                f"- Non-commercial completeness: `{row.get('noncommercial_completeness')}`",
                f"- Non-reportable completeness: `{row.get('nonreportable_completeness')}`",
                f"- Cross-group completeness: `{row.get('cross_group_completeness')}`",
                f"- Inspector completeness: `{row.get('inspector_completeness')}`",
                f"- First failing stage: `{row.get('first_failing_stage')}`",
            ]
        )
        for fail in (row.get("failures") or [])[:12]:
            lines.append(
                f"- FAIL `{fail.get('report_date')}` {fail.get('participant_group')} "
                f"{fail.get('field')} @ {fail.get('pipeline_stage')}: {fail.get('suspected_cause')}"
            )
        if len(row.get("failures") or []) > 12:
            lines.append(f"- … {len(row['failures']) - 12} more failures")
        lines.append("")

    lines.extend(
        [
            "## OVERALL STATUS",
            "",
            f"PASS: {s.get('pass_count')}",
            f"FAIL: {s.get('fail_count')}",
            f"OVERALL STATUS: {s.get('overall_status')}",
            "",
        ]
    )
    return "\n".join(lines)


def write_derived_cot_integrity_audit(report: dict[str, Any] | None = None) -> dict[str, Any]:
    report = report or run_derived_cot_integrity_audit()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md = render_markdown(report)
    OUT_MD.write_text(md, encoding="utf-8")
    (DATA / "derived_cot_integrity_audit.md").write_text(md, encoding="utf-8")
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def run_derived_cot_integrity_gate(
    *,
    weekly_inspector: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = write_derived_cot_integrity_audit(
        run_derived_cot_integrity_audit(weekly_inspector=weekly_inspector)
    )
    summary = report.get("summary") or {}
    return {
        "passed": bool(summary.get("gate_open")),
        "overall_status": summary.get("overall_status"),
        "pass_count": summary.get("pass_count"),
        "fail_count": summary.get("fail_count"),
        "failing_instruments": list(report.get("failing_instruments") or []),
        "report_md": str(OUT_MD),
        "report_json": str(OUT_JSON),
    }
