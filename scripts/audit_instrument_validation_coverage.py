#!/usr/bin/env python3
"""Audit validation coverage for every TARGET_MARKETS instrument.

Produces:
  data/audits/instrument_validation_coverage.json
  data/audits/instrument_validation_coverage.md

Optional --repair: safe export/confluence backfill (no invented data).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

DATA = ROOT / "data"
PUBLIC = ROOT / "web-dashboard" / "public" / "data"
OUT_JSON = DATA / "audits" / "instrument_validation_coverage.json"
OUT_MD = DATA / "audits" / "instrument_validation_coverage.md"
PUBLIC_JSON = PUBLIC / "instrument_validation_coverage.json"

CLASSIFICATIONS = (
    "ready",
    "parser-repairable",
    "mapping-required",
    "source-unavailable",
    "intentionally-unsupported",
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _status_label(kind: str, *, wired: bool | None = None, reason: str | None = None, missing: bool = False) -> str:
    if missing:
        return f"MISSING:{kind} export row absent"
    if wired is True:
        return f"WIRED:{kind}"
    if reason:
        short = reason.replace("\n", " ").strip()[:120]
        return f"UNAVAILABLE:{short}"
    if wired is False:
        return f"UNAVAILABLE:{kind} not wired"
    return f"UNKNOWN:{kind}"


def _load_confluence_latest() -> tuple[str, dict[str, dict[str, Any]]]:
    path = PUBLIC / "confluence_history_latest.json"
    if not path.exists():
        return "", {}
    doc = _read_json(path)
    records = doc.get("records") or []
    if not records:
        return "", {}
    latest = max(str(r.get("date") or "") for r in records if r.get("date"))
    by_market = {
        str(r.get("market") or ""): r
        for r in records
        if str(r.get("date") or "") == latest and r.get("market")
    }
    return latest, by_market


def _cot_status(market: str, cov_row: dict[str, Any] | None, conf_row: dict[str, Any] | None) -> tuple[str, list[str]]:
    missing: list[str] = []
    spec_status = (cov_row or {}).get("data_status")
    has_cot = (cov_row or {}).get("has_cot_mapping")
    cot_resolved = (cov_row or {}).get("cot_resolved_latest_week")
    proxy_of = (cov_row or {}).get("cot_proxy_of")

    if conf_row:
        label = str(conf_row.get("cot_status_label") or "").strip()
        bias = str(conf_row.get("cot_bias") or "").strip().upper()
        if label:
            if bias and bias != "N/A" and cot_resolved:
                return f"WIRED:COT ({label})", missing
            if has_cot and cot_resolved:
                return f"WIRED:COT ({label})", missing
            if proxy_of:
                return f"PROXY:{proxy_of} ({label})", missing
            return f"UNAVAILABLE:{label}", missing

    if cov_row:
        if cot_resolved:
            return "WIRED:COT (coverage audit)", missing
        if has_cot and spec_status == "cot_mapping_missing":
            missing.append("cot_mapping")
            return "MISSING:COT mapping", missing
        if not has_cot:
            return "UNAVAILABLE:No direct pair COT", missing
        return f"UNAVAILABLE:{spec_status or 'COT not resolved'}", missing

    missing.append("cot_coverage")
    return "MISSING:COT coverage audit", missing


def _pillar_status(
    pillar: str,
    export_row: dict[str, Any] | None,
    conf_row: dict[str, Any] | None,
    *,
    wired_key: str,
    bias_key: str,
    reason_key: str,
) -> tuple[str, list[str]]:
    missing: list[str] = []
    if export_row is None:
        missing.append(f"{pillar}_export")
        return f"MISSING:{pillar} export row absent", missing

    wired = export_row.get("wired")
    reason = export_row.get(reason_key) or export_row.get(f"{pillar}_reason")
    bias = export_row.get(bias_key)

    if conf_row:
        conf_wired = conf_row.get(wired_key)
        conf_reason = conf_row.get(reason_key)
        conf_bias = conf_row.get(bias_key)
        if conf_wired is True and conf_bias and str(conf_bias).upper() != "UNAVAILABLE":
            return _status_label(pillar, wired=True), missing
        if conf_reason:
            return _status_label(pillar, wired=False, reason=str(conf_reason)), missing
        if conf_bias and str(conf_bias).upper() == "UNAVAILABLE":
            return _status_label(pillar, wired=False, reason=str(conf_reason or f"{pillar} unavailable")), missing

    if wired is True and bias and str(bias).upper() != "UNAVAILABLE":
        return _status_label(pillar, wired=True), missing
    return _status_label(pillar, wired=bool(wired), reason=str(reason) if reason else None), missing


def _price_status(market: str, integrity: Any) -> tuple[str, list[str]]:
    missing: list[str] = []
    if integrity.status == "PASS":
        src = integrity.actual_source or integrity.expected_source or "native"
        return f"WIRED:{src} ({integrity.daily_bars}d/{integrity.weekly_bars}w)", missing
    reasons = integrity.reasons or ["integrity FAIL"]
    if "no supported native price source" in reasons[0]:
        return f"UNAVAILABLE:{'; '.join(reasons[:2])}", missing
    return f"FAIL:{'; '.join(reasons[:2])}", missing


def _dashboard_status(conf_row: dict[str, Any] | None) -> tuple[str, list[str]]:
    missing: list[str] = []
    if conf_row is None:
        missing.append("confluence_row")
        return "FAIL:missing confluence row (latest week)", missing

    blank_checks = [
        ("cot_status_label", conf_row.get("cot_status_label")),
        ("data_integrity", conf_row.get("data_integrity")),
    ]
    blanks = [k for k, v in blank_checks if v is None or v == ""]
    if blanks:
        return f"FAIL:blank fields ({', '.join(blanks)})", missing

    integrity = str(conf_row.get("data_integrity") or "")
    has_pillar_reason = any(
        conf_row.get(k)
        for k in (
            "location_reason",
            "valuation_reason",
            "seasonality_reason",
            "data_integrity_reasons",
        )
    )
    if integrity == "FAIL" and has_pillar_reason:
        return "PASS:explicit unavailable (integrity gated)", missing
    if integrity == "PASS":
        return "PASS:loaded", missing
    return f"PASS:loaded ({integrity or 'unknown integrity'})", missing


def _classify(
    *,
    asset_class: str,
    positioning_status: str | None,
    missing_fields: list[str],
    cot_status: str,
    price_status: str,
    dashboard_status: str,
    has_cot_mapping: bool,
) -> str:
    if dashboard_status.startswith("FAIL"):
        if "confluence_row" in missing_fields or any("_export" in m for m in missing_fields):
            return "parser-repairable"
        return "mapping-required"

    if asset_class in {"macro", "bonds"} and not has_cot_mapping:
        return "intentionally-unsupported"

    if positioning_status == "no_direct_pair_cot" and not has_cot_mapping:
        if price_status.startswith("UNAVAILABLE") and cot_status.startswith("UNAVAILABLE"):
            return "intentionally-unsupported"

    if any("_export" in m for m in missing_fields):
        return "parser-repairable"
    if "cot_mapping" in missing_fields:
        return "mapping-required"
    if price_status.startswith("FAIL") or price_status.startswith("UNAVAILABLE"):
        if "no supported native price source" in price_status:
            return "source-unavailable"
        return "mapping-required"

    if dashboard_status.startswith("PASS"):
        return "ready"
    return "source-unavailable"


def _repair_action(classification: str, missing_fields: list[str], row: dict[str, Any]) -> str:
    if classification == "ready":
        return "none"
    if classification == "parser-repairable":
        actions = []
        if "confluence_row" in missing_fields:
            actions.append("backfill confluence no-COT records")
        if any("seasonality_export" in m for m in missing_fields):
            actions.append("rebuild seasonality_latest.json")
        if any("valuation_export" in m for m in missing_fields):
            actions.append("rebuild valuation_latest.json")
        if any("location_export" in m for m in missing_fields):
            actions.append("rebuild location_latest.json")
        return "; ".join(actions) or "rebuild stale pillar exports"
    if classification == "mapping-required":
        return "add registry/price/COT mapping — do not invent data"
    if classification == "source-unavailable":
        return "document UNAVAILABLE; no native price/COT source"
    if classification == "intentionally-unsupported":
        return "keep explicit UNAVAILABLE (FX cross / macro input / no COT by design)"
    return "review manually"


def audit_validation_coverage() -> dict[str, Any]:
    from hptl.markets.instrument_registry import TARGET_MARKETS, get_instrument
    from hptl.prices.data_integrity import integrity_status_for

    cov_doc = _read_json(DATA / "instrument_coverage_audit.json")
    cov_by_id = {r["instrument_id"]: r for r in cov_doc.get("instruments") or [] if r.get("instrument_id")}

    sea_doc = _read_json(DATA / "seasonality_latest.json").get("instruments") or {}
    val_doc = _read_json(DATA / "valuation_latest.json").get("instruments") or {}
    loc_doc = _read_json(DATA / "location_latest.json").get("instruments") or {}

    latest_week, conf_by_market = _load_confluence_latest()

    rows: list[dict[str, Any]] = []
    for market in TARGET_MARKETS:
        spec = get_instrument(market)
        asset_class = spec.asset_class if spec else "other"
        symbol = spec.oanda_symbol or spec.cot_market_code if spec else None

        integrity = integrity_status_for(market)
        cov_row = cov_by_id.get(market)

        cot_st, cot_miss = _cot_status(market, cov_row, conf_by_market.get(market))
        sea_st, sea_miss = _pillar_status(
            "seasonality",
            sea_doc.get(market),
            conf_by_market.get(market),
            wired_key="seasonality_wired",
            bias_key="seasonality_bias",
            reason_key="seasonality_reason",
        )
        if market not in sea_doc:
            sea_st, sea_miss = f"MISSING:seasonality export row absent", ["seasonality_export"]

        val_st, val_miss = _pillar_status(
            "valuation",
            val_doc.get(market),
            conf_by_market.get(market),
            wired_key="valuation_wired",
            bias_key="valuation_bias",
            reason_key="valuation_reason",
        )
        if market not in val_doc:
            val_st, val_miss = f"MISSING:valuation export row absent", ["valuation_export"]

        loc_st, loc_miss = _pillar_status(
            "location",
            loc_doc.get(market),
            conf_by_market.get(market),
            wired_key="location_wired",
            bias_key="location_bias",
            reason_key="location_reason",
        )
        if market not in loc_doc:
            loc_st, loc_miss = f"MISSING:location export row absent", ["location_export"]

        price_st, price_miss = _price_status(market, integrity)
        dash_st, dash_miss = _dashboard_status(conf_by_market.get(market))

        missing_fields = sorted(set(cot_miss + sea_miss + val_miss + loc_miss + price_miss + dash_miss))
        positioning_status = (spec.positioning_status if spec else None) or (cov_row or {}).get("positioning_status")
        classification = _classify(
            asset_class=asset_class,
            positioning_status=positioning_status,
            missing_fields=missing_fields,
            cot_status=cot_st,
            price_status=price_st,
            dashboard_status=dash_st,
            has_cot_mapping=bool(spec and spec.has_cot_mapping),
        )
        repair_action = _repair_action(classification, missing_fields, {})

        rows.append(
            {
                "instrument_id": market,
                "asset_class": asset_class,
                "symbol": symbol,
                "cot_status": cot_st,
                "seasonality_status": sea_st,
                "valuation_status": val_st,
                "price_status": price_st,
                "location_status": loc_st,
                "dashboard_status": dash_st,
                "missing_fields": missing_fields,
                "classification": classification,
                "repair_action": repair_action,
            }
        )

    def _passes(row: dict[str, Any]) -> bool:
        return str(row.get("dashboard_status") or "").startswith("PASS")

    summary = {
        "total": len(rows),
        "pass": sum(1 for r in rows if _passes(r)),
        "fail": sum(1 for r in rows if not _passes(r)),
        "by_classification": dict(Counter(r["classification"] for r in rows)),
        "latest_confluence_week": latest_week,
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "scripts.audit_instrument_validation_coverage",
        "summary": summary,
        "instruments": rows,
    }


def _write_outputs(payload: dict[str, Any]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    OUT_JSON.write_text(text, encoding="utf-8")
    PUBLIC.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(text, encoding="utf-8")

    s = payload["summary"]
    lines = [
        "# Instrument validation coverage",
        "",
        f"Generated: {payload['generated_at']}",
        "",
    ]
    if payload.get("before_summary"):
        b = payload["before_summary"]
        lines.extend(
            [
                "## Repair report",
                "",
                f"| Phase | Dashboard PASS |",
                f"|-------|---------------:|",
                f"| Before repair | {b.get('pass', '—')}/{b.get('total', '—')} |",
                f"| After repair | {s['pass']}/{s['total']} |",
                "",
                "### Actions taken",
                "",
            ]
        )
        for action in payload.get("repair_actions") or []:
            lines.append(f"- {action}")
        repaired = payload.get("instruments_repaired") or []
        if repaired:
            lines.append(f"- Instruments backfilled in confluence: {', '.join(repaired)}")
        still_failing = [
            r for r in payload.get("instruments") or [] if not str(r.get("dashboard_status", "")).startswith("PASS")
        ]
        lines.extend(["", "### Still failing", ""])
        if still_failing:
            for r in still_failing:
                lines.append(
                    f"- **{r['instrument_id']}**: {r['dashboard_status']} — {', '.join(r.get('missing_fields') or []) or r.get('repair_action')}"
                )
        else:
            lines.append("- None — all instruments have explicit dashboard validation status.")
        lines.append("")

    lines.extend(
        [
            "## Summary",
            "",
            f"| Metric | Count |",
            f"|--------|------:|",
            f"| Total instruments | {s['total']} |",
            f"| Dashboard PASS | {s['pass']} |",
            f"| Dashboard FAIL | {s['fail']} |",
            f"| Latest confluence week | {s.get('latest_confluence_week') or '—'} |",
            "",
            "### By classification",
            "",
        ]
    )
    for k, v in sorted((s.get("by_classification") or {}).items()):
        lines.append(f"- **{k}**: {v}")
    lines.extend(["", "## Coverage matrix", ""])
    lines.append(
        "| instrument_id | asset_class | classification | dashboard | cot | seasonality | valuation | price | location | repair_action |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for r in payload["instruments"]:
        def _short(st: str) -> str:
            return (st or "")[:48].replace("|", "/")

        lines.append(
            f"| {r['instrument_id']} | {r['asset_class']} | {r['classification']} | {_short(r['dashboard_status'])} "
            f"| {_short(r['cot_status'])} | {_short(r['seasonality_status'])} | {_short(r['valuation_status'])} "
            f"| {_short(r['price_status'])} | {_short(r['location_status'])} | {_short(r['repair_action'])} |"
        )

    failing = [r for r in payload["instruments"] if not str(r.get("dashboard_status", "")).startswith("PASS")]
    if failing:
        lines.extend(["", "## Remaining failures", ""])
        for r in failing:
            lines.append(f"### {r['instrument_id']}")
            lines.append(f"- **Classification**: {r['classification']}")
            lines.append(f"- **Dashboard**: {r['dashboard_status']}")
            lines.append(f"- **Missing**: {', '.join(r['missing_fields']) or '—'}")
            lines.append(f"- **Repair**: {r['repair_action']}")
            lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def run_repairs() -> dict[str, Any]:
    """Safe repairs only — stale exports and missing confluence rows."""
    actions: list[str] = []

    from hptl.seasonality.export import write_seasonality_exports

    write_seasonality_exports()
    actions.append("rebuilt seasonality_latest.json (data + public)")

    from hptl.confluence.repair_missing_markets import repair_confluence_missing_markets

    conf_result = repair_confluence_missing_markets()
    if conf_result.get("records_added"):
        actions.append(
            f"backfilled confluence for {conf_result.get('repaired')} "
            f"({conf_result.get('records_added')} rows across {conf_result.get('weeks')} weeks)"
        )
    else:
        actions.append("confluence: no missing markets to backfill")

    from hptl.confluence.repair_missing_markets import refresh_latest_confluence_validation_fields

    refresh_result = refresh_latest_confluence_validation_fields()
    actions.append(
        f"refreshed validation fields on {refresh_result.get('updated')} latest-week confluence rows"
    )

    return {"actions": actions, "confluence": conf_result, "refresh": refresh_result}


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit instrument validation coverage")
    ap.add_argument("--repair", action="store_true", help="Run safe export/confluence repairs then re-audit")
    args = ap.parse_args()

    before = audit_validation_coverage()
    repair_report: dict[str, Any] | None = None
    after = before

    if args.repair:
        repair_report = run_repairs()
        after = audit_validation_coverage()
        after["repair"] = repair_report
        after["before_summary"] = before["summary"]
        after["after_summary"] = after["summary"]

    payload = after if args.repair else before
    if args.repair:
        payload["before_summary"] = before["summary"]
        payload["after_summary"] = after["summary"]
        payload["instruments_repaired"] = repair_report.get("confluence", {}).get("repaired") if repair_report else []
        payload["repair_actions"] = repair_report.get("actions") if repair_report else []

    _write_outputs(payload)

    s = payload["summary"]
    print(f"Validation coverage: {s['pass']}/{s['total']} dashboard PASS")
    if args.repair:
        b = before["summary"]
        print(f"Before repair: {b['pass']}/{b['total']} PASS")
        print(f"After repair:  {s['pass']}/{s['total']} PASS")
        if repair_report:
            for a in repair_report.get("actions") or []:
                print(f"  - {a}")
    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_MD}")
    print(f"Wrote {PUBLIC_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
