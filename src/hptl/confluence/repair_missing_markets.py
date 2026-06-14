"""Backfill confluence records for TARGET_MARKETS missing from the export."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.confluence.dashboard_export import OUT_PATH, write_dashboard_exports
from hptl.fx.fx_valuation_attach import fx_valuation_fields_for_market
from hptl.markets.instrument_registry import export_registry_json, instrument_meta_for_record, load_registry
from hptl.pillars.confluence_attach import pillar_fields_for_market_week

CONFLUENCE_PATH = OUT_PATH


def _minimal_no_cot_record(market: str, date_str: str) -> dict[str, Any]:
    inst_meta = instrument_meta_for_record(market)
    positioning_status = inst_meta.get("positioning_status") or "no_direct_pair_cot"
    cot_status_label = "No direct pair COT"
    if positioning_status == "proxy_required" and inst_meta.get("cot_proxy_of"):
        cot_status_label = f"Proxy required (see {inst_meta['cot_proxy_of']})"

    record: dict[str, Any] = {
        "date": date_str,
        "market": market,
        "latest_report_date": "N/A",
        "cot_bias": "N/A",
        "cot_score": "N/A",
        "cot_reason": f"N/A: no direct COT row for {market} on {date_str}.",
        "missing_reason": f"no mapped raw COT row for {market} on {date_str}",
        "positioning_status": positioning_status,
        "cot_status_label": cot_status_label,
        "instrument_meta": inst_meta,
        "macro_regime": "N/A",
        "macro_score": "N/A",
        "final_context": "N/A",
        "positioning_state": "N/A",
        "technical_action_note": cot_status_label,
        "final_context_reason": "Cannot score positioning without COT — macro layer only.",
        **fx_valuation_fields_for_market(market),
    }
    record.update(pillar_fields_for_market_week(market, date_str))
    meta = instrument_meta_for_record(market, record)
    record["instrument_meta"] = meta
    return record


def missing_markets_from_confluence(payload: dict[str, Any]) -> list[str]:
    records = payload.get("records") or []
    if not records:
        return list(TARGET_MARKETS)
    latest_date = max(str(r.get("date") or "") for r in records if r.get("date"))
    present = {
        str(r.get("market") or "")
        for r in records
        if str(r.get("date") or "") == latest_date and r.get("market")
    }
    return sorted(m for m in TARGET_MARKETS if m not in present)


def refresh_latest_confluence_validation_fields(
    *,
    confluence_path: Path | None = None,
    markets: list[str] | None = None,
) -> dict[str, Any]:
    """Re-attach pillar + data_integrity fields on latest-week rows (explicit status, no invented data)."""
    from hptl.prices.data_integrity import integrity_status_for

    path = confluence_path or CONFLUENCE_PATH
    if not path.exists():
        return {"updated": 0, "error": f"missing {path}"}

    payload = json.loads(path.read_text(encoding="utf-8"))
    records: list[dict[str, Any]] = list(payload.get("records") or [])
    if not records:
        return {"updated": 0}

    latest_date = max(str(r.get("date") or "") for r in records if r.get("date"))
    target = set(markets or TARGET_MARKETS)
    updated = 0
    for rec in records:
        if str(rec.get("date") or "") != latest_date:
            continue
        market = str(rec.get("market") or "")
        if market not in target:
            continue
        integrity = integrity_status_for(market)
        rec["data_integrity"] = integrity.status
        if integrity.reasons:
            rec["data_integrity_reasons"] = integrity.reasons
        rec.update(pillar_fields_for_market_week(market, latest_date))
        meta = instrument_meta_for_record(market, rec)
        rec["instrument_meta"] = meta
        updated += 1

    payload["records"] = records
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    write_dashboard_exports(payload)
    return {"updated": updated, "latest_week": latest_date}


def repair_confluence_missing_markets(
    *,
    confluence_path: Path | None = None,
    markets: list[str] | None = None,
) -> dict[str, Any]:
    """Append no-COT records for missing markets across all confluence weeks."""
    path = confluence_path or CONFLUENCE_PATH
    if not path.exists():
        return {"repaired": [], "records_added": 0, "error": f"missing {path}"}

    payload = json.loads(path.read_text(encoding="utf-8"))
    records: list[dict[str, Any]] = list(payload.get("records") or [])
    to_repair = markets or missing_markets_from_confluence(payload)
    if not to_repair:
        return {"repaired": [], "records_added": 0}

    existing = {(str(r.get("date") or ""), str(r.get("market") or "")) for r in records}
    all_dates = sorted({str(r.get("date") or "") for r in records if r.get("date")})
    added = 0
    for market in to_repair:
        for date_str in all_dates:
            key = (date_str, market)
            if key in existing:
                continue
            records.append(_minimal_no_cot_record(market, date_str))
            existing.add(key)
            added += 1

    payload["records"] = records
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    reg = load_registry()
    payload["instrument_registry"] = {
        "version": 1,
        "generated_from": "hptl.markets.instrument_registry",
        "markets": [reg[k].to_dict() for k in TARGET_MARKETS if k in reg],
        "legacy_cot_markets": payload.get("instrument_registry", {}).get("legacy_cot_markets") or [],
        "total": len(TARGET_MARKETS),
    }
    write_dashboard_exports(payload)
    export_registry_json()
    return {"repaired": to_repair, "records_added": added, "weeks": len(all_dates)}
