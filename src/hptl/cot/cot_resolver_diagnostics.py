"""COT market resolver diagnostics — bundle load, mapping, and scanner resolution status."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.cot.cot_quarantine import load_quarantine_doc
from hptl.cot.legacy_cot_loader import legacy_cot_latest_path, load_legacy_cot_document, scoring_eligible_markets
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, TFF_MACRO_MARKETS

DATA_OUT = Path("data/cot_resolver_diagnostics_latest.json")
PUBLIC_OUT = Path("web-dashboard/public/data/cot_resolver_diagnostics_latest.json")


def _resolved_from_record(rec: dict[str, Any]) -> bool:
    if not rec:
        return False
    bias = str(rec.get("cot_bias") or "").strip().upper()
    if not bias or bias == "N/A":
        return False
    reason = str(rec.get("missing_reason") or "")
    if "no mapped raw COT" in reason or "no master row" in reason:
        return False
    flow = str(rec.get("institutional_flow_summary") or "")
    if "N/A: no COT row" in flow:
        return False
    if rec.get("long_value") is not None and rec.get("net_value") is not None:
        return True
    return bias not in ("", "N/A")


def build_cot_resolver_diagnostics(
    *,
    cot: pd.DataFrame | None,
    records: list[dict[str, Any]] | None,
    latest_cot_report_date: str | None = None,
) -> dict[str, Any]:
    """Summarize COT bundle coverage and per-market resolution for the scanner."""
    cot = cot if cot is not None else pd.DataFrame()
    records = records or []

    legacy_doc = load_legacy_cot_document()
    eligible = set(scoring_eligible_markets(legacy_doc))
    quarantine = set(load_quarantine_doc().get("quarantined_instruments") or [])

    bundle_date = None
    raw_rows_loaded = 0
    markets_in_cot: set[str] = set()
    if not cot.empty and "cot_report_date" in cot.columns:
        raw_rows_loaded = len(cot)
        markets_in_cot = set(cot["market"].dropna().astype(str).unique())
        latest_ts = cot["cot_report_date"].max()
        if pd.notna(latest_ts):
            bundle_date = pd.Timestamp(latest_ts).strftime("%Y-%m-%d")

    bundle_date = bundle_date or (str(latest_cot_report_date or "")[:10] or None)

    latest_by_market: dict[str, dict[str, Any]] = {}
    for rec in records:
        market = str(rec.get("market") or "")
        if not market:
            continue
        d = str(rec.get("cot_report_date") or rec.get("date") or "")[:10]
        if not d:
            continue
        prev = latest_by_market.get(market)
        if prev is None or d >= str(prev.get("cot_report_date") or "")[:10]:
            latest_by_market[market] = rec

    direct_cot = [m for m in TARGET_MARKETS if m in LEGACY_COT_MARKETS or m in eligible]
    resolved: list[str] = []
    unresolved: list[dict[str, Any]] = []

    for market in direct_cot:
        rec = latest_by_market.get(market)
        in_cot = market in markets_in_cot
        in_legacy = market in (legacy_doc.get("instruments") or {})
        legacy_inst = (legacy_doc.get("instruments") or {}).get(market) or {}
        legacy_weeks = (
            (legacy_inst.get("groups") or {}).get("noncommercials", {}).get("weeks") or []
        )
        legacy_latest = str(legacy_weeks[-1].get("report_date") or "")[:10] if legacy_weeks else None

        if _resolved_from_record(rec):
            resolved.append(market)
            continue

        reason_parts: list[str] = []
        if market in quarantine:
            reason_parts.append("integrity_gate_quarantine")
        if not in_legacy:
            reason_parts.append("missing_from_legacy_cot_bundle")
        elif legacy_inst.get("mapping_status") != "PASS":
            reason_parts.append(f"legacy_mapping_{legacy_inst.get('mapping_status') or 'unknown'}")
        elif not legacy_weeks:
            reason_parts.append("legacy_bundle_empty_weeks")
        if not in_cot:
            reason_parts.append("absent_from_cot_master_frame")
        if rec is None:
            reason_parts.append("no_confluence_export_row")
        elif not _resolved_from_record(rec):
            mr = rec.get("missing_reason")
            if mr:
                reason_parts.append(str(mr))
            else:
                reason_parts.append("confluence_row_unresolved")
        if market in TFF_MACRO_MARKETS:
            reason_parts.append("tff_overlay_available_in_dashboard")

        unresolved.append(
            {
                "market": market,
                "reason": "; ".join(reason_parts) if reason_parts else "unknown",
                "legacy_latest_report_date": legacy_latest,
                "in_cot_master": in_cot,
                "in_confluence_export": rec is not None,
                "quarantined": market in quarantine,
            }
        )

    sample_unresolved = unresolved[:12]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_bundle_date": bundle_date,
        "legacy_cot_source": str(legacy_cot_latest_path()),
        "raw_cot_rows_loaded": raw_rows_loaded,
        "cot_master_markets": sorted(markets_in_cot),
        "direct_cot_market_count": len(direct_cot),
        "resolved_market_count": len(resolved),
        "unresolved_market_count": len(unresolved),
        "quarantined_count": len([m for m in direct_cot if m in quarantine]),
        "resolved_markets": sorted(resolved),
        "unresolved_markets": unresolved,
        "sample_unresolved": sample_unresolved,
        "confluence_record_count": len(records),
        "confluence_latest_cot_report_date": latest_cot_report_date,
    }


def write_cot_resolver_diagnostics(doc: dict[str, Any]) -> dict[str, Path]:
    text = json.dumps(doc, indent=2, ensure_ascii=False)
    for path in (DATA_OUT, PUBLIC_OUT):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return {"data": DATA_OUT, "public": PUBLIC_OUT}
