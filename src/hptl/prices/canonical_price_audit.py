"""Audit that all HPTL price consumers trace to the canonical timeline."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.markets.instrument_registry import all_instrument_ids
from hptl.prices.canonical_timeline import (
    COT_MATCH_METHOD,
    DERIVED_WEEKLY_ISO,
    build_canonical_timeline,
    load_canonical_timeline,
)

COT_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json"
SEA_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "seasonality_price_latest.json"
VAL_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "valuation_latest.json"
OUT_JSON = PROCESSED_DIR / "canonical_price_consumer_audit.json"
OUT_MD = PROCESSED_DIR / "canonical_price_consumer_audit.md"

ACCEPTANCE_INSTRUMENTS = [
    "Gold",
    "US Dollar Index / DX",
    "NZ Dollar / 6N",
    "Copper / HG",
    "Natural Gas / NG",
]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def audit_instrument(instrument_id: str) -> dict[str, Any]:
    tl = build_canonical_timeline(instrument_id)
    cot_doc = _load_json(COT_PATH)
    sea_doc = _load_json(SEA_PATH)
    val_doc = _load_json(VAL_PATH)

    cot_blk = (cot_doc.get("markets") or {}).get(instrument_id) or {}
    sea_blk = (sea_doc.get("markets") or {}).get(instrument_id) or {}
    val_blk = (val_doc.get("instruments") or {}).get(instrument_id) or {}

    if not tl:
        return {
            "instrument": instrument_id,
            "canonical_source": None,
            "canonical_symbol": None,
            "date_range": None,
            "bars": 0,
            "proxy": None,
            "used_by_cot": False,
            "used_by_seasonality": False,
            "used_by_valuation": bool(val_blk.get("wired")),
            "status": "FAIL",
            "reason": "no_canonical_timeline",
        }

    summary = tl.to_summary()
    cot_audit = cot_blk.get("price_audit") or {}
    cot_store = cot_audit.get("price_store_key")
    sea_store = sea_blk.get("price_store_key")
    sea_method = sea_blk.get("bar_source") or sea_blk.get("price_derivation")

    cot_ok = (
        bool(cot_blk.get("has_price"))
        and cot_store == tl.resolved_store_key
        and cot_audit.get("canonical_source") == tl.canonical_source
    )
    sea_ok = (
        sea_blk.get("available")
        and sea_store == tl.resolved_store_key
        and sea_blk.get("canonical_source") == tl.canonical_source
        and (sea_blk.get("price_derivation") or sea_method) == DERIVED_WEEKLY_ISO
    )
    val_ok = not val_blk.get("wired") or val_blk.get("canonical_source") == tl.canonical_source

    status = "PASS" if cot_ok and sea_ok and val_ok else "FAIL"
    reasons: list[str] = []
    if cot_blk.get("has_price") and not cot_ok:
        reasons.append(f"COT store key {cot_store!r} != canonical {tl.resolved_store_key!r}")
    if sea_blk.get("available") and not sea_ok:
        reasons.append(
            f"Seasonality store={sea_store!r} method={sea_method!r} "
            f"(expected {tl.resolved_store_key!r}, {DERIVED_WEEKLY_ISO})"
        )
    if val_blk.get("wired") and not val_ok:
        reasons.append("Valuation not wired to canonical source metadata")

    return {
        "instrument": instrument_id,
        "canonical_source": tl.canonical_source,
        "canonical_symbol": tl.canonical_symbol,
        "resolved_store_key": tl.resolved_store_key,
        "date_range": f"{tl.date_start} → {tl.date_end}" if tl.date_start else None,
        "bars": tl.bar_count,
        "proxy": tl.proxy,
        "proxy_explanation": tl.proxy_explanation,
        "used_by_cot": bool(cot_blk.get("has_price")),
        "used_by_seasonality": bool(sea_blk.get("available")),
        "used_by_valuation": bool(val_blk.get("wired")),
        "cot_match_method": COT_MATCH_METHOD,
        "seasonality_derivation": DERIVED_WEEKLY_ISO,
        "status": status,
        "reasons": reasons,
    }


def run_audit(instrument_ids: list[str] | None = None) -> dict[str, Any]:
    ids = instrument_ids or all_instrument_ids()
    rows = [audit_instrument(iid) for iid in ids]
    acceptance = {iid: audit_instrument(iid) for iid in ACCEPTANCE_INSTRUMENTS}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "acceptance_instruments": acceptance,
        "rows": rows,
        "summary": {
            "total": len(rows),
            "pass": sum(1 for r in rows if r["status"] == "PASS"),
            "fail": sum(1 for r in rows if r["status"] == "FAIL"),
        },
    }


def render_md(report: dict[str, Any]) -> str:
    lines = [
        "# Canonical price consumer audit",
        "",
        f"Generated: {report['generated_at']}",
        "",
        "## Acceptance instruments",
        "",
        "| Instrument | Canonical source | Canonical symbol | Date range | Bars | Proxy? | COT | Seasonality | Valuation | Status |",
        "|---|---|---|---|---:|---|---|---|---|---|",
    ]
    for iid, row in report["acceptance_instruments"].items():
        lines.append(
            f"| {row['instrument']} | {row.get('canonical_source') or '—'} | "
            f"{row.get('canonical_symbol') or '—'} | {row.get('date_range') or '—'} | "
            f"{row.get('bars') or 0} | {row.get('proxy')} | "
            f"{'Y' if row.get('used_by_cot') else 'N'} | "
            f"{'Y' if row.get('used_by_seasonality') else 'N'} | "
            f"{'Y' if row.get('used_by_valuation') else 'N'} | "
            f"**{row['status']}** |"
        )

    lines.extend(["", "## All instruments", ""])
    for row in report["rows"]:
        if row["status"] == "FAIL" and row.get("reasons"):
            lines.append(f"- **{row['instrument']}** FAIL: {'; '.join(row['reasons'])}")
    lines.append("")
    s = report["summary"]
    lines.append(f"PASS: {s['pass']} / {s['total']}")
    return "\n".join(lines) + "\n"


def write_audit(instrument_ids: list[str] | None = None) -> Path:
    report = run_audit(instrument_ids)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_md(report), encoding="utf-8")
    return OUT_JSON
