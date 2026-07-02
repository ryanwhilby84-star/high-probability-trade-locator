"""Foundation audit for Cocoa, Coffee, Cotton."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.prices.canonical_timeline import build_canonical_timeline
from hptl.prices.softs_foundation import FRED_PRIMARY_SOFTS, SOFTS_INSTRUMENTS
from hptl.seasonality.seasonality_trust import attach_trust_metadata
from hptl.seasonality.seasonality_price_bars import weekly_closes_for_instrument

COT_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json"
SEA_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "seasonality_price_latest.json"
VAL_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "valuation_latest.json"
LOC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "location_latest.json"
OUT_JSON = PROCESSED_DIR / "softs_foundation_audit.json"
OUT_MD = PROCESSED_DIR / "softs_foundation_audit.md"


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _cot_coverage(cot_blk: dict[str, Any]) -> dict[str, Any]:
    audit = cot_blk.get("price_audit") or {}
    weeks = cot_blk.get("weeks") or 0
    matched = cot_blk.get("price_weeks") or 0
    pct = round((matched / weeks) * 1000) / 10 if weeks else None
    status = "PASS" if pct is not None and pct >= 95 else "FAIL" if weeks else "MISSING"
    return {
        "cot_weeks": weeks,
        "price_matched": matched,
        "coverage_pct": pct,
        "status": status,
        "price_store_key": audit.get("price_store_key"),
        "canonical_source": audit.get("canonical_source"),
    }


def _valuation_readiness(
    instrument_id: str,
    val_blk: dict[str, Any],
    loc_blk: dict[str, Any],
    sea_blk: dict[str, Any],
    cot_blk: dict[str, Any],
) -> dict[str, Any]:
    tl = build_canonical_timeline(instrument_id)
    inputs = {
        "price_history": bool(tl and tl.bar_count),
        "usd_dxy": True,
        "seasonality": bool(sea_blk.get("available")),
        "cot_positioning": bool(cot_blk.get("has_price")),
        "supply_demand_proxy": instrument_id in FRED_PRIMARY_SOFTS,
        "inventory_proxy": False,
        "weather_proxy": False,
        "placeholder_model": None,
        "location_model": loc_blk.get("location_reason") if loc_blk.get("wired") else None,
    }
    missing = [k for k, v in inputs.items() if v is False]
    ready = inputs["price_history"] and inputs["cot_positioning"]
    return {
        "inputs": inputs,
        "missing": missing,
        "readiness": "READY" if ready and not missing else "PARTIAL" if ready else "NOT_READY",
        "note": "Full valuation model not built — data contract inventory only.",
    }


def audit_instrument(instrument_id: str) -> dict[str, Any]:
    tl = build_canonical_timeline(instrument_id)
    cot_blk = (_load(COT_PATH).get("markets") or {}).get(instrument_id) or {}
    sea_blk = (_load(SEA_PATH).get("markets") or {}).get(instrument_id) or {}
    val_blk = (_load(VAL_PATH).get("instruments") or {}).get(instrument_id) or {}
    loc_blk = (_load(LOC_PATH).get("instruments") or {}).get(instrument_id) or {}

    weekly, bar_method, _ = weekly_closes_for_instrument(instrument_id)
    trust = attach_trust_metadata({"available": bool(weekly)}, weekly) if weekly else {"trust_grade": "C"}

    cot_cov = _cot_coverage(cot_blk)
    fred_meta = FRED_PRIMARY_SOFTS.get(instrument_id)

    price_panel = {
        "source": tl.canonical_source if tl else None,
        "symbol": tl.canonical_symbol if tl else (fred_meta[0] if fred_meta else None),
        "fred_series": fred_meta[0] if fred_meta else None,
        "date_range": f"{tl.date_start} → {tl.date_end}" if tl and tl.date_start else None,
        "daily_bars": tl.bar_count if tl else 0,
        "weekly_derived": len(weekly),
        "weekly_method": bar_method,
        "proxy": tl.proxy if tl else None,
        "proxy_explanation": tl.proxy_explanation if tl else None,
        "confidence": tl.confidence if tl else None,
        "note": fred_meta[1] if fred_meta else None,
    }

    seasonality_grade = trust.get("trust_grade") or ("C" if not sea_blk.get("available") else "B")
    val_ready = _valuation_readiness(instrument_id, val_blk, loc_blk, sea_blk, cot_blk)

    price_cov_status = "PASS" if tl and tl.bar_count >= 120 else "PARTIAL" if tl and tl.bar_count else "FAIL"
    overall = "PASS"
    if price_cov_status != "PASS" or cot_cov["status"] != "PASS" or seasonality_grade == "C":
        overall = "PARTIAL" if cot_cov["status"] == "PASS" and tl else "FAIL"

    return {
        "instrument": instrument_id,
        "canonical_price": price_panel,
        "cot_alignment": cot_cov,
        "seasonality": {
            "available": bool(sea_blk.get("available")),
            "grade": seasonality_grade,
            "bar_source": sea_blk.get("price_derivation") or sea_blk.get("bar_source"),
            "weeks_available": sea_blk.get("seasonal_3y_weeks"),
            "trust_notes": sea_blk.get("trust_notes") or trust.get("trust_notes"),
        },
        "workstation": {
            "price": bool(cot_blk.get("has_price")),
            "non_commercial": bool(cot_blk.get("weeks")),
            "retail": bool(cot_blk.get("has_retail")),
            "seasonality_panel": seasonality_grade in {"A", "B"} and bool(sea_blk.get("available")),
            "valuation_status": val_blk.get("valuation_state") or val_blk.get("valuation_bias") or "UNAVAILABLE",
            "location_status": loc_blk.get("location_state") or loc_blk.get("location_bias"),
        },
        "valuation_readiness": val_ready,
        "price_coverage_status": price_cov_status,
        "overall_status": overall,
    }


def run_audit() -> dict[str, Any]:
    rows = [audit_instrument(iid) for iid in SOFTS_INSTRUMENTS]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "instruments": {r["instrument"]: r for r in rows},
        "summary_table": [
            {
                "instrument": r["instrument"],
                "price_coverage": r["price_coverage_status"],
                "cot_coverage": r["cot_alignment"]["status"],
                "seasonality_grade": r["seasonality"]["grade"],
                "valuation_readiness": r["valuation_readiness"]["readiness"],
                "status": r["overall_status"],
            }
            for r in rows
        ],
    }


def render_md(report: dict[str, Any]) -> str:
    lines = [
        "# Softs foundation audit — Cocoa, Coffee, Cotton",
        "",
        f"Generated: {report['generated_at']}",
        "",
        "## Final audit table",
        "",
        "| Instrument | Price Coverage | COT Coverage | Seasonality Grade | Valuation Readiness | Status |",
        "|---|---|---|---|---|---|",
    ]
    for row in report["summary_table"]:
        lines.append(
            f"| {row['instrument']} | {row['price_coverage']} | {row['cot_coverage']} | "
            f"{row['seasonality_grade']} | {row['valuation_readiness']} | **{row['status']}** |"
        )

    for iid, block in report["instruments"].items():
        cp = block["canonical_price"]
        lines.extend(
            [
                "",
                f"## {iid}",
                "",
                "### 1. Canonical price history",
                f"- Source: `{cp.get('source')}` · Symbol: `{cp.get('symbol')}`",
                f"- FRED series: `{cp.get('fred_series')}`",
                f"- Date range: {cp.get('date_range') or '—'}",
                f"- Daily bars: {cp.get('daily_bars')} · Weekly derived: {cp.get('weekly_derived')} ({cp.get('weekly_method')})",
                f"- Proxy: {cp.get('proxy')} · Confidence: {cp.get('confidence')}",
                f"- Note: {cp.get('note') or '—'}",
                "",
                "### 2. COT alignment",
                f"- COT weeks: {block['cot_alignment']['cot_weeks']} · Price matched: {block['cot_alignment']['price_matched']} · Coverage: {block['cot_alignment']['coverage_pct']}% · **{block['cot_alignment']['status']}**",
                "",
                "### 3. Seasonality",
                f"- Grade **{block['seasonality']['grade']}** · Available: {block['seasonality']['available']}",
                f"- {block['seasonality'].get('trust_notes') or '—'}",
                "",
                "### 4. Workstation",
                f"- Price: {block['workstation']['price']} · NC: {block['workstation']['non_commercial']} · Retail: {block['workstation']['retail']} · Seasonality: {block['workstation']['seasonality_panel']}",
                "",
                "### 5. Valuation readiness (data contract)",
            ]
        )
        for k, v in block["valuation_readiness"]["inputs"].items():
            lines.append(f"- {k}: {v}")
        if block["valuation_readiness"]["missing"]:
            lines.append(f"- Missing: {', '.join(block['valuation_readiness']['missing'])}")
        lines.append(f"- Readiness: **{block['valuation_readiness']['readiness']}**")

    return "\n".join(lines) + "\n"


def write_audit() -> Path:
    report = run_audit()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_md(report), encoding="utf-8")
    public = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "softs_foundation_audit.json"
    public.parent.mkdir(parents=True, exist_ok=True)
    public.write_text(OUT_JSON.read_text(encoding="utf-8"), encoding="utf-8")
    return OUT_JSON
