"""Full instrument price-vs-COT history coverage audit.

Compares weekly COT report dates to aligned price closes for every HPTL instrument.

Writes:
  data/audits/price_history_coverage_audit.csv
  data/audits/price_history_coverage_audit.json
  web-dashboard/public/data/price_history_coverage_audit.json

Status rules (instruments with COT history):
  OK      >= 95% of COT weeks have a matched price
  PARTIAL 50–94%
  FAIL    < 50%
  NO_COT  no COT rows (price-only row in audit)
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.cot.cot_3y_series_export import (
    MASTER_PATH,
    PRICES_PATH,
    _build_price_series_for_market,
    _load_price_index,
    _series_for_market,
)
from hptl.markets.instrument_registry import all_instrument_ids, get_instrument
from hptl.prices.coverage import load_price_coverage, select_price_source

AUDIT_DIR = DATA_DIR / "audits"
AUDIT_CSV = AUDIT_DIR / "price_history_coverage_audit.csv"
AUDIT_JSON = AUDIT_DIR / "price_history_coverage_audit.json"
PUBLIC_AUDIT_JSON = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "price_history_coverage_audit.json"

CSV_FIELDS = [
    "instrument_id",
    "asset_class",
    "cot_start_date",
    "cot_end_date",
    "cot_weeks_available",
    "price_start_date",
    "price_end_date",
    "price_weeks_available",
    "matched_cot_weeks_with_price",
    "match_percentage",
    "price_source_used",
    "status",
    "reason",
]


def _price_source_label(
    audit: dict[str, Any] | None,
    *,
    coverage_source: str | None,
    store_key: str | None,
) -> str:
    audit = audit or {}
    parts: list[str] = []
    store = store_key or audit.get("price_store_key")
    if store:
        src = coverage_source or "store"
        parts.append(f"{src}:{store}")
    if audit.get("fred_fallback_series"):
        parts.append(f"fred:{audit['fred_fallback_series']}")
    if audit.get("oanda_fallback_symbol"):
        parts.append(f"oanda:{audit['oanda_fallback_symbol']}")
    if not parts:
        return "none"
    return "+".join(parts)


def _coverage_status(cot_weeks: int, match_pct: float | None) -> tuple[str, str | None]:
    if cot_weeks <= 0:
        return "NO_COT", "No COT history for this instrument."
    if match_pct is None:
        return "FAIL", "Unable to compute price match percentage."
    if match_pct >= 95.0:
        return "OK", None
    if match_pct >= 50.0:
        return (
            "PARTIAL",
            f"Price matched for {match_pct:.1f}% of COT weeks (target >= 95%).",
        )
    return (
        "FAIL",
        f"Price matched for {match_pct:.1f}% of COT weeks (target >= 95%).",
    )



def _store_price_span(instrument_id: str, price_index: dict[str, list[tuple[str, float]]]) -> tuple[str | None, str | None, int]:
    bars = price_index.get(instrument_id)
    if not bars:
        return None, None, 0
    return bars[0][0], bars[-1][0], len(bars)


def _cot_dates_for_market(master: pd.DataFrame, market: str) -> list[str]:
    if master.empty or "market" not in master.columns:
        return []
    sub = master.loc[master["market"] == market].sort_values("cot_report_date")
    if sub.empty:
        return []
    return [str(d)[:10] for d in sub["cot_report_date"].dt.strftime("%Y-%m-%d")]


def audit_instrument_row(
    instrument_id: str,
    *,
    master: pd.DataFrame,
    price_index: dict[str, list[tuple[str, float]]],
    cot_blocks: dict[str, Any],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    spec = get_instrument(instrument_id)
    asset_class = spec.asset_class if spec else "unknown"
    coverage_source = select_price_source(instrument_id, coverage)

    block = cot_blocks.get(instrument_id)
    cot_dates = _cot_dates_for_market(master, instrument_id)

    if block:
        cot_start = block.get("earliest_date")
        cot_end = block.get("latest_date")
        cot_weeks = int(block.get("weeks") or 0)
        audit = block.get("price_audit") or {}
        matched = int(block.get("price_weeks") or 0)
        price_start = audit.get("earliest_price_bar_date")
        price_end = audit.get("latest_price_bar_date")
        price_weeks_avail = int(audit.get("price_bar_count") or 0)
        price_source = _price_source_label(
            audit,
            coverage_source=coverage_source,
            store_key=audit.get("price_store_key"),
        )
    elif cot_dates:
        g = master.loc[master["market"] == instrument_id]
        built = _series_for_market(g, price_index)
        cot_start = built.get("earliest_date")
        cot_end = built.get("latest_date")
        cot_weeks = int(built.get("weeks") or len(cot_dates))
        audit = built.get("price_audit") or {}
        matched = int(built.get("price_weeks") or 0)
        cot_earliest = cot_dates[0] if cot_dates else None
        price_series, _, price_meta = _build_price_series_for_market(
            instrument_id, price_index, cot_earliest
        )
        price_start = price_series[0][0] if price_series else None
        price_end = price_series[-1][0] if price_series else None
        price_weeks_avail = len(price_series) if price_series else 0
        audit = {**audit, **{k: price_meta.get(k) for k in ("fred_series", "oanda_symbol", "store_key")}}
        price_source = _price_source_label(
            audit,
            coverage_source=coverage_source,
            store_key=price_meta.get("store_key"),
        )
    else:
        cot_start = cot_end = None
        cot_weeks = 0
        matched = 0
        store_start, store_end, store_weeks = _store_price_span(instrument_id, price_index)
        price_start, price_end, price_weeks_avail = store_start, store_end, store_weeks
        price_source = f"{coverage_source}:{instrument_id}" if coverage_source and store_weeks else "none"

    match_pct: float | None
    if cot_weeks > 0:
        match_pct = round(100.0 * matched / cot_weeks, 2)
    else:
        match_pct = None

    status, reason = _coverage_status(cot_weeks, match_pct)

    return {
        "instrument_id": instrument_id,
        "asset_class": asset_class,
        "cot_start_date": cot_start,
        "cot_end_date": cot_end,
        "cot_weeks_available": cot_weeks,
        "price_start_date": price_start,
        "price_end_date": price_end,
        "price_weeks_available": price_weeks_avail,
        "matched_cot_weeks_with_price": matched,
        "match_percentage": match_pct,
        "price_source_used": price_source,
        "status": status,
        "reason": reason,
    }


def build_price_history_coverage_audit() -> dict[str, Any]:
    master = pd.read_csv(MASTER_PATH) if MASTER_PATH.exists() else pd.DataFrame()
    if not master.empty and "cot_report_date" in master.columns:
        master["cot_report_date"] = pd.to_datetime(master["cot_report_date"], errors="coerce")

    price_index, price_source_file = _load_price_index()
    coverage = load_price_coverage()

    cot_blocks: dict[str, Any] = {}
    cot_json = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json"
    if cot_json.exists():
        try:
            doc = json.loads(cot_json.read_text(encoding="utf-8"))
            cot_blocks = doc.get("markets") or {}
        except (OSError, ValueError):
            cot_blocks = {}

    instrument_ids = all_instrument_ids()
    rows = [
        audit_instrument_row(
            iid,
            master=master,
            price_index=price_index,
            cot_blocks=cot_blocks,
            coverage=coverage,
        )
        for iid in instrument_ids
    ]

    cot_rows = [r for r in rows if r["cot_weeks_available"] > 0]
    summary = {
        "instruments_total": len(rows),
        "with_cot_history": len(cot_rows),
        "status_ok": sum(1 for r in cot_rows if r["status"] == "OK"),
        "status_partial": sum(1 for r in cot_rows if r["status"] == "PARTIAL"),
        "status_fail": sum(1 for r in cot_rows if r["status"] == "FAIL"),
        "status_no_cot": sum(1 for r in rows if r["status"] == "NO_COT"),
        "target_markets_in_audit": sum(1 for r in rows if r["instrument_id"] in TARGET_MARKETS),
    }

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_from": "hptl.prices.price_history_coverage_audit",
        "sources": {
            "cot_master": str(MASTER_PATH),
            "cot_series": str(cot_json),
            "prices": str(PRICES_PATH),
            "price_source_file": price_source_file,
        },
        "rules": {
            "ok_pct": 95,
            "partial_pct_min": 50,
            "fail_pct_max": 49.99,
        },
        "summary": summary,
        "instruments": rows,
        "by_id": {r["instrument_id"]: r for r in rows},
    }


def write_price_history_coverage_audit(payload: dict[str, Any] | None = None) -> tuple[Path, Path]:
    payload = payload or build_price_history_coverage_audit()
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    AUDIT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    PUBLIC_AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_AUDIT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    with AUDIT_CSV.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in payload.get("instruments") or []:
            writer.writerow(row)

    return AUDIT_JSON, AUDIT_CSV


def run() -> dict[str, Any]:
    payload = build_price_history_coverage_audit()
    json_path, csv_path = write_price_history_coverage_audit(payload)
    s = payload["summary"]
    print("=" * 72)
    print("PRICE_HISTORY_COVERAGE_AUDIT")
    print("=" * 72)
    print(f"  instruments: {s['instruments_total']}  with_cot: {s['with_cot_history']}")
    print(f"  OK: {s['status_ok']}  PARTIAL: {s['status_partial']}  FAIL: {s['status_fail']}  NO_COT: {s['status_no_cot']}")
    print(f"  JSON: {json_path}")
    print(f"  CSV:  {csv_path}")
    print("=" * 72)
    for row in payload["instruments"]:
        if row["cot_weeks_available"] <= 0:
            continue
        if row["status"] != "OK":
            print(
                f"  {row['status']:7} {row['instrument_id']:28} "
                f"match={row['match_percentage']}% "
                f"cot={row['cot_weeks_available']} matched={row['matched_cot_weeks_with_price']}"
            )
    return payload
