"""Canonical price database — processed store + dashboard export."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.prices.models import InstrumentPriceRecord, build_history_meta, compute_range_52w, record_to_public

PRICES_DIR = PROCESSED_DIR / "prices"
STORE_INDEX_PATH = PRICES_DIR / "index.json"
CANONICAL_PATH = PROCESSED_DIR / "prices_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "prices_latest.json"
DIST_PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "prices_latest.json"

SCHEMA_VERSION = 1


def _safe_filename(instrument_id: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", instrument_id.strip())
    return s.strip("_") or "instrument"


def write_instrument_record(
    record: InstrumentPriceRecord,
    *,
    fetched_via: str | None = None,
    historical_via: str | None = None,
) -> Path:
    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    path = PRICES_DIR / f"{_safe_filename(record['instrument_id'])}.json"
    internal = dict(record)
    if fetched_via:
        internal["_fetched_via"] = fetched_via
    if historical_via:
        internal["_historical_via"] = historical_via
    internal["stored_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(internal, indent=2), encoding="utf-8")
    return path


def load_instrument_record(instrument_id: str) -> InstrumentPriceRecord | None:
    internal = load_instrument_record_internal(instrument_id)
    if internal is None:
        return None
    return _record_from_internal(internal)


def load_instrument_record_internal(instrument_id: str) -> dict[str, Any] | None:
    """Load per-instrument JSON including internal fetch metadata."""
    path = PRICES_DIR / f"{_safe_filename(instrument_id)}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _record_from_internal(doc: dict[str, Any]) -> InstrumentPriceRecord:
    iid = str(doc.get("instrument_id") or "").strip()
    return {
        "instrument_id": iid,
        "price": doc.get("price"),
        "daily": doc.get("daily") or [],
        "weekly": doc.get("weekly") or [],
        "forming_daily": doc.get("forming_daily"),
        "forming_weekly": doc.get("forming_weekly"),
        "range_52w": doc.get("range_52w"),
        "history": doc.get("history"),
        "error": doc.get("error"),
        "price_scale": doc.get("price_scale"),
    }


def _resolve_historical_via(existing: dict[str, Any] | None) -> str | None:
    if not existing:
        return None
    historical = existing.get("_historical_via")
    if historical:
        return str(historical)
    if existing.get("_fetched_via") == "oanda_backfill":
        return "oanda_backfill"
    return None


def merge_fetched_into_production(
    existing: dict[str, Any] | None,
    fetched: InstrumentPriceRecord,
    *,
    fetched_via: str,
) -> tuple[InstrumentPriceRecord, dict[str, str | None]]:
    """Merge a live API fetch into the stored production record.

    Preserves older historical bars; incoming bars replace same-date entries.
    """
    from hptl.prices.fx_daily_backfill import merge_daily_bars_refresh
    from hptl.seasonality.seasonality_v2 import normalize_daily_bars

    existing = existing or {}
    existing_daily = existing.get("daily") or []
    existing_weekly = existing.get("weekly") or []

    merged_daily, _ = merge_daily_bars_refresh(existing_daily, fetched.get("daily") or [])
    merged_weekly, _ = merge_daily_bars_refresh(existing_weekly, fetched.get("weekly") or [])
    daily = normalize_daily_bars(merged_daily)
    weekly = normalize_daily_bars(merged_weekly)
    range_52w = compute_range_52w(daily)
    history = build_history_meta(daily, weekly, range_52w) if daily or weekly else None

    err = fetched.get("error")
    has_incoming = bool(fetched.get("daily") or fetched.get("weekly"))
    if err in ("unsupported_instrument", "unknown_instrument") and daily and not has_incoming:
        err = existing.get("error")
    elif err and daily:
        err = None

    rec: InstrumentPriceRecord = {
        "instrument_id": fetched["instrument_id"],
        "price": fetched.get("price") if fetched.get("price") is not None else existing.get("price"),
        "daily": daily,
        "weekly": weekly,
        "forming_daily": fetched.get("forming_daily")
        if fetched.get("forming_daily") is not None
        else existing.get("forming_daily"),
        "forming_weekly": fetched.get("forming_weekly")
        if fetched.get("forming_weekly") is not None
        else existing.get("forming_weekly"),
        "range_52w": range_52w,
        "history": history,
        "error": err,
        "price_scale": fetched.get("price_scale") or existing.get("price_scale"),
    }
    meta = {
        "fetched_via": fetched_via,
        "historical_via": _resolve_historical_via(existing),
    }
    return rec, meta


def build_store_payload(
    records: dict[str, InstrumentPriceRecord],
    *,
    coverage_generated_at: str | None = None,
) -> dict[str, Any]:
    instruments: dict[str, Any] = {}
    ok = 0
    err = 0
    for iid, rec in sorted(records.items()):
        instruments[iid] = record_to_public(rec)
        if rec.get("error"):
            err += 1
        elif rec.get("daily") or rec.get("weekly"):
            ok += 1
        else:
            err += 1

    return {
        "version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_from": "hptl.prices.price_store",
        "coverage_audit_at": coverage_generated_at,
        "summary": {
            "instruments_total": len(records),
            "with_daily_bars": ok,
            "with_errors": err,
        },
        "instruments": instruments,
    }


def write_price_store(
    records: dict[str, InstrumentPriceRecord],
    *,
    coverage_generated_at: str | None = None,
) -> Path:
    payload = build_store_payload(records, coverage_generated_at=coverage_generated_at)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    if DIST_PUBLIC_PATH.parent.exists():
        DIST_PUBLIC_PATH.write_text(text, encoding="utf-8")

    index = {
        "version": SCHEMA_VERSION,
        "updated_at": payload["generated_at"],
        "instruments": sorted(records.keys()),
        "files": {iid: str(PRICES_DIR / f"{_safe_filename(iid)}.json") for iid in records},
    }
    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    STORE_INDEX_PATH.write_text(json.dumps(index, indent=2), encoding="utf-8")
    return CANONICAL_PATH


def write_price_store_merged(
    updates: dict[str, InstrumentPriceRecord],
    *,
    coverage_generated_at: str | None = None,
) -> Path:
    """Merge ``updates`` into all on-disk instrument records, then rewrite the bundle."""
    all_records = load_all_instrument_records()
    all_records.update(updates)
    return write_price_store(all_records, coverage_generated_at=coverage_generated_at)


def load_all_instrument_records() -> dict[str, InstrumentPriceRecord]:
    """Load every per-instrument JSON under ``data/processed/prices/``."""
    records: dict[str, InstrumentPriceRecord] = {}
    if not PRICES_DIR.exists():
        return records
    for path in sorted(PRICES_DIR.glob("*.json")):
        if path.name == "index.json":
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        iid = str(doc.get("instrument_id") or "").strip()
        if not iid:
            continue
        records[iid] = {
            "instrument_id": iid,
            "price": doc.get("price"),
            "daily": doc.get("daily") or [],
            "weekly": doc.get("weekly") or [],
            "forming_daily": doc.get("forming_daily"),
            "forming_weekly": doc.get("forming_weekly"),
            "range_52w": doc.get("range_52w"),
            "history": doc.get("history"),
            "error": doc.get("error"),
            "price_scale": doc.get("price_scale"),
        }
    return records


def rebuild_price_store_from_disk(
    *,
    coverage_generated_at: str | None = None,
) -> Path:
    """Merge all on-disk instrument price files into canonical + public store."""
    from hptl.prices.coverage import load_price_coverage

    cov = load_price_coverage()
    records = load_all_instrument_records()
    return write_price_store(
        records,
        coverage_generated_at=coverage_generated_at or cov.get("generated_at"),
    )


def load_price_store(path: Path | None = None) -> dict[str, Any]:
    p = path or CANONICAL_PATH
    if not p.exists():
        p = PUBLIC_PATH
    if not p.exists():
        return {"version": SCHEMA_VERSION, "instruments": {}}
    return json.loads(p.read_text(encoding="utf-8"))
