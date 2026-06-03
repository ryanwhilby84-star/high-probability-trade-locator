"""Persist trade journal entries to local JSON (no broker integration)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.journal.models import TradeJournalEntry, normalize_payload

JOURNAL_PATH = PROCESSED_DIR / "trade_journal.json"
EXPORT_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "trade_journal_latest.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_journal() -> dict[str, Any]:
    if not JOURNAL_PATH.exists():
        return {"version": 1, "updated_at": _now_iso(), "entries": []}
    raw = json.loads(JOURNAL_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {"version": 1, "updated_at": _now_iso(), "entries": []}
    entries = raw.get("entries")
    if not isinstance(entries, list):
        raw["entries"] = []
    return raw


def save_journal(doc: dict[str, Any]) -> None:
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    doc["updated_at"] = _now_iso()
    JOURNAL_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def export_journal(doc: dict[str, Any] | None = None) -> Path:
    payload = doc if doc is not None else load_journal()
    payload = {
        **payload,
        "exported_at": _now_iso(),
        "disclaimer": "Trade journal — planning log only. No orders executed via HPTL.",
    }
    EXPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    EXPORT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return EXPORT_PATH


def list_entries(*, status: str | None = None, market: str | None = None) -> list[dict[str, Any]]:
    doc = load_journal()
    rows = [e for e in doc.get("entries") or [] if isinstance(e, dict)]
    if market:
        m = market.strip()
        rows = [e for e in rows if str(e.get("market") or "") == m]
    if status:
        s = status.strip().lower()
        rows = [e for e in rows if str(e.get("status") or "").lower() == s]
    rows.sort(key=lambda e: str(e.get("updated_at") or e.get("created_at") or ""), reverse=True)
    return rows


def get_entry(trade_id: str) -> dict[str, Any] | None:
    tid = str(trade_id or "").strip()
    if not tid:
        return None
    for e in load_journal().get("entries") or []:
        if isinstance(e, dict) and str(e.get("trade_id")) == tid:
            return e
    return None


def upsert_entry(payload: dict[str, Any], *, source: str = "manual") -> dict[str, Any]:
    normalized = normalize_payload(payload, source=source)
    doc = load_journal()
    entries: list[dict[str, Any]] = list(doc.get("entries") or [])
    tid = normalized["trade_id"]
    existing_idx = next(
        (i for i, e in enumerate(entries) if isinstance(e, dict) and str(e.get("trade_id")) == tid),
        None,
    )
    if existing_idx is not None:
        prev = entries[existing_idx]
        normalized["created_at"] = str(prev.get("created_at") or normalized["created_at"])
        normalized["updated_at"] = _now_iso()
        if not normalized.get("dashboard_snapshot") and isinstance(prev.get("dashboard_snapshot"), dict):
            normalized["dashboard_snapshot"] = prev["dashboard_snapshot"]
        entries[existing_idx] = normalized
    else:
        normalized["created_at"] = normalized.get("created_at") or _now_iso()
        normalized["updated_at"] = _now_iso()
        entries.append(normalized)
    doc["entries"] = entries
    save_journal(doc)
    export_journal(doc)
    return normalized


def create_entry(payload: dict[str, Any], *, source: str = "manual") -> dict[str, Any]:
    if payload.get("trade_id"):
        return upsert_entry(payload, source=source)
    return upsert_entry({**payload, "trade_id": ""}, source=source)
