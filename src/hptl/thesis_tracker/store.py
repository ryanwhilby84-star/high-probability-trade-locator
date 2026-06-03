"""Canonical JSON persistence for the Thesis Tracker (mirrors Trade Journal).

- Canonical store: ``data/processed/thesis_tracker.json``
- Dashboard export: ``web-dashboard/public/data/thesis_tracker_latest.json``

Derived fields (age, conviction, trend, auto summary) are recomputed on every
write so the export is always self-consistent. No scoring is performed here —
conviction is composed from numbers that already live on each snapshot.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.thesis_tracker.conviction import (
    annotate_conviction,
    compute_age_weeks,
    compute_trend,
    current_conviction,
)
from hptl.thesis_tracker.decision import build_decision
from hptl.thesis_tracker.opportunity import build_opportunity
from hptl.thesis_tracker.models import (
    TERMINAL_STATUSES,
    new_thesis_id,
    norm_status,
    normalize_log_entry,
    normalize_snapshot,
    normalize_thesis,
    now_iso,
)
from hptl.thesis_tracker.narrative import build_auto_summary, build_evolution_note

TRACKER_PATH = PROCESSED_DIR / "thesis_tracker.json"
EXPORT_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "thesis_tracker_latest.json"
DIST_EXPORT_PATH = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "thesis_tracker_latest.json"

SCHEMA_VERSION = 1
DISCLAIMER = (
    "Thesis Tracker — multi-week planning narrative only. Conviction is a composite of "
    "currently-wired components (COT, macro, structural); valuation, seasonality and retail "
    "positioning are placeholders until those engines exist. No orders executed via HPTL."
)


def _empty_doc() -> dict[str, Any]:
    return {"version": SCHEMA_VERSION, "updated_at": now_iso(), "theses": []}


def load_tracker() -> dict[str, Any]:
    if not TRACKER_PATH.exists():
        return _empty_doc()
    try:
        raw = json.loads(TRACKER_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _empty_doc()
    if not isinstance(raw, dict):
        return _empty_doc()
    if not isinstance(raw.get("theses"), list):
        raw["theses"] = []
    return raw


def _recompute_derived(thesis: dict[str, Any]) -> dict[str, Any]:
    snaps = thesis.get("snapshots") or []
    annotate_conviction(snaps)
    thesis["age_weeks"] = compute_age_weeks(snaps)
    thesis["conviction_current"] = current_conviction(snaps)
    thesis["conviction_trend"] = compute_trend(snaps)
    thesis["last_update_week"] = (snaps[-1].get("week") if snaps else None) or None
    thesis["summary_auto"] = build_auto_summary(thesis)
    thesis["decision"] = build_decision(thesis)
    thesis["opportunity"] = build_opportunity(thesis)
    return thesis


def save_tracker(doc: dict[str, Any]) -> None:
    TRACKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    for t in doc.get("theses") or []:
        if isinstance(t, dict):
            _recompute_derived(t)
    doc["updated_at"] = now_iso()
    TRACKER_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def _summary_counts(theses: list[dict[str, Any]]) -> dict[str, Any]:
    active = [t for t in theses if not t.get("archived")]
    by_status: dict[str, int] = {}
    for t in active:
        s = norm_status(t.get("status"))
        by_status[s] = by_status.get(s, 0) + 1
    return {
        "total": len(theses),
        "active": len(active),
        "archived": len(theses) - len(active),
        "by_status": by_status,
    }


def export_tracker(doc: dict[str, Any] | None = None) -> list[Path]:
    payload = doc if doc is not None else load_tracker()
    for t in payload.get("theses") or []:
        if isinstance(t, dict):
            _recompute_derived(t)
    out = {
        **payload,
        "summary": _summary_counts([t for t in payload.get("theses") or [] if isinstance(t, dict)]),
        "exported_at": now_iso(),
        "disclaimer": DISCLAIMER,
    }
    written: list[Path] = []
    for path in (EXPORT_PATH, DIST_EXPORT_PATH):
        if path is DIST_EXPORT_PATH and not path.parent.parent.exists():
            continue  # dist/ only exists after a frontend build
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(out, indent=2), encoding="utf-8")
        written.append(path)
    return written


def save_and_export(doc: dict[str, Any]) -> list[Path]:
    save_tracker(doc)
    return export_tracker(doc)


# ---- queries -----------------------------------------------------------------

def list_theses(*, include_archived: bool = True) -> list[dict[str, Any]]:
    rows = [t for t in load_tracker().get("theses") or [] if isinstance(t, dict)]
    if not include_archived:
        rows = [t for t in rows if not t.get("archived")]
    rows.sort(
        key=lambda t: (
            (t.get("opportunity") or {}).get("rank_score") or -1,
            t.get("conviction_current") or -1,
        ),
        reverse=True,
    )
    return rows


def get_thesis(thesis_id: str) -> dict[str, Any] | None:
    tid = str(thesis_id or "").strip()
    if not tid:
        return None
    for t in load_tracker().get("theses") or []:
        if isinstance(t, dict) and str(t.get("thesis_id")) == tid:
            return t
    return None


def find_by_market(market: str, *, include_archived: bool = False) -> dict[str, Any] | None:
    m = str(market or "").strip()
    for t in load_tracker().get("theses") or []:
        if not isinstance(t, dict):
            continue
        if str(t.get("market") or "").strip() == m and (include_archived or not t.get("archived")):
            return t
    return None


# ---- mutations ---------------------------------------------------------------

def add_thesis(
    payload: dict[str, Any],
    *,
    source: str = "manual",
    initial_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a new thesis (idempotent per active market)."""
    market = str(payload.get("market") or "").strip()
    if not market:
        raise ValueError("market is required")
    existing = find_by_market(market, include_archived=False)
    if existing is not None:
        return existing  # already tracked; do not duplicate

    thesis = normalize_thesis({**payload, "thesis_id": payload.get("thesis_id") or new_thesis_id(), "source": source})
    if initial_snapshot:
        snap = normalize_snapshot(initial_snapshot)
        thesis["snapshots"] = [snap]
        thesis["created_week"] = thesis.get("created_week") or snap.get("week")
    annotate_conviction(thesis["snapshots"])
    note = build_evolution_note(None, thesis["snapshots"][-1]) if thesis["snapshots"] else "Thesis opened."
    thesis["evolution_log"] = [normalize_log_entry({
        "week": thesis.get("created_week") or (thesis["snapshots"][-1].get("week") if thesis["snapshots"] else None),
        "auto": True,
        "text": note,
    })]

    doc = load_tracker()
    doc["theses"] = [*(doc.get("theses") or []), thesis]
    save_and_export(doc)
    return thesis


def _mutate(thesis_id: str, fn) -> dict[str, Any]:
    doc = load_tracker()
    theses = list(doc.get("theses") or [])
    idx = next((i for i, t in enumerate(theses) if isinstance(t, dict) and str(t.get("thesis_id")) == str(thesis_id)), None)
    if idx is None:
        raise KeyError(f"thesis_id not found: {thesis_id}")
    theses[idx] = fn(theses[idx])
    doc["theses"] = theses
    save_and_export(doc)
    return theses[idx]


def update_status(thesis_id: str, status: str, *, note: str | None = None) -> dict[str, Any]:
    new_status = norm_status(status, default="")
    if not new_status:
        raise ValueError(f"invalid status: {status}")

    def _fn(t: dict[str, Any]) -> dict[str, Any]:
        prev = t.get("status")
        t["status"] = new_status
        text = note or f"Status {prev} → {new_status}."
        t["evolution_log"] = [*(t.get("evolution_log") or []), normalize_log_entry({
            "week": t.get("last_update_week"),
            "auto": note is None,
            "text": text,
        })]
        if new_status in TERMINAL_STATUSES:
            t["archived"] = True
            t["archived_at"] = now_iso()
        return t

    return _mutate(thesis_id, _fn)


def add_note(thesis_id: str, text: str, *, week: str | None = None) -> dict[str, Any]:
    clean = str(text or "").strip()
    if not clean:
        raise ValueError("note text is required")

    def _fn(t: dict[str, Any]) -> dict[str, Any]:
        t["evolution_log"] = [*(t.get("evolution_log") or []), normalize_log_entry({
            "week": week or t.get("last_update_week"),
            "auto": False,
            "text": clean,
        })]
        return t

    return _mutate(thesis_id, _fn)


def set_summary(thesis_id: str, summary: str) -> dict[str, Any]:
    def _fn(t: dict[str, Any]) -> dict[str, Any]:
        t["summary_manual"] = str(summary or "").strip() or None
        return t

    return _mutate(thesis_id, _fn)


def archive_thesis(thesis_id: str, *, reason: str | None = None) -> dict[str, Any]:
    def _fn(t: dict[str, Any]) -> dict[str, Any]:
        t["archived"] = True
        t["archived_at"] = now_iso()
        t["evolution_log"] = [*(t.get("evolution_log") or []), normalize_log_entry({
            "week": t.get("last_update_week"),
            "auto": reason is None,
            "text": reason or "Thesis archived.",
        })]
        return t

    return _mutate(thesis_id, _fn)


def remove_thesis(thesis_id: str) -> bool:
    doc = load_tracker()
    theses = [t for t in doc.get("theses") or [] if isinstance(t, dict)]
    remaining = [t for t in theses if str(t.get("thesis_id")) != str(thesis_id)]
    if len(remaining) == len(theses):
        return False
    doc["theses"] = remaining
    save_and_export(doc)
    return True


def append_snapshot(thesis_id: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    """Append a weekly snapshot if its week is not already captured."""
    snap = normalize_snapshot(snapshot)
    annotate_conviction([snap])
    week = snap.get("week")

    def _fn(t: dict[str, Any]) -> dict[str, Any]:
        snaps = list(t.get("snapshots") or [])
        if week and any(str(s.get("week")) == str(week) for s in snaps):
            return t  # already captured this week
        prev = snaps[-1] if snaps else None
        snaps.append(snap)
        snaps.sort(key=lambda s: str(s.get("week") or ""))
        t["snapshots"] = snaps
        annotate_conviction(t["snapshots"])
        t["evolution_log"] = [*(t.get("evolution_log") or []), normalize_log_entry({
            "week": week,
            "auto": True,
            "text": build_evolution_note(prev, snap),
        })]
        return t

    return _mutate(thesis_id, _fn)
