"""Append valuation snapshots for daily/weekly change (derived from prior exports)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR

HISTORY_PATH = PROCESSED_DIR / "fx_valuation_history.json"
MAX_SNAPSHOTS = 14


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def load_valuation_history() -> dict[str, Any]:
    if not HISTORY_PATH.exists():
        return {"snapshots": []}
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"snapshots": []}


def _snapshot_pairs(payload: dict[str, Any]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for block in payload.get("pairs") or []:
        if not block or block.get("supported") is False:
            continue
        pid = str(block.get("pair") or "")
        if not pid:
            continue
        adj = _num(block.get("positioning_adjusted_score_differential"))
        raw = _num(block.get("pair_score_differential"))
        diff = adj if adj is not None else raw
        if diff is None:
            continue
        out[pid] = {"adjusted_differential": diff, "gap_pct": _num(block.get("valuation_gap_pct")) or 0.0}
    return out


def append_valuation_snapshot(payload: dict[str, Any]) -> None:
    doc = load_valuation_history()
    snaps: list[dict[str, Any]] = list(doc.get("snapshots") or [])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entry = {"date": today, "generated_at": payload.get("generated_at"), "pairs": _snapshot_pairs(payload)}
    snaps = [s for s in snaps if str(s.get("date") or "")[:10] != today]
    snaps.append(entry)
    snaps = snaps[-MAX_SNAPSHOTS:]
    doc["snapshots"] = snaps
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")


def history_deltas_for_pair(pair: str, current_diff: float | None) -> dict[str, float | None]:
    """Compare current adjusted differential to prior export snapshots."""
    if current_diff is None:
        return {"daily": None, "weekly": None}
    snaps = list(load_valuation_history().get("snapshots") or [])
    if not snaps:
        return {"daily": None, "weekly": None}
    daily = None
    weekly = None
    prev = snaps[-1]
    pd = _num((prev.get("pairs") or {}).get(pair, {}).get("adjusted_differential"))
    if pd is not None:
        daily = round(current_diff - pd, 1)
    old = snaps[max(0, len(snaps) - 8)] if len(snaps) >= 2 else None
    if old and old is not prev:
        pw = _num((old.get("pairs") or {}).get(pair, {}).get("adjusted_differential"))
        if pw is not None:
            weekly = round(current_diff - pw, 1)
    return {"daily": daily, "weekly": weekly}


def history_lookup_for_panels() -> dict[str, Any]:
    """Deprecated — use history_deltas_for_pair at build time."""
    return {"pairs": {}}
