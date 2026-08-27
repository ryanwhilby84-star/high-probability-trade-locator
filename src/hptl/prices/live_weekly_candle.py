"""Active weekly-candle live updates.

Completed historical weekly candles in ``workstation_ohlc_latest.json`` are the
source of truth and are never mutated here. This tracker maintains the single
*active* (in-progress) weekly candle per instrument and updates it from incoming
live midpoints:

    open  = preserved weekly opening price (from the existing in-progress week,
            else the first live mid once a new week begins)
    high  = max(existing weekly high, incoming live mid)
    low   = min(existing weekly low,  incoming live mid)
    close = incoming live mid

Only the active candle is updated — no new candle is created per quote. When the
calendar week rolls over, a fresh active candle is started with ``open`` = the
first live mid of the new week.

Weekly candles in the export are labelled with the week-ending Sunday, so the
active candle's ``date`` is computed the same way for continuity.
"""

from __future__ import annotations

import json
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any

from hptl.config import PROCESSED_DIR

_WS_OHLC_FILE = PROCESSED_DIR / "workstation_ohlc_latest.json"


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def week_end_sunday(d: date) -> str:
    """Week-ending Sunday label (matches the weekly OHLC export convention)."""
    days_until_sunday = (6 - d.weekday()) % 7
    return (d + timedelta(days=days_until_sunday)).isoformat()


class LiveWeeklyCandleTracker:
    """Maintain the active weekly candle per instrument from live midpoints."""

    def __init__(self, ohlc_path=_WS_OHLC_FILE) -> None:
        self._path = ohlc_path
        self._lock = threading.Lock()
        self._latest_completed: dict[str, dict[str, Any]] = {}
        self._active: dict[str, dict[str, Any]] = {}
        self._loaded = False

    def load(self) -> None:
        """Read the latest completed weekly candle per instrument (once)."""
        latest: dict[str, dict[str, Any]] = {}
        try:
            doc = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            doc = {}
        for key, block in (doc.get("instruments") or {}).items():
            weekly = block.get("weekly_ohlc") or block.get("weekly") or []
            if not weekly:
                continue
            last = weekly[-1]
            o, h, low, c = (
                _num(last.get("open")),
                _num(last.get("high")),
                _num(last.get("low")),
                _num(last.get("close")),
            )
            if None in (o, h, low, c):
                continue
            latest[key] = {
                "date": last.get("date"),
                "open": o,
                "high": h,
                "low": low,
                "close": c,
            }
        with self._lock:
            self._latest_completed = latest
            self._active = {}
            self._loaded = True

    def _current_week_label(self) -> str:
        return week_end_sunday(datetime.now(timezone.utc).date())

    def update(self, internal_key: str, mid: float | None, ts: str | None = None) -> dict[str, Any] | None:
        """Fold a live midpoint into the active weekly candle. Returns the candle."""
        m = _num(mid)
        if m is None:
            return None
        if not self._loaded:
            self.load()
        now_iso = ts or datetime.now(timezone.utc).isoformat()
        week = self._current_week_label()

        with self._lock:
            active = self._active.get(internal_key)
            if active is None or active.get("date") != week:
                base = self._latest_completed.get(internal_key)
                if active is None and base is not None and base.get("date") == week:
                    # Export already contains the in-progress week: preserve its open.
                    active = {
                        "date": week,
                        "open": base["open"],
                        "high": base["high"],
                        "low": base["low"],
                        "close": base["close"],
                        "source": "historical_week",
                    }
                else:
                    # New week (or nothing to seed from): open at first live mid.
                    active = {
                        "date": week,
                        "open": m,
                        "high": m,
                        "low": m,
                        "close": m,
                        "source": "live_open",
                    }
            active["high"] = max(active["high"], m)
            active["low"] = min(active["low"], m)
            active["close"] = m
            active["updated_at"] = now_iso
            active["live"] = True
            self._active[internal_key] = active
            return dict(active)

    def get(self, internal_key: str) -> dict[str, Any] | None:
        with self._lock:
            active = self._active.get(internal_key)
            return dict(active) if active else None

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._active.items()}
