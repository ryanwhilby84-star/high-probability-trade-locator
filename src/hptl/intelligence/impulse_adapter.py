"""Intermarket impulse from processed COT master (related markets) — no live price feeds."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import PROCESSED_DIR
from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED, instrument_profile

TRACKED_MASTER_FILENAME = "cot_tracked_master_normalized.csv"


def tracked_master_path() -> Path:
    return PROCESSED_DIR / TRACKED_MASTER_FILENAME


def _latest_weekly_change(df: pd.DataFrame, market: str) -> float | None:
    sub = df.loc[df["market"] == market].sort_values("cot_report_date")
    if sub.empty or "weekly_change" not in sub.columns:
        return None
    last = sub.iloc[-1]
    v = last.get("weekly_change")
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def compute_simple_impulse(
    instrument: str,
    *,
    catalyst_cfg: dict[str, Any],
    master_csv_path: Path | None = None,
) -> dict[str, Any]:
    """Align latest weekly COT flow with related markets (same report week, processed file only).

    Returns ``impulse_score`` in [0, 10] when enough data exists; otherwise availability message.
    """
    path = master_csv_path or tracked_master_path()
    if not path.exists():
        return {
            "instrument": instrument,
            "impulse_score": None,
            "impulse_detail": [],
            "availability": SOURCE_NOT_CONFIGURED,
            "source_path": str(path),
        }

    try:
        df = pd.read_csv(path, low_memory=False)
    except OSError:
        return {
            "instrument": instrument,
            "impulse_score": None,
            "impulse_detail": [],
            "availability": "not available — could not read COT master",
            "source_path": str(path),
        }

    if "market" not in df.columns or "cot_report_date" not in df.columns:
        return {
            "instrument": instrument,
            "impulse_score": None,
            "impulse_detail": [],
            "availability": "not available — COT master missing required columns",
            "source_path": str(path),
        }

    df = df.copy()
    df["cot_report_date"] = pd.to_datetime(df["cot_report_date"], errors="coerce").dt.normalize()

    prof = instrument_profile(catalyst_cfg, instrument) or {}
    related = prof.get("related_markets")
    if not isinstance(related, list) or not related:
        return {
            "instrument": instrument,
            "impulse_score": 5.0,
            "impulse_detail": [],
            "availability": "neutral — no related_markets in catalyst config",
            "source_path": str(path),
        }

    d0 = _latest_weekly_change(df, instrument)
    detail: list[dict[str, Any]] = []
    if d0 is None:
        return {
            "instrument": instrument,
            "impulse_score": None,
            "impulse_detail": [],
            "availability": "not available — missing weekly_change for instrument",
            "source_path": str(path),
        }

    support = 0
    conflict = 0
    usable = 0
    for rm in related:
        if not isinstance(rm, str) or not rm.strip():
            continue
        dr = _latest_weekly_change(df, rm)
        row: dict[str, Any] = {"related_market": rm, "weekly_change": dr}
        if dr is None:
            detail.append(row)
            continue
        usable += 1
        if (d0 > 0 and dr > 0) or (d0 < 0 and dr < 0) or (d0 == 0 and dr == 0):
            support += 1
            row["alignment"] = "same_sign"
        elif d0 == 0 or dr == 0:
            row["alignment"] = "neutral_leg"
        else:
            conflict += 1
            row["alignment"] = "opposite_sign"
        detail.append(row)

    if usable == 0:
        return {
            "instrument": instrument,
            "impulse_score": None,
            "impulse_detail": detail,
            "availability": "not available — no related market weekly_change in master",
            "source_path": str(path),
        }

    raw = 5.0 + 5.0 * (support - conflict) / usable
    score = float(max(0.0, min(10.0, raw)))
    return {
        "instrument": instrument,
        "impulse_score": round(score, 2),
        "impulse_detail": detail,
        "availability": "cot_tracked_master_normalized.csv",
        "source_path": str(path),
    }
