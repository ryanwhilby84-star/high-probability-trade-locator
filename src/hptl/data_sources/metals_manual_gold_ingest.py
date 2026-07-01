"""Phase 4E — manual Gold driver ingest from data/manual/metals/."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import PROJECT_ROOT

MANUAL_DIR = PROJECT_ROOT / "data" / "manual" / "metals"
MIN_OBS = 52
MAX_STALE_DAYS = 45

GOLD_CB_STEM = "gold_cb_purchases"
GOLD_ETF_STEM = "gold_etf_holdings"
CB_CACHE_REL = "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json"
ETF_CACHE_REL = "data/cache/metals_drivers/gold_etf_holdings.json"


@dataclass
class ManualIngestResult:
    driver_id: str
    manual_path: str | None
    cache_path: str
    status: str  # ok | missing | blocked | error
    latest_date: str | None
    observation_count: int
    source_name: str
    source_id: str
    cache_written: bool
    blocker_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "driver_id": self.driver_id,
            "manual_path": self.manual_path,
            "cache_path": self.cache_path,
            "status": self.status,
            "latest_date": self.latest_date,
            "observation_count": self.observation_count,
            "source_name": self.source_name,
            "source_id": self.source_id,
            "cache_written": self.cache_written,
            "blocker_reason": self.blocker_reason,
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def find_manual_file(stem: str) -> Path | None:
    if not MANUAL_DIR.is_dir():
        return None
    for ext in (".csv", ".xlsx", ".xls", ".json"):
        path = MANUAL_DIR / f"{stem}{ext}"
        if path.is_file():
            return path
    return None


def _is_finite_num(v: Any) -> bool:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


def _normalize_date(raw: Any) -> str | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    dt = pd.to_datetime(raw, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _pick_columns(df: pd.DataFrame) -> tuple[str, str] | None:
    cols = {str(c).strip().lower(): c for c in df.columns}
    date_col = next(
        (
            cols[k]
            for k in cols
            if k in ("date", "observation_date", "period", "quarter", "month", "time")
            or "date" in k
            or "period" in k
            or "quarter" in k
        ),
        None,
    )
    val_col = next(
        (
            cols[k]
            for k in cols
            if k in ("value", "amount", "tonnes", "tons", "net", "holdings", "shares", "shares_outstanding")
            or "value" in k
            or "tonne" in k
            or "purchase" in k
            or "holding" in k
            or "share" in k
        ),
        None,
    )
    if date_col is None and len(df.columns) >= 2:
        date_col = df.columns[0]
    if val_col is None and len(df.columns) >= 2:
        val_col = df.columns[1]
    if date_col is None or val_col is None:
        return None
    return str(date_col), str(val_col)


def _observations_from_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    picked = _pick_columns(df)
    if not picked:
        return []
    date_col, val_col = picked
    obs: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        d = _normalize_date(row[date_col])
        if not d:
            continue
        if not _is_finite_num(row[val_col]):
            continue
        obs.append({"date": d, "value": float(row[val_col])})
    obs.sort(key=lambda x: x["date"])
    return obs


def _load_manual_observations(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        doc = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(doc, list):
            rows = doc
        else:
            rows = doc.get("observations") or doc.get("series") or doc.get("data") or []
        obs: list[dict[str, Any]] = []
        if isinstance(rows, dict):
            for k, v in rows.items():
                d = _normalize_date(k)
                if d and _is_finite_num(v):
                    obs.append({"date": d, "value": float(v)})
        else:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                d = _normalize_date(row.get("date") or row.get("observation_date"))
                val = row.get("value")
                if d and _is_finite_num(val):
                    obs.append({"date": d, "value": float(val)})
        obs.sort(key=lambda x: x["date"])
        return obs

    if suffix in {".csv"}:
        df = pd.read_csv(path)
        return _observations_from_dataframe(df)

    if suffix in {".xlsx", ".xls"}:
        xl = pd.ExcelFile(path)
        best: list[dict[str, Any]] = []
        for sheet in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sheet)
            obs = _observations_from_dataframe(df)
            if len(obs) > len(best):
                best = obs
        return best

    return []


def _validate_observations(obs: list[dict[str, Any]]) -> tuple[bool, str | None]:
    if not obs:
        return False, "no observations parsed"
    if len(obs) < MIN_OBS:
        return False, f"insufficient observations ({len(obs)} < {MIN_OBS})"
    if any(o.get("value") is None for o in obs):
        return False, "empty values present"
    latest = obs[-1]["date"]
    try:
        delta = (datetime.now(timezone.utc).date() - datetime.strptime(latest, "%Y-%m-%d").date()).days
    except ValueError:
        return False, f"invalid latest date {latest}"
    if delta > MAX_STALE_DAYS:
        return False, f"latest date {latest} stale ({delta} days old)"
    return True, None


def _write_cache(
    rel_path: str,
    *,
    driver_id: str,
    unit: str,
    source_name: str,
    source_id: str,
    manual_path: Path,
    observations: list[dict[str, Any]],
    notes: str,
) -> Path:
    path = PROJECT_ROOT / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "driver_id": driver_id,
        "generated_at": _now_iso(),
        "source_name": source_name,
        "source_id": source_id,
        "manual_source_path": str(manual_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "unit": unit,
        "notes": notes,
        "observation_count": len(observations),
        "latest_date": observations[-1]["date"],
        "observations": observations,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def ingest_manual_gold_cb() -> ManualIngestResult:
    manual = find_manual_file(GOLD_CB_STEM)
    if manual is None:
        return ManualIngestResult(
            driver_id="cb_net_purchases",
            manual_path=None,
            cache_path=CB_CACHE_REL,
            status="missing",
            latest_date=None,
            observation_count=0,
            source_name="Manual WGC export",
            source_id=GOLD_CB_STEM,
            cache_written=False,
            blocker_reason=f"Place file at data/manual/metals/{GOLD_CB_STEM}.csv|.xlsx|.json",
        )
    try:
        obs = _load_manual_observations(manual)
        ok, reason = _validate_observations(obs)
        if not ok:
            return ManualIngestResult(
                driver_id="cb_net_purchases",
                manual_path=str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                cache_path=CB_CACHE_REL,
                status="blocked",
                latest_date=obs[-1]["date"] if obs else None,
                observation_count=len(obs),
                source_name="Manual WGC export",
                source_id=manual.name,
                cache_written=False,
                blocker_reason=reason,
            )
        _write_cache(
            CB_CACHE_REL,
            driver_id="cb_net_purchases",
            unit="tonnes",
            source_name="Manual WGC export",
            source_id=manual.name,
            manual_path=manual,
            observations=obs,
            notes="Central bank net gold purchases from manual WGC export.",
        )
        return ManualIngestResult(
            driver_id="cb_net_purchases",
            manual_path=str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            cache_path=CB_CACHE_REL,
            status="ok",
            latest_date=obs[-1]["date"],
            observation_count=len(obs),
            source_name="Manual WGC export",
            source_id=manual.name,
            cache_written=True,
        )
    except Exception as exc:
        return ManualIngestResult(
            driver_id="cb_net_purchases",
            manual_path=str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            cache_path=CB_CACHE_REL,
            status="error",
            latest_date=None,
            observation_count=0,
            source_name="Manual WGC export",
            source_id=manual.name,
            cache_written=False,
            blocker_reason=str(exc),
        )


def ingest_manual_gold_etf() -> ManualIngestResult:
    manual = find_manual_file(GOLD_ETF_STEM)
    if manual is None:
        return ManualIngestResult(
            driver_id="gold_etf_holdings",
            manual_path=None,
            cache_path=ETF_CACHE_REL,
            status="missing",
            latest_date=None,
            observation_count=0,
            source_name="Manual GLD export",
            source_id=GOLD_ETF_STEM,
            cache_written=False,
            blocker_reason=f"Place file at data/manual/metals/{GOLD_ETF_STEM}.csv|.xlsx|.json",
        )
    try:
        obs = _load_manual_observations(manual)
        ok, reason = _validate_observations(obs)
        if not ok:
            return ManualIngestResult(
                driver_id="gold_etf_holdings",
                manual_path=str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                cache_path=ETF_CACHE_REL,
                status="blocked",
                latest_date=obs[-1]["date"] if obs else None,
                observation_count=len(obs),
                source_name="Manual GLD export",
                source_id=manual.name,
                cache_written=False,
                blocker_reason=reason,
            )
        _write_cache(
            ETF_CACHE_REL,
            driver_id="gold_etf_holdings",
            unit="shares_outstanding",
            source_name="Manual GLD export",
            source_id=manual.name,
            manual_path=manual,
            observations=obs,
            notes="GLD ETF holdings/shares outstanding from manual export.",
        )
        return ManualIngestResult(
            driver_id="gold_etf_holdings",
            manual_path=str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            cache_path=ETF_CACHE_REL,
            status="ok",
            latest_date=obs[-1]["date"],
            observation_count=len(obs),
            source_name="Manual GLD export",
            source_id=manual.name,
            cache_written=True,
        )
    except Exception as exc:
        return ManualIngestResult(
            driver_id="gold_etf_holdings",
            manual_path=str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            cache_path=ETF_CACHE_REL,
            status="error",
            latest_date=None,
            observation_count=0,
            source_name="Manual GLD export",
            source_id=manual.name,
            cache_written=False,
            blocker_reason=str(exc),
        )


def run_phase4e_gold_manual_ingest() -> dict[str, Any]:
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)
    results = [ingest_manual_gold_cb(), ingest_manual_gold_etf()]
    all_ok = all(r.status == "ok" and r.cache_written for r in results)
    return {
        "phase": "4E",
        "generated_at": _now_iso(),
        "ready_for_model": all_ok,
        "drivers": [r.to_dict() for r in results],
    }
