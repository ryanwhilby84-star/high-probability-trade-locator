"""Central bank gold net purchases ingest (WGC manual xlsx + optional Goldhub session)."""

from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from hptl.config import PROJECT_ROOT

MANUAL_DIR = PROJECT_ROOT / "data" / "manual" / "metals"
CACHE_REL = "data/cache/metals_drivers/wgc_cb_gold_net_purchases.json"
STATUS_REL = "data/processed/gold_cb_driver_status_latest.json"
GOLD_CB_STEM = "gold_cb_purchases"
WGC_RESERVES_PAGE = "https://www.gold.org/goldhub/data/gold-reserves-by-country"
USER_AGENT = "Mozilla/5.0 (compatible; HPTL/cb-gold-ingest)"

MIN_OBS_DAILY = 52
MIN_OBS_MONTHLY = 36
MIN_OBS_QUARTERLY = 12
MAX_STALE_DAYS_MONTHLY = 120
MAX_STALE_DAYS_QUARTERLY = 120


@dataclass
class CbGoldIngestResult:
    driver_id: str = "cb_net_purchases"
    cache_path: str = CACHE_REL
    status: str = "missing"  # ok | blocked | missing | error
    latest_date: str | None = None
    observation_count: int = 0
    frequency: str = "unknown"
    source_name: str = "World Gold Council"
    source_id: str = ""
    manual_path: str | None = None
    cache_written: bool = False
    blocker_reason: str | None = None
    next_update_hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "driver_id": self.driver_id,
            "cache_path": self.cache_path,
            "status": self.status,
            "latest_date": self.latest_date,
            "observation_count": self.observation_count,
            "frequency": self.frequency,
            "source_name": self.source_name,
            "source_id": self.source_id,
            "manual_path": self.manual_path,
            "cache_written": self.cache_written,
            "blocker_reason": self.blocker_reason,
            "next_update_hint": self.next_update_hint,
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def find_manual_file() -> Path | None:
    if not MANUAL_DIR.is_dir():
        return None
    for ext in (".csv", ".xlsx", ".xls", ".json"):
        path = MANUAL_DIR / f"{GOLD_CB_STEM}{ext}"
        if path.is_file():
            return path
    return None


def _is_finite(v: Any) -> bool:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


def _normalize_date(raw: Any) -> str | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, str):
        s = raw.strip()
        qm = re.match(r"^Q([1-4])\s*[-/]?\s*(\d{4})$", s, re.I)
        if qm:
            q, y = int(qm.group(1)), int(qm.group(2))
            month = q * 3
            end = pd.Timestamp(year=y, month=month, day=1) + pd.offsets.MonthEnd(0)
            return end.strftime("%Y-%m-%d")
    dt = pd.to_datetime(raw, errors="coerce")
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt).strftime("%Y-%m-%d")


def infer_frequency(dates: list[str]) -> str:
    if len(dates) < 2:
        return "unknown"
    gaps = []
    for a, b in zip(dates[:-1], dates[1:]):
        try:
            gaps.append(
                (
                    datetime.strptime(b[:10], "%Y-%m-%d")
                    - datetime.strptime(a[:10], "%Y-%m-%d")
                ).days
            )
        except ValueError:
            continue
    if not gaps:
        return "unknown"
    med = sorted(gaps)[len(gaps) // 2]
    if med <= 7:
        return "daily"
    if med <= 35:
        return "monthly"
    if med <= 100:
        return "quarterly"
    return "annual"


def min_obs_for_frequency(freq: str) -> int:
    if freq == "quarterly":
        return MIN_OBS_QUARTERLY
    if freq == "monthly":
        return MIN_OBS_MONTHLY
    return MIN_OBS_DAILY


def max_stale_for_frequency(freq: str) -> int:
    if freq in {"quarterly", "monthly", "annual"}:
        return MAX_STALE_DAYS_MONTHLY
    return 45


def expand_to_month_end(obs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in obs:
        d = str(row["date"])[:10]
        y, m = int(d[:4]), int(d[5:7])
        end = (pd.Timestamp(year=y, month=m, day=1) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
        out.append({"date": end, "value": float(row["value"])})
    return out


def _parse_long_format(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = {str(c).strip().lower(): c for c in df.columns}
    date_col = next(
        (
            cols[k]
            for k in cols
            if k in ("date", "period", "quarter", "month", "time", "observation_date")
            or "date" in k
            or "period" in k
        ),
        None,
    )
    val_col = next(
        (
            cols[k]
            for k in cols
            if k in ("value", "net", "change", "tonnes", "tons", "net_purchases", "net purchases")
            or "value" in k
            or "tonne" in k
            or "change" in k
            or "net" in k
        ),
        None,
    )
    if date_col is None or val_col is None:
        return []
    obs: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        d = _normalize_date(row[date_col])
        if not d or not _is_finite(row[val_col]):
            continue
        obs.append({"date": d, "value": float(row[val_col])})
    obs.sort(key=lambda x: x["date"])
    return obs


def _parse_wgc_wide_sheet(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Parse WGC 'Changes in World Official Gold Reserves' wide layout."""
    if df.empty:
        return []
    header_row = None
    for i in range(min(15, len(df))):
        row = df.iloc[i].astype(str).str.lower()
        if row.str.contains("tonne", na=False).any() or row.str.contains("change", na=False).any():
            header_row = i
            break
    if header_row is None:
        header_row = 0
    body = df.iloc[header_row:].copy()
    body.columns = body.iloc[0]
    body = body.iloc[1:]
    body = body.dropna(how="all")
    if body.empty:
        return body  # type: ignore[return-value]

    label_col = body.columns[0]
    labels = body[label_col].astype(str).str.strip().str.lower()
    world_mask = labels.str.contains(r"^(world|total|all countries|global)\b", regex=True, na=False)
    target = body.loc[world_mask] if world_mask.any() else body

    period_cols: list[Any] = []
    for col in body.columns[1:]:
        d = _normalize_date(col)
        if d:
            period_cols.append(col)
    if not period_cols:
        for col in body.columns[1:]:
            try:
                pd.to_datetime(col, errors="raise")
                period_cols.append(col)
            except (TypeError, ValueError):
                continue

    obs: list[dict[str, Any]] = []
    for col in period_cols:
        d = _normalize_date(col)
        if not d:
            continue
        vals = pd.to_numeric(target[col], errors="coerce").dropna()
        if vals.empty:
            continue
        obs.append({"date": d, "value": float(vals.sum())})
    obs.sort(key=lambda x: x["date"])
    return obs


def parse_wgc_xlsx(path: Path) -> list[dict[str, Any]]:
    xl = pd.ExcelFile(path)
    best: list[dict[str, Any]] = []
    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet, header=None)
        long_df = pd.read_excel(xl, sheet_name=sheet)
        obs = _parse_long_format(long_df)
        if len(obs) <= len(best):
            obs = _parse_wgc_wide_sheet(df)
        if len(obs) > len(best):
            best = obs
    return best


def load_observations(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        doc = json.loads(path.read_text(encoding="utf-8"))
        rows = doc if isinstance(doc, list) else doc.get("observations") or doc.get("series") or []
        obs: list[dict[str, Any]] = []
        if isinstance(rows, dict):
            for k, v in rows.items():
                d = _normalize_date(k)
                if d and _is_finite(v):
                    obs.append({"date": d, "value": float(v)})
        else:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                d = _normalize_date(row.get("date") or row.get("observation_date"))
                if d and _is_finite(row.get("value")):
                    obs.append({"date": d, "value": float(row["value"])})
        obs.sort(key=lambda x: x["date"])
        return obs
    if suffix == ".csv":
        return _parse_long_format(pd.read_csv(path))
    if suffix in {".xlsx", ".xls"}:
        return parse_wgc_xlsx(path)
    return []


def validate_observations(obs: list[dict[str, Any]]) -> tuple[bool, str | None, str]:
    if not obs:
        return False, "no observations parsed", "unknown"
    freq = infer_frequency([o["date"] for o in obs])
    min_obs = min_obs_for_frequency(freq)
    if len(obs) < min_obs:
        return False, f"insufficient observations ({len(obs)} < {min_obs} for {freq})", freq
    latest = obs[-1]["date"]
    try:
        delta = (datetime.now(timezone.utc).date() - datetime.strptime(latest, "%Y-%m-%d").date()).days
    except ValueError:
        return False, f"invalid latest date {latest}", freq
    max_stale = max_stale_for_frequency(freq)
    if delta > max_stale:
        return False, f"latest date {latest} stale ({delta} days old; max {max_stale} for {freq})", freq
    return True, None, freq


def _write_cache(
    observations: list[dict[str, Any]],
    *,
    source_name: str,
    source_id: str,
    manual_path: Path | None,
    frequency: str,
    notes: str,
) -> Path:
    expanded = expand_to_month_end(observations)
    path = PROJECT_ROOT / CACHE_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "driver_id": "cb_net_purchases",
        "generated_at": _now_iso(),
        "source_name": source_name,
        "source_id": source_id,
        "manual_source_path": (
            str(manual_path.relative_to(PROJECT_ROOT)).replace("\\", "/") if manual_path else None
        ),
        "unit": "tonnes",
        "frequency": frequency,
        "notes": notes,
        "observation_count": len(expanded),
        "raw_observation_count": len(observations),
        "latest_date": expanded[-1]["date"],
        "observations": expanded,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def _fetch_wgc_changes_via_session() -> tuple[bytes | None, str | None]:
    cookie = os.environ.get("WGC_GOLDHUB_COOKIE", "").strip()
    if not cookie:
        return None, None
    page = requests.get(WGC_RESERVES_PAGE, timeout=60, headers={"User-Agent": USER_AGENT})
    page.raise_for_status()
    rel_links = re.findall(
        r'href="(/download/file/\d+/Changes[^"]+\.xlsx)"',
        page.text,
        flags=re.I,
    )
    if not rel_links:
        return None, "WGC Changes xlsx link not found on Goldhub page"
    url = "https://www.gold.org" + rel_links[0]
    resp = requests.get(
        url,
        timeout=90,
        headers={"User-Agent": USER_AGENT, "Cookie": cookie},
    )
    if resp.status_code != 200 or resp.content[:4] != b"PK\x03\x04":
        return None, f"WGC authenticated download failed (HTTP {resp.status_code})"
    return resp.content, url


def ingest_cb_gold_purchases(*, write_status: bool = True) -> CbGoldIngestResult:
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)
    manual = find_manual_file()

    if manual is not None:
        try:
            obs = load_observations(manual)
            ok, reason, freq = validate_observations(obs)
            rel = str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/")
            if not ok:
                result = CbGoldIngestResult(
                    status="blocked",
                    latest_date=obs[-1]["date"] if obs else None,
                    observation_count=len(obs),
                    frequency=freq,
                    source_id=manual.name,
                    manual_path=rel,
                    blocker_reason=reason,
                    next_update_hint="Update monthly from WGC Goldhub → Changes in World Official Gold Reserves",
                )
                if write_status:
                    write_driver_status(result)
                return result
            _write_cache(
                obs,
                source_name="Manual WGC export",
                source_id=manual.name,
                manual_path=manual,
                frequency=freq,
                notes="Central bank net gold purchases from WGC 'Changes in World Official Gold Reserves' manual export.",
            )
            result = CbGoldIngestResult(
                status="ok",
                latest_date=obs[-1]["date"],
                observation_count=len(obs),
                frequency=freq,
                source_id=manual.name,
                manual_path=rel,
                cache_written=True,
                next_update_hint="Next WGC monthly update due within first 10 days of month (data ~2 months in arrears)",
            )
            if write_status:
                write_driver_status(result)
            return result
        except Exception as exc:
            result = CbGoldIngestResult(
                status="error",
                source_id=manual.name,
                manual_path=str(manual.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                blocker_reason=str(exc),
            )
            if write_status:
                write_driver_status(result)
            return result

    content, src = _fetch_wgc_changes_via_session()
    if content:
        tmp = MANUAL_DIR / "_wgc_session_changes.xlsx"
        tmp.write_bytes(content)
        try:
            obs = load_observations(tmp)
            ok, reason, freq = validate_observations(obs)
            if ok:
                _write_cache(
                    obs,
                    source_name="World Gold Council",
                    source_id=src or "WGC Changes xlsx",
                    manual_path=tmp,
                    frequency=freq,
                    notes="Automated WGC Changes download via WGC_GOLDHUB_COOKIE session.",
                )
                result = CbGoldIngestResult(
                    status="ok",
                    latest_date=obs[-1]["date"],
                    observation_count=len(obs),
                    frequency=freq,
                    source_id=src or "WGC Changes xlsx",
                    manual_path=str(tmp.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                    cache_written=True,
                )
                if write_status:
                    write_driver_status(result)
                return result
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    result = CbGoldIngestResult(
        status="missing",
        blocker_reason=(
            "WGC Changes xlsx requires free Goldhub login (HTTP 403 without session). "
            "Download 'Changes in World Official Gold Reserves' from gold.org/goldhub/data/gold-reserves-by-country "
            f"and save as data/manual/metals/{GOLD_CB_STEM}.xlsx — or set WGC_GOLDHUB_COOKIE for automated fetch."
        ),
        next_update_hint="Monthly: drop updated WGC xlsx or append row to gold_cb_purchases.csv (<1 min)",
    )
    if write_status:
        write_driver_status(result)
    return result


def write_driver_status(result: CbGoldIngestResult) -> Path:
    path = PROJECT_ROOT / STATUS_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    cache_exists = (PROJECT_ROOT / CACHE_REL).is_file()
    doc = {
        "generated_at": _now_iso(),
        "driver_id": result.driver_id,
        "cache_exists": cache_exists,
        "ingest": result.to_dict(),
        "update_instructions": {
            "monthly_csv": f"Append one row (date,value) to data/manual/metals/{GOLD_CB_STEM}.csv",
            "monthly_xlsx": "Replace data/manual/metals/gold_cb_purchases.xlsx with latest WGC Changes export",
            "wgc_url": WGC_RESERVES_PAGE,
            "automated_option": "Set WGC_GOLDHUB_COOKIE to browser session cookie after gold.org login",
        },
    }
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def run_gold_cb_ingest() -> dict[str, Any]:
    result = ingest_cb_gold_purchases()
    return {"cb_net_purchases": result.to_dict()}
