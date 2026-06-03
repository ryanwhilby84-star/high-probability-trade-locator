"""FRED Web Services API — series metadata check and observation download.

When ``FRED_API_KEY`` is unset, callers should fall back to public graph CSV
(``rates_downloader``) or report ``source unavailable``.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests

from hptl.config import get_fred_api_key, get_settings

FRED_API_ROOT = "https://api.stlouisfed.org/fred/"

# Column name -> FRED series id (aligned with ``rates_downloader.SERIES``).
SERIES_IDS = {
    "dgs2": "DGS2",
    "dgs10": "DGS10",
    "dgs30": "DGS30",
    "fed_funds": "DFF",
    "t10y2y": "T10Y2Y",
}


def check_fred_api_connectivity(api_key: str | None = None) -> tuple[bool, str]:
    """Lightweight GET (series metadata). Returns ``(ok, message)``."""
    key = (api_key or get_fred_api_key()).strip()
    if not key:
        return False, "source unavailable — FRED_API_KEY not set"
    settings = get_settings()
    params = {"series_id": "DGS10", "api_key": key, "file_type": "json"}
    url = f"{FRED_API_ROOT}series?{urlencode(params)}"
    try:
        r = requests.get(url, timeout=settings.request_timeout_seconds)
    except requests.RequestException as exc:
        return False, f"source unavailable — FRED request failed: {type(exc).__name__}: {exc}"
    if r.status_code != 200:
        return False, f"source unavailable — FRED HTTP {r.status_code}"
    try:
        payload = r.json()
    except ValueError:
        return False, "source unavailable — FRED response not JSON"
    if not isinstance(payload, dict) or "seriess" not in payload:
        return False, "source unavailable — unexpected FRED series payload"
    series_list = payload.get("seriess")
    if not isinstance(series_list, list) or not series_list:
        return False, "source unavailable — empty FRED series list"
    return True, "ok"


def _fetch_observations_frame(series_id: str, api_key: str, *, observation_start: str) -> pd.DataFrame:
    settings = get_settings()
    params: dict[str, str] = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "sort_order": "asc",
    }
    url = f"{FRED_API_ROOT}series/observations?{urlencode(params)}"
    r = requests.get(url, timeout=settings.request_timeout_seconds)
    r.raise_for_status()
    payload = r.json()
    obs = payload.get("observations") if isinstance(payload, dict) else None
    if not isinstance(obs, list):
        raise ValueError(f"FRED observations missing for {series_id}")
    rows: list[dict[str, Any]] = []
    for o in obs:
        if not isinstance(o, dict):
            continue
        d = str(o.get("date") or "").strip()
        v = o.get("value")
        if not d:
            continue
        rows.append({"date": d, "value": v})
    if not rows:
        raise ValueError(f"FRED returned zero observation rows for {series_id}")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
    df = df.dropna(subset=["date"])
    return df


def download_macro_series_via_fred_api(api_key: str, *, observation_start: str | None = None) -> pd.DataFrame:
    """Download all macro yield series via FRED API; same wide shape as graph ``download_all``."""
    from hptl.macro.rates_downloader import RAW_PATH, START_DATE

    start = observation_start or START_DATE
    key = api_key.strip()
    if not key:
        raise ValueError("FRED API key is empty")

    merged: pd.DataFrame | None = None
    for col_name, series_id in SERIES_IDS.items():
        sdf = _fetch_observations_frame(series_id, key, observation_start=start)
        sdf = sdf.rename(columns={"value": col_name})[["date", col_name]]
        merged = sdf if merged is None else merged.merge(sdf, on="date", how="outer")

    if merged is None or merged.empty:
        raise ValueError("No macro/rates data was downloaded from FRED API")

    merged = merged.sort_values("date").reset_index(drop=True)
    merged = merged[merged["date"] >= pd.Timestamp(start)]
    RAW_PATH.mkdir(parents=True, exist_ok=True)
    raw_file = RAW_PATH / "rates_raw.csv"
    merged.to_csv(raw_file, index=False)
    return merged


def fetch_latest_observations(
    api_key: str | None = None,
    *,
    series_ids: tuple[str, ...] = ("DGS2", "DGS10", "DGS30", "DFF", "T10Y2Y"),
) -> dict[str, Any]:
    """Most recent non-missing observation per series (for diagnostics / tests)."""
    key = (api_key or get_fred_api_key()).strip()
    out: dict[str, Any] = {"api_configured": bool(key), "series": {}}
    if not key:
        out["error"] = "source unavailable — FRED_API_KEY not set"
        return out
    settings = get_settings()
    for sid in series_ids:
        try:
            df = _fetch_observations_frame(sid, key, observation_start="2020-01-01")
        except (requests.RequestException, ValueError) as exc:
            out["series"][sid] = {"date": None, "value": None, "error": f"source unavailable — {exc}"}
            continue
        ok = df[df["value"].notna()]
        if ok.empty:
            out["series"][sid] = {"date": None, "value": None, "error": "source unavailable — no numeric values"}
            continue
        last = ok.iloc[-1]
        out["series"][sid] = {
            "date": pd.Timestamp(last["date"]).strftime("%Y-%m-%d"),
            "value": float(last["value"]),
        }
    return out
