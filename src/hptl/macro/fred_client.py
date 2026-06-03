"""Resilient FRED series client: cache-first, live-second, never lose data.

This module is the Stage B replacement for the bare ``requests.get`` that made
FRED a single point of failure. It provides:

  * Persistent on-disk cache (CSV data + JSON metadata sidecar) per series.
  * Retry with exponential backoff + jitter on live fetches.
  * Cache fallback so a transient FRED outage (timeout / 502 / 504) can never
    blank an existing series — the last good copy is served instead.
  * A refresh log tracking last successful / last failed refresh (global + per
    series) for the macro audit and dashboard health panel.

It does NOT change any scoring, confluence, COT, valuation, relative-strength or
radar logic. It only hardens how macro price/driver series are obtained.

Environment switches:
  HPTL_SKIP_LIVE_FEEDS=1   -> cache-only mode (no network); used by the COT/build job.
  HPTL_MACRO_CACHE=0       -> kill switch: bypass cache, legacy direct fetch.
  HPTL_MACRO_CACHE_DIR     -> override cache directory.
  HPTL_MACRO_FETCH_RETRIES -> live-fetch attempts (default 4).
  HPTL_MACRO_FETCH_TIMEOUT -> per-attempt timeout seconds (default 45).
"""

from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

FRED_GRAPH_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"

_DEFAULT_RETRIES = 4
_DEFAULT_TIMEOUT = 45
_BACKOFF_BASE_S = 2.0
_BACKOFF_FACTOR = 2.0
_BACKOFF_MAX_S = 30.0

# Per-process record of where the last get_series_df result came from.
_LAST_SOURCE: dict[tuple[str, str], str] = {}


class FredUnavailable(RuntimeError):
    """Raised when a series cannot be served live or from cache."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _skip_live_feeds() -> bool:
    return str(os.environ.get("HPTL_SKIP_LIVE_FEEDS", "")).strip().lower() in {"1", "true", "yes"}


def _cache_enabled() -> bool:
    return str(os.environ.get("HPTL_MACRO_CACHE", "1")).strip().lower() not in {"0", "false", "no"}


def cache_dir() -> Path:
    override = os.environ.get("HPTL_MACRO_CACHE_DIR", "").strip()
    if override:
        d = Path(override)
    else:
        # Resolve project data dir without importing config (avoids cycles).
        try:
            from hptl.config import DATA_DIR  # type: ignore

            d = Path(DATA_DIR) / "macro_cache"
        except Exception:
            d = Path("data") / "macro_cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _retries() -> int:
    try:
        return max(1, int(os.environ.get("HPTL_MACRO_FETCH_RETRIES", _DEFAULT_RETRIES)))
    except ValueError:
        return _DEFAULT_RETRIES


def _timeout() -> int:
    try:
        return max(5, int(os.environ.get("HPTL_MACRO_FETCH_TIMEOUT", _DEFAULT_TIMEOUT)))
    except ValueError:
        return _DEFAULT_TIMEOUT


def _safe_key(series_id: str, observation_start: str) -> str:
    raw = f"{series_id}__{observation_start}"
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in raw)


def _data_path(series_id: str, observation_start: str) -> Path:
    return cache_dir() / f"{_safe_key(series_id, observation_start)}.csv"


def _meta_path(series_id: str, observation_start: str) -> Path:
    return cache_dir() / f"{_safe_key(series_id, observation_start)}.meta.json"


def _refresh_log_path() -> Path:
    return cache_dir() / "_refresh_log.json"


# --------------------------------------------------------------------------- #
# Parsing / validation
# --------------------------------------------------------------------------- #

def _parse_fred_csv(text: str, series_id: str) -> pd.DataFrame:
    df = pd.read_csv(StringIO(text))
    if df.shape[1] < 2:
        raise ValueError(f"Unexpected FRED CSV shape for {series_id}")
    df = df.iloc[:, :2].copy()
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
    df = df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
    if len(df) < 2:
        raise ValueError(f"FRED CSV for {series_id} has too few valid rows ({len(df)})")
    return df


def _observation_end(df: pd.DataFrame) -> str | None:
    if df.empty:
        return None
    try:
        return pd.Timestamp(df["date"].iloc[-1]).strftime("%Y-%m-%d")
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Cache I/O
# --------------------------------------------------------------------------- #

def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _save_cache(series_id: str, observation_start: str, df: pd.DataFrame, *, http_status: int) -> dict[str, Any]:
    meta = {
        "series_id": series_id,
        "observation_start": observation_start,
        "fetched_at": _now_iso(),
        "http_status": http_status,
        "row_count": int(len(df)),
        "observation_end": _observation_end(df),
        "source_url": f"{FRED_GRAPH_CSV}?id={series_id}&cosd={observation_start}",
    }
    if not _cache_enabled():
        return meta
    try:
        _write_atomic(_data_path(series_id, observation_start), df.to_csv(index=False))
        _write_atomic(_meta_path(series_id, observation_start), json.dumps(meta, ensure_ascii=False, indent=2))
    except OSError:
        pass
    return meta


def _load_cache(series_id: str, observation_start: str) -> tuple[pd.DataFrame | None, dict[str, Any] | None]:
    if not _cache_enabled():
        return None, None
    dpath = _data_path(series_id, observation_start)
    if not dpath.exists():
        return None, None
    try:
        df = pd.read_csv(dpath)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
        if df.empty:
            return None, None
    except (OSError, ValueError, KeyError):
        return None, None
    meta = read_meta(series_id, observation_start)
    return df, meta


def read_meta(series_id: str, observation_start: str) -> dict[str, Any] | None:
    """Read the cached metadata sidecar for a series (no network)."""
    if not _cache_enabled():
        return None
    mpath = _meta_path(series_id, observation_start)
    if not mpath.exists():
        return None
    try:
        return json.loads(mpath.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def last_source(series_id: str, observation_start: str) -> str | None:
    """Where the most recent get_series_df result for this series came from."""
    return _LAST_SOURCE.get((series_id, observation_start))


# --------------------------------------------------------------------------- #
# Refresh log
# --------------------------------------------------------------------------- #

def _read_refresh_log() -> dict[str, Any]:
    path = _refresh_log_path()
    if not path.exists():
        return {"last_success": None, "last_failure": None, "series": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("series", {})
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {"last_success": None, "last_failure": None, "series": {}}


def _record_refresh(series_id: str, observation_start: str, *, ok: bool, error: str | None = None) -> None:
    if not _cache_enabled():
        return
    log = _read_refresh_log()
    ts = _now_iso()
    key = _safe_key(series_id, observation_start)
    entry = log["series"].get(key, {})
    if ok:
        log["last_success"] = ts
        entry["last_success"] = ts
        entry["last_status"] = "ok"
        entry["last_error"] = None
    else:
        log["last_failure"] = ts
        entry["last_failure"] = ts
        entry["last_status"] = "fail"
        entry["last_error"] = (error or "")[:300]
    entry["series_id"] = series_id
    log["series"][key] = entry
    try:
        _write_atomic(_refresh_log_path(), json.dumps(log, ensure_ascii=False, indent=2))
    except OSError:
        pass


def refresh_log() -> dict[str, Any]:
    """Public read of the refresh log (last success/failure timestamps)."""
    return _read_refresh_log()


# --------------------------------------------------------------------------- #
# Live fetch with retry + backoff
# --------------------------------------------------------------------------- #

def _http_fetch(series_id: str, observation_start: str) -> tuple[pd.DataFrame, int]:
    url = f"{FRED_GRAPH_CSV}?id={series_id}&cosd={observation_start}"
    r = requests.get(url, timeout=_timeout())
    status = r.status_code
    r.raise_for_status()
    return _parse_fred_csv(r.text, series_id), status


def _fetch_with_retry(series_id: str, observation_start: str) -> tuple[pd.DataFrame, int]:
    attempts = _retries()
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return _http_fetch(series_id, observation_start)
        except Exception as exc:  # network/HTTP/parse — all transient-ish
            last_exc = exc
            if attempt < attempts:
                backoff = min(_BACKOFF_MAX_S, _BACKOFF_BASE_S * (_BACKOFF_FACTOR ** (attempt - 1)))
                backoff += random.uniform(0, backoff * 0.25)  # jitter
                time.sleep(backoff)
    raise last_exc if last_exc else FredUnavailable(f"fetch failed: {series_id}")


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def get_series_df(
    series_id: str,
    observation_start: str,
    *,
    allow_live: bool | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Return a FRED series as a DataFrame, cache-first with live fallback.

    Order of operations:
      1. Kill switch (HPTL_MACRO_CACHE=0): legacy direct fetch, no cache.
      2. Cache-only mode (HPTL_SKIP_LIVE_FEEDS=1 or allow_live=False): serve the
         cached copy if present; otherwise raise FredUnavailable.
      3. Live allowed: attempt live fetch with retry/backoff; on success refresh
         the cache; on failure fall back to the cached copy (never lose data);
         if there is no cache either, raise FredUnavailable.

    Raises FredUnavailable when no data can be produced (the caller's existing
    try/except turns this into a per-market ``available: False`` payload).
    """
    key = (series_id, observation_start)

    if not _cache_enabled():
        df, status = _fetch_with_retry(series_id, observation_start)
        _LAST_SOURCE[key] = "live"
        return df

    skip_live = (allow_live is False) or (allow_live is None and _skip_live_feeds())
    cached_df, _cached_meta = _load_cache(series_id, observation_start)

    if skip_live:
        if cached_df is not None:
            _LAST_SOURCE[key] = "cache"
            return cached_df
        raise FredUnavailable(
            f"Cache-only mode (HPTL_SKIP_LIVE_FEEDS) and no cached copy for {series_id}."
        )

    if not force_refresh and cached_df is None:
        # No cache yet — must go live (handled below).
        pass

    try:
        df, status = _fetch_with_retry(series_id, observation_start)
        _save_cache(series_id, observation_start, df, http_status=status)
        _record_refresh(series_id, observation_start, ok=True)
        _LAST_SOURCE[key] = "live"
        return df
    except Exception as exc:
        _record_refresh(series_id, observation_start, ok=False, error=f"{type(exc).__name__}: {exc}")
        if cached_df is not None:
            _LAST_SOURCE[key] = "cache"
            return cached_df
        raise FredUnavailable(
            f"FRED fetch failed and no cache for {series_id}: {type(exc).__name__}: {exc}"
        ) from exc
