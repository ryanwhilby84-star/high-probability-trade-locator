"""Stage A validation: rebuild macro relationship maps with live FRED feeds enabled.

This is a self-contained validation harness. It does NOT touch COT, scoring,
confluence, radar eligibility, valuation, or UI. It only:

  1. Forces HPTL_SKIP_LIVE_FEEDS=0 for this process.
  2. Rebuilds the macro relationship maps from live FRED data.
  3. Regenerates web-dashboard/public/data/macro_relationship_maps_latest.json
     (and the dist mirror) in the existing schema.
  4. Emits a per-asset verification report + before/after coverage report as JSON
     on stdout under the marker line ``=== VERIFY_JSON ===``.

Run from repo root:
    python scripts/verify_macro_relationship_maps.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

# Disable the feed gate for THIS rebuild only (process-local; no .env mutation).
os.environ["HPTL_SKIP_LIVE_FEEDS"] = "0"

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hptl.macro.macro_relationship_maps import (  # noqa: E402
    MACRO_RELATIONSHIP_MARKETS,
    _profiles,
)
import hptl.macro.fred_relationship_pair as frp  # noqa: E402
from hptl.macro.fred_relationship_pair import build_relationship_payload  # noqa: E402

# --- Validation-harness-only resilience (no production code is modified) -------
# The production FRED fetch uses a 45s timeout, no retry, and no cache, so a tight
# sequential burst gets throttled / hit by FRED-side 504s. For this validation we
# wrap the fetch with: (a) an in-run cache so the ~15 unique series shared across
# 13 markets are each fetched only once (DGS10 alone drives 7 markets), and
# (b) fail-fast retries. The downstream merge/rebasing/correlation logic is the
# unmodified production code.
import pickle  # noqa: E402

RETRY_ATTEMPTS = 2
RETRY_SLEEP_S = 4.0
PACING_SLEEP_S = 1.0
# Longer than production's 45s: FRED is slow right now and the large daily-history
# series (DGS10/SP500/DJIA/etc. from 2018) need more headroom than a 30s budget.
FETCH_TIMEOUT_S = 90
# Round-robin prefetch: FRED's fredgraph.csv is intermittently returning 502/504
# gateway errors right now, so we cycle through the unique series across several
# spaced rounds and lock each one in as it succeeds (cache survives across rounds).
PREFETCH_ROUNDS = 12
PREFETCH_INTRA_SLEEP_S = 4.0
PREFETCH_ROUND_SLEEP_S = 15.0

# Persistent disk cache so successful fetches survive across runs / FRED recovery
# windows and are never lost when the public endpoint flaps.
CACHE_DIR = REPO_ROOT / "data" / "_fred_validation_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_SERIES_CACHE: dict = {}
_orig_fetch = frp._fred_series_csv


def _cache_file(series_id: str, observation_start: str) -> Path:
    safe = f"{series_id}__{observation_start}".replace("/", "_")
    return CACHE_DIR / f"{safe}.pkl"


def _load_disk(series_id: str, observation_start: str):
    f = _cache_file(series_id, observation_start)
    if f.exists():
        try:
            return pickle.loads(f.read_bytes())
        except Exception:
            return None
    return None


def _save_disk(series_id: str, observation_start: str, df) -> None:
    try:
        _cache_file(series_id, observation_start).write_bytes(pickle.dumps(df))
    except Exception:
        pass


def _raw_fetch(series_id: str, observation_start: str):
    import requests
    import pandas as pd
    from io import StringIO

    url = f"{frp.FRED_GRAPH_CSV}?id={series_id}&cosd={observation_start}"
    r = requests.get(url, timeout=FETCH_TIMEOUT_S)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    if df.shape[1] < 2:
        raise ValueError(f"Unexpected FRED CSV for {series_id}")
    df = df.iloc[:, :2].copy()
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
    df = df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
    return df


def _cached_fetch(series_id: str, observation_start: str):
    """Cache-only wrapper used by the production build path (memory -> disk -> net)."""
    key = (series_id, observation_start)
    if key in _SERIES_CACHE:
        return _SERIES_CACHE[key].copy()
    disk = _load_disk(series_id, observation_start)
    if disk is not None:
        _SERIES_CACHE[key] = disk
        return disk.copy()
    df = _raw_fetch(series_id, observation_start)
    _SERIES_CACHE[key] = df
    _save_disk(series_id, observation_start, df)
    return df.copy()


frp._fred_series_csv = _cached_fetch


def warm_series_cache() -> dict:
    """Round-robin prefetch every unique (series_id, obs_start) needed by profiles."""
    profiles = _profiles()
    needed: set = set()
    for prof in profiles.values():
        obs = prof.get("observation_start") or frp.DEFAULT_OBS_START
        needed.add((prof["price_fred_id"], obs))
        needed.add((prof["driver_fred_id"], obs))
    pending = set()
    for key in needed:
        sid, obs = key
        disk = _load_disk(sid, obs)
        if disk is not None:
            _SERIES_CACHE[key] = disk
            print(f"    [disk] {sid} ({len(disk)} rows, cached)", flush=True)
        else:
            pending.add(key)
    print(f"[prefetch] {len(needed)} unique series; {len(pending)} need network", flush=True)
    for rnd in range(1, PREFETCH_ROUNDS + 1):
        if not pending:
            break
        print(f"[prefetch] round {rnd}: {len(pending)} pending", flush=True)
        for key in list(pending):
            sid, obs = key
            t = time.time()
            try:
                df = _raw_fetch(sid, obs)
                _SERIES_CACHE[key] = df
                _save_disk(sid, obs, df)
                pending.discard(key)
                print(f"    [ok] {sid} ({len(df)} rows, {round(time.time()-t,1)}s)", flush=True)
            except Exception as exc:
                print(f"    [miss] {sid}: {type(exc).__name__} ({round(time.time()-t,1)}s)", flush=True)
            time.sleep(PREFETCH_INTRA_SLEEP_S)
        if pending:
            time.sleep(PREFETCH_ROUND_SLEEP_S)
    return {
        "unique_series": len(needed),
        "cached": sorted(f"{k[0]}" for k in _SERIES_CACHE),
        "still_missing": sorted(f"{k[0]}" for k in pending),
    }


def _is_transient(err: str) -> bool:
    low = (err or "").lower()
    return any(t in low for t in ("timeout", "timed out", "connection", "temporarily", "429", "503", "502"))


def build_maps_resilient() -> dict:
    """Paced + retried per-market rebuild using the SAME production builders."""
    profiles = _profiles()
    out: dict = {}
    for market in MACRO_RELATIONSHIP_MARKETS:
        prof = profiles.get(market)
        if not prof:
            out[market] = {
                "available": False,
                "market": market,
                "driver_id": "unknown",
                "error": "No macro relationship profile configured for this market.",
            }
            continue
        payload = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                payload = build_relationship_payload(prof)
            except Exception as exc:  # mirror production per-market guard
                payload = {
                    "available": False,
                    "market": market,
                    "driver_id": str(prof.get("driver_id", "unknown")),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            if payload.get("available") is True:
                break
            err = str(payload.get("error") or "")
            if attempt < RETRY_ATTEMPTS and _is_transient(err):
                print(f"[retry] {market} attempt {attempt} failed ({err[:80]}); sleeping {RETRY_SLEEP_S}s", flush=True)
                time.sleep(RETRY_SLEEP_S)
                continue
            break
        out[market] = payload
        status = "OK" if payload.get("available") else "FAIL"
        print(f"[{status}] {market}", flush=True)
        time.sleep(PACING_SLEEP_S)
    return out

PUBLIC_JSON = REPO_ROOT / "web-dashboard" / "public" / "data" / "macro_relationship_maps_latest.json"
DIST_JSON = REPO_ROOT / "web-dashboard" / "dist" / "data" / "macro_relationship_maps_latest.json"

# The 9 assets the validation explicitly requires a report for.
REQUIRED_ASSETS = (
    "Copper / HG",
    "Gold",
    "Silver",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Wheat",
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
)


def _coverage(maps: dict) -> dict:
    available = sorted(k for k, v in maps.items() if v.get("available") is True)
    skipped = sorted(k for k, v in maps.items() if v.get("skipped") is True)
    missing = sorted(
        k
        for k, v in maps.items()
        if v.get("available") is not True and v.get("skipped") is not True
    )
    return {
        "available": available,
        "skipped": skipped,
        "missing": missing,
        "counts": {
            "available": len(available),
            "skipped": len(skipped),
            "missing": len(missing),
            "total": len(maps),
        },
    }


def _latency_days(latest: str | None) -> int | None:
    if not latest:
        return None
    try:
        d = datetime.strptime(latest, "%Y-%m-%d").date()
    except ValueError:
        return None
    return (date.today() - d).days


def _example_output(payload: dict) -> dict:
    """A compact slice proving the map carries real, populated data."""
    dates = payload.get("dates") or []
    price = payload.get("price_rebased_pct") or []
    driver = payload.get("driver_rebased_pct") or []
    corr20 = payload.get("rolling_corr_20") or []

    def _tail(seq, n=3):
        return seq[-n:] if isinstance(seq, list) else seq

    return {
        "observation_window": f"{payload.get('observation_start')} -> {payload.get('observation_end')}",
        "last_3_dates": _tail(dates),
        "last_3_price_rebased_pct": _tail(price),
        "last_3_driver_rebased_pct": _tail(driver),
        "last_3_rolling_corr_primary": _tail(corr20),
        "latest_rolling_corr_20": payload.get("latest_rolling_corr_20"),
        "latest_rolling_corr_30": payload.get("latest_rolling_corr_30"),
        "latest_rolling_corr_60": payload.get("latest_rolling_corr_60"),
        "correlation_regime": payload.get("correlation_regime"),
        "digest": payload.get("digest"),
    }


def _asset_report(name: str, payload: dict) -> dict:
    available = payload.get("available") is True
    skipped = payload.get("skipped") is True
    if available:
        status = "working"
    elif skipped:
        status = "skipped"
    else:
        status = "error"

    n_obs = len(payload.get("dates") or []) if available else 0
    latest = payload.get("latest_date") if available else None
    source = None
    if available:
        source = {
            "price_series_id": payload.get("price_series_id"),
            "price_series_display": payload.get("price_series_display"),
            "driver_series_id": payload.get("driver_series_id"),
            "driver_series_display": payload.get("driver_series_display"),
            "cadence": payload.get("cadence"),
        }

    return {
        "asset": name,
        "relationship_map_generated": "yes" if available else "no",
        "source_series_used": source,
        "latest_observation_date": latest,
        "latency_days": _latency_days(latest),
        "num_observations": n_obs,
        "status": status,
        "error": payload.get("error"),
        "example_output": _example_output(payload) if available else None,
    }


def main() -> int:
    before = {}
    if PUBLIC_JSON.exists():
        try:
            before = json.loads(PUBLIC_JSON.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            before = {}
    before_maps = before.get("macro_relationship_maps") or {}
    latest_cot_report_date = before.get("latest_cot_report_date")

    prefetch_summary = warm_series_cache()
    maps = build_maps_resilient()

    generated_at = datetime.now(timezone.utc).isoformat()
    new_doc = {
        "generated_at": generated_at,
        "latest_cot_report_date": latest_cot_report_date,
        "macro_relationship_maps": maps,
    }
    payload_json = json.dumps(new_doc, ensure_ascii=False, separators=(",", ":"))
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(payload_json, encoding="utf-8")
    if DIST_JSON.parent.exists():
        DIST_JSON.write_text(payload_json, encoding="utf-8")
        dist_written = str(DIST_JSON)
    else:
        dist_written = None

    report = {
        "generated_at": generated_at,
        "feed_gate": {"HPTL_SKIP_LIVE_FEEDS": os.environ.get("HPTL_SKIP_LIVE_FEEDS")},
        "prefetch_summary": prefetch_summary,
        "files_written": [str(PUBLIC_JSON)] + ([dist_written] if dist_written else []),
        "required_assets": [
            _asset_report(name, maps.get(name, {"available": False, "error": "not configured"}))
            for name in REQUIRED_ASSETS
        ],
        "coverage_before": _coverage(before_maps),
        "coverage_after": _coverage(maps),
    }

    print("=== VERIFY_JSON ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
