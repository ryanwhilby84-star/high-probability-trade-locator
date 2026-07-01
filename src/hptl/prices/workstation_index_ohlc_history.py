"""Workstation index OHLC history — visualization layer only.

Fetches multi-year real OHLC for US index workstation candles from OANDA CFD
instruments (e.g. NAS100_USD for NASDAQ / NQ). Does not modify prices_latest,
canonical timeline, COT, valuation, or seasonality exports.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR, get_fmp_api_key, get_oanda_api_key
from hptl.data_sources.audit_fmp import _extract_rows
from hptl.data_sources.fmp_client import FmpApiError, FmpClient
from hptl.prices.fx_oanda_backfill_feasibility_audit import OANDA_MAX_COUNT, _iso_from, _probe_candles

logger = logging.getLogger(__name__)

CACHE_DIR = DATA_DIR / "cache" / "workstation_ohlc"
CACHE_MAX_AGE_DAYS = 7

# Verified OANDA instrument names (underscore form). Registry cot_code uses NAS100USD.
WORKSTATION_INDEX_SOURCES: dict[str, dict[str, str]] = {
    "NASDAQ / NQ": {
        "oanda_symbol": "NAS100_USD",
        "fmp_fallback": "QQQ",
        "av_fallback": "QQQ",
        "proxy_note": "OANDA NAS100 USD CFD — tradable NQ index proxy with real OHLC",
    },
    "US Nas 100": {
        "oanda_symbol": "NAS100_USD",
        "fmp_fallback": "QQQ",
        "av_fallback": "QQQ",
        "proxy_note": "OANDA NAS100 USD CFD (proxy of NASDAQ / NQ)",
    },
    "S&P 500 / ES": {
        "oanda_symbol": "SPX500_USD",
        "fmp_fallback": "SPY",
        "av_fallback": "SPY",
        "proxy_note": "OANDA SPX500 USD CFD",
    },
    "US SPX 500": {
        "oanda_symbol": "SPX500_USD",
        "fmp_fallback": "SPY",
        "av_fallback": "SPY",
        "proxy_note": "OANDA SPX500 USD CFD (proxy of S&P 500 / ES)",
    },
    "Dow / YM": {
        "oanda_symbol": "US30_USD",
        "fmp_fallback": "DIA",
        "av_fallback": "DIA",
        "proxy_note": "OANDA US30 USD CFD",
    },
    "US Wall St 30": {
        "oanda_symbol": "US30_USD",
        "fmp_fallback": "DIA",
        "av_fallback": "DIA",
        "proxy_note": "OANDA US30 USD CFD (proxy of Dow / YM)",
    },
}


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _is_real_ohlc(open_: float | None, high: float | None, low: float | None, close: float | None) -> bool:
    if open_ is None or high is None or low is None or close is None:
        return False
    return high > low


def _cache_path(oanda_symbol: str) -> Path:
    safe = oanda_symbol.replace("/", "_")
    return CACHE_DIR / f"{safe}.json"


def _parse_window_start(window_start: str | None) -> date:
    if window_start:
        try:
            return datetime.strptime(str(window_start)[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    return date.today() - timedelta(days=10 * 366)


def _filter_daily_bars(
    bars: list[dict[str, Any]],
    *,
    window_start: date | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
  stats = {"input_rows": len(bars), "rejected_flat_rows": 0, "rejected_invalid_rows": 0, "accepted_rows": 0}
    out: list[dict[str, Any]] = []
    for bar in bars:
        d = str(bar.get("date") or "")[:10]
        o, h, l, c = _num(bar.get("open")), _num(bar.get("high")), _num(bar.get("low")), _num(bar.get("close"))
        if not d:
            stats["rejected_invalid_rows"] += 1
            continue
        if window_start and d < window_start.isoformat():
            continue
        if not _is_real_ohlc(o, h, l, c):
            if o is not None and c is not None and o == h == l == c:
                stats["rejected_flat_rows"] += 1
            else:
                stats["rejected_invalid_rows"] += 1
            continue
        out.append(
            {
                "date": d,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": _num(bar.get("volume")),
                "source": bar.get("source"),
            }
        )
    out.sort(key=lambda b: b["date"])
    stats["accepted_rows"] = len(out)
    return out, stats


def _fetch_oanda_daily(oanda_symbol: str, *, window_start: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    meta: dict[str, Any] = {
        "provider": "oanda",
        "symbol": oanda_symbol,
        "error": None,
    }
    if not get_oanda_api_key():
        meta["error"] = "OANDA_API_KEY not set"
        return [], meta

    fetch_from = window_start - timedelta(days=14)
    bars, probe = _probe_candles(
        oanda_symbol,
        from_time=_iso_from(fetch_from),
        count=OANDA_MAX_COUNT,
    )
    meta["probe"] = {k: v for k, v in probe.items() if k != "params"}
    if probe.get("error"):
        meta["error"] = probe["error"]
        return [], meta

    for bar in bars:
        bar["source"] = f"oanda:{oanda_symbol}"
    meta["fetched_rows"] = len(bars)
    meta["earliest_date"] = bars[0]["date"] if bars else None
    meta["latest_date"] = bars[-1]["date"] if bars else None
    return bars, meta


def _fetch_fmp_daily(symbol: str, *, window_start: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    meta: dict[str, Any] = {"provider": "fmp", "symbol": symbol, "error": None}
    if not get_fmp_api_key():
        meta["error"] = "FMP_API_KEY not set"
        return [], meta
    client = FmpClient()
    try:
        payload = client.get(f"api/v3/historical-price-full/{symbol}")
    except FmpApiError as exc:
        meta["error"] = str(exc)
        return [], meta

    rows = _extract_rows(payload)
    bars: list[dict[str, Any]] = []
    start_s = window_start.isoformat()
    for row in rows:
        d = str(row.get("date") or "")[:10]
        if not d or d < start_s:
            continue
        o, h, l, c = _num(row.get("open")), _num(row.get("high")), _num(row.get("low")), _num(row.get("close"))
        if o is None or c is None:
            continue
        bars.append(
            {
                "date": d,
                "open": o,
                "high": h if h is not None else o,
                "low": l if l is not None else o,
                "close": c,
                "volume": _num(row.get("volume")),
                "source": f"fmp:{symbol}",
            }
        )
    bars.sort(key=lambda b: b["date"])
    meta["fetched_rows"] = len(bars)
    meta["earliest_date"] = bars[0]["date"] if bars else None
    meta["latest_date"] = bars[-1]["date"] if bars else None
    return bars, meta


def _fetch_av_daily(symbol: str, *, window_start: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    meta: dict[str, Any] = {"provider": "alpha_vantage", "symbol": symbol, "error": None}
    try:
        from hptl.alpha_vantage.client import AlphaVantageApiError, _get
    except ImportError as exc:
        meta["error"] = str(exc)
        return [], meta

    bars: list[dict[str, Any]] = []
    for outputsize in ("full", "compact"):
        try:
            doc = _get("TIME_SERIES_DAILY", symbol=symbol, outputsize=outputsize)
        except AlphaVantageApiError as exc:
            meta["error"] = str(exc)
            continue
        series_key = next((k for k in doc if "Time Series" in k), None)
        if not series_key:
            continue
        series = doc[series_key]
        if not isinstance(series, dict):
            continue
        start_s = window_start.isoformat()
        for date_str, row in series.items():
            d = str(date_str)[:10]
            if d < start_s:
                continue
            o = _num(row.get("1. open") or row.get("open"))
            h = _num(row.get("2. high") or row.get("high"))
            l = _num(row.get("3. low") or row.get("low"))
            c = _num(row.get("4. close") or row.get("close"))
            if o is None or c is None:
                continue
            bars.append(
                {
                    "date": d,
                    "open": o,
                    "high": h if h is not None else o,
                    "low": l if l is not None else o,
                    "close": c,
                    "volume": _num(row.get("5. volume") or row.get("volume")),
                    "source": f"alpha_vantage:{symbol}",
                }
            )
        if bars:
            break

    bars.sort(key=lambda b: b["date"])
    meta["fetched_rows"] = len(bars)
    meta["earliest_date"] = bars[0]["date"] if bars else None
    meta["latest_date"] = bars[-1]["date"] if bars else None
    return bars, meta


def _load_cache(oanda_symbol: str) -> dict[str, Any] | None:
    path = _cache_path(oanda_symbol)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _cache_is_fresh(doc: dict[str, Any]) -> bool:
    ts = doc.get("generated_at")
    if not ts:
        return False
    try:
        gen = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if gen.tzinfo is None:
            gen = gen.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - gen
        return age.days < CACHE_MAX_AGE_DAYS
    except (TypeError, ValueError):
        return False


def _write_cache(oanda_symbol: str, payload: dict[str, Any]) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(oanda_symbol)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def resolve_workstation_index_source(instrument_id: str) -> dict[str, str] | None:
    return WORKSTATION_INDEX_SOURCES.get(instrument_id)


def load_workstation_index_daily_bars(
    instrument_id: str,
    *,
    window_start: str | None = None,
    refresh: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Load multi-year daily OHLC for workstation index candles."""
    spec = resolve_workstation_index_source(instrument_id)
    if not spec:
        return [], {"instrument_id": instrument_id, "configured": False}

    ws = _parse_window_start(window_start)
    oanda_symbol = spec["oanda_symbol"]
    diagnostics: dict[str, Any] = {
        "instrument_id": instrument_id,
        "configured": True,
        "oanda_symbol": oanda_symbol,
        "fmp_fallback": spec.get("fmp_fallback"),
        "av_fallback": spec.get("av_fallback"),
        "proxy_note": spec.get("proxy_note"),
        "window_start": ws.isoformat(),
        "source": None,
        "source_symbol": None,
        "cache_hit": False,
        "rejected_flat_rows": 0,
        "rejected_invalid_rows": 0,
        "input_rows": 0,
        "accepted_rows": 0,
    }

    cached = _load_cache(oanda_symbol)
    if cached and not refresh and _cache_is_fresh(cached):
        daily = cached.get("daily_bars") or []
        diagnostics.update(cached.get("diagnostics") or {})
        diagnostics["cache_hit"] = True
        filtered, stats = _filter_daily_bars(daily, window_start=ws)
        diagnostics.update(stats)
        diagnostics["source"] = cached.get("source")
        diagnostics["source_symbol"] = cached.get("source_symbol")
        return filtered, diagnostics

    raw: list[dict[str, Any]] = []
    fetch_meta: dict[str, Any] = {}
    for fetcher, sym in (
        (_fetch_oanda_daily, oanda_symbol),
        (_fetch_fmp_daily, spec.get("fmp_fallback") or ""),
        (_fetch_av_daily, spec.get("av_fallback") or ""),
    ):
        if not sym:
            continue
        raw, fetch_meta = fetcher(sym, window_start=ws)
        if raw:
            diagnostics["source"] = fetch_meta.get("provider")
            diagnostics["source_symbol"] = sym
            break
        diagnostics.setdefault("fetch_attempts", []).append(fetch_meta)

    filtered, stats = _filter_daily_bars(raw, window_start=ws)
    diagnostics.update(stats)
    diagnostics["fetch_meta"] = fetch_meta

    if filtered:
        cache_doc = {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "instrument_id": instrument_id,
            "oanda_symbol": oanda_symbol,
            "source": diagnostics["source"],
            "source_symbol": diagnostics["source_symbol"],
            "daily_bars": raw,
            "diagnostics": {k: diagnostics.get(k) for k in ("source", "source_symbol", "proxy_note", "fetch_meta")},
        }
        _write_cache(oanda_symbol, cache_doc)

    return filtered, diagnostics


def backfill_workstation_index(
    instrument_id: str,
    *,
    window_start: str | None = None,
    refresh: bool = True,
) -> dict[str, Any]:
    daily, diag = load_workstation_index_daily_bars(
        instrument_id,
        window_start=window_start,
        refresh=refresh,
    )
    return {
        "instrument_id": instrument_id,
        "daily_rows": len(daily),
        "first_date": daily[0]["date"] if daily else None,
        "last_date": daily[-1]["date"] if daily else None,
        "diagnostics": diag,
    }


def run_backfill(instrument_ids: list[str] | None = None, *, window_start: str = "2017-01-03") -> list[dict[str, Any]]:
    ids = instrument_ids or list(WORKSTATION_INDEX_SOURCES.keys())
    results: list[dict[str, Any]] = []
    for iid in ids:
        logger.info("Workstation index OHLC backfill: %s", iid)
        results.append(backfill_workstation_index(iid, window_start=window_start, refresh=True))
    return results
