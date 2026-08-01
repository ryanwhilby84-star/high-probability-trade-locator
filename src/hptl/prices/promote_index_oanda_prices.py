"""Promote OANDA index CFD histories into the canonical price store for COT charts.

Workstation cache already holds multi-year NAS100_USD / SPX500_USD / US30_USD OHLC.
This module copies those bars into ``prices_latest`` so ``cot_3y_series_export``
stops using Alpha Vantage QQQ/SPY/DIA ETF proxies.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from hptl.prices.models import build_history_meta, compute_range_52w
from hptl.prices.price_store import load_all_instrument_records, write_instrument_record, write_price_store
from hptl.prices.workstation_index_ohlc_history import (
    WORKSTATION_INDEX_SOURCES,
    _cache_path,
    run_backfill,
)
from hptl.seasonality.seasonality_v2 import normalize_daily_bars

INDEX_INSTRUMENTS: tuple[str, ...] = ("NASDAQ / NQ", "S&P 500 / ES", "Dow / YM")


def _weekly_from_daily(daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """ISO-week OHLC: open=first, high=max, low=min, close=last."""
    buckets: dict[str, list[dict[str, Any]]] = {}
    for bar in daily:
        d = str(bar.get("date") or "")[:10]
        if not d:
            continue
        try:
            wk = pd.Timestamp(d).strftime("%G-W%V")
        except (TypeError, ValueError):
            continue
        buckets.setdefault(wk, []).append(bar)
    out: list[dict[str, Any]] = []
    for wk in sorted(buckets):
        rows = sorted(buckets[wk], key=lambda b: str(b.get("date") or ""))
        opens = [float(r["open"]) for r in rows if r.get("open") is not None]
        highs = [float(r["high"]) for r in rows if r.get("high") is not None]
        lows = [float(r["low"]) for r in rows if r.get("low") is not None]
        closes = [float(r["close"]) for r in rows if r.get("close") is not None]
        if not closes:
            continue
        out.append(
            {
                "date": str(rows[-1]["date"])[:10],
                "open": opens[0] if opens else closes[0],
                "high": max(highs) if highs else closes[-1],
                "low": min(lows) if lows else closes[-1],
                "close": closes[-1],
                "volume": None,
            }
        )
    return out


def _load_workstation_daily(instrument_id: str) -> tuple[list[dict[str, Any]], str]:
    cfg = WORKSTATION_INDEX_SOURCES[instrument_id]
    symbol = cfg["oanda_symbol"]
    path = _cache_path(symbol)
    if not path.is_file():
        raise FileNotFoundError(f"Workstation OHLC cache missing for {symbol}: {path}")
    doc = json.loads(path.read_text(encoding="utf-8"))
    bars = doc.get("daily_bars") or []
    daily = normalize_daily_bars(
        [
            {
                "date": str(b.get("date") or "")[:10],
                "open": b.get("open"),
                "high": b.get("high"),
                "low": b.get("low"),
                "close": b.get("close"),
                "volume": b.get("volume"),
            }
            for b in bars
            if b.get("close") is not None
        ]
    )
    return daily, symbol


def promote_index_instrument(instrument_id: str, *, refresh_cache: bool = True) -> dict[str, Any]:
    cfg = WORKSTATION_INDEX_SOURCES[instrument_id]
    symbol = cfg["oanda_symbol"]
    previous = None
    from hptl.prices.price_store import load_instrument_record_internal

    prev_rec = load_instrument_record_internal(instrument_id) or {}
    prev_daily = prev_rec.get("daily") or []
    previous = {
        "source": prev_rec.get("_fetched_via") or (prev_rec.get("price_scale") or {}).get("source"),
        "symbol": (prev_rec.get("price_scale") or {}).get("symbol"),
        "latest_date": prev_daily[-1]["date"] if prev_daily else None,
        "daily_bars": len(prev_daily),
    }

    if refresh_cache:
        run_backfill([instrument_id], window_start="2017-01-03")

    daily, symbol = _load_workstation_daily(instrument_id)
    if not daily:
        raise RuntimeError(f"No OANDA daily bars for {instrument_id} ({symbol})")

    weekly = _weekly_from_daily(daily)
    range_52w = compute_range_52w(daily)
    rec = {
        "instrument_id": instrument_id,
        "price": {"mid": daily[-1]["close"], "as_of": daily[-1]["date"]},
        "daily": daily,
        "weekly": weekly,
        "range_52w": range_52w,
        "history": build_history_meta(daily, weekly, range_52w),
        "error": None,
        "price_scale": {
            "source": "oanda",
            "symbol": symbol,
            "is_proxy": False,
            "proxy_note": None,
            "canonical_note": cfg.get("proxy_note"),
        },
    }
    write_instrument_record(rec, fetched_via="oanda", historical_via="oanda_workstation_promote")
    records = load_all_instrument_records() or {}
    records[instrument_id] = rec
    write_price_store(records)

    prev_dates = {str(b.get("date"))[:10] for b in prev_daily}
    new_dates = {str(b.get("date"))[:10] for b in daily}
    backfilled = sorted(new_dates - prev_dates)

    return {
        "instrument_id": instrument_id,
        "previous_source": previous.get("source") or "alpha_vantage",
        "corrected_source": f"oanda:{symbol}",
        "previous_latest_date": previous.get("latest_date"),
        "corrected_latest_date": daily[-1]["date"],
        "previous_daily_bars": previous.get("daily_bars") or 0,
        "corrected_daily_bars": len(daily),
        "missing_rows_backfilled": len(backfilled),
        "weekly_bars_rebuilt": len(weekly),
        "earliest_date": daily[0]["date"],
        "promoted_at": datetime.now(timezone.utc).isoformat(),
    }


def promote_all_index_oanda(*, refresh_cache: bool = True) -> list[dict[str, Any]]:
    return [promote_index_instrument(iid, refresh_cache=refresh_cache) for iid in INDEX_INSTRUMENTS]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument", action="append", dest="instruments")
    parser.add_argument("--no-refresh", action="store_true", help="Use existing workstation cache only")
    args = parser.parse_args(argv)
    ids = args.instruments or list(INDEX_INSTRUMENTS)
    out = []
    for iid in ids:
        if iid not in WORKSTATION_INDEX_SOURCES:
            print(f"ERROR: {iid} not in WORKSTATION_INDEX_SOURCES", file=sys.stderr)
            return 1
        row = promote_index_instrument(iid, refresh_cache=not args.no_refresh)
        out.append(row)
        print(json.dumps(row, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
