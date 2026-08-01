"""Dense daily softs (Coffee / Cocoa / Cotton) via Yahoo continuous futures.

FRED IMF monthly series (PCOFFOTMUSDM / PCOCOUSDM / PCOTTINDUSDM) currently stop
at 2026-05-01 and cannot align COT weeks through 2026-07. Yahoo ICE continuous
futures provide daily OHLC suitable for COT chart price attachment.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from hptl.prices.coffee_foundation_backfill import fetch_yahoo_daily
from hptl.prices.models import build_history_meta, compute_range_52w
from hptl.prices.price_store import (
    load_all_instrument_records,
    load_instrument_record_internal,
    write_instrument_record,
    write_price_store,
)
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

SOFTS_YAHOO: dict[str, dict[str, str]] = {
    "Coffee": {
        "yahoo_symbol": "KC=F",
        "note": "ICE Coffee C continuous futures (Yahoo KC=F) daily OHLC for COT alignment.",
    },
    "Cocoa": {
        "yahoo_symbol": "CC=F",
        "note": "ICE Cocoa continuous futures (Yahoo CC=F) daily OHLC for COT alignment.",
    },
    "Cotton": {
        "yahoo_symbol": "CT=F",
        "note": "ICE Cotton No. 2 continuous futures (Yahoo CT=F) daily OHLC for COT alignment.",
    },
    # CME Japanese Yen futures — USD per JPY (rises when yen strengthens).
    # Economically matches CFTC Japanese Yen / 6J; NOT OANDA USD_JPY (inverse quote).
    "Japanese Yen / 6J": {
        "yahoo_symbol": "6J=F",
        "note": (
            "CME Japanese Yen continuous futures (Yahoo 6J=F), quoted as USD per JPY. "
            "Aligned with TradingView 6J / yen value. Replaces inverted OANDA USD_JPY."
        ),
    },
}


def _weekly_from_daily(daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def promote_soft_futures(instrument_id: str) -> dict[str, Any]:
    cfg = SOFTS_YAHOO[instrument_id]
    yahoo_symbol = cfg["yahoo_symbol"]
    prev = load_instrument_record_internal(instrument_id) or {}
    prev_daily = prev.get("daily") or []
    previous_source = prev.get("_fetched_via") or (prev.get("price_scale") or {}).get("source") or "fred"
    previous_latest = prev_daily[-1]["date"] if prev_daily else None

    daily = normalize_daily_bars(fetch_yahoo_daily(yahoo_symbol))
    if not daily:
        raise RuntimeError(f"Yahoo returned no bars for {yahoo_symbol}")

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
            "source": "yahoo",
            "yahoo_symbol": yahoo_symbol,
            "is_proxy": True,
            "proxy_note": cfg["note"],
            "canonical_note": cfg["note"],
        },
    }
    write_instrument_record(
        rec,
        fetched_via="yahoo_futures",
        historical_via=f"yahoo:{yahoo_symbol}",
    )
    records = load_all_instrument_records() or {}
    records[instrument_id] = rec
    write_price_store(records)

    prev_dates = {str(b.get("date"))[:10] for b in prev_daily}
    new_dates = {str(b.get("date"))[:10] for b in daily}
    backfilled = sorted(d for d in (new_dates - prev_dates) if d > (previous_latest or ""))

    return {
        "instrument_id": instrument_id,
        "previous_source": f"{previous_source}",
        "corrected_source": f"yahoo:{yahoo_symbol}",
        "previous_latest_date": previous_latest,
        "corrected_latest_date": daily[-1]["date"],
        "previous_daily_bars": len(prev_daily),
        "corrected_daily_bars": len(daily),
        "missing_rows_backfilled": len(backfilled),
        "weekly_bars_rebuilt": len(weekly),
        "years_spanned": round(years_spanned(daily), 2),
        "earliest_date": daily[0]["date"],
        "promoted_at": datetime.now(timezone.utc).isoformat(),
    }


def promote_all_softs_futures() -> list[dict[str, Any]]:
    return [promote_soft_futures(iid) for iid in SOFTS_YAHOO]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument", action="append", dest="instruments")
    args = parser.parse_args(argv)
    ids = args.instruments or list(SOFTS_YAHOO)
    for iid in ids:
        if iid not in SOFTS_YAHOO:
            print(f"ERROR: unsupported soft {iid}", file=sys.stderr)
            return 1
        print(json.dumps(promote_soft_futures(iid), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
