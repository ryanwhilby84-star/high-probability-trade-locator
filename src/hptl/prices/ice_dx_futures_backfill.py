"""ICE U.S. Dollar Index futures (Yahoo DX-Y.NYB) — never FRED DTWEXBGS.

Writes the same ICE DX series to:
  - US Dollar Index / DXY — ICE DX futures  (explicit seasonality / chart id)
  - US Dollar Index / DX                     (COT/TFF id; price must be ICE, not FRED)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from hptl.markets.usd_index_identity import (
    DX_COT_ID,
    ICE_DX_YAHOO_SYMBOL,
    ICE_DXY_ID,
    ICE_DX_PRICE_IDS,
)
from hptl.prices.coffee_foundation_backfill import fetch_yahoo_daily
from hptl.prices.models import build_history_meta, compute_range_52w
from hptl.prices.price_store import (
    load_all_instrument_records,
    load_instrument_record_internal,
    write_instrument_record,
    write_price_store,
)
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

ICE_DX_NOTE = (
    "ICE U.S. Dollar Index continuous futures (Yahoo DX-Y.NYB). "
    "This is NOT FRED DTWEXBGS (Nominal Broad USD Index)."
)


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


def _build_ice_record(instrument_id: str, daily: list[dict[str, Any]]) -> dict[str, Any]:
    weekly = _weekly_from_daily(daily)
    range_52w = compute_range_52w(daily)
    return {
        "instrument_id": instrument_id,
        "price": {"mid": daily[-1]["close"], "as_of": daily[-1]["date"]},
        "daily": daily,
        "weekly": weekly,
        "range_52w": range_52w,
        "history": build_history_meta(daily, weekly, range_52w),
        "error": None,
        "price_scale": {
            "source": "yahoo",
            "yahoo_symbol": ICE_DX_YAHOO_SYMBOL,
            "is_proxy": False,
            "is_fred_broad": False,
            "canonical_note": ICE_DX_NOTE,
            "instrument_label": "ICE DX futures (DX-Y.NYB)",
        },
    }


def promote_ice_dx_futures(
    instrument_ids: tuple[str, ...] | list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch Yahoo ICE DX once and write to each ICE-bound instrument id."""
    targets = tuple(instrument_ids or ICE_DX_PRICE_IDS)
    for iid in targets:
        if iid not in ICE_DX_PRICE_IDS:
            raise ValueError(f"Refused: {iid} is not an ICE DX price id")

    daily = normalize_daily_bars(fetch_yahoo_daily(ICE_DX_YAHOO_SYMBOL))
    if not daily:
        raise RuntimeError(f"Yahoo returned no bars for {ICE_DX_YAHOO_SYMBOL}")

    # Refuse to overwrite with FRED-scale garbage if Yahoo levels look like broad USD
    last = float(daily[-1]["close"])
    if last > 130 or last < 70:
        raise RuntimeError(
            f"ICE DX Yahoo close {last} outside expected futures band (~70–130); abort"
        )

    results: list[dict[str, Any]] = []
    records = load_all_instrument_records() or {}
    for iid in targets:
        prev = load_instrument_record_internal(iid) or {}
        prev_scale = prev.get("price_scale") or {}
        if prev_scale.get("series_id") == "DTWEXBGS" or prev_scale.get("is_proxy"):
            # Drop FRED bars — do not merge ICE into FRED history
            prev_daily: list = []
        else:
            prev_daily = prev.get("daily") or []

        rec = _build_ice_record(iid, daily)
        write_instrument_record(
            rec,
            fetched_via="yahoo_futures",
            historical_via=f"yahoo:{ICE_DX_YAHOO_SYMBOL}",
        )
        records[iid] = rec
        results.append(
            {
                "instrument_id": iid,
                "previous_source": prev_scale.get("source") or prev.get("_fetched_via"),
                "corrected_source": f"yahoo:{ICE_DX_YAHOO_SYMBOL}",
                "previous_daily_bars": len(prev_daily),
                "corrected_daily_bars": len(daily),
                "years_spanned": round(years_spanned(daily), 2),
                "earliest_date": daily[0]["date"],
                "latest_date": daily[-1]["date"],
                "latest_close": daily[-1]["close"],
                "promoted_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    write_price_store(records)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instruments",
        help=f"Default: {ICE_DXY_ID!r} and {DX_COT_ID!r}",
    )
    args = parser.parse_args(argv)
    try:
        out = promote_ice_dx_futures(args.instruments)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
