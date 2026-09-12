"""Dense daily Corn price foundation via Yahoo Finance ZC=F continuous futures.

Alpha Vantage CORN is an ETF (~$20–250/share) — wrong unit basis for USDA
stocks-to-use valuation. CBOT ZC is quoted in cents/bushel on Yahoo; we store
USD/bushel (close / 100) to align with Wheat/Soybeans OANDA scale.

Yahoo can expose sparse/legacy observations far back in the history request. Those
old rows are not suitable for daily seasonality and previously produced hundreds of
false discontinuities. The production Corn foundation therefore starts in 2000,
which still gives more than enough clean daily history for the 5Y/10Y/15Y/20Y
workstation and scanner horizons.

Usage:
    python -m hptl.prices.corn_foundation_backfill --dry-run
    python -m hptl.prices.corn_foundation_backfill --execute
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from hptl.prices.coffee_foundation_backfill import fetch_yahoo_daily
from hptl.prices.models import OhlcBar, build_history_meta, compute_range_52w
from hptl.prices.price_store import load_all_instrument_records, write_instrument_record, write_price_store
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned
from hptl.seasonality_workstation.integrity import audit_daily_series_for_lookback

logger = logging.getLogger(__name__)

CORN_INSTRUMENT_ID = "Corn"
YAHOO_SYMBOL = "ZC=F"
CENTS_TO_USD = 100.0
YAHOO_HISTORY_START = datetime(2000, 1, 1, tzinfo=timezone.utc)
YAHOO_HISTORY_START_TS = int(YAHOO_HISTORY_START.timestamp())
YAHOO_NOTE = (
    "CBOT Corn continuous futures (Yahoo ZC=F) daily OHLC from 2000 onward. "
    "Stored as USD/bushel (cents/100). Legacy sparse pre-2000 Yahoo observations "
    "are deliberately excluded from the seasonality foundation."
)
MIN_YEARS_TARGET = 20.0


def _scale_bars_to_usd_per_bushel(bars: list[OhlcBar]) -> list[OhlcBar]:
    out: list[OhlcBar] = []
    for b in bars:
        out.append(
            {
                "date": b["date"],
                "open": round(float(b["open"]) / CENTS_TO_USD, 4),
                "high": round(float(b["high"]) / CENTS_TO_USD, 4),
                "low": round(float(b["low"]) / CENTS_TO_USD, 4),
                "close": round(float(b["close"]) / CENTS_TO_USD, 4),
                "volume": b.get("volume"),
            }
        )
    return normalize_daily_bars(out)


def _fetch_clean_corn_daily() -> list[OhlcBar]:
    raw = fetch_yahoo_daily(YAHOO_SYMBOL, period1=YAHOO_HISTORY_START_TS)
    daily = _scale_bars_to_usd_per_bushel(raw)
    if not daily:
        raise RuntimeError("Yahoo returned no Corn daily bars")

    closes = [(str(b["date"])[:10], float(b["close"])) for b in daily]
    audit = audit_daily_series_for_lookback(
        CORN_INSTRUMENT_ID,
        closes,
        source="yahoo_futures",
        lookback_years=20,
        asof=closes[-1][0],
    )
    if audit.get("status") != "PASS":
        raise RuntimeError(
            "Refusing to promote Corn: clean Yahoo ZC=F audit failed: "
            + ", ".join(audit.get("issues") or ["unknown_integrity_failure"])
        )
    return daily


def promote_corn_daily(
    daily: list[OhlcBar],
    *,
    source: str,
    price_scale: dict[str, Any],
) -> dict[str, Any]:
    daily = normalize_daily_bars(daily)
    if not daily:
        raise ValueError("No daily bars to promote")
    range_52w = compute_range_52w(daily)
    rec = {
        "instrument_id": CORN_INSTRUMENT_ID,
        "price": {"mid": daily[-1]["close"], "as_of": daily[-1]["date"]},
        "daily": daily,
        "weekly": [],
        "range_52w": range_52w,
        "history": build_history_meta(daily, [], range_52w),
        "error": None,
        "price_scale": price_scale,
    }
    write_instrument_record(rec, fetched_via="yahoo_futures", historical_via=source)
    records = load_all_instrument_records() or {}
    records[CORN_INSTRUMENT_ID] = rec
    write_price_store(records)
    return rec


def probe_corn_foundation() -> dict[str, Any]:
    try:
        daily = _fetch_clean_corn_daily()
    except Exception as exc:
        return {"status": "probe_failed", "source": f"yahoo:{YAHOO_SYMBOL}", "error": str(exc)[:300]}
    return {
        "status": "ok",
        "source": f"yahoo:{YAHOO_SYMBOL}",
        "history_start_requested": YAHOO_HISTORY_START.date().isoformat(),
        "bar_count": len(daily),
        "years_spanned": round(years_spanned(daily), 2),
        "earliest_date": daily[0]["date"] if daily else None,
        "latest_close_usd_per_bushel": daily[-1]["close"] if daily else None,
        "latest_date": daily[-1]["date"] if daily else None,
        "unit": "USD/bushel (Yahoo cents/100)",
    }


def run_corn_foundation_backfill(*, execute: bool = False) -> dict[str, Any]:
    daily = _fetch_clean_corn_daily()
    yrs = years_spanned(daily)
    price_scale = {
        "source": "yahoo",
        "yahoo_symbol": YAHOO_SYMBOL,
        "unit": "USD/bushel",
        "raw_unit": "cents/bushel",
        "scale_factor": 1.0 / CENTS_TO_USD,
        "history_start": YAHOO_HISTORY_START.date().isoformat(),
        "note": YAHOO_NOTE,
    }
    result: dict[str, Any] = {
        "instrument": CORN_INSTRUMENT_ID,
        "source": f"yahoo:{YAHOO_SYMBOL}",
        "bar_count": len(daily),
        "years_spanned": round(yrs, 2),
        "earliest_date": daily[0]["date"] if daily else None,
        "latest_close": daily[-1]["close"] if daily else None,
        "latest_date": daily[-1]["date"] if daily else None,
        "execute": execute,
    }
    if not execute:
        result["status"] = "dry_run"
        return result
    if yrs < MIN_YEARS_TARGET:
        raise RuntimeError(f"Corn history {yrs:.1f}y < required {MIN_YEARS_TARGET:.1f}y")
    promote_corn_daily(daily, source=f"yahoo:{YAHOO_SYMBOL}", price_scale=price_scale)
    result["status"] = "promoted"
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Corn ZC=F foundation backfill")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    if args.probe:
        print(probe_corn_foundation())
        return 0
    result = run_corn_foundation_backfill(execute=args.execute and not args.dry_run)
    print(result)
    return 0 if result.get("status") in {"dry_run", "promoted", "ok"} else 1


if __name__ == "__main__":
    sys.exit(main())
