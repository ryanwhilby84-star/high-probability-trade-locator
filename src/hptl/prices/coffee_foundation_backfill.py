"""Dense daily Coffee price foundation via Yahoo Finance KC=F continuous futures.

ICE KC coffee is not on OANDA (account: CORN/WHEAT/SUGAR/SOYBN only).
FMP KCUSX is premium-gated. FRED PCOFFOTMUSDM is monthly (~12 ISO weeks/year).
Alpha Vantage TIME_SERIES_DAILY full for JO is rate-limited on the free tier.

Yahoo Finance ``KC=F`` (Coffee Mar futures continuous) provides dense daily OHLC
from 2000+ suitable for ISO-week seasonality. Labelled as proxy vs CFTC settlement.

Usage:
    python -m hptl.prices.coffee_foundation_backfill --dry-run
    python -m hptl.prices.coffee_foundation_backfill --execute
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

from hptl.alpha_vantage.client import AlphaVantageApiError, _get
from hptl.prices.models import OhlcBar, build_history_meta, compute_range_52w
from hptl.prices.price_store import load_all_instrument_records, write_instrument_record, write_price_store
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

logger = logging.getLogger(__name__)

COFFEE_INSTRUMENT_ID = "Coffee"
YAHOO_SYMBOL = "KC=F"
YAHOO_NOTE = (
    "ICE Coffee C continuous futures (Yahoo KC=F) daily OHLC. "
    "Proxy aligned with CFTC COFFEE C positioning; not IMF/FRED monthly benchmark."
)
JO_SYMBOL = "JO"
MIN_YEARS_TARGET = 10.0
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def fetch_yahoo_daily(symbol: str = YAHOO_SYMBOL, *, period1: int = 0) -> list[OhlcBar]:
    """Fetch max daily OHLC from Yahoo Finance chart API."""
    period2 = int(datetime.now(timezone.utc).timestamp())
    url = YAHOO_CHART_URL.format(symbol=requests.utils.quote(symbol, safe=""))
    resp = requests.get(
        url,
        params={
            "interval": "1d",
            "period1": str(period1),
            "period2": str(period2),
            "includePrePost": "false",
        },
        timeout=90,
        headers={"User-Agent": "Mozilla/5.0 (compatible; HPTL/1.0)"},
    )
    resp.raise_for_status()
    result = (((resp.json() or {}).get("chart") or {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"Yahoo chart empty for {symbol}")

    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []

    bars: list[OhlcBar] = []
    for i, ts in enumerate(timestamps):
        close = closes[i] if i < len(closes) else None
        if close is None:
            continue
        try:
            close_f = float(close)
        except (TypeError, ValueError):
            continue
        if close_f != close_f:
            continue
        date_str = datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%d")
        open_px = float(opens[i]) if i < len(opens) and opens[i] is not None else close_f
        high_px = float(highs[i]) if i < len(highs) and highs[i] is not None else close_f
        low_px = float(lows[i]) if i < len(lows) and lows[i] is not None else close_f
        bars.append(
            {
                "date": date_str,
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_f,
                "volume": None,
            }
        )
    return normalize_daily_bars(bars)


def _parse_av_daily(doc: dict[str, Any]) -> list[OhlcBar]:
    series_key = next((k for k in doc if "Time Series" in k), None)
    if not series_key:
        return []
    series = doc[series_key]
    if not isinstance(series, dict):
        return []
    out: list[OhlcBar] = []
    for date_str, row in series.items():
        if not isinstance(row, dict):
            continue
        try:
            o = float(row.get("1. open") or row.get("1. Open") or row.get("open"))
            h = float(row.get("2. high") or row.get("2. High") or row.get("high"))
            l = float(row.get("3. low") or row.get("3. Low") or row.get("low"))
            c = float(row.get("4. close") or row.get("4. Close") or row.get("close"))
        except (TypeError, ValueError):
            continue
        if c != c:
            continue
        out.append(
            {
                "date": str(date_str)[:10],
                "open": o,
                "high": h if h == h else o,
                "low": l if l == l else o,
                "close": c,
                "volume": None,
            }
        )
    out.sort(key=lambda b: b["date"])
    return out


def promote_coffee_daily(
    daily: list[OhlcBar],
    *,
    source: str,
    price_scale: dict[str, Any],
) -> dict[str, Any]:
    """Replace Coffee production daily with dense history."""
    daily = normalize_daily_bars(daily)
    if not daily:
        raise ValueError("No daily bars to promote")
    range_52w = compute_range_52w(daily)
    rec = {
        "instrument_id": COFFEE_INSTRUMENT_ID,
        "price": {"mid": daily[-1]["close"], "as_of": daily[-1]["date"]},
        "daily": daily,
        "weekly": [],
        "range_52w": range_52w,
        "history": build_history_meta(daily, [], range_52w),
        "error": None,
        "price_scale": price_scale,
    }
    write_instrument_record(rec, fetched_via="coffee_foundation_backfill", historical_via=source)
    records = load_all_instrument_records() or {}
    records[COFFEE_INSTRUMENT_ID] = rec
    write_price_store(records)
    return {
        "instrument": COFFEE_INSTRUMENT_ID,
        "source": source,
        "total_daily_bars": len(daily),
        "earliest_date": daily[0]["date"],
        "latest_date": daily[-1]["date"],
        "years_spanned": round(years_spanned(daily), 2),
        "can_10y_seasonality": years_spanned(daily) >= MIN_YEARS_TARGET,
        "promoted_at": datetime.now(timezone.utc).isoformat(),
    }


def plan_coffee_backfill() -> dict[str, Any]:
    """Probe Yahoo KC=F without writing production store."""
    try:
        daily = fetch_yahoo_daily(YAHOO_SYMBOL)
    except Exception as exc:
        return {"status": "probe_failed", "source": f"yahoo:{YAHOO_SYMBOL}", "error": str(exc)[:200]}
    return {
        "status": "probe_ok",
        "source": f"yahoo:{YAHOO_SYMBOL}",
        "daily_bars": len(daily),
        "range": [daily[0]["date"], daily[-1]["date"]] if daily else None,
        "years_spanned": round(years_spanned(daily), 2) if daily else 0.0,
        "note": YAHOO_NOTE,
    }


def run_coffee_backfill(*, execute: bool = False) -> dict[str, Any]:
    plan = plan_coffee_backfill()
    if not execute:
        return {"execute": False, "plan": plan}

    if plan.get("status") != "probe_ok":
        raise RuntimeError(f"Coffee backfill probe failed: {plan}")

    daily = fetch_yahoo_daily(YAHOO_SYMBOL)
    promotion = promote_coffee_daily(
        daily,
        source=f"yahoo:{YAHOO_SYMBOL}",
        price_scale={
            "source": "yahoo",
            "yahoo_symbol": YAHOO_SYMBOL,
            "is_proxy": True,
            "proxy_note": YAHOO_NOTE,
            "canonical_note": YAHOO_NOTE,
        },
    )
    return {"execute": True, "plan": plan, "promotion": promotion}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Coffee dense daily backfill via Yahoo KC=F")
    parser.add_argument("--execute", action="store_true", help="Fetch KC=F and promote to price store")
    parser.add_argument("--dry-run", action="store_true", help="Probe Yahoo KC=F only")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    execute = args.execute and not args.dry_run
    result = run_coffee_backfill(execute=execute)
    print(result)
    if execute:
        promo = result.get("promotion") or {}
        print(
            f"Coffee promoted: {promo.get('total_daily_bars')} bars "
            f"{promo.get('earliest_date')} .. {promo.get('latest_date')} "
            f"({promo.get('years_spanned')}Y)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
