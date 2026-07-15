"""Backfill production price history for COT-tracked instruments with coverage gaps.

Uses OANDA paginated daily history where available, FRED series otherwise.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from hptl.prices.fx_daily_backfill import (
    BackfillPair,
    STAGING_DIR,
    _write_staging_record,
    run_backfill,
    staging_path,
)
from hptl.prices.models import OhlcBar, build_history_meta, compute_range_52w
from hptl.prices.promote_price_backfill import promote_staging_backfill
from hptl.seasonality.seasonality_v2 import normalize_daily_bars

logger = logging.getLogger(__name__)

# (display label, OANDA instrument, price-store key)
OANDA_COT_FAIL_PAIRS: tuple[BackfillPair, ...] = (
    ("WTI", "WTICO_USD", "Crude Oil / CL"),
    ("NATGAS", "NATGAS_USD", "Natural Gas / NG"),
    ("WHEAT", "WHEAT_USD", "Wheat"),
    ("SOYBEAN", "SOYBN_USD", "Soybeans"),
    ("SUGAR", "SUGAR_USD", "Sugar"),
    ("COPPER", "XCU_USD", "Copper / HG"),
    ("PLAT", "XPT_USD", "Platinum"),
    ("PALL", "XPD_USD", "Palladium"),
    ("BTC", "BTC_USD", "Bitcoin"),
)

# instrument id -> FRED series id (daily or monthly — weekly COT aligns via prior close)
FRED_COT_FAIL_SERIES: dict[str, str] = {
    "Cocoa": "PCOCOUSDM",
    "Coffee": "PCOFFOTMUSDM",
    "Cotton": "PCOTTINDUSDM",
    "US Dollar Index / DX": "DTWEXBGS",
}


def fred_series_to_daily_bars(series_id: str, *, observation_start: str = "2016-01-01") -> list[OhlcBar]:
    from hptl.macro import fred_client

    df = fred_client.get_series_df(series_id, observation_start)
    if df is None or df.empty:
        return []
    bars: list[OhlcBar] = []
    for _, row in df.iterrows():
        try:
            close = float(row["value"])
        except (TypeError, ValueError):
            continue
        if close != close:
            continue
        d = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
        bars.append(
            {
                "date": d,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": None,
            }
        )
    bars.sort(key=lambda b: b["date"])
    return bars


def backfill_fred_instrument(
    store_key: str,
    series_id: str,
    *,
    observation_start: str = "2016-01-01",
) -> dict[str, Any]:
    """Write FRED observations to staging for one instrument."""
    daily = normalize_daily_bars(fred_series_to_daily_bars(series_id, observation_start=observation_start))
    if not daily:
        return {
            "instrument": store_key,
            "series_id": series_id,
            "status": "failed",
            "total_daily_bars": 0,
            "error": "no_fred_bars",
        }
    range_52w = compute_range_52w(daily)
    record = {
        "instrument_id": store_key,
        "price": {"mid": daily[-1]["close"], "as_of": daily[-1]["date"]},
        "daily": daily,
        "weekly": [],
        "range_52w": range_52w,
        "history": build_history_meta(daily, [], range_52w),
        "error": None,
        "price_scale": {
            "source": "fred",
            "series_id": series_id,
            "is_fallback": store_key == "US Dollar Index / DX",
            "fallback_note": (
                "FRED DTWEXBGS broad USD index — not ICE DX futures price."
                if store_key == "US Dollar Index / DX"
                else None
            ),
        },
    }
    _write_staging_record(
        store_key,
        record,
        backfill_meta={
            "source": "fred",
            "fred_series": series_id,
            "observation_start": observation_start,
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    return {
        "instrument": store_key,
        "series_id": series_id,
        "status": "completed",
        "total_daily_bars": len(daily),
        "earliest_date": daily[0]["date"],
        "latest_date": daily[-1]["date"],
        "staging_path": str(staging_path(store_key)),
    }


def run_oanda_backfill(*, years: int = 10) -> dict[str, Any]:
    return run_backfill(pairs=OANDA_COT_FAIL_PAIRS, years=years)


def run_fred_backfill(*, observation_start: str = "2016-01-01") -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for store_key, series_id in FRED_COT_FAIL_SERIES.items():
        logger.info("FRED backfill %s <- %s", store_key, series_id)
        results.append(
            backfill_fred_instrument(store_key, series_id, observation_start=observation_start)
        )
    return results


def run_all(*, years: int = 10, promote: bool = True) -> dict[str, Any]:
    """OANDA + FRED staging backfill; optionally promote all to production."""
    oanda_summary = run_oanda_backfill(years=years)
    fred_results = run_fred_backfill()

    promote_keys = [p[2] for p in OANDA_COT_FAIL_PAIRS] + list(FRED_COT_FAIL_SERIES.keys())
    promotion = promote_staging_backfill(promote_keys) if promote else {"promoted": [], "count": 0}

    return {
        "oanda": oanda_summary,
        "fred": fred_results,
        "promotion": promotion,
        "staging_dir": str(STAGING_DIR),
    }
