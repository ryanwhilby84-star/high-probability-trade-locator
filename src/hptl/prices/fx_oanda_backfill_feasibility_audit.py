"""Historical daily price backfill feasibility audit — OANDA depth vs local store.

Usage:
    python -m hptl.prices.fx_oanda_backfill_feasibility_audit

Writes:
    data/audits/fx_oanda_backfill_feasibility.json
    data/audits/fx_oanda_backfill_feasibility.md

Audit-only. Does not modify live seasonality, valuation, COT, or production price data.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

from hptl.config import DATA_DIR, get_oanda_api_host, get_oanda_api_key
from hptl.oanda.oanda_client import OandaApiError, api_get
from hptl.oanda.oanda_prices import _parse_candles
from hptl.prices.data_integrity import actual_fetch_meta
from hptl.prices.models import DAILY_BAR_TARGET, WEEKLY_BAR_TARGET, WEEKLY_LOOKBACK_DAYS
from hptl.prices.price_store import load_price_store
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

AUDIT_JSON = DATA_DIR / "audits" / "fx_oanda_backfill_feasibility.json"
AUDIT_MD = DATA_DIR / "audits" / "fx_oanda_backfill_feasibility.md"

OANDA_MAX_COUNT = 5000
RECOMMENDED_CHUNK_DAYS = 500
MIN_YEARS_10Y = 10.0
MIN_BARS_10Y = 252 * 8

TEST_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("EURUSD", "EUR_USD", "Euro FX / 6E"),
    ("GBPUSD", "GBP_USD", "British Pound / 6B"),
    ("AUDUSD", "AUD_USD", "Australian Dollar / 6A"),
    ("NZDUSD", "NZD_USD", "NZ Dollar / 6N"),
    ("USDJPY", "USD_JPY", "Japanese Yen / 6J"),
    ("USDCAD", "USD_CAD", "Canadian Dollar / 6C"),
    ("USDCHF", "USD_CHF", "Swiss Franc / 6S"),
    ("EURJPY", "EUR_JPY", "EUR/JPY"),
)


def _loader_analysis() -> dict[str, Any]:
    return {
        "module": "src/hptl/oanda/oanda_prices.py",
        "refresh_entry": "src/hptl/prices/run_price_refresh.py",
        "adapter": "src/hptl/prices/unified_adapter.py",
        "daily_bar_target": DAILY_BAR_TARGET,
        "weekly_bar_target": WEEKLY_BAR_TARGET,
        "weekly_lookback_days": WEEKLY_LOOKBACK_DAYS,
        "fetch_parameters": ["granularity", "count", "price=M"],
        "uses_from_to_pagination": False,
        "uses_chunking": False,
        "storage": "data/processed/prices/{instrument}.json via write_instrument_record",
        "merge_strategy": "full replace on refresh (no historical merge)",
        "verdict": "local_loader_limit",
        "explanation": (
            f"OANDA fetch uses count={DAILY_BAR_TARGET} only (~1 trading year). "
            "No from/to date-range pagination is implemented in the live loader."
        ),
    }


def _stored_daily_stats(store_key: str, instruments: dict[str, Any]) -> dict[str, Any]:
    rec = instruments.get(store_key) or {}
    daily = normalize_daily_bars(rec.get("daily") or [])
    src, sym, _dn, _wn, err = actual_fetch_meta(store_key, public=instruments)
    return {
        "store_key": store_key,
        "earliest_stored_daily_bar": daily[0]["date"] if daily else None,
        "latest_stored_daily_bar": daily[-1]["date"] if daily else None,
        "stored_daily_bar_count": len(daily),
        "stored_years_of_coverage": round(years_spanned(daily), 2) if daily else 0.0,
        "data_source": src,
        "fetch_symbol": sym,
        "store_error": err,
    }


def _probe_candles(
    instrument: str,
    *,
    count: int | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params: dict[str, str] = {"granularity": "D", "price": "M"}
    if count is not None:
        params["count"] = str(min(count, OANDA_MAX_COUNT))
    if from_time:
        params["from"] = from_time
    if to_time:
        params["to"] = to_time

    meta: dict[str, Any] = {
        "params": {k: v for k, v in params.items()},
        "http_status": None,
        "error": None,
        "candles_returned": 0,
    }
    try:
        doc = api_get(f"/v3/instruments/{instrument}/candles", params=params)
        # _parse_candles returns (completed_bars, forming_bar).  This audit and
        # workstation history only consume completed bars.
        bars, _forming = _parse_candles(doc)
        meta["candles_returned"] = len(bars)
        meta["http_status"] = 200
        return bars, meta
    except OandaApiError as exc:
        meta["error"] = str(exc)[:300]
        meta["http_status"] = exc.status_code
        return [], meta


def _iso_from(d: date) -> str:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


def _audit_pair_live(
    display: str,
    oanda_symbol: str,
    store_key: str,
    *,
    instruments: dict[str, Any],
    years: int = 10,
) -> dict[str, Any]:
    stored = _stored_daily_stats(store_key, instruments)
    stored_earliest = stored["earliest_stored_daily_bar"]
    today = date.today()
    ten_year_start = today - timedelta(days=years * 366)

    probes: list[dict[str, Any]] = []

    bars_a, meta_a = _probe_candles(oanda_symbol, count=DAILY_BAR_TARGET)
    probes.append({"name": "live_loader_equivalent", **meta_a, "earliest": bars_a[0]["date"] if bars_a else None, "latest": bars_a[-1]["date"] if bars_a else None})

    bars_b, meta_b = _probe_candles(oanda_symbol, count=OANDA_MAX_COUNT)
    probes.append({"name": "max_count_5000", **meta_b, "earliest": bars_b[0]["date"] if bars_b else None, "latest": bars_b[-1]["date"] if bars_b else None})

    bars_c, meta_c = _probe_candles(
        oanda_symbol,
        from_time=_iso_from(ten_year_start),
        count=OANDA_MAX_COUNT,
    )
    probes.append({"name": "from_10y_start", **meta_c, "earliest": bars_c[0]["date"] if bars_c else None, "latest": bars_c[-1]["date"] if bars_c else None})

    bars_d: list[dict[str, Any]] = []
    meta_d: dict[str, Any] = {"name": "before_stored_earliest", "params": {}, "error": None}
    can_fetch_older = False
    if stored_earliest:
        try:
            earliest_dt = datetime.strptime(stored_earliest[:10], "%Y-%m-%d").date()
            to_before = earliest_dt - timedelta(days=1)
            bars_d, meta_d = _probe_candles(oanda_symbol, to_time=_iso_from(to_before), count=500)
            meta_d["name"] = "before_stored_earliest"
            if bars_d:
                oldest_d = bars_d[0]["date"]
                can_fetch_older = oldest_d < stored_earliest[:10]
                meta_d["earliest"] = oldest_d
                meta_d["latest"] = bars_d[-1]["date"]
        except ValueError:
            meta_d["error"] = "invalid stored earliest date"
    probes.append(meta_d)

    max_probe = max((len(bars_a), len(bars_b), len(bars_c)), default=0)
    earliest_live = min((b[0]["date"] for b in (bars_a, bars_b, bars_c) if b), default=None)
    latest_live = max((b[-1]["date"] for b in (bars_a, bars_b, bars_c) if b), default=None)
    years_live = 0.0
    if earliest_live and latest_live:
        years_live = (datetime.strptime(latest_live, "%Y-%m-%d").date() - datetime.strptime(earliest_live, "%Y-%m-%d").date()).days / 365.25

    return {
        "display": display,
        "oanda_symbol": oanda_symbol,
        "stored": stored,
        "probes": probes,
        "max_probe_rows": max_probe,
        "earliest_live": earliest_live,
        "latest_live": latest_live,
        "live_years": round(years_live, 2),
        "can_fetch_older_than_store": can_fetch_older,
        "meets_10y_target": bool(years_live >= MIN_YEARS_10Y or max_probe >= MIN_BARS_10Y),
    }


def run_live_audit() -> dict[str, Any]:
    store = load_price_store()
    instruments = store.get("instruments") or {}
    rows = []
    for display, oanda_symbol, store_key in TEST_PAIRS:
        rows.append(_audit_pair_live(display, oanda_symbol, store_key, instruments=instruments))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "oanda_host": get_oanda_api_host(),
        "loader_analysis": _loader_analysis(),
        "pairs": rows,
    }


def write_audit(payload: dict[str, Any]) -> None:
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# OANDA historical backfill feasibility", ""]
    for row in payload.get("pairs") or []:
        lines.append(
            f"- {row['display']}: live {row.get('earliest_live')} → {row.get('latest_live')} "
            f"({row.get('live_years')}y), older-than-store={row.get('can_fetch_older_than_store')}"
        )
    AUDIT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not get_oanda_api_key():
        print("OANDA_API_KEY not set")
        return 2
    payload = run_live_audit()
    write_audit(payload)
    print(f"Wrote {AUDIT_JSON}")
    print(f"Wrote {AUDIT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
